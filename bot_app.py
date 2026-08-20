import os
import math
import time
import threading
import sqlite3
import io
import json
import re
from datetime import datetime, timedelta, timezone

import telebot
from telebot.apihelper import ApiTelegramException
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

import polars as pl

# ---------- CONFIG ----------
import dotenv
dotenv.load_dotenv()

API_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ADMIN_ID = os.environ.get("ADMIN_ID")  # env vars are strings
PARQUET_FILE = os.environ.get("DATA_PATH", "Iran_Data.parquet")
PDF_GUIDE_FILE = os.environ.get("GUIDE_PATH", "Help.pdf")
DB_PATH = os.environ.get("DB_PATH", "users.db")

# Iran has used UTC+03:30 year-round since 2022.  Keeping the business date
# explicit prevents a server configured in another timezone from expiring a
# one-day override too early or too late.
IRAN_TIMEZONE = timezone(timedelta(hours=3, minutes=30))

BUTTONS_PER_ROW = 2
PAGE_SIZE = 16

# Debounce repeated callbacks (same button tapped multiple times quickly)
DEBOUNCE_WINDOW_SECONDS = 1.5

REQUIRED_CHANNELS = ["HydroCodeChannel"]
ALLOWED_MEMBERSHIP_STATUSES = {"creator", "administrator", "member"}

# ----------------------------
if not API_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

# ---------- DATABASE SETUP ----------
DB_LOCK = threading.Lock()
conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)

with DB_LOCK:
    cur = conn.cursor()
    # WAL improves concurrency for multi-threaded polling
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS downloads (
        user_id INTEGER,
        username TEXT,
        station_name TEXT,
        download_date TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_seen TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS download_limit_exemptions (
        user_id INTEGER PRIMARY KEY,
        added_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_download_overrides (
        user_id INTEGER PRIMARY KEY,
        override_date TEXT NOT NULL,
        mode TEXT NOT NULL CHECK (mode IN ('station_limit', 'regions')),
        station_limit INTEGER,
        allowed_regions TEXT,
        added_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
        completion_notified_at TEXT,
        CHECK (
            (mode = 'station_limit' AND station_limit > 0 AND allowed_regions IS NULL)
            OR
            (mode = 'regions' AND station_limit IS NULL AND allowed_regions IS NOT NULL)
        )
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_downloads_user_date ON downloads(user_id, download_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_downloads_date ON downloads(download_date)")
    conn.commit()

def _ensure_users_table_schema() -> None:
    """
    Make users table backward-compatible with older deployments.
    Some existing databases may already have `users` without `last_seen`.
    """
    with DB_LOCK:
        c = conn.cursor()
        c.execute("PRAGMA table_info(users)")
        rows = c.fetchall()
        existing_columns = {row[1] for row in rows} if rows else set()

        if not existing_columns:
            c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_seen TEXT
            )
            """)
        elif "last_seen" not in existing_columns:
            c.execute("ALTER TABLE users ADD COLUMN last_seen TEXT")

        conn.commit()

_ensure_users_table_schema()

def _ensure_daily_overrides_table_schema() -> None:
    """Add notification state when upgrading an existing bot database."""
    with DB_LOCK:
        c = conn.cursor()
        c.execute("PRAGMA table_info(daily_download_overrides)")
        existing_columns = {row[1] for row in c.fetchall()}
        if "completion_notified_at" not in existing_columns:
            c.execute(
                "ALTER TABLE daily_download_overrides "
                "ADD COLUMN completion_notified_at TEXT"
            )
        conn.commit()

_ensure_daily_overrides_table_schema()

def _db_fetchone(sql: str, params=()):
    with DB_LOCK:
        c = conn.cursor()
        c.execute(sql, params)
        return c.fetchone()

def _db_fetchall(sql: str, params=()):
    with DB_LOCK:
        c = conn.cursor()
        c.execute(sql, params)
        return c.fetchall()

def _db_execute(sql: str, params=()):
    with DB_LOCK:
        c = conn.cursor()
        c.execute(sql, params)
        conn.commit()

def _today() -> str:
    """Return the current calendar date in Iran."""
    return datetime.now(IRAN_TIMEZONE).date().isoformat()

# ---------- LOAD DATA (Polars LazyFrame) ----------
df = pl.scan_parquet(PARQUET_FILE)

# Build caches once at startup: region list + region->stations mapping.
# This makes province/station browsing instant and avoids repeated parquet scans.
REGIONS: list[str] = []
REGION_TO_STATIONS: dict[str, list[str]] = {}

# Precomputed cache for (region, station)->(min_date, max_date)
# Built once at startup (fast lookups; no parquet scan on user clicks).
DATE_RANGE_CACHE: dict[tuple[str, str], tuple[str | None, str | None]] = {}

def _build_region_station_cache():
    global REGIONS, REGION_TO_STATIONS
    pairs = (
        df.select([pl.col("region_name"), pl.col("station_name")])
          .unique()
          .collect()
    )
    mapping: dict[str, set[str]] = {}
    for r, s in zip(pairs["region_name"].to_list(), pairs["station_name"].to_list()):
        mapping.setdefault(r, set()).add(s)

    REGION_TO_STATIONS = {r: sorted(list(sts)) for r, sts in mapping.items()}
    REGIONS = sorted(REGION_TO_STATIONS.keys())


def _build_date_range_cache():
    """Precompute min/max date for every (region, station) at startup."""
    global DATE_RANGE_CACHE
    try:
        agg = (
            df.group_by([pl.col("region_name"), pl.col("station_name")])
              .agg([
                  pl.col("date").min().alias("min_date"),
                  pl.col("date").max().alias("max_date"),
              ])
              .collect(engine="streaming")
        )
        cache: dict[tuple[str, str], tuple[str | None, str | None]] = {}
        for r, s, mn, mx in zip(
            agg["region_name"].to_list(),
            agg["station_name"].to_list(),
            agg["min_date"].to_list(),
            agg["max_date"].to_list(),
        ):
            cache[(str(r), str(s))] = (str(mn) if mn is not None else None, str(mx) if mx is not None else None)
        DATE_RANGE_CACHE = cache
    except Exception as e:
        # If this fails for any reason, keep cache empty and fall back to on-demand queries (safe).
        print("Error building date range cache:", e)
        DATE_RANGE_CACHE = {}

try:
    _build_region_station_cache()
except Exception as e:
    print("Error building region/station cache:", e)
    REGIONS = []
    REGION_TO_STATIONS = {}

try:
    _build_date_range_cache()
except Exception:
    pass

# Cache PDF bytes in memory (avoids disk I/O on every request)
PDF_BYTES: bytes | None = None
try:
    with open(PDF_GUIDE_FILE, "rb") as f:
        PDF_BYTES = f.read()
except Exception as e:
    print("Warning: could not read PDF guide:", e)
    PDF_BYTES = None

# ---------- TELEGRAM BOT ----------
# Keep thread count modest; more isn't always faster
bot = telebot.TeleBot(API_TOKEN, threaded=True, num_threads=4)

# ---------------- Safe edit helpers ----------------
def safe_edit_message_text(bot, text, chat_id, message_id, reply_markup=None, **kwargs):
    """Ignore Telegram's 'message is not modified' error."""
    try:
        return bot.edit_message_text(
            text,
            chat_id,
            message_id,
            reply_markup=reply_markup,
            **kwargs
        )
    except ApiTelegramException as e:
        if "message is not modified" in str(e):
            return None
        raise

def safe_edit_message_reply_markup(bot, chat_id, message_id, reply_markup=None, **kwargs):
    """Ignore Telegram's 'message is not modified' error."""
    try:
        return bot.edit_message_reply_markup(
            chat_id,
            message_id,
            reply_markup=reply_markup,
            **kwargs
        )
    except ApiTelegramException as e:
        if "message is not modified" in str(e):
            return None
        raise


def safe_answer_callback_query(bot, call_id, text=None, show_alert=False, **kwargs):
    """Safely answer callback queries. If already answered or expired, ignore."""
    try:
        if text is None:
            return bot.answer_callback_query(call_id, **kwargs)
        return bot.answer_callback_query(call_id, text, show_alert=show_alert, **kwargs)
    except Exception:
        return None

# ---------------------------------------------------

def upsert_user(user) -> None:
    _db_execute(
        """
        INSERT INTO users(user_id, username, first_name, last_seen)
        VALUES (?, ?, ?, date('now', 'localtime'))
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_seen=date('now', 'localtime')
        """,
        (user.id, user.username, user.first_name)
    )

def _membership_ok(member) -> bool:
    status = getattr(member, "status", "")
    if status in ALLOWED_MEMBERSHIP_STATUSES:
        return True
    if status == "restricted":
        return bool(getattr(member, "is_member", False))
    return False

def get_channel_membership_state(user_id: int) -> tuple[list[str], list[str]]:
    """
    Returns (missing_channels, unknown_channels).
    unknown_channels means membership could not be verified (e.g., bot lacks access).
    """
    missing: list[str] = []
    unknown: list[str] = []
    for channel in REQUIRED_CHANNELS:
        channel_ref = f"@{channel}"
        try:
            member = bot.get_chat_member(channel_ref, user_id)
            if not _membership_ok(member):
                missing.append(channel_ref)
        except Exception:
            unknown.append(channel_ref)
    return missing, unknown

def build_join_channels_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    for channel in REQUIRED_CHANNELS:
        markup.add(InlineKeyboardButton(f"عضویت در @{channel}", url=f"https://t.me/{channel}"))
    markup.add(InlineKeyboardButton("✅ بررسی عضویت", callback_data="check_join"))
    return markup

def require_membership(message_or_call) -> bool:
    user_id = message_or_call.from_user.id
    if hasattr(message_or_call, "message"):
        chat_id = message_or_call.message.chat.id
        reply_to_message_id = message_or_call.message.message_id
    else:
        chat_id = message_or_call.chat.id
        reply_to_message_id = None

    missing, unknown = get_channel_membership_state(user_id)
    if not missing and not unknown:
        return True

    blocked_channels = missing + unknown
    text = "برای استفاده از ربات باید در کانال‌های زیر عضو باشید:\n" + "\n".join(f"• {c}" for c in blocked_channels)
    if unknown:
        text += (
            "\n\n⚠️ وضعیت عضویت در برخی کانال‌ها قابل بررسی نیست. "
            "اگر عضو هستید ولی باز خطا می‌بینید، ادمین باید ربات را داخل کانال‌ها اضافه کند."
        )
    markup = build_join_channels_markup()
    if hasattr(message_or_call, "data"):
        safe_answer_callback_query(bot, message_or_call.id, "ابتدا در کانال‌ها عضو شوید.", show_alert=True)
    send_kwargs = {"reply_markup": markup}
    if reply_to_message_id is not None:
        send_kwargs["reply_to_message_id"] = reply_to_message_id
    bot.send_message(chat_id, text, **send_kwargs)
    return False

# ---------- RATE LIMIT HELPERS ----------
# For each (chat_id, message_id) remember last callback_data + timestamp
_LAST_CALLBACK: dict[tuple[int, int], tuple[str, float]] = {}
_USER_DOWNLOAD_LOCKS: dict[int, threading.Lock] = {}
_USER_DOWNLOAD_LOCKS_GUARD = threading.Lock()

def is_debounced(chat_id: int, message_id: int, callback_data: str) -> bool:
    key = (chat_id, message_id)
    now = time.time()
    last = _LAST_CALLBACK.get(key)
    if last and last[0] == callback_data and (now - last[1]) < DEBOUNCE_WINDOW_SECONDS:
        return True
    _LAST_CALLBACK[key] = (callback_data, now)
    return False

def get_user_download_lock(user_id: int) -> threading.Lock:
    """Serialize checks/download logging for one user to prevent quota races."""
    with _USER_DOWNLOAD_LOCKS_GUARD:
        return _USER_DOWNLOAD_LOCKS.setdefault(user_id, threading.Lock())

# ---------- HELPER FUNCTIONS ----------
STATIC_EXCLUDE_IDS = {str(ADMIN_ID), "107479525"}
NORMAL_DAILY_LIMIT = 1
NORMAL_MONTHLY_LIMIT = 10

def is_main_admin(user_id: int) -> bool:
    return str(user_id) == str(ADMIN_ID)

def is_download_limit_exempt(user_id: int) -> bool:
    """Return True only for permanent/static download-limit exemptions."""
    if str(user_id) in STATIC_EXCLUDE_IDS:
        return True
    row = _db_fetchone(
        "SELECT 1 FROM download_limit_exemptions WHERE user_id=?",
        (user_id,)
    )
    return row is not None

def add_download_limit_exemption(user_id: int) -> bool:
    """Make a user permanently exempt, replacing any one-day override."""
    with DB_LOCK:
        c = conn.cursor()
        c.execute(
            "INSERT OR IGNORE INTO download_limit_exemptions(user_id) VALUES (?)",
            (user_id,)
        )
        created = c.rowcount > 0
        c.execute("DELETE FROM daily_download_overrides WHERE user_id=?", (user_id,))
        conn.commit()
        return created

def set_daily_station_limit(user_id: int, station_limit: int) -> None:
    """Allow up to ``station_limit`` downloads today, ignoring normal limits."""
    with DB_LOCK:
        c = conn.cursor()
        c.execute("DELETE FROM download_limit_exemptions WHERE user_id=?", (user_id,))
        c.execute(
            """
            INSERT INTO daily_download_overrides(
                user_id, override_date, mode, station_limit, allowed_regions, added_at
            ) VALUES (?, ?, 'station_limit', ?, NULL, datetime('now', 'localtime'))
            ON CONFLICT(user_id) DO UPDATE SET
                override_date=excluded.override_date,
                mode=excluded.mode,
                station_limit=excluded.station_limit,
                allowed_regions=NULL,
                added_at=excluded.added_at,
                completion_notified_at=NULL
            """,
            (user_id, _today(), station_limit)
        )
        conn.commit()

def set_daily_region_override(user_id: int, regions: list[str]) -> None:
    """Allow unlimited downloads today, but only from the supplied regions."""
    encoded_regions = json.dumps(regions, ensure_ascii=False)
    with DB_LOCK:
        c = conn.cursor()
        c.execute("DELETE FROM download_limit_exemptions WHERE user_id=?", (user_id,))
        c.execute(
            """
            INSERT INTO daily_download_overrides(
                user_id, override_date, mode, station_limit, allowed_regions, added_at
            ) VALUES (?, ?, 'regions', NULL, ?, datetime('now', 'localtime'))
            ON CONFLICT(user_id) DO UPDATE SET
                override_date=excluded.override_date,
                mode=excluded.mode,
                station_limit=NULL,
                allowed_regions=excluded.allowed_regions,
                added_at=excluded.added_at,
                completion_notified_at=NULL
            """,
            (user_id, _today(), encoded_regions)
        )
        conn.commit()

def remove_download_limit_override(user_id: int) -> bool:
    """Remove either managed permanent or one-day override for a user."""
    with DB_LOCK:
        c = conn.cursor()
        c.execute("DELETE FROM download_limit_exemptions WHERE user_id=?", (user_id,))
        removed = c.rowcount
        c.execute("DELETE FROM daily_download_overrides WHERE user_id=?", (user_id,))
        removed += c.rowcount
        conn.commit()
        return removed > 0

def get_active_daily_override(user_id: int) -> dict | None:
    row = _db_fetchone(
        """
        SELECT mode, station_limit, allowed_regions
        FROM daily_download_overrides
        WHERE user_id=? AND override_date=? AND completion_notified_at IS NULL
        """,
        (user_id, _today())
    )
    if not row:
        return None

    mode, station_limit, allowed_regions = row
    if mode == "station_limit":
        return {"mode": mode, "station_limit": int(station_limit)}

    try:
        regions = json.loads(allowed_regions)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(regions, list):
        return None
    return {"mode": mode, "regions": [str(region) for region in regions]}

def _download_counts(user_id: int) -> tuple[int, int]:
    today = _today()
    month_start = today[:8] + "01"
    daily_row = _db_fetchone(
        "SELECT COUNT(*) FROM downloads WHERE user_id=? AND download_date=?",
        (user_id, today)
    )
    monthly_row = _db_fetchone(
        """
        SELECT COUNT(*) FROM downloads
        WHERE user_id=? AND download_date BETWEEN ? AND ?
        """,
        (user_id, month_start, today)
    )
    return (
        int(daily_row[0]) if daily_row else 0,
        int(monthly_row[0]) if monthly_row else 0,
    )

def check_download_access(user_id: int, region: str) -> tuple[bool, str | None]:
    """Evaluate the user's active policy for one station download."""
    if is_download_limit_exempt(user_id):
        return True, None

    daily_override = get_active_daily_override(user_id)
    if daily_override:
        if daily_override["mode"] == "station_limit":
            daily_used, _ = _download_counts(user_id)
            if daily_used < daily_override["station_limit"]:
                return True, None
            return False, "❌ سهمیه دانلود امروز شما به پایان رسیده است."

        if region in daily_override["regions"]:
            return True, None
        allowed = "، ".join(daily_override["regions"])
        return False, f"❌ امروز فقط دانلود از این شهرستان‌ها مجاز است:\n{allowed}"

    daily_used, monthly_used = _download_counts(user_id)
    if daily_used >= NORMAL_DAILY_LIMIT:
        return False, "❌ سهمیه روزانه شما (۱ ایستگاه) به پایان رسیده است."
    if monthly_used >= NORMAL_MONTHLY_LIMIT:
        return False, "❌ سهمیه ماهانه شما (۱۰ ایستگاه) به پایان رسیده است."
    return True, None

def send_limit_notification(user_id: int, text: str) -> bool:
    """Send a policy notification, returning False when the user is unreachable."""
    try:
        bot.send_message(user_id, text)
        return True
    except Exception as exc:
        print(f"Could not send limit notification to {user_id}: {exc}")
        return False

def process_completed_override_notifications(user_id: int | None = None) -> int:
    """Notify users whose one-day access expired or whose quota was consumed."""
    sql = """
        SELECT user_id, override_date, mode, station_limit, allowed_regions
        FROM daily_download_overrides
        WHERE completion_notified_at IS NULL
    """
    params: tuple = ()
    if user_id is not None:
        sql += " AND user_id=?"
        params = (user_id,)

    today = _today()
    notified = 0
    for uid, override_date, mode, station_limit, _ in _db_fetchall(sql, params):
        message_text: str | None = None
        if override_date < today:
            message_text = (
                "⏰ دسترسی موقت دانلود شما به پایان رسید.\n\n"
                "🔒 محدودیت عادی شما دوباره فعال شد:\n"
                f"• روزانه {NORMAL_DAILY_LIMIT} ایستگاه\n"
                f"• ماهانه {NORMAL_MONTHLY_LIMIT} ایستگاه"
            )
        elif override_date == today and mode == "station_limit":
            daily_used, _ = _download_counts(uid)
            if daily_used >= int(station_limit):
                message_text = (
                    f"✅ سهمیه موقت {station_limit} ایستگاه امروز شما کامل مصرف شد.\n\n"
                    "🔒 محدودیت عادی شما دوباره فعال شد:\n"
                    f"• روزانه {NORMAL_DAILY_LIMIT} ایستگاه\n"
                    f"• ماهانه {NORMAL_MONTHLY_LIMIT} ایستگاه"
                )

        if message_text is None or not send_limit_notification(uid, message_text):
            continue

        _db_execute(
            """
            UPDATE daily_download_overrides
            SET completion_notified_at=?
            WHERE user_id=? AND override_date=? AND completion_notified_at IS NULL
            """,
            (datetime.now(IRAN_TIMEZONE).isoformat(timespec="seconds"), uid, override_date)
        )
        notified += 1

    return notified

_NOTIFICATION_STOP_EVENT = threading.Event()

def run_limit_notification_worker() -> None:
    """Check durable one-day policies periodically, including after restarts."""
    while not _NOTIFICATION_STOP_EVENT.is_set():
        try:
            process_completed_override_notifications()
        except Exception as exc:
            print(f"[Limit Notification Error] {exc}")
        _NOTIFICATION_STOP_EVENT.wait(30)

def log_download(user_id: int, username: str, station_name: str) -> None:
    _db_execute(
        "INSERT INTO downloads(user_id, username, station_name, download_date) VALUES (?, ?, ?, ?)",
        (user_id, username, station_name, _today())
    )

def get_stations_for(region: str) -> list[str]:
    return REGION_TO_STATIONS.get(region, [])

def get_date_range(region_name: str, station_name: str) -> tuple[str | None, str | None]:
    """Return precomputed (min_date, max_date) for a station."""
    return DATE_RANGE_CACHE.get((region_name, station_name), (None, None))

def build_keyboard(options: list[str], callback_prefix: str, page: int = 0) -> InlineKeyboardMarkup:
    """ساخت کیبورد چندستونه با پیمایش"""
    markup = InlineKeyboardMarkup()
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_items = options[start:end]

    row = []
    for i, option in enumerate(page_items, 1):
        row.append(InlineKeyboardButton(option, callback_data=f"{callback_prefix}|{option}"))
        if i % BUTTONS_PER_ROW == 0:
            markup.row(*row)
            row = []
    if row:
        markup.row(*row)

    total_pages = math.ceil(len(options) / PAGE_SIZE) if options else 1
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{callback_prefix}_page|{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"{callback_prefix}_page|{page+1}"))
        markup.row(*nav_row)

    return markup

# ---------- MAIN MENU (Province list + utilities) ----------
def build_region_menu(user_id: int, page: int = 0) -> InlineKeyboardMarkup:
    """Province selection keyboard plus utility buttons (e.g., download limit)."""
    visible_regions = REGIONS
    daily_override = get_active_daily_override(user_id)
    if daily_override and daily_override["mode"] == "regions":
        allowed = set(daily_override["regions"])
        visible_regions = [region for region in REGIONS if region in allowed]

    markup = build_keyboard(visible_regions, "region", page)
    # Utility button that used to exist before optimizations
    markup.row(
        InlineKeyboardButton("📊 Check download limit", callback_data="check_download_limit")
    )
    markup = _add_admin_button(markup, user_id)
    return markup

def download_limit_status_text(user_id: int) -> str:
    if is_download_limit_exempt(user_id):
        return "✅ حساب شما محدودیت دانلود ندارد."

    daily_used, monthly_used = _download_counts(user_id)
    daily_override = get_active_daily_override(user_id)
    if daily_override and daily_override["mode"] == "station_limit":
        daily_limit = daily_override["station_limit"]
        return (
            "📊 سهمیه موقت امروز\n\n"
            f"• دانلود امروز: {daily_used}/{daily_limit}\n"
            f"• باقی‌مانده: {max(0, daily_limit - daily_used)}\n"
            "• پایان اعتبار: پایان امروز به وقت ایران"
        )
    if daily_override and daily_override["mode"] == "regions":
        allowed = "، ".join(daily_override["regions"])
        return (
            "📍 دسترسی موقت شهرستانی امروز\n\n"
            f"• شهرستان‌های مجاز: {allowed}\n"
            f"• دانلود امروز: {daily_used}\n"
            "• پایان اعتبار: پایان امروز به وقت ایران"
        )

    return (
        "📊 محدودیت دانلود\n\n"
        f"• امروز: {daily_used}/{NORMAL_DAILY_LIMIT} "
        f"(باقی‌مانده: {max(0, NORMAL_DAILY_LIMIT - daily_used)})\n"
        f"• این ماه: {monthly_used}/{NORMAL_MONTHLY_LIMIT} "
        f"(باقی‌مانده: {max(0, NORMAL_MONTHLY_LIMIT - monthly_used)})"
    )

def _add_admin_button(markup: InlineKeyboardMarkup, user_id: int) -> InlineKeyboardMarkup:
    if is_main_admin(user_id):
        markup.row(
            InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"),
            InlineKeyboardButton("🔓 مدیریت محدودیت", callback_data="admin_exemptions")
        )
    return markup

def build_exemptions_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    permanent_rows = _db_fetchall("""
        SELECT e.user_id, u.username, u.first_name
        FROM download_limit_exemptions AS e
        LEFT JOIN users AS u ON u.user_id = e.user_id
        ORDER BY e.added_at, e.user_id
    """)
    daily_rows = _db_fetchall("""
        SELECT o.user_id, u.username, u.first_name, o.mode
        FROM daily_download_overrides AS o
        LEFT JOIN users AS u ON u.user_id = o.user_id
        WHERE o.override_date=? AND o.completion_notified_at IS NULL
        ORDER BY o.added_at, o.user_id
    """, (_today(),))
    for uid, username, first_name in permanent_rows:
        label = f"❌ دائمی | {username or first_name or uid} ({uid})"
        markup.add(InlineKeyboardButton(label, callback_data=f"exemption_remove|{uid}"))
    for uid, username, first_name, mode in daily_rows:
        mode_label = "سهمیه امروز" if mode == "station_limit" else "شهرستان امروز"
        label = f"❌ {mode_label} | {username or first_name or uid} ({uid})"
        markup.add(InlineKeyboardButton(label, callback_data=f"exemption_remove|{uid}"))
    markup.add(InlineKeyboardButton("➕ راهنمای افزودن", callback_data="exemption_add_help"))
    markup.add(InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_provinces"))
    return markup

def exemptions_text() -> str:
    permanent_rows = _db_fetchall("""
        SELECT e.user_id, u.username, u.first_name
        FROM download_limit_exemptions AS e
        LEFT JOIN users AS u ON u.user_id = e.user_id
        ORDER BY e.added_at, e.user_id
    """)
    daily_rows = _db_fetchall("""
        SELECT o.user_id, u.username, u.first_name,
               o.mode, o.station_limit, o.allowed_regions
        FROM daily_download_overrides AS o
        LEFT JOIN users AS u ON u.user_id = o.user_id
        WHERE o.override_date=? AND o.completion_notified_at IS NULL
        ORDER BY o.added_at, o.user_id
    """, (_today(),))

    lines = [
        f"• {uid} — {username or first_name or 'نامشخص'} — بدون محدودیت دائمی"
        for uid, username, first_name in permanent_rows
    ]
    for uid, username, first_name, mode, station_limit, encoded_regions in daily_rows:
        name = username or first_name or "نامشخص"
        if mode == "station_limit":
            description = f"امروز تا {station_limit} ایستگاه"
        else:
            try:
                regions = json.loads(encoded_regions)
            except (TypeError, json.JSONDecodeError):
                regions = []
            description = "امروز فقط: " + "، ".join(regions)
        lines.append(f"• {uid} — {name} — {description}")

    if not lines:
        listing = "فعلاً هیچ کاربری در فهرست قابل‌مدیریت نیست."
    else:
        listing = "\n".join(lines)
    return (
        "🔓 مدیریت محدودیت دانلود\n\n"
        f"{listing}\n\n"
        "۱) دائمی: /limit_add user_id\n"
        "۲) سهمیه امروز: /limit_add user_id daily تعداد\n"
        "۳) مناطق امروز: /limit_add user_id regions نام۱، نام۲\n"
        "حذف: /limit_remove user_id\n"
        "نمایش فهرست: /limit_list\n\n"
        "برای حذف سریع، روی دکمه همان کاربر بزنید."
    )

def _send_pdf(chat_id: int):
    if not PDF_BYTES:
        bot.send_message(chat_id, "⚠️ PDF guide file is not available on the server.")
        return
    pdf_buf = io.BytesIO(PDF_BYTES)
    pdf_buf.name = os.path.basename(PDF_GUIDE_FILE)  # Telebot uses name for filename
    pdf_buf.seek(0)
    bot.send_document(chat_id, pdf_buf)

def _send_station_csv(chat_id: int, region: str, station: str, min_date: str, max_date: str):
    station_df = (
        df.filter(
            (pl.col("station_name") == station) &
            (pl.col("region_name") == region)
        )
        .sort("date")
        .collect(engine="streaming")
    )

    csv_filename = f"{region}_{station}_{min_date}_{max_date}.csv"
    buf = io.BytesIO()
    # Polars can write to file-like objects
    station_df.write_csv(buf)
    buf.name = csv_filename
    buf.seek(0)
    bot.send_document(chat_id, buf)

# ---------- BOT HANDLERS ----------
@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    upsert_user(message.from_user)

    if not require_membership(message):
        return

    markup = build_region_menu(user_id)
    bot.send_message(message.chat.id, f"👋 Welcome {username}!\nPlease select a province:", reply_markup=markup)

@bot.message_handler(commands=['help'])
def help_command(message):
    if not require_membership(message):
        return

    bot.send_message(
        message.chat.id,
        "ℹ️ *Help & Usage Guide*\n\n"
        "1️⃣ Use /start to begin.\n"
        "2️⃣ Select a province, then choose a synoptic station.\n"
        "3️⃣ Download the available data (CSV + PDF).\n\n"
        "⚠️ Limit: One station per day per user.\n"
        "📌 This bot is for academic and research purposes only.",
        parse_mode="Markdown"
    )

@bot.message_handler(commands=['report'])
def report_command(message):
    user_id = message.from_user.id
    if not require_membership(message):
        return
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return

    rows = _db_fetchall("""
        SELECT user_id, username, station_name, download_date
        FROM downloads
        WHERE download_date = ?
    """, (_today(),))

    if not rows:
        bot.send_message(message.chat.id, "📭 No downloads recorded today.")
        return

    report_lines = ["📊 Daily Download Report"]
    for uid, uname, station, ddate in rows:
        uname_display = uname if uname else "N/A"
        report_lines.append(f"- 👤 {uname_display} (ID: {uid})\n  📍 {station} | {ddate}")

    bot.send_message(message.chat.id, "\n\n".join(report_lines))

@bot.message_handler(commands=['user'])
def user_info(message):
    user_id = message.from_user.id
    if not require_membership(message):
        return
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return

    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(message, "❌ فرمت صحیح:\n/user user_id\nمثال:\n/user 244146213")
            return

        target_user_id = parts[1]
        row = _db_fetchone("""
            SELECT COUNT(*) AS total_downloads,
                   GROUP_CONCAT(DISTINCT station_name)
            FROM downloads
            WHERE user_id = ?
        """, (target_user_id,))

        if row is None or row[0] == 0:
            bot.reply_to(message, f"ℹ️ اطلاعاتی برای user_id `{target_user_id}` پیدا نشد.", parse_mode="Markdown")
            return

        total_downloads = row[0]
        stations = row[1].split(",") if row[1] else []

        response = (
            f"👤 *User ID:* `{target_user_id}`\n"
            f"⬇️ *تعداد کل دانلودها:* {total_downloads}\n\n"
            f"📡 *ایستگاه‌ها:*"
        )
        for s in stations:
            response += f"\n• {s}"

        bot.reply_to(message, response, parse_mode="Markdown")

    except Exception as e:
        bot.reply_to(message, f"⚠️ خطا:\n{str(e)}")

@bot.message_handler(commands=['users_count'])
def users_count(message):
    user_id = message.from_user.id
    if not require_membership(message):
        return
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return

    row = _db_fetchone("SELECT COUNT(DISTINCT user_id) FROM downloads")
    count = row[0] if row else 0
    bot.reply_to(message, f"👥 تعداد کل کاربران:\n{count}")

def _parse_positive_user_id(message, command_name: str) -> int | None:
    parts = (message.text or "").split()
    if len(parts) != 2:
        bot.reply_to(
            message,
            f"❌ فرمت صحیح:\n/{command_name} user_id\nمثال:\n/{command_name} 244146213"
        )
        return None
    try:
        user_id = int(parts[1])
        if user_id <= 0:
            raise ValueError
        return user_id
    except ValueError:
        bot.reply_to(message, "❌ user_id باید یک عدد صحیح مثبت باشد.")
        return None

def _limit_add_help() -> str:
    return (
        "فرمت‌های دستور /limit_add:\n\n"
        "۱) حذف دائمی همه محدودیت‌ها:\n"
        "/limit_add user_id\n"
        "/limit_add user_id permanent\n\n"
        "۲) سهمیه تعداد ایستگاه فقط برای امروز:\n"
        "/limit_add user_id daily تعداد\n"
        "مثال: /limit_add 244146213 daily 5\n\n"
        "۳) دسترسی نامحدود امروز، فقط برای شهرستان‌ها/مناطق مشخص:\n"
        "/limit_add user_id regions نام۱، نام۲\n"
        "مثال: /limit_add 244146213 regions Khorasan Razavi, North Khorasan\n\n"
        "نام مناطق را دقیقاً مطابق دکمه‌های ربات وارد کنید. "
        "حالت‌های موقت در پایان امروز به وقت ایران خودکار غیرفعال می‌شوند."
    )

def _normalize_region_name(value: str) -> str:
    return " ".join(
        value.strip().replace("ي", "ی").replace("ك", "ک").replace("\u200c", " ").split()
    ).casefold()

def _resolve_regions(raw_regions: str) -> tuple[list[str], list[str]]:
    requested = [part.strip() for part in re.split(r"[،,;|\n]+", raw_regions) if part.strip()]
    region_lookup = {_normalize_region_name(region): region for region in REGIONS}
    resolved: list[str] = []
    invalid: list[str] = []
    for requested_region in requested:
        region = region_lookup.get(_normalize_region_name(requested_region))
        if region is None:
            invalid.append(requested_region)
        elif region not in resolved:
            resolved.append(region)
    return resolved, invalid

@bot.message_handler(commands=['limit_add'])
def limit_add(message):
    if not is_main_admin(message.from_user.id):
        bot.reply_to(message, "⛔ شما اجازه استفاده از این دستور را ندارید.")
        return

    parts = (message.text or "").split(maxsplit=3)
    if len(parts) < 2:
        bot.reply_to(message, _limit_add_help())
        return
    try:
        target_user_id = int(parts[1])
        if target_user_id <= 0:
            raise ValueError
    except ValueError:
        bot.reply_to(message, "❌ user_id باید یک عدد صحیح مثبت باشد.\n\n" + _limit_add_help())
        return

    if str(target_user_id) in STATIC_EXCLUDE_IDS:
        bot.reply_to(message, f"ℹ️ کاربر {target_user_id} از قبل استثنای ثابت است.")
        return

    mode = parts[2].casefold() if len(parts) >= 3 else "permanent"
    if mode in {"1", "permanent", "unlimited", "دائم", "دایمی"}:
        created = add_download_limit_exemption(target_user_id)
        if created:
            notification_sent = send_limit_notification(
                target_user_id,
                "🔓 محدودیت دانلود حساب شما به‌صورت دائمی برداشته شد.\n"
                "از این پس محدودیت روزانه و ماهانه برای شما اعمال نمی‌شود."
            )
            warning = "" if notification_sent else "\n⚠️ ارسال پیام به کاربر ممکن نبود."
            bot.reply_to(
                message,
                f"✅ محدودیت کاربر {target_user_id} به‌صورت دائمی برداشته شد.{warning}"
            )
        else:
            bot.reply_to(message, f"ℹ️ کاربر {target_user_id} از قبل بدون محدودیت دائمی است.")
        return

    if mode in {"2", "daily", "today", "count", "روزانه", "امروز"}:
        if len(parts) != 4:
            bot.reply_to(message, "❌ تعداد ایستگاه مشخص نشده است.\n\n" + _limit_add_help())
            return
        try:
            station_limit = int(parts[3])
            if station_limit <= 0:
                raise ValueError
        except ValueError:
            bot.reply_to(message, "❌ تعداد ایستگاه باید یک عدد صحیح مثبت باشد.")
            return
        set_daily_station_limit(target_user_id, station_limit)
        notification_sent = send_limit_notification(
            target_user_id,
            "🔓 سهمیه دانلود امروز شما افزایش یافت.\n\n"
            f"امروز می‌توانید در مجموع تا {station_limit} ایستگاه دانلود کنید.\n"
            "پس از مصرف این سهمیه یا پایان امروز، محدودیت عادی دوباره فعال می‌شود."
        )
        # If the user had already downloaded this many stations today, close
        # the override and notify them immediately instead of waiting 30s.
        process_completed_override_notifications(target_user_id)
        warning = "" if notification_sent else "\n⚠️ ارسال پیام به کاربر ممکن نبود."
        bot.reply_to(
            message,
            f"✅ کاربر {target_user_id} تا پایان امروز اجازه دانلود {station_limit} ایستگاه را دارد."
            f"{warning}"
        )
        return

    if mode in {"3", "regions", "region", "counties", "county", "شهرستان", "شهرستانها", "شهرستان‌ها"}:
        if len(parts) != 4:
            bot.reply_to(message, "❌ نام شهرستان‌ها مشخص نشده است.\n\n" + _limit_add_help())
            return
        regions, invalid_regions = _resolve_regions(parts[3])
        if invalid_regions:
            invalid_text = "، ".join(invalid_regions)
            bot.reply_to(
                message,
                f"❌ این نام‌ها در فهرست ربات پیدا نشدند:\n{invalid_text}\n\n"
                "نام‌ها را دقیقاً مطابق دکمه‌های /start وارد کنید و با ویرگول جدا کنید."
            )
            return
        if not regions:
            bot.reply_to(message, "❌ حداقل یک شهرستان معتبر وارد کنید.")
            return
        set_daily_region_override(target_user_id, regions)
        regions_text = "، ".join(regions)
        notification_sent = send_limit_notification(
            target_user_id,
            "🔓 دسترسی موقت دانلود برای شما فعال شد.\n\n"
            f"تا پایان امروز می‌توانید از این مناطق دانلود کنید:\n{regions_text}\n\n"
            "در پایان امروز، محدودیت عادی شما دوباره فعال می‌شود."
        )
        warning = "" if notification_sent else "\n⚠️ ارسال پیام به کاربر ممکن نبود."
        bot.reply_to(
            message,
            f"✅ کاربر {target_user_id} تا پایان امروز فقط از این شهرستان‌ها دسترسی دارد:\n"
            f"{regions_text}{warning}"
        )
        return

    bot.reply_to(message, "❌ نوع محدودیت شناخته نشد.\n\n" + _limit_add_help())

@bot.message_handler(commands=['limit_remove'])
def limit_remove(message):
    if not is_main_admin(message.from_user.id):
        bot.reply_to(message, "⛔ شما اجازه استفاده از این دستور را ندارید.")
        return
    target_user_id = _parse_positive_user_id(message, "limit_remove")
    if target_user_id is None:
        return
    if str(target_user_id) in STATIC_EXCLUDE_IDS:
        bot.reply_to(message, "⛔ استثنای ثابت مدیر/سیستم از داخل بات قابل حذف نیست.")
        return
    if remove_download_limit_override(target_user_id):
        notification_sent = send_limit_notification(
            target_user_id,
            "🔒 دسترسی ویژه دانلود شما توسط مدیر پایان یافت.\n"
            "محدودیت عادی روزانه و ماهانه دوباره فعال شد."
        )
        warning = "" if notification_sent else "\n⚠️ ارسال پیام به کاربر ممکن نبود."
        bot.reply_to(
            message,
            f"✅ تنظیم ویژه کاربر {target_user_id} حذف و محدودیت عادی فعال شد.{warning}"
        )
    else:
        bot.reply_to(message, "ℹ️ این کاربر در فهرست بدون محدودیت نبود.")

@bot.message_handler(commands=['limit_list'])
def limit_list(message):
    if not is_main_admin(message.from_user.id):
        bot.reply_to(message, "⛔ شما اجازه استفاده از این دستور را ندارید.")
        return
    bot.send_message(
        message.chat.id,
        exemptions_text(),
        reply_markup=build_exemptions_markup()
    )

@bot.message_handler(commands=['send'])
def send_to_all(message):
    user_id = message.from_user.id
    if not require_membership(message):
        return
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return

    if not message.reply_to_message:
        bot.reply_to(message, "❌ روی یک پیام ریپلای کنید و /send بزنید.")
        return

    source_msg = message.reply_to_message
    rows = _db_fetchall("SELECT user_id FROM users")
    recipients = sorted({int(r[0]) for r in rows if r and r[0]})

    success = 0
    failed = 0
    for uid in recipients:
        try:
            bot.copy_message(uid, message.chat.id, source_msg.message_id)
            success += 1
        except Exception:
            failed += 1

    bot.reply_to(message, f"✅ ارسال انجام شد. موفق: {success} | ناموفق: {failed}")

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    upsert_user(call.from_user)
    user_id = call.from_user.id
    username = call.from_user.username or call.from_user.first_name
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    answered = False

    # Debounce repeated taps
    if is_debounced(chat_id, message_id, call.data):
        safe_answer_callback_query(bot, call.id)
        return

    if call.data == "check_join":
        missing, unknown = get_channel_membership_state(user_id)
        if missing or unknown:
            safe_answer_callback_query(bot, call.id, "هنوز عضو همه کانال‌ها نیستید.", show_alert=True)
            msg = "لطفا ابتدا عضو شوید و دوباره بررسی کنید."
            if unknown:
                msg += "\n\n⚠️ بررسی برخی کانال‌ها ممکن نیست؛ ربات باید در کانال‌ها عضو باشد."
            bot.send_message(chat_id, msg, reply_markup=build_join_channels_markup())
            return

        safe_answer_callback_query(bot, call.id, "عضویت تایید شد ✅")
        markup = build_region_menu(user_id)
        bot.send_message(chat_id, "✅ عضویت شما تایید شد. حالا می‌توانید از ربات استفاده کنید.", reply_markup=markup)
        return

    if not require_membership(call):
        return

    # ---------- Admin report ----------
    if call.data == "admin_report" and is_main_admin(user_id):
        today = _today()
        rows = _db_fetchall("SELECT username, station_name FROM downloads WHERE download_date=?", (today,))
        report = "\n".join([f"{u} -> {s}" for u, s in rows]) if rows else "No downloads today."
        bot.send_message(chat_id, f"📊 Today's downloads:\n{report}")
        return

    # ---------- Admin download-limit exemptions ----------
    if call.data == "admin_exemptions":
        if not is_main_admin(user_id):
            safe_answer_callback_query(bot, call.id, "دسترسی غیرمجاز", show_alert=True)
            return
        safe_edit_message_text(
            bot,
            exemptions_text(),
            chat_id,
            message_id,
            reply_markup=build_exemptions_markup()
        )
        return

    if call.data == "exemption_add_help":
        if not is_main_admin(user_id):
            safe_answer_callback_query(bot, call.id, "دسترسی غیرمجاز", show_alert=True)
            return
        safe_answer_callback_query(bot, call.id, "راهنمای دستور ارسال شد.")
        bot.send_message(chat_id, _limit_add_help())
        return

    if call.data.startswith("exemption_remove|"):
        if not is_main_admin(user_id):
            safe_answer_callback_query(bot, call.id, "دسترسی غیرمجاز", show_alert=True)
            return
        try:
            target_user_id = int(call.data.split("|", 1)[1])
        except ValueError:
            safe_answer_callback_query(bot, call.id, "شناسه نامعتبر است.", show_alert=True)
            return
        removed = remove_download_limit_override(target_user_id)
        notification_sent = True
        if removed:
            notification_sent = send_limit_notification(
                target_user_id,
                "🔒 دسترسی ویژه دانلود شما توسط مدیر پایان یافت.\n"
                "محدودیت عادی روزانه و ماهانه دوباره فعال شد."
            )
        result_text = "محدودیت کاربر دوباره فعال شد." if removed else "کاربر در فهرست نبود."
        if removed and not notification_sent:
            result_text += " ارسال پیام به کاربر ممکن نبود."
        safe_answer_callback_query(
            bot,
            call.id,
            result_text,
            show_alert=True
        )
        safe_edit_message_text(
            bot,
            exemptions_text(),
            chat_id,
            message_id,
            reply_markup=build_exemptions_markup()
        )
        return

    # ---------- Pagination ----------
    if "_page|" in call.data:
        prefix, page_str = call.data.split("_page|", 1)
        page = int(page_str)

        if prefix.startswith("region"):
            markup = build_region_menu(user_id, page)
            safe_edit_message_reply_markup(bot, chat_id, message_id, reply_markup=markup)
            return

        if prefix.startswith("station"):
            parts = prefix.split("|", 1)
            if len(parts) == 2:
                region = parts[1]
                stations = get_stations_for(region)
                markup = build_keyboard(stations, f"station|{region}", page)
                markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
                safe_edit_message_reply_markup(bot, chat_id, message_id, reply_markup=markup)
            return

    # ---------- Back button ----------
    if call.data == "back_to_provinces":
        markup = build_region_menu(user_id)
        safe_edit_message_text(bot, "🔙 Back to province selection:", chat_id, message_id, reply_markup=markup)
        return

    # ---------- Check download limit (menu utility) ----------
    if call.data == "check_download_limit":
        text = download_limit_status_text(user_id)

        # Show as an alert for instant visibility + keep menu intact
        safe_answer_callback_query(bot, call.id, text.replace("*", ""), show_alert=True)
        answered = True

        # Also refresh markup (in case it disappeared due to earlier edits)
        markup = build_region_menu(user_id)
        safe_edit_message_reply_markup(bot, chat_id, message_id, reply_markup=markup)
        return

    # ---------- Region selection ----------
    if call.data.startswith("region|"):
        region = call.data.split("|", 1)[1]
        daily_override = get_active_daily_override(user_id)
        if (
            daily_override
            and daily_override["mode"] == "regions"
            and region not in daily_override["regions"]
        ):
            safe_answer_callback_query(
                bot,
                call.id,
                "❌ این شهرستان در دسترسی موقت امروز شما نیست.",
                show_alert=True
            )
            return
        stations = get_stations_for(region)
        if not stations:
            bot.send_message(chat_id, "⚠️ No stations found for this province.")
            return

        markup = build_keyboard(stations, f"station|{region}")
        markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
        safe_edit_message_text(
            bot,
            f"🏞 Selected province: {region}\nPlease select a synoptic station:",
            chat_id,
            message_id,
            reply_markup=markup
        )
        return

    # ---------- Station selection ----------
    if call.data.startswith("station|"):
        # station|<region>|<station_name>
        parts = call.data.split("|")
        if len(parts) < 3:
            bot.send_message(chat_id, "⚠️ Invalid selection.")
            return

        region = parts[1]
        station = parts[-1]

        # Keep the quota check and successful log sequential per user. This
        # prevents two simultaneous button taps from both consuming the final
        # available slot.
        with get_user_download_lock(user_id):
            access_allowed, denial_message = check_download_access(user_id, region)
            if not access_allowed:
                safe_answer_callback_query(
                    bot,
                    call.id,
                    denial_message or "❌ امکان دانلود برای شما وجود ندارد.",
                    show_alert=True
                )
                answered = True
                return

            min_date, max_date = get_date_range(region, station)
            if min_date is None or max_date is None:
                bot.send_message(chat_id, "No data available for this station.")
                return

            safe_answer_callback_query(bot, call.id)
            answered = True
            bot.send_message(chat_id, f"🌡 Selected station: {station}\nData available from {min_date} to {max_date}")

            try:
                bot.send_message(7690029281, f"- 👤 {username} (ID: {user_id})\n  📍{station}\n")
            except Exception:
                pass

            _send_station_csv(chat_id, region, station, min_date, max_date)
            _send_pdf(chat_id)
            log_download(user_id, username, station)
            process_completed_override_notifications(user_id)

        # Offer start menu again (single message)
        markup = build_region_menu(user_id)
        bot.send_message(chat_id, "Please select a province again:", reply_markup=markup)
        return

    # Fallback: ensure callback is answered (removes Telegram loading state)
    if not answered:
        safe_answer_callback_query(bot, call.id)

# ---------- MAIN ----------
def run_bot():
    while True:
        try:
            # skip_pending avoids processing old updates after restart
            bot.infinity_polling(timeout=60, long_polling_timeout=60, skip_pending=True)
        except Exception as e:
            print(f"[Bot Error] {e}. Restarting in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    notification_thread = threading.Thread(
        target=run_limit_notification_worker,
        daemon=True
    )
    notification_thread.start()
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    while True:
        time.sleep(1)
