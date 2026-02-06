import os
import math
import time
import threading
import sqlite3
import io

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

BUTTONS_PER_ROW = 2
PAGE_SIZE = 16

# Debounce repeated callbacks (same button tapped multiple times quickly)
DEBOUNCE_WINDOW_SECONDS = 1.5

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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_downloads_user_date ON downloads(user_id, download_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_downloads_date ON downloads(download_date)")
    conn.commit()

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
# ---------------------------------------------------

# ---------- RATE LIMIT HELPERS ----------
# For each (chat_id, message_id) remember last callback_data + timestamp
_LAST_CALLBACK: dict[tuple[int, int], tuple[str, float]] = {}

def is_debounced(chat_id: int, message_id: int, callback_data: str) -> bool:
    key = (chat_id, message_id)
    now = time.time()
    last = _LAST_CALLBACK.get(key)
    if last and last[0] == callback_data and (now - last[1]) < DEBOUNCE_WINDOW_SECONDS:
        return True
    _LAST_CALLBACK[key] = (callback_data, now)
    return False

# ---------- HELPER FUNCTIONS ----------
EXCLUDE_IDs = {str(ADMIN_ID), "107479525"}  # daily/monthly limits won't apply to these

def can_download_daily(user_id: int) -> bool:
    if str(user_id) in EXCLUDE_IDs:
        return True
    today = time.strftime("%Y-%m-%d")
    row = _db_fetchone("SELECT 1 FROM downloads WHERE user_id=? AND download_date=? LIMIT 1", (user_id, today))
    return row is None

def can_download_monthly(user_id: int) -> bool:
    if str(user_id) in EXCLUDE_IDs:
        return True
    today = time.strftime("%Y-%m-%d")
    month_start = today[:8] + "01"
    row = _db_fetchone(
        "SELECT COUNT(*) FROM downloads WHERE user_id=? AND download_date >= ?",
        (user_id, month_start)
    )
    count = row[0] if row else 0
    return count < 10

def log_download(user_id: int, username: str, station_name: str) -> None:
    today = time.strftime("%Y-%m-%d")
    _db_execute(
        "INSERT INTO downloads(user_id, username, station_name, download_date) VALUES (?, ?, ?, ?)",
        (user_id, username, station_name, today)
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

def _add_admin_button(markup: InlineKeyboardMarkup, user_id: int) -> InlineKeyboardMarkup:
    if str(user_id) == str(ADMIN_ID):
        markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
    return markup

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

    markup = build_keyboard(REGIONS, "region")
    markup = _add_admin_button(markup, user_id)
    bot.send_message(message.chat.id, f"👋 Welcome {username}!\nPlease select a province:", reply_markup=markup)

@bot.message_handler(commands=['help'])
def help_command(message):
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
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return

    rows = _db_fetchall("""
        SELECT user_id, username, station_name, download_date
        FROM downloads
        WHERE download_date = date('now', 'localtime')
    """)

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
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return

    row = _db_fetchone("SELECT COUNT(DISTINCT user_id) FROM downloads")
    count = row[0] if row else 0
    bot.reply_to(message, f"👥 تعداد کل کاربران:\n{count}")

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    user_id = call.from_user.id
    username = call.from_user.username or call.from_user.first_name
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # End Telegram "loading..." quickly
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    # Debounce repeated taps
    if is_debounced(chat_id, message_id, call.data):
        return

    # ---------- Admin report ----------
    if call.data == "admin_report" and str(user_id) == str(ADMIN_ID):
        today = time.strftime("%Y-%m-%d")
        rows = _db_fetchall("SELECT username, station_name FROM downloads WHERE download_date=?", (today,))
        report = "\n".join([f"{u} -> {s}" for u, s in rows]) if rows else "No downloads today."
        bot.send_message(chat_id, f"📊 Today's downloads:\n{report}")
        return

    # ---------- Pagination ----------
    if "_page|" in call.data:
        prefix, page_str = call.data.split("_page|", 1)
        page = int(page_str)

        if prefix.startswith("region"):
            markup = build_keyboard(REGIONS, "region", page)
            markup = _add_admin_button(markup, user_id)
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
        markup = build_keyboard(REGIONS, "region")
        markup = _add_admin_button(markup, user_id)
        safe_edit_message_text(bot, "🔙 Back to province selection:", chat_id, message_id, reply_markup=markup)
        return

    # ---------- Region selection ----------
    if call.data.startswith("region|"):
        region = call.data.split("|", 1)[1]
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

        # ---------- Check download limit ----------
        if not can_download_daily(user_id) or not can_download_monthly(user_id):
            try:
                bot.answer_callback_query(
                    call.id,
                    "❌ You have already downloaded a station today or reached the 10-stations-per-month limit.",
                    show_alert=True
                )
            except Exception:
                pass
            return

        min_date, max_date = get_date_range(region, station)
        if min_date is None or max_date is None:
            bot.send_message(chat_id, "No data available for this station.")
            return

        # Send a single info message (less API chatter)
        bot.send_message(chat_id, f"🌡 Selected station: {station}\nData available from {min_date} to {max_date}")

        # Optional: log to admin channel if you want (kept from your original code)
        try:
            bot.send_message(7690029281, f"- 👤 {username} (ID: {user_id})\n  📍{station}\n")
        except Exception:
            pass

        # Send CSV (in-memory) + PDF (in-memory)
        _send_station_csv(chat_id, region, station, min_date, max_date)
        _send_pdf(chat_id)

        # Log download
        log_download(user_id, username, station)

        # Offer start menu again (single message)
        markup = build_keyboard(REGIONS, "region")
        markup = _add_admin_button(markup, user_id)
        bot.send_message(chat_id, "Please select a province again:", reply_markup=markup)
        return

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
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    while True:
        time.sleep(1)
