import os
import math
import io
import asyncio
from datetime import date
from typing import Dict, List, Tuple, Optional

import polars as pl
import aiosqlite
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)

# ---------- CONFIG ----------
load_dotenv()
API_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ADMIN_ID = str(os.environ.get("ADMIN_ID", "")).strip()

PARQUET_FILE = os.environ.get("DATA_PATH", "Iran_Data.parquet")
PDF_GUIDE_FILE = os.environ.get("GUIDE_PATH", "Help.pdf")
DB_PATH = "users.db"

BUTTONS_PER_ROW = 2
PAGE_SIZE = 16
# ---------------------------

# ---------- POLARS (LAZY) ----------
LF = pl.scan_parquet(PARQUET_FILE)

# Cacheهای در حافظه برای سرعت
REGIONS: List[str] = []
STATIONS_BY_REGION: Dict[str, List[str]] = {}  # region -> [stations...]
# اگر خواستی می‌تونی date-range cache هم بزنی؛ ولی ممکنه حافظه زیاد شود.
# DATE_RANGE_CACHE: Dict[Tuple[str, str], Tuple[str, str]] = {}

# ---------- DB ----------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS downloads (
    user_id INTEGER,
    username TEXT,
    station_name TEXT,
    download_date TEXT
)
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TABLE_SQL)
        await db.commit()

async def can_download(user_id: int) -> bool:
    if str(user_id) == ADMIN_ID:
        return True
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM downloads WHERE user_id=? AND download_date=? LIMIT 1",
            (user_id, today)
        ) as cur:
            row = await cur.fetchone()
            return row is None

async def log_download(user_id: int, username: str, station_name: str):
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO downloads (user_id, username, station_name, download_date) VALUES (?, ?, ?, ?)",
            (user_id, username, station_name, today)
        )
        await db.commit()

async def daily_report_text() -> str:
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, username, station_name, download_date FROM downloads WHERE download_date=?",
            (today,)
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        return "📭 No downloads recorded today."

    lines = ["📊 Today's downloads:"]
    for uid, uname, station, ddate in rows:
        uname_display = uname or "N/A"
        lines.append(f"- 👤 {uname_display} (ID: {uid})\n  📍 {station} | {ddate}")
    return "\n\n".join(lines)

async def user_stats_text(target_user_id: str) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT COUNT(*) AS total_downloads,
                   GROUP_CONCAT(DISTINCT station_name)
            FROM downloads
            WHERE user_id = ?
            """,
            (target_user_id,)
        ) as cur:
            row = await cur.fetchone()

    if not row or row[0] == 0:
        return f"ℹ️ اطلاعاتی برای user_id `{target_user_id}` پیدا نشد."

    total_downloads = row[0]
    stations_csv = row[1] or ""
    stations = stations_csv.split(",") if stations_csv else []

    text = (
        f"👤 *User ID:* `{target_user_id}`\n"
        f"⬇️ *تعداد کل دانلودها:* {total_downloads}\n\n"
        f"📡 *ایستگاه‌ها:*"
    )
    for s in stations:
        text += f"\n• {s}"
    return text

async def users_count_text() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(DISTINCT user_id) FROM downloads") as cur:
            row = await cur.fetchone()
    return f"👥 تعداد کل کاربران:\n{row[0] if row else 0}"

# ---------- DATA PRELOAD / CACHE ----------
def _build_region_station_cache_sync() -> Tuple[List[str], Dict[str, List[str]]]:
    """
    این تابع sync است و با asyncio.to_thread صدا زده می‌شود.
    خروجی: REGIONS و STATIONS_BY_REGION
    """
    # فقط دو ستون لازم را می‌گیریم و unique می‌کنیم (خیلی سریع‌تر از فیلترهای متعدد)
    pairs = (
        LF.select([pl.col("region_name"), pl.col("station_name")])
          .unique()
          .collect(streaming=True)
    )

    regions = sorted(pairs["region_name"].unique().to_list())

    stations_by_region: Dict[str, List[str]] = {}
    # group_by روی دیتای کوچک‌تر (unique شده) انجام می‌شود
    gb = pairs.group_by("region_name").agg(pl.col("station_name").unique().sort())
    for r, st_list in zip(gb["region_name"].to_list(), gb["station_name"].to_list()):
        stations_by_region[str(r)] = list(st_list)

    return regions, stations_by_region

async def preload_cache():
    global REGIONS, STATIONS_BY_REGION
    REGIONS, STATIONS_BY_REGION = await asyncio.to_thread(_build_region_station_cache_sync)

# ---------- POLARS HELPERS ----------
def _get_date_range_sync(region_name: str, station_name: str) -> Tuple[Optional[str], Optional[str]]:
    agg = (
        LF.filter(
            (pl.col("station_name") == station_name) &
            (pl.col("region_name") == region_name)
        )
        .select([
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date")
        ])
        .collect(streaming=True)
    )
    if agg.height == 0:
        return None, None
    return str(agg["min_date"][0]), str(agg["max_date"][0])

def _build_station_csv_bytes_sync(region: str, station: str) -> bytes:
    # دیتای ایستگاه را collect می‌کنیم و CSV را در حافظه می‌سازیم
    station_df = (
        LF.filter(
            (pl.col("station_name") == station) &
            (pl.col("region_name") == region)
        )
        .sort("date")
        .collect(streaming=True)
    )

    buf = io.BytesIO()
    station_df.write_csv(buf)  # polars می‌تواند در BytesIO بنویسد
    return buf.getvalue()

# ---------- UI (KEYBOARD) ----------
def build_keyboard(options: List[str], callback_prefix: str, page: int = 0) -> InlineKeyboardMarkup:
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_items = options[start:end]

    rows = []
    row = []
    for i, opt in enumerate(page_items, 1):
        row.append(InlineKeyboardButton(text=opt, callback_data=f"{callback_prefix}|{opt}"))
        if i % BUTTONS_PER_ROW == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    total_pages = math.ceil(len(options) / PAGE_SIZE) if options else 1
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"{callback_prefix}_page|{page-1}"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"{callback_prefix}_page|{page+1}"))
        rows.append(nav)

    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------- BOT ----------
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name or "User"

    kb = build_keyboard(REGIONS, "region", page=0)
    rows = kb.inline_keyboard

    if str(user_id) == ADMIN_ID:
        rows.append([InlineKeyboardButton(text="📊 Admin Report", callback_data="admin_report")])

    await message.answer(f"👋 Welcome {username}!\nPlease select a province:", reply_markup=kb)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ *Help & Usage Guide*\n\n"
        "1️⃣ Use /start to begin.\n"
        "2️⃣ Select a province, then choose a synoptic station.\n"
        "3️⃣ Download the available data (CSV + PDF).\n\n"
        "⚠️ Limit: One station per day per user.\n"
        "📌 This bot is for academic and research purposes only.",
        parse_mode="Markdown"
    )

@dp.message(Command("report"))
async def cmd_report(message: Message):
    if str(message.from_user.id) != ADMIN_ID:
        await message.answer("⛔ You are not authorized to use this command.")
        return
    text = await daily_report_text()
    await message.answer(text)

@dp.message(Command("user"))
async def cmd_user(message: Message):
    if str(message.from_user.id) != ADMIN_ID:
        await message.answer("⛔ You are not authorized to use this command.")
        return

    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("❌ فرمت صحیح:\n/user user_id\nمثال:\n/user 244146213")
        return

    target_user_id = parts[1]
    text = await user_stats_text(target_user_id)
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("users_count"))
async def cmd_users_count(message: Message):
    if str(message.from_user.id) != ADMIN_ID:
        await message.answer("⛔ You are not authorized to use this command.")
        return
    await message.answer(await users_count_text())

# ---------- CALLBACKS ----------
@dp.callback_query(F.data == "admin_report")
async def cb_admin_report(call: CallbackQuery):
    if str(call.from_user.id) != ADMIN_ID:
        await call.answer("⛔ Not authorized", show_alert=True)
        return
    text = await daily_report_text()
    await call.message.answer(text)
    await call.answer()

@dp.callback_query(F.data.contains("_page|"))
async def cb_pagination(call: CallbackQuery):
    prefix, page_str = call.data.split("_page|", 1)
    page = int(page_str)

    user_id = call.from_user.id

    if prefix.startswith("region"):
        kb = build_keyboard(REGIONS, "region", page=page)
        if str(user_id) == ADMIN_ID:
            kb.inline_keyboard.append([InlineKeyboardButton(text="📊 Admin Report", callback_data="admin_report")])
        await call.message.edit_reply_markup(reply_markup=kb)
        await call.answer()
        return

    if prefix.startswith("station"):
        # prefix شکل: station|<region>
        parts = prefix.split("|", 1)
        if len(parts) == 2:
            region = parts[1]
            stations = STATIONS_BY_REGION.get(region, [])
            kb = build_keyboard(stations, f"station|{region}", page=page)
            kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Back to Provinces", callback_data="back_to_provinces")])
            await call.message.edit_reply_markup(reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "back_to_provinces")
async def cb_back_to_provinces(call: CallbackQuery):
    user_id = call.from_user.id
    kb = build_keyboard(REGIONS, "region", page=0)
    if str(user_id) == ADMIN_ID:
        kb.inline_keyboard.append([InlineKeyboardButton(text="📊 Admin Report", callback_data="admin_report")])
    await call.message.edit_text("🔙 Back to province selection:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data.startswith("region|"))
async def cb_region(call: CallbackQuery):
    # محدودیت دانلود را اینجا چک نمی‌کنیم؛ فقط وقتی کاربر ایستگاه را انتخاب کرد
    region = call.data.split("|", 1)[1]
    stations = STATIONS_BY_REGION.get(region, [])

    kb = build_keyboard(stations, f"station|{region}", page=0)
    kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Back to Provinces", callback_data="back_to_provinces")])

    await call.message.edit_text(
        f"🏞 Selected province: {region}\nPlease select a synoptic station:",
        reply_markup=kb
    )
    await call.answer()

@dp.callback_query(F.data.startswith("station|"))
async def cb_station(call: CallbackQuery):
    user_id = call.from_user.id
    username = call.from_user.username or call.from_user.first_name or "User"

    if not await can_download(user_id):
        await call.answer("❌ You have already downloaded a station today.", show_alert=True)
        return

    # call.data شکل: station|<region>|<station>
    parts = call.data.split("|")
    region = parts[1]
    station = parts[-1]

    # date range
    min_date, max_date = await asyncio.to_thread(_get_date_range_sync, region, station)
    if min_date is None:
        await call.message.answer("No data available for this station.")
        await call.answer()
        return

    await call.message.answer(f"🌡 Selected station: {station}\nData available from {min_date} to {max_date}")

    # CSV bytes (بدون فایل روی دیسک)
    csv_bytes = await asyncio.to_thread(_build_station_csv_bytes_sync, region, station)
    safe_name = f"{region}_{station}_{min_date}_{max_date}.csv"

    csv_file = BufferedInputFile(csv_bytes, filename=safe_name)
    await call.message.answer_document(csv_file)

    # PDF guide
    try:
        with open(PDF_GUIDE_FILE, "rb") as f:
            pdf_bytes = f.read()
        pdf_file = BufferedInputFile(pdf_bytes, filename=os.path.basename(PDF_GUIDE_FILE))
        await call.message.answer_document(pdf_file)
    except FileNotFoundError:
        await call.message.answer("⚠️ Guide PDF not found on server.")

    # log download
    await log_download(user_id, username, station)

    # بازگشت به انتخاب استان
    kb = build_keyboard(REGIONS, "region", page=0)
    if str(user_id) == ADMIN_ID:
        kb.inline_keyboard.append([InlineKeyboardButton(text="📊 Admin Report", callback_data="admin_report")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Back to Provinces", callback_data="back_to_provinces")])

    await call.message.answer("Please select a province again:", reply_markup=kb)
    await call.answer()

# ---------- MAIN ----------
async def main():
    await init_db()
    await preload_cache()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
