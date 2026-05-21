import os
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import pandas as pd
import polars as pl
import sqlite3
from datetime import datetime, date
import threading
import math
import io
import csv
import time

# ---------- CONFIG ----------
import dotenv
dotenv.load_dotenv()
API_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ADMIN_ID = os.environ.get("ADMIN_ID")
PARQUET_FILE = os.environ.get("DATA_PATH", "Iran_Data.parquet")
PDF_GUIDE_FILE = os.environ.get("GUIDE_PATH", "Help.pdf")
DB_PATH = "users.db"
BUTTONS_PER_ROW = 2
PAGE_SIZE = 16
REQUIRED_CHANNELS = ["@HydroCodeChannel", "@weri_fum"]
# ----------------------------

# ---------- DATABASE SETUP ----------
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS downloads (
    user_id INTEGER,
    username TEXT,
    station_name TEXT,
    download_date TEXT
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    joined_at TEXT
)
""")
conn.commit()

# ---------- LOAD DATA ----------
# df = pd.read_parquet(PARQUET_FILE)
# df = pl.read_parquet(PARQUET_FILE)
df = pl.scan_parquet(PARQUET_FILE)

def load_regions():
    # returns a python list of region names (sorted)
    regions_df = df.select(pl.col("region_name")).unique().collect()
    return sorted(regions_df["region_name"].to_list())

def load_stations_for(region):
    # returns python list of station names for given region
    st_df = df.filter(pl.col("region_name") == region).select(pl.col("station_name")).unique().collect()
    return sorted(st_df["station_name"].to_list())

try:
    REGIONS = load_regions()
except Exception as e:
    print("Error loading regions:", e)
    REGIONS = []


# ---------- TELEGRAM BOT ----------
bot = telebot.TeleBot(API_TOKEN)

# ---------- HELPER FUNCTIONS ----------
EXCLUDE_IDs = [str(ADMIN_ID), str(107479525)]

def register_user(user):
    cursor.execute(
        """
        INSERT INTO users (user_id, username, first_name, joined_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name
        """,
        (
            user.id,
            user.username,
            user.first_name,
            datetime.utcnow().isoformat()
        )
    )
    conn.commit()

def is_member_of_required_channels(user_id):
    for channel in REQUIRED_CHANNELS:
        try:
            member = bot.get_chat_member(channel, user_id)
            if member.status in ["left", "kicked"]:
                return False
        except Exception:
            return False
    return True

def ensure_membership(message):
    if str(message.from_user.id) == str(ADMIN_ID):
        return True
    if is_member_of_required_channels(message.from_user.id):
        return True
    channels_text = "\n".join([f"• {c}" for c in REQUIRED_CHANNELS])
    bot.reply_to(
        message,
        f"⛔ برای استفاده از ربات ابتدا باید در این کانال‌ها عضو شوید:\n{channels_text}\n\nپس از عضویت دوباره /start را بزنید."
    )
    return False
def can_download_daily(user_id):
    if str(user_id) in EXCLUDE_IDs:
        return True
    today = date.today().isoformat()
    cursor.execute("SELECT * FROM downloads WHERE user_id=? AND download_date=?", (user_id, today))
    return cursor.fetchone() is None

def can_download_monthly(user_id):
    if str(user_id) in EXCLUDE_IDs:
        return True
    today = date.today()
    month_start = today.replace(day=1).isoformat()

    cursor.execute("""
        SELECT COUNT(*) FROM downloads
        WHERE user_id = ?
        AND download_date >= ?
    """, (user_id, month_start))

    count = cursor.fetchone()[0]

    return count < 10

def log_download(user_id, username, station_name):
    today = date.today().isoformat()
    cursor.execute("INSERT INTO downloads VALUES (?, ?, ?, ?)", (user_id, username, station_name, today))
    conn.commit()

def get_date_range(region_name, station_name):
    # data = df.filter(df["station_name"] == station_name)
    # min_date = pd.to_datetime(data['date'].min()).strftime("%Y-%m-%d")
    # max_date = pd.to_datetime(data['date'].max()).strftime("%Y-%m-%d")
    # return min_date, max_date
    # aggregate min & max date on the lazyframe (efficient)
    agg = df.filter(
        (pl.col("station_name") == station_name) &
        (pl.col("region_name") == region_name)
    ).select([
        pl.col("date").min().alias("min_date"),
        pl.col("date").max().alias("max_date")
    ]).collect()
    if agg.height == 0:
        return None, None
    min_d = agg["min_date"][0]
    max_d = agg["max_date"][0]
    return str(min_d), str(max_d)

def build_keyboard(options, callback_prefix, page=0):
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

    # دکمه‌های پیمایش
    total_pages = math.ceil(len(options) / PAGE_SIZE)
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{callback_prefix}_page|{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"{callback_prefix}_page|{page+1}"))
        markup.row(*nav_row)

    return markup

# ---------- BOT HANDLERS ----------
@bot.message_handler(commands=['start'])
def start(message):
    register_user(message.from_user)
    if not ensure_membership(message):
        return
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    # regions = sorted(df['region_name'].unique())
    markup = build_keyboard(REGIONS, "region")
    if str(user_id) == str(ADMIN_ID):
        markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
    bot.send_message(message.chat.id, f"👋 Welcome {username}!\nPlease select a province:", reply_markup=markup)


@bot.message_handler(commands=['help'])
def help_command(message):
    register_user(message.from_user)
    if not ensure_membership(message):
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
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    
    c.execute("""
        SELECT user_id, username, station_name, download_date
        FROM downloads
        WHERE download_date = date('now', 'localtime')
    """)
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        bot.send_message(message.chat.id, "📭 No downloads recorded today.")
        return
    
    # ساخت متن گزارش
    report_lines = ["📊 *Daily Download Report*"]
    for r in rows:
        uid, uname, station, ddate = r
        uname_display = uname if uname else "N/A"
        report_lines.append(f"- 👤 {uname_display} (ID: {uid})\n  📍{station} | {ddate}")
    
    report_text = "\n\n".join(report_lines)
    print(report_text)
    
    bot.send_message(message.chat.id, report_text, parse_mode="HTML")




@bot.message_handler(commands=['user'])
def user_info(message):
    user_id = message.from_user.id
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            bot.reply_to(
                message,
                "❌ فرمت صحیح:\n/user user_id\nمثال:\n/user 244146213"
            )
            return

        target_user_id = parts[1]

        conn = sqlite3.connect("users.db")
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                COUNT(*) AS total_downloads,
                GROUP_CONCAT(DISTINCT station_name)
            FROM downloads
            WHERE user_id = ?
        """, (target_user_id,))

        row = cur.fetchone()
        conn.close()

        if row is None or row[0] == 0:
            bot.reply_to(
                message,
                f"ℹ️ اطلاعاتی برای user_id `{target_user_id}` پیدا نشد.",
                parse_mode="Markdown"
            )
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

    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]

    bot.reply_to(
        message,
        f"👥 تعداد کل کاربران:\n{count}"
    )


@bot.message_handler(commands=['send'])
def send_to_all(message):
    user_id = message.from_user.id
    if str(user_id) != str(ADMIN_ID):
        bot.reply_to(message, "⛔ You are not authorized to use this command.")
        return

    if not message.reply_to_message:
        bot.reply_to(message, "❌ این دستور باید روی یک پیام ریپلای شود.")
        return

    source_chat_id = message.reply_to_message.chat.id
    source_message_id = message.reply_to_message.message_id

    cursor.execute("SELECT user_id FROM users")
    user_rows = cursor.fetchall()

    sent_count = 0
    fail_count = 0

    for (target_user_id,) in user_rows:
        try:
            bot.copy_message(target_user_id, source_chat_id, source_message_id)
            sent_count += 1
        except Exception:
            fail_count += 1

    bot.reply_to(
        message,
        f"📣 ارسال همگانی انجام شد.\n✅ موفق: {sent_count}\n❌ ناموفق: {fail_count}"
    )





@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    register_user(call.from_user)

    if str(call.from_user.id) != str(ADMIN_ID) and not is_member_of_required_channels(call.from_user.id):
        channels_text = "\n".join([f"• {c}" for c in REQUIRED_CHANNELS])
        bot.answer_callback_query(call.id, "ابتدا در کانال‌های اجباری عضو شوید.", show_alert=True)
        bot.send_message(
            call.message.chat.id,
            f"⛔ برای ادامه استفاده از ربات ابتدا در این کانال‌ها عضو شوید:\n{channels_text}"
        )
        return

    user_id = call.from_user.id
    username = call.from_user.username or call.from_user.first_name

    # ---------- Admin report ----------
    if call.data == "admin_report" and str(user_id) == str(ADMIN_ID):
        today = date.today().isoformat()
        cursor.execute("SELECT username, station_name FROM downloads WHERE download_date=?", (today,))
        rows = cursor.fetchall()
        report = "\n".join([f"{u} -> {s}" for u, s in rows]) if rows else "No downloads today."
        bot.send_message(call.message.chat.id, f"📊 Today's downloads:\n{report}")
        return

    # ---------- Pagination ----------
    if "_page|" in call.data:
        prefix, page = call.data.split("_page|")
        page = int(page)
        if prefix.startswith("region"):
            # regions = sorted(df['region_name'].unique())
            markup = build_keyboard(REGIONS, "region", page)
            if str(user_id) == str(ADMIN_ID):
                markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        elif prefix.startswith("station"):
        #     region = prefix.split("|")[1]
        #     # stations = sorted(df[df['region_name'] == region]['station_name'].unique())
        #     stations = sorted(df.filter(df["region_name"] == region)['station_name'].unique())
        #     markup = build_keyboard(stations, f"station|{region}", page)
        #     # اضافه کردن دکمه برگشت به استان‌ها
        #     markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
        #     bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        # return
            parts = prefix.split("|", 1)
            if len(parts) == 2:
                region = parts[1]
                stations = load_stations_for(region)
                markup = build_keyboard(stations, f"station|{region}", page)
                markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        return

    # ---------- Check download limit ----------
    if not can_download_daily(user_id) or not can_download_monthly(user_id):
        bot.answer_callback_query(call.id, "❌ You have already downloaded a station today or reached the 10-stations-per-month limit.")
        return

    # ---------- Back button ----------
    if call.data == "back_to_provinces":
        # regions = sorted(df['region_name'].unique())
        markup = build_keyboard(REGIONS, "region")
        if str(user_id) == str(ADMIN_ID):
            markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
        bot.edit_message_text("🔙 Back to province selection:", call.message.chat.id, call.message.message_id, reply_markup=markup)
        return

    # ---------- Region selection ----------
    if call.data.startswith("region|"):
        region = call.data.split("|")[1]
        # stations = sorted(df[df['region_name'] == region]['station_name'].unique())
        # stations = sorted(df.filter(df["region_name"] == region)['station_name'].unique())
        stations = load_stations_for(region)
        markup = build_keyboard(stations, f"station|{region}")
        # اضافه کردن دکمه برگشت به استان‌ها
        markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
        bot.edit_message_text(f"🏞 Selected province: {region}\nPlease select a synoptic station:", call.message.chat.id, call.message.message_id, reply_markup=markup)

    # ---------- Station selection ----------
    elif call.data.startswith("station|"):
        parts = call.data.split("|")
        region = parts[1]
        station = parts[-1]
        min_date, max_date = get_date_range(region, station)
        
        if min_date is None:
            bot.send_message(call.message.chat.id, "No data available for this station.")
            return
        # ایجاد فایل CSV با نام Province_Station_YYYY-MM-DD.csv
        # csv_filename = f"{region}_{station}_{min_date}_{max_date}.csv"
        # data = df[df['station_name'] == station]
        station_df = df.filter(
            (pl.col("station_name") == station) &
            (pl.col("region_name") == region)
        ).sort("date").collect(engine="streaming")
        csv_filename = f"{region}_{station}_{min_date}_{max_date}.csv"
        station_df.write_csv(csv_filename)
        # data = df.filter(df["station_name"] == station)
        # data.sort_values(by='date', inplace=True)
        # data = data.sort(by='date')
        # data.to_csv(csv_filename, index=False)
        # buffer = io.StringIO()
        # station_data.write_csv(buffer)
        # buffer.seek(0)
        
        # data.write_csv(csv_filename)
        
        # ارسال فایل CSV و PDF
        bot.send_message(call.message.chat.id, f"🌡 Selected station: {station}\nData available from {min_date} to {max_date}")
        bot.send_message(7690029281, f"- 👤 {username} (ID: {user_id})\n  📍{station}\n\n")
        # bot.send_document(call.message.chat.id, open(csv_filename, 'rb'))
        # bot.send_document(call.message.chat.id, ("data.csv", buffer.getvalue().encode("utf-8")))
        # bot.send_document(call.message.chat.id, open(PDF_GUIDE_FILE, 'rb'))
        with open(csv_filename, "rb") as f:
            bot.send_document(call.message.chat.id, f)
        with open(PDF_GUIDE_FILE, "rb") as f2:
            bot.send_document(call.message.chat.id, f2)
        
        # حذف فایل CSV پس از ارسال
        os.remove(csv_filename)
        
        # ثبت دانلود در دیتابیس
        log_download(user_id, username, station)
        
        # بازگشت به منوی انتخاب استان با دکمه برگشت
        # regions = sorted(df['region_name'].unique())
        markup = build_keyboard(REGIONS, "region")
        if str(user_id) == str(ADMIN_ID):
            markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
        markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
        bot.send_message(call.message.chat.id, "Please select a province again:", reply_markup=markup)

# ---------- MAIN ----------
# def run_bot():
#     bot.infinity_polling()
    

def run_bot():
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"[Bot Error] {e}. Restarting in 5s...")
            time.sleep(5)


# if __name__ == "__main__":
#     bot_thread = threading.Thread(target=run_bot)
#     bot_thread.start()

if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    while True:
        time.sleep(1)
