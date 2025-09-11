import os
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import pandas as pd
import sqlite3
from datetime import datetime, date
import threading
import math

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
conn.commit()

# ---------- LOAD DATA ----------
df = pd.read_parquet(PARQUET_FILE)

# ---------- TELEGRAM BOT ----------
bot = telebot.TeleBot(API_TOKEN)

# ---------- HELPER FUNCTIONS ----------
def can_download(user_id):
    today = date.today().isoformat()
    cursor.execute("SELECT * FROM downloads WHERE user_id=? AND download_date=?", (user_id, today))
    return cursor.fetchone() is None

def log_download(user_id, username, station_name):
    today = date.today().isoformat()
    cursor.execute("INSERT INTO downloads VALUES (?, ?, ?, ?)", (user_id, username, station_name, today))
    conn.commit()

def get_date_range(station_name):
    data = df[df['station_name'] == station_name]
    min_date = pd.to_datetime(data['date'].min()).strftime("%Y-%m-%d")
    max_date = pd.to_datetime(data['date'].max()).strftime("%Y-%m-%d")
    return min_date, max_date

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
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    regions = sorted(df['region_name'].unique())
    markup = build_keyboard(regions, "region")
    if user_id == ADMIN_ID:
        markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
    bot.send_message(message.chat.id, f"👋 Welcome {username}!\nPlease select a province:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    user_id = call.from_user.id
    username = call.from_user.username or call.from_user.first_name

    # ---------- Admin report ----------
    if call.data == "admin_report" and user_id == ADMIN_ID:
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
            regions = sorted(df['region_name'].unique())
            markup = build_keyboard(regions, "region", page)
            if user_id == ADMIN_ID:
                markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        elif prefix.startswith("station"):
            region = prefix.split("|")[1]
            stations = sorted(df[df['region_name'] == region]['station_name'].unique())
            markup = build_keyboard(stations, f"station|{region}", page)
            # اضافه کردن دکمه برگشت به استان‌ها
            markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
        return

    # ---------- Check download limit ----------
    if user_id != ADMIN_ID and not can_download(user_id):
        bot.answer_callback_query(call.id, "❌ You have already downloaded a station today.")
        return

    # ---------- Back button ----------
    if call.data == "back_to_provinces":
        regions = sorted(df['region_name'].unique())
        markup = build_keyboard(regions, "region")
        if user_id == ADMIN_ID:
            markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
        bot.edit_message_text("🔙 Back to province selection:", call.message.chat.id, call.message.message_id, reply_markup=markup)
        return

    # ---------- Region selection ----------
    if call.data.startswith("region|"):
        region = call.data.split("|")[1]
        stations = sorted(df[df['region_name'] == region]['station_name'].unique())
        markup = build_keyboard(stations, f"station|{region}")
        # اضافه کردن دکمه برگشت به استان‌ها
        markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
        bot.edit_message_text(f"🏞 Selected province: {region}\nPlease select a synoptic station:", call.message.chat.id, call.message.message_id, reply_markup=markup)

    # ---------- Station selection ----------
    elif call.data.startswith("station|"):
        parts = call.data.split("|")
        region = parts[1]
        station = parts[-1]
        min_date, max_date = get_date_range(station)
        
        # ایجاد فایل CSV با نام Province_Station_YYYY-MM-DD.csv
        csv_filename = f"{region}_{station}_{min_date}_{max_date}.csv"
        data = df[df['station_name'] == station]
        data.to_csv(csv_filename, index=False)
        
        # ارسال فایل CSV و PDF
        bot.send_message(call.message.chat.id, f"🌡 Selected station: {station}\nData available from {min_date} to {max_date}")
        bot.send_document(call.message.chat.id, open(csv_filename, 'rb'))
        bot.send_document(call.message.chat.id, open(PDF_GUIDE_FILE, 'rb'))
        
        # حذف فایل CSV پس از ارسال
        os.remove(csv_filename)
        
        # ثبت دانلود در دیتابیس
        log_download(user_id, username, station)
        
        # بازگشت به منوی انتخاب استان با دکمه برگشت
        regions = sorted(df['region_name'].unique())
        markup = build_keyboard(regions, "region")
        if user_id == ADMIN_ID:
            markup.add(InlineKeyboardButton("📊 Admin Report", callback_data="admin_report"))
        markup.add(InlineKeyboardButton("🔙 Back to Provinces", callback_data="back_to_provinces"))
        bot.send_message(call.message.chat.id, "Please select a province again:", reply_markup=markup)

# ---------- MAIN ----------
def run_bot():
    bot.infinity_polling()

if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_bot)
    bot_thread.start()



