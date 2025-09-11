import os
import tempfile
import logging
import pandas as pd
import telebot
import sqlite3
import time
from datetime import datetime
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

# --- تنظیمات ---
import dotenv
dotenv.load_dotenv()
DATA_PATH = os.environ.get("DATA_PATH", "Iran_Data.parquet")
PDF_GUIDE_PATH = os.environ.get("PDF_GUIDE_PATH", "Help.pdf")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ADMIN_ID = os.environ.get("ADMIN_ID")
DB_PATH = "downloads.db"


# --- لاگ ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
telebot.logger.setLevel(logging.DEBUG)

# --- بات ---
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

# --- دیتاست ---
logger.info("Loading dataset...")
df = pd.read_parquet(DATA_PATH)
df["station_id"] = df["station_id"].astype(str)
df["region_name"] = df["region_name"].astype(str)
logger.info("Dataset loaded")

provinces = sorted(df["region_name"].dropna().unique())

# --- دیتابیس ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            username TEXT,
            last_download TEXT
        )
    """)
    conn.commit()
    conn.close()

def can_download(user_id: int) -> bool:
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT last_download FROM downloads WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row and row[0] == today:
        return False
    return True

def register_download(user_id: int, first_name: str, username: str):
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO downloads (user_id, first_name, username, last_download)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET last_download=excluded.last_download
    """, (user_id, first_name, username, today))
    conn.commit()
    conn.close()

def get_today_downloads():
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id, first_name, username FROM downloads WHERE last_download=?", (today,))
    users = cur.fetchall()
    conn.close()
    return users

init_db()

# --- /start ---
@bot.message_handler(commands=["start"])
def send_welcome(message):
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(KeyboardButton("🌦 شروع دانلود داده‌های ایستگاه‌های سینوپتیک"))
    bot.send_message(message.chat.id,
                     "سلام 👋\nبرای شروع دانلود داده‌های هواشناسی روی دکمه زیر کلیک کنید.\nبرای راهنما دستور /help را بزنید.",
                     reply_markup=markup)

# --- /help ---
@bot.message_handler(commands=["help"])
def send_help(message):
    bot.send_message(message.chat.id,
                     "ℹ️ راهنما:\n"
                     "۱. دکمه «🌦 شروع دانلود...» را بزنید.\n"
                     "۲. استان و ایستگاه موردنظر را انتخاب کنید.\n"
                     "۳. فایل CSV و PDF راهنما برای شما ارسال می‌شود.\n\n"
                     "⚠️ هر کاربر فقط روزی یک بار می‌تواند داده دانلود کند.\n"
                     "دستور /start برای شروع مجدد.")

# --- /admin ---
@bot.message_handler(commands=["admin"])
def admin_panel(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, "⛔ شما ادمین نیستید.")
        return
    users = get_today_downloads()
    if not users:
        bot.send_message(message.chat.id, "📊 امروز هیچ دانلودی ثبت نشده است.")
    else:
        text = "📊 کاربران امروز:\n"
        for uid, first_name, username in users:
            uname = username if username else "N/A"
            text += f"- {first_name} (@{uname}) [{uid}]\n"
        bot.send_message(message.chat.id, text)

# --- دکمه شروع ---
@bot.message_handler(func=lambda msg: msg.text == "🌦 شروع دانلود داده‌های ایستگاه‌های سینوپتیک")
def handle_download_start(message):
    markup = InlineKeyboardMarkup()
    for prov in provinces:
        cb = f"p|{prov[:15]}"  # کوتاه و امن
        markup.add(InlineKeyboardButton(prov, callback_data=cb))
    bot.send_message(message.chat.id, "🏙 لطفاً یک استان انتخاب کنید:", reply_markup=markup)

# --- انتخاب استان ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("p|"))
def handle_province(call):
    try:
        logger.info(f"Province callback: {call.data} from user {call.from_user.id}")
        province = call.data.split("|")[1]
        stations = df[df["region_name"].str.startswith(province)][["station_id", "station_name"]].drop_duplicates()
        markup = InlineKeyboardMarkup()
        for _, row in stations.iterrows():
            sid = str(row["station_id"])
            cb = f"s|{sid}"  # فقط ID
            markup.add(InlineKeyboardButton(row["station_name"], callback_data=cb))
        bot.edit_message_text(f"📍 استان انتخاب شد: *{province}*\nحالا ایستگاه را انتخاب کنید:",
                              call.message.chat.id, call.message.message_id,
                              reply_markup=markup, parse_mode="Markdown")
    except Exception:
        logger.exception("Error in handle_province")
        bot.answer_callback_query(call.id, "⚠️ خطا در انتخاب استان!")

# --- انتخاب ایستگاه ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("s|"))
def handle_station(call):
    try:
        logger.info(f"Station callback: {call.data} from user {call.from_user.id}")
        user_id = call.from_user.id
        first_name = call.from_user.first_name or ""
        username = call.from_user.username or ""

        if not can_download(user_id):
            bot.answer_callback_query(call.id, "⚠️ امروز قبلاً دانلود انجام شده.", show_alert=True)
            return

        station_id = call.data.split("|")[1]
        sdata = df[df["station_id"] == station_id]
        if sdata.empty:
            bot.answer_callback_query(call.id, "⛔ داده‌ای برای این ایستگاه وجود ندارد.")
            return

        province = sdata["region_name"].iloc[0]
        station_name = sdata["station_name"].iloc[0]
        min_date = pd.to_datetime(sdata["date"]).min().strftime("%Y-%m-%d")
        max_date = pd.to_datetime(sdata["date"]).max().strftime("%Y-%m-%d")
        filename = f"{province.replace(' ','_')}_{station_name.replace(' ','_')}_{station_id}.csv"

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            sdata.to_csv(tmp.name, index=False, encoding="utf-8-sig")
            csv_path = tmp.name

        caption = (f"✅ ایستگاه انتخاب شد: *{station_name}*\n"
                   f"📍 استان: {province}\n"
                   f"🆔 کد ایستگاه: {station_id}\n"
                   f"📅 بازه داده‌ها: {min_date} تا {max_date}")

        with open(csv_path, "rb") as fcsv:
            bot.send_document(call.message.chat.id, fcsv, caption=caption,
                              parse_mode="Markdown", visible_file_name=filename, timeout=120)

        if os.path.exists(PDF_GUIDE_PATH):
            with open(PDF_GUIDE_PATH, "rb") as fpdf:
                bot.send_document(call.message.chat.id, fpdf, caption="📘 راهنمای استفاده از داده‌ها")

        register_download(user_id, first_name, username)
        if os.path.exists(csv_path):
            os.remove(csv_path)

    except Exception:
        logger.exception("Error in handle_station")
        bot.answer_callback_query(call.id, "⚠️ خطا در پردازش ایستگاه!")

# --- حلقه امن infinity_polling ---
while True:
    try:
        print("Bot started...")
        bot.infinity_polling()
    except Exception:
        logger.exception("Bot crashed, restarting in 5 seconds...")
        time.sleep(5)
