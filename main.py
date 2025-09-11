import os
import tempfile
import logging
import sqlite3
import dotenv
import pandas as pd
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from threading import Thread
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton


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

# --- FastAPI ---
app = FastAPI(title="Telegram Meteorological Bot Dashboard")

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
    return not (row and row[0] == today)

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

# --- بات ---
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

# --- FastAPI endpoint برای داشبورد ---
@app.get("/downloads/today")
def downloads_today():
    users = get_today_downloads()
    return JSONResponse(content={"count": len(users), "users": [{"user_id": u[0], "first_name": u[1], "username": u[2]} for u in users]})

# --- اجرای بات در Thread ---
def start_bot():
    while True:
        try:
            logger.info("Bot started...")
            bot.infinity_polling()
        except Exception:
            logger.exception("Bot crashed, restarting in 5 seconds...")
            import time
            time.sleep(5)

Thread(target=start_bot, daemon=True).start()

# --- پیام‌ها و هندلرهای بات ---
@bot.message_handler(commands=["start"])
def send_welcome(message):
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(KeyboardButton("🌦 شروع دانلود داده‌های ایستگاه‌های سینوپتیک"))
    bot.send_message(message.chat.id,
                     "سلام 👋 برای شروع دانلود داده‌ها روی دکمه زیر کلیک کنید.",
                     reply_markup=markup)

@bot.message_handler(commands=["help"])
def send_help(message):
    bot.send_message(message.chat.id,
                     "ℹ️ راهنما: انتخاب استان و ایستگاه و دانلود CSV و PDF.\nمحدودیت: یک دانلود در روز.")

@bot.message_handler(func=lambda msg: msg.text == "🌦 شروع دانلود داده‌های ایستگاه‌های سینوپتیک")
def handle_download_start(message):
    markup = InlineKeyboardMarkup()
    for prov in provinces:
        cb = f"p|{prov[:15]}"
        markup.add(InlineKeyboardButton(prov, callback_data=cb))
    bot.send_message(message.chat.id, "🏙 لطفاً یک استان انتخاب کنید:", reply_markup=markup)

# --- callback انتخاب استان ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("p|"))
def handle_province(call):
    try:
        province = call.data.split("|")[1]
        stations = df[df["region_name"].str.startswith(province)][["station_id", "station_name"]].drop_duplicates()
        markup = InlineKeyboardMarkup()
        for _, row in stations.iterrows():
            cb = f"s|{row['station_id']}"
            markup.add(InlineKeyboardButton(row["station_name"], callback_data=cb))
        bot.edit_message_text(f"📍 استان انتخاب شد: {province}\nحالا ایستگاه را انتخاب کنید:",
                              call.message.chat.id, call.message.message_id,
                              reply_markup=markup)
    except Exception:
        logger.exception("Error in handle_province")
        bot.answer_callback_query(call.id, "⚠️ خطا در انتخاب استان!")

# --- callback انتخاب ایستگاه ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("s|"))
def handle_station(call):
    try:
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
