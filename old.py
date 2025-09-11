"""
Telegram bot to let users download station data (from Iran_Data.parquet) as CSV.
Requires:
 - TELEGRAM_BOT_TOKEN environment variable set to your bot token
 - Iran_Data.parquet in same folder (or edit DATA_PATH)
 - Help.pdf (pre-prepared) in same folder (or edit GUIDE_PATH)
"""

import os
import io
import tempfile
import logging
from typing import List, Tuple

import pandas as pd
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, CallbackQuery

# ---------- Configuration ----------
import dotenv
dotenv.load_dotenv()
DATA_PATH = os.environ.get("DATA_PATH", "Iran_Data.parquet")
GUIDE_PATH = os.environ.get("GUIDE_PATH", "Help.pdf")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Please set TELEGRAM_BOT_TOKEN environment variable.")

# Adjust maximum buttons per row / page
BUTTONS_PER_ROW = 2
MAX_BUTTONS_PER_PAGE = 20

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Load dataset into memory on startup ----------
logger.info("Loading dataset from %s", DATA_PATH)
df = pd.read_parquet(DATA_PATH)
logger.info("Loaded dataset from %s", DATA_PATH)


if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"])
else:
    raise RuntimeError("Iran_Data.parquet must contain a 'date' column")

required_cols = {
    "station_id", "station_name", "region_id", "region_name",
    "lat", "lon", "station_elevation", "date",
}
missing = required_cols - set(df.columns)
if missing:
    raise RuntimeError(f"Missing columns in Iran_Data.parquet: {missing}")


regions_df = df[["region_id", "region_name"]].drop_duplicates().sort_values("region_name")

stations_by_region = {}
for rid, group in df.groupby("region_id"):
    stations_by_region[rid] = (
        group[["station_id", "station_name"]]
        .drop_duplicates()
        .sort_values("station_name")
        .to_dict(orient="records")
    )

region_name_by_id = dict(regions_df[["region_id", "region_name"]].values)
station_name_by_id = dict(df[["station_id", "station_name"]].drop_duplicates().values)


# ---------- Telebot setup ----------
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)


# Helper: chunk list into pages
def paginate(items: List[Tuple[str, str]], page: int, page_size: int) -> Tuple[List[Tuple[str,str]], int]:
    """
    items: list of (id, label)
    page: 0-based
    returns (page_items, total_pages)
    """
    total = len(items)
    total_pages = max(1, (total + page_size - 1) // page_size)
    start = page * page_size
    end = start + page_size
    return items[start:end], total_pages

# Helper: build inline keyboard from list of (callback_data, label), with optional back button & pagination
def build_inline_keyboard(pairs: List[Tuple[str, str]], per_row: int = BUTTONS_PER_ROW,
                          page: int = 0, page_size: int = MAX_BUTTONS_PER_PAGE,
                          back_button: Tuple[str,str] = None) -> InlineKeyboardMarkup:
    page_items, total_pages = paginate(pairs, page, page_size)
    markup = InlineKeyboardMarkup()
    # add rows
    row = []
    for idx, (cbdata, label) in enumerate(page_items):
        row.append(InlineKeyboardButton(label, callback_data=cbdata))
        if len(row) >= per_row:
            markup.row(*row)
            row = []
    if row:
        markup.row(*row)
    # add pagination row if needed
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("« Prev", callback_data=f"paginate|{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data=f"noop"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next »", callback_data=f"paginate|{page+1}"))
        markup.row(*nav_buttons)
    # back button if provided
    if back_button:
        cb, lab = back_button
        markup.row(InlineKeyboardButton(lab, callback_data=cb))
    return markup

# ---------- /start handler ----------
@bot.message_handler(commands=["start", "help"])
def send_welcome(message):
    """
    Show greeting and the list of regions as inline buttons.
    We'll attach page=0 state as part of callback data where needed.
    """
    chat_id = message.chat.id
    text = (
        "سلام 👋\n"
        "به بات دانلود داده‌های هواشناسی خوش آمدید.\n\n"
        "لطفاً یک استان را انتخاب کنید:"
    )
    # build list of (callback_data, label)
    pairs = []
    for rid, rname in regions_df[["region_id", "region_name"]].values:
        cb = f"region|{rid}"
        pairs.append((cb, str(rname)))
    markup = build_inline_keyboard(pairs, page=0, back_button=None)
    bot.send_message(chat_id, text, reply_markup=markup)

# ---------- callback handler ----------
@bot.callback_query_handler(func=lambda call: True)
def on_callback(call: CallbackQuery):
    data = call.data or ""
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    # noop (used for page indicator)
    if data == "noop":
        bot.answer_callback_query(call.id)
        return

    # paginate action - used for generic pagination. We need to know which list to paginate.
    # We'll encode current page requests in a simple way: when user presses paginate, bot will
    # look up the message text to figure out what to show. Simpler approach: we'll embed the context
    # in the message via prefixes when creating the buttons (see below). For this implementation,
    # paginate callbacks are only used within region->station selection flows where the existing
    # callbacks remain the same (region|id or station|id). So a paginate click just edits the same
    # message with a new page of the same set.
    if data.startswith("paginate|"):
        try:
            new_page = int(data.split("|",1)[1])
        except:
            new_page = 0
        # Parse the original message text to determine what set to repopulate
        orig = call.message.text or ""
        # If text contains "لطفاً یک استان" -> paginate regions
        if "لطفاً یک استان" in orig:
            # rebuild region pairs
            pairs = [(f"region|{rid}", str(rname)) for rid,rname in regions_df[["region_id","region_name"]].values]
            markup = build_inline_keyboard(pairs, page=new_page, back_button=None)
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=markup)
            bot.answer_callback_query(call.id)
            return
        # If text contains "انتخاب ایستگاه" -> extract region_id from a hidden marker on message (we might have included it)
        # We'll attempt to find "REGION_ID:" in the message text (we appended it invisibly when building station lists)
        rid_token = None
        for line in orig.splitlines():
            if line.startswith("REGION_ID:"):
                rid_token = line.split(":",1)[1].strip()
                break
        if rid_token:
            rid = rid_token
            station_records = stations_by_region.get(rid, [])
            pairs = [(f"station|{r['station_id']}", r['station_name']) for r in station_records]
            markup = build_inline_keyboard(pairs, page=new_page, back_button=("back|regions", "⬅️ Back"))
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=markup)
            bot.answer_callback_query(call.id)
            return
        # otherwise just ack
        bot.answer_callback_query(call.id, "صفحه‌بندی نشد")
        return

    # Back navigation
    if data == "back|regions":
        text = (
            "لطفاً یک استان را انتخاب کنید:"
        )
        pairs = [(f"region|{rid}", str(rname)) for rid,rname in regions_df[["region_id","region_name"]].values]
        markup = build_inline_keyboard(pairs, page=0)
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
        bot.answer_callback_query(call.id)
        return

    # Region selected
    if data.startswith("region|"):
        _, region_id = data.split("|", 1)
        region_name = region_name_by_id.get(region_id, "—")
        # build station list
        station_records = stations_by_region.get(region_id, [])
        if not station_records:
            bot.answer_callback_query(call.id, f"هیچ ایستگاهی برای استان «{region_name}» وجود ندارد.")
            return
        pairs = [(f"station|{rec['station_id']}", rec['station_name']) for rec in station_records]
        # We'll include a small invisible marker line to help pagination handler determine region context
        text = (
            f"استان منتخب: {region_name}\n\n"
            "لطفاً یک ایستگاه را انتخاب کنید:\n\n"
            f"REGION_ID:{region_id}"  # token used by pagination logic above
        )
        markup = build_inline_keyboard(pairs, page=0, back_button=("back|regions", "« بازگشت به استان‌ها"))
        bot.edit_message_text(chat_id=chat_id, text=text, message_id=message_id, reply_markup=markup)
        bot.answer_callback_query(call.id)
        return

    # Station selected
    if data.startswith("station|"):
        _, station_id = data.split("|",1)
        sname = station_name_by_id.get(station_id, "—")
        # filter df for that station
        sdf = df[df["station_id"] == station_id]
        if sdf.empty:
            bot.answer_callback_query(call.id, "اطلاعاتی برای این ایستگاه موجود نیست.")
            return
        min_date = sdf["date"].min().date()
        max_date = sdf["date"].max().date()
        text = (
            f"ایستگاه انتخاب شده: {sname}\n"
            f"شناسه ایستگاه: {station_id}\n\n"
            f"بازه تاریخ موجود برای این ایستگاه:\n{min_date}  →  {max_date}\n\n"
            "برای دریافت فایل CSV داده‌های این ایستگاه روی دکمه زیر کلیک کنید."
        )
        # send button to download CSV. We'll encode station id in callback.
        markup = InlineKeyboardMarkup()
        markup.row(InlineKeyboardButton("دریافت CSV", callback_data=f"download|{station_id}"))
        markup.row(InlineKeyboardButton("« بازگشت", callback_data="back|regions"))
        bot.edit_message_text(chat_id=chat_id, text=text, message_id=message_id, reply_markup=markup)
        bot.answer_callback_query(call.id)
        return

    # Download requested
    if data.startswith("download|"):
        _, station_id = data.split("|",1)
        sname = station_name_by_id.get(station_id, station_id)
        bot.answer_callback_query(call.id, "در حال آماده‌سازی فایل...")

        try:
            bot.edit_message_text(f"در حال ساخت فایل CSV برای ایستگاه: {sname} ...",
                                  chat_id, message_id)
        except Exception:
            pass  # اگر نتونه پیام رو ویرایش کنه اشکالی نداره

        # فیلتر داده‌ها برای ایستگاه انتخاب‌شده
        sdf = df[df["station_id"] == station_id].sort_values("date")
        region_name = sdf["region_name"].iloc[0] if not sdf.empty else "UnknownRegion"
        if sdf.empty:
            bot.send_message(chat_id, "خطا: هیچ داده‌ای برای این ایستگاه موجود نیست.")
            return

        # ساخت مسیر فایل موقت
        out_name = f"{region_name}_{sname}_{station_id}.csv"
        out_path = os.path.join(tempfile.gettempdir(), out_name)

        # ذخیره CSV
        sdf.to_csv(out_path, index=False)

        # فشرده‌سازی به ZIP
        zip_name = f"{region_name}_{sname}_{station_id}.zip"
        zip_path = os.path.join(tempfile.gettempdir(), zip_name)
        import zipfile
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(out_path, arcname=out_name)

        try:
            # ارسال فایل ZIP
            with open(zip_path, "rb") as f:
                caption = f"داده‌های ایستگاه: {sname} (station_id: {station_id})"
                bot.send_document(chat_id, f, caption=caption, timeout=120)

            # ارسال PDF راهنما (اگر وجود داشت)
            if os.path.exists(GUIDE_PATH):
                with open(GUIDE_PATH, "rb") as pdf_f:
                    bot.send_document(chat_id, pdf_f, caption="راهنمای پارامترها و واحدها", timeout=120)
            else:
                bot.send_message(chat_id, "راهنمای PDF پیدا نشد؛ لطفاً Help.pdf را در سرور قرار دهید.")

        except Exception as e:
            logger.exception("Error while sending files: %s", e)
            bot.send_message(chat_id, f"خطا هنگام ارسال فایل: {e}")
        finally:
            # پاک‌سازی فایل‌های موقت
            for p in (out_path, zip_path):
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass

        return


    # unknown callback
    bot.answer_callback_query(call.id, "عملیات ناشناخته یا منقضی شده است. /start را بزنید.")

# ---------- Start polling ----------
def main():
    logger.info("Bot started, polling...")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)

if __name__ == "__main__":
    main()
