import asyncio
import logging
import os
import random
import re
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from zoneinfo import ZoneInfo

import aiosqlite
import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BusinessConnection, BusinessMessagesDeleted,
    FSInputFile, BufferedInputFile, LabeledPrice, PreCheckoutQuery,
)
from dotenv import load_dotenv

try:
    import yt_dlp
    YTDLP_OK = True
except ImportError:
    YTDLP_OK = False

# ============== CONFIG ==============
load_dotenv()
api_token = os.getenv("API_TOKEN", "").strip()
try:
    owner_id = int(os.getenv("OWNER_ID", "0"))
except ValueError:
    owner_id = 0

BOT_NAME = "BobMod"

# ============== СИСТЕМНЫЙ ПРОМПТ ==============
SYSTEM_PROMPT = (
    "Ты BobMod — неофициальный ассистент в бизнес-чате. "
    "Отвечай на русском, просто, по-дружески, без официоза и занудства. "
    "Можно использовать сленг и эмодзи, но без перебора. "
    "Кратко — 2-4 предложения. Не пиши 'конечно!' и 'отличный вопрос!'. "
    "Если не знаешь — скажи прямо, не выдумывай."
)
# ============================================

DB_PATH = "bot.db"
KEEP_PER_CHAT = 100
MUTE_NOTICE_SECONDS = 4
FREE_DAILY_LIMIT = 10
PREMIUM_DAILY_LIMIT = 50
PREMIUM_PRICE_STARS = 50
PREMIUM_DAYS = 30
DOWNLOAD_MAX_MB = 45
ANIM_MAX_CHARS = 150
ANIM_DELAY = 0.25

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bobmod")
router = Router()

# ---------------- State ----------------
guess_games = {}
roulette_games = {}
ttt_games = {}
pending_inputs = {}
name_update_lock = asyncio.Lock()

# ---------------- AI providers ----------------
AI_PROVIDERS = [
    {
        "name": "groq",
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "key_env": "GROQ_API_KEY",
        "model": "openai/gpt-oss-120b",       # из твоего вывода check_keys
    },
    {
        "name": "groq-20b",
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "key_env": "GROQ_API_KEY",
        "model": "openai/gpt-oss-20b",        # лёгкая версия, тоже должна быть
    },
    {
        "name": "openrouter",
        "url": "https://openrouter.ai/api/v1/chat/completions",
        "key_env": "OPENROUTER_API_KEY",
        "model": "openai/gpt-oss-120b:free",  # бесплатная версия той же модели
    },
    {
        "name": "mistral",
        "url": "https://api.mistral.ai/v1/chat/completions",
        "key_env": "MISTRAL_API_KEY",
        "model": "mistral-small-latest",      # была в твоём выводе
    },
]


async def ask_ai(prompt: str) -> str:
    last_err = None
    errors_log = []

    for p in AI_PROVIDERS:
        key = os.getenv(p["key_env"], "").strip()
        if not key:
            errors_log.append(f"{p['name']}: нет ключа")
            continue

        for attempt in (1, 2):
            try:
                if p["name"] == "mistral":
                    await asyncio.sleep(1.1)

                async with httpx.AsyncClient(timeout=60) as client:
                    r = await client.post(
                        p["url"],
                        headers={
                            "Authorization": f"Bearer {key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": p["model"],
                            "messages": [
                                {"role": "system", "content": SYSTEM_PROMPT},
                                {"role": "user", "content": prompt},
                            ],
                            "max_tokens": 1024,
                            "temperature": 0.8,
                        },
                    )
                if r.status_code == 200:
                    log.info("AI ответил через %s", p["name"])
                    return r.json()["choices"][0]["message"]["content"].strip()

                if r.status_code == 429 and attempt == 1:
                    log.warning("%s: 429, ждём 3 сек", p["name"])
                    await asyncio.sleep(3)
                    continue

                last_err = f"{p['name']}: HTTP {r.status_code} {r.text[:120]}"
                errors_log.append(last_err)
                break
            except Exception as e:
                last_err = f"{p['name']}: {e}"
                errors_log.append(last_err)
                break

    log.error("Все ИИ упали: %s", " | ".join(errors_log))
    raise RuntimeError("ИИ недоступен. " + " | ".join(errors_log)[:300])


# ---------------- Utilities ----------------
def utcnow():
    return datetime.now(timezone.utc)


def local_now(tz_name="UTC"):
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        return datetime.now(timezone.utc)


def esc(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


URL_RE = re.compile(r"https?://[^\s]+")


# ---------------- DB ----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            user_id INTEGER,
            user_name TEXT,
            text TEXT,
            content_type TEXT,
            media_file_id TEXT,
            date TEXT,
            is_outgoing INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_messages_chat_mid ON messages(chat_id, message_id);

        CREATE TABLE IF NOT EXISTS settings (
            user_id INTEGER PRIMARY KEY,
            answer_first_enabled INTEGER DEFAULT 0,
            answer_first_text TEXT DEFAULT '',
            time_in_nick_enabled INTEGER DEFAULT 0,
            timezone TEXT DEFAULT 'UTC',
            off_hours_enabled INTEGER DEFAULT 0,
            off_hours_start TEXT DEFAULT '22:00',
            off_hours_end TEXT DEFAULT '08:00',
            off_hours_text TEXT DEFAULT 'Сплю, отвечу утром',
            forward_to_owner_enabled INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS blacklist (user_id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS mutes (
            chat_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            mute_until TEXT NOT NULL,
            PRIMARY KEY(chat_id, target_user_id)
        );
        CREATE TABLE IF NOT EXISTS contacts (
            chat_id INTEGER PRIMARY KEY,
            first_seen TEXT NOT NULL,
            message_count INTEGER DEFAULT 0,
            user_id INTEGER,
            user_name TEXT,
            username TEXT
        );
        CREATE TABLE IF NOT EXISTS notes (
            chat_id INTEGER PRIMARY KEY,
            note TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS user_limits (
            user_id INTEGER PRIMARY KEY,
            date TEXT NOT NULL,
            requests_count INTEGER DEFAULT 0,
            is_premium INTEGER DEFAULT 0,
            premium_until TEXT
        );
        """)
        await db.commit()


async def get_settings(user=None):
    user = user if user is not None else owner_id
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM settings WHERE user_id=?", (user,))
        row = await cur.fetchone()
        if row:
            return dict(row)
        await db.execute("INSERT OR IGNORE INTO settings(user_id) VALUES(?)", (user,))
        await db.commit()
        cur = await db.execute("SELECT * FROM settings WHERE user_id=?", (user,))
        return dict(await cur.fetchone())


async def set_setting(key, value, user=None):
    user = user if user is not None else owner_id
    allowed = {
        "answer_first_enabled", "answer_first_text", "time_in_nick_enabled", "timezone",
        "off_hours_enabled", "off_hours_start", "off_hours_end", "off_hours_text",
        "forward_to_owner_enabled",
    }
    if key not in allowed:
        return
    await get_settings(user)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE settings SET {key}=? WHERE user_id=?", (value, user))
        await db.commit()


async def is_blacklisted(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM blacklist WHERE user_id=?", (user_id,))
        return await cur.fetchone() is not None


async def add_blacklist(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO blacklist(user_id) VALUES(?)", (user_id,))
        await db.commit()


async def remove_blacklist(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM blacklist WHERE user_id=?", (user_id,))
        await db.commit()


async def get_note(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT note, updated_at FROM notes WHERE chat_id=?", (chat_id,))
        return await cur.fetchone()


async def set_note(chat_id, text):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO notes(chat_id,note,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(chat_id) DO UPDATE SET note=excluded.note, updated_at=excluded.updated_at",
            (chat_id, text, utcnow().isoformat())
        )
        await db.commit()


# ---------------- Limits ----------------
async def check_limit(user_id):
    today = utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM user_limits WHERE user_id=?", (user_id,))
        row = await cur.fetchone()

        if not row or row["date"] != today:
            await db.execute(
                "INSERT OR REPLACE INTO user_limits(user_id, date, requests_count, is_premium, premium_until) "
                "VALUES(?,?,0,?,?)",
                (user_id, today,
                 row["is_premium"] if row else 0,
                 row["premium_until"] if row else None)
            )
            await db.commit()
            used = 0
            is_premium = bool(row["is_premium"]) if row else False
            premium_until = row["premium_until"] if row else None
        else:
            used = row["requests_count"]
            is_premium = bool(row["is_premium"])
            premium_until = row["premium_until"]

        if is_premium and premium_until:
            try:
                if datetime.fromisoformat(premium_until) < utcnow():
                    await db.execute(
                        "UPDATE user_limits SET is_premium=0, premium_until=NULL WHERE user_id=?",
                        (user_id,)
                    )
                    await db.commit()
                    is_premium = False
            except Exception:
                pass

        limit = PREMIUM_DAILY_LIMIT if is_premium else FREE_DAILY_LIMIT
        return used < limit, used, limit, is_premium


async def increment_usage(user_id):
    today = utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE user_limits SET requests_count = requests_count + 1 WHERE user_id=? AND date=?",
            (user_id, today)
        )
        await db.commit()


async def activate_premium(user_id, days=PREMIUM_DAYS):
    until = (utcnow() + timedelta(days=days)).isoformat()
    today = utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO user_limits(user_id, date, requests_count, is_premium, premium_until) "
            "VALUES(?,?,0,1,?)",
            (user_id, today, until)
        )
        await db.commit()


# ---------------- Message payload ----------------
def message_payload(message: Message):
    text = message.text or message.caption or ""
    ctype, file_id = "text", None
    if message.photo:
        ctype, file_id = "photo", message.photo[-1].file_id
    elif message.video:
        ctype, file_id = "video", message.video.file_id
    elif message.animation:
        ctype, file_id = "animation", message.animation.file_id
    elif message.document:
        ctype, file_id = "document", message.document.file_id
    elif message.voice:
        ctype, file_id = "voice", message.voice.file_id
    elif message.audio:
        ctype, file_id = "audio", message.audio.file_id
    elif message.video_note:
        ctype, file_id = "video_note", message.video_note.file_id
    elif message.sticker:
        ctype, file_id = "sticker", message.sticker.file_id
    elif message.location:
        ctype = "location"
    elif message.contact:
        ctype = "contact"
    return text, ctype, file_id


async def save_business_message(message: Message):
    chat_id = message.chat.id
    sender = message.from_user
    uid = sender.id if sender else None
    outgoing = bool(sender and uid == owner_id)
    if uid is not None and await is_blacklisted(uid):
        return
    text, ctype, file_id = message_payload(message)
    username = sender.username if sender else None
    display = (sender.full_name if sender else message.chat.full_name if message.chat else "Unknown")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO messages(chat_id,message_id,user_id,user_name,text,content_type,
                                     media_file_id,date,is_outgoing)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (chat_id, message.message_id, uid, display, text, ctype, file_id,
             message.date.isoformat() if message.date else utcnow().isoformat(), int(outgoing))
        )
        await db.execute(
            """DELETE FROM messages WHERE chat_id=? AND id NOT IN
               (SELECT id FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT ?)""",
            (chat_id, chat_id, KEEP_PER_CHAT)
        )
        await db.execute(
            """INSERT INTO contacts(chat_id,first_seen,message_count,user_id,user_name,username)
               VALUES(?,?,1,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET message_count=message_count+1,
               user_id=excluded.user_id,user_name=excluded.user_name,username=excluded.username""",
            (chat_id, utcnow().isoformat(), uid, display, username)
        )
        await db.commit()


async def find_message(chat_id, message_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM messages WHERE chat_id=? AND message_id=? ORDER BY id DESC LIMIT 1",
            (chat_id, message_id)
        )
        row = await cur.fetchone()
        return dict(row) if row else None


# ---------------- Menus ----------------
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings")],
        [InlineKeyboardButton(text="🤖 Bob AI", callback_data="menu:ai")],
        [InlineKeyboardButton(text="📖 Что умею", callback_data="menu:help")],
    ])


def back_button(target="menu:main"):
    return [InlineKeyboardButton(text="⬅️ Назад", callback_data=target)]


# ============================================================
# PRIVATE HANDLERS (owner)
# ============================================================

@router.message(Command("start"))
async def cmd_start(message: Message):
    await get_settings()
    await message.answer(
        f"👋 <b>Здарова! Я {BOT_NAME}.</b>\n\n"
        "Работаю в твоём Telegram Business.\n"
        "• отвечаю клиентам пока ты занят\n"
        "• сохраняю удалённое\n"
        "• помню всё по чатам\n"
        "• болтаю через <code>.ai</code>\n\n"
        "<i>Подключи меня: Настройки → Telegram Business → Чат-автоматизация</i>",
        reply_markup=main_menu()
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "<b>📖 Что умею</b>\n\n"
        "<b>В бизнес-чатах (с точки):</b>\n"
        "<code>.ai вопрос</code> — спросить Bob AI\n"
        "<code>.save</code> (ответом на ссылку) — скачать видео\n"
        "<code>.anim текст</code> — прикольная анимация\n"
        "<code>.qr текст</code> — QR-код\n"
        "<code>.whois</code> — инфа о собеседнике\n"
        "<code>.typing</code> — делаю вид что печатаю\n"
        "<code>.mute 60</code> / <code>.unmute</code> — мут\n"
        "<code>.muted</code> — список мутов\n"
        "<code>.note текст</code> — заметка о чате\n"
        "<code>.id</code> / <code>.ping</code>\n"
        "<code>.game</code> — игрушки\n\n"
        "В личке: /start /settings /stats",
        reply_markup=main_menu()
    )


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    if message.from_user and message.from_user.id != owner_id:
        await message.answer("⛔ Только для хозяина.")
        return
    s = await get_settings()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💬 Первый ответ: {'✅' if s['answer_first_enabled'] else '❌'}",
                              callback_data="toggle:answer_first_enabled")],
        [InlineKeyboardButton(text="✏️ Текст первого ответа", callback_data="prompt:answer_first_text")],
        [InlineKeyboardButton(text=f"🌙 Автоответ: {'✅' if s['off_hours_enabled'] else '❌'}",
                              callback_data="toggle:off_hours_enabled")],
        [InlineKeyboardButton(text=f"🕒 Часы: {s['off_hours_start']}–{s['off_hours_end']}",
                              callback_data="prompt:off_hours")],
        [InlineKeyboardButton(text="✏️ Текст автоответа", callback_data="prompt:off_hours_text")],
        [InlineKeyboardButton(text=f"📥 Пересылка мне: {'✅' if s['forward_to_owner_enabled'] else '❌'}",
                              callback_data="toggle:forward_to_owner_enabled")],
        [InlineKeyboardButton(text=f"🌍 Пояс: {s['timezone']}", callback_data="prompt:timezone")],
        back_button(),
    ])
    await message.answer("⚙️ <b>Настройки</b>", reply_markup=kb)


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not message.from_user or message.from_user.id != owner_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async def count(q):
            cur = await db.execute(q)
            return (await cur.fetchone())[0]
        messages = await count("SELECT COUNT(*) FROM messages")
        chats = await count("SELECT COUNT(*) FROM contacts")
        bl = await count("SELECT COUNT(*) FROM blacklist")
        mutes = await count("SELECT COUNT(*) FROM mutes")
    await message.answer(
        "📊 <b>Стата</b>\n"
        f"💾 Сообщений: {messages}\n"
        f"👥 Чатов: {chats}\n"
        f"🚫 В ЧС: {bl}\n"
        f"🔇 Мутов: {mutes}"
    )


@router.callback_query(F.data == "menu:main")
async def cb_main(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только хозяин", show_alert=True)
        return
    await call.message.edit_text("🏠 <b>Главное</b>", reply_markup=main_menu())
    await call.answer()


@router.callback_query(F.data == "menu:help")
async def cb_help(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только хозяин", show_alert=True)
        return
    await call.message.edit_text(
        "<b>📖 Что умею</b>\n\n"
        "В бизнес-чатах с точки:\n"
        "<code>.ai</code> · <code>.save</code> · <code>.anim</code>\n"
        "<code>.qr</code> · <code>.whois</code> · <code>.typing</code>\n"
        "<code>.mute</code> · <code>.unmute</code> · <code>.muted</code>\n"
        "<code>.note</code> · <code>.id</code> · <code>.ping</code> · <code>.game</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[back_button()])
    )
    await call.answer()


@router.callback_query(F.data == "menu:settings")
async def cb_settings(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только хозяин", show_alert=True)
        return
    await cmd_settings(call.message)
    await call.answer()


@router.callback_query(F.data == "menu:ai")
async def cb_ai_menu(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только хозяин", show_alert=True)
        return
    used, limit, is_premium = 0, FREE_DAILY_LIMIT, False
    async with aiosqlite.connect(DB_PATH) as db:
        today = utcnow().strftime("%Y-%m-%d")
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM user_limits WHERE user_id=?", (owner_id,))
        row = await cur.fetchone()
        if row and row["date"] == today:
            used = row["requests_count"]
            is_premium = bool(row["is_premium"])
        if is_premium:
            limit = PREMIUM_DAILY_LIMIT
    status = "💎 <b>Premium</b>" if is_premium else "🆓 <b>Бесплатно</b>"
    await call.message.edit_text(
        f"🤖 <b>Bob AI</b>\n"
        f"─────────────────\n"
        f"{status}\n"
        f"Сегодня: <b>{used}/{limit}</b>\n\n"
        f"💎 Premium: <b>{PREMIUM_DAILY_LIMIT} запросов/день</b>\n"
        f"Цена: <b>{PREMIUM_PRICE_STARS} ⭐</b> / {PREMIUM_DAYS} дней\n\n"
        f"Юзай: <code>.ai вопрос</code> в бизнес-чате",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⭐ Взять Premium ({PREMIUM_PRICE_STARS}⭐)",
                                  callback_data="buy_premium")],
            back_button(),
        ])
    )
    await call.answer()


@router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только хозяин", show_alert=True)
        return
    key = call.data.split(":", 1)[1]
    s = await get_settings()
    if key not in s:
        await call.answer("?", show_alert=True)
        return
    await set_setting(key, 0 if s[key] else 1)
    await cmd_settings(call.message)
    await call.answer("✅")


@router.callback_query(F.data.startswith("prompt:"))
async def cb_prompt(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только хозяин", show_alert=True)
        return
    action = call.data.split(":", 1)[1]
    prompts = {
        "answer_first_text": "✏️ Кидай текст первого ответа.",
        "off_hours_text": "✏️ Кидай текст автоответа.",
        "off_hours": "🕒 Кидай часы типа <code>22:00-08:00</code>.",
        "timezone": "🌍 Кидай пояс типа <code>Europe/Moscow</code>.",
    }
    if action not in prompts:
        await call.answer()
        return
    pending_inputs[owner_id] = action
    await call.message.answer(prompts[action])
    await call.answer()


@router.message(F.chat.type == "private", F.from_user.id == owner_id, ~F.text.startswith("/"))
async def receive_setting_value(message: Message):
    if owner_id not in pending_inputs:
        return
    action = pending_inputs.pop(owner_id)
    value = (message.text or "").strip()
    try:
        if action == "answer_first_text":
            await set_setting("answer_first_text", value)
        elif action == "off_hours_text":
            await set_setting("off_hours_text", value)
        elif action == "off_hours":
            m = re.fullmatch(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})", value)
            if not m:
                raise ValueError("Надо ЧЧ:ММ-ЧЧ:ММ")
            for t in m.groups():
                datetime.strptime(t, "%H:%M")
            await set_setting("off_hours_start", m.group(1))
            await set_setting("off_hours_end", m.group(2))
        elif action == "timezone":
            ZoneInfo(value)
            await set_setting("timezone", value)
        await message.answer("✅ Готово")
    except Exception as e:
        await message.answer(f"⚠️ {esc(e)}")
    await cmd_settings(message)


# ---------------- Payments ----------------
@router.callback_query(F.data == "buy_premium")
async def cb_buy_premium(call: CallbackQuery, bot: Bot):
    if call.from_user.id != owner_id:
        await call.answer("Покупка только для хозяина", show_alert=True)
        return
    try:
        await bot.send_invoice(
            chat_id=call.from_user.id,
            title="Bob AI Premium",
            description=f"{PREMIUM_DAILY_LIMIT} запросов в день на {PREMIUM_DAYS} дней",
            payload="premium_30d",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"Premium {PREMIUM_DAYS} дней",
                                 amount=PREMIUM_PRICE_STARS)],
        )
    except Exception as e:
        await call.answer(f"Ошибка: {e}", show_alert=True)
    await call.answer()


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def payment_ok(message: Message):
    p = message.successful_payment
    if p.invoice_payload == "premium_30d":
        await activate_premium(message.from_user.id, PREMIUM_DAYS)
        await message.answer(
            f"✅ <b>Premium активирован!</b>\n\n"
            f"Теперь <b>{PREMIUM_DAILY_LIMIT} запросов в день</b> на {PREMIUM_DAYS} дней 🚀"
        )


# ============================================================
# VIDEO DOWNLOAD HELPERS
# ============================================================

def _ytdlp_download(url: str, outdir: str) -> tuple[str, str]:
    opts = {
        "outtmpl": os.path.join(outdir, "%(id)s.%(ext)s"),
        "format": "mp4/best[ext=mp4]/best",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "max_filesize": DOWNLOAD_MAX_MB * 1024 * 1024,
        "retries": 2,
        "socket_timeout": 30,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        return ydl.prepare_filename(info), info.get("title", "") or "Видео"


async def _cobalt_download(url: str, outdir: str) -> tuple[str, str]:
    endpoints = [
        "https://api.cobalt.tools/",
        "https://co.wuk.sh/api/json",
        "https://cobalt-api.kwiatekmiki.com/",
        "https://cobalt-api.meowing.de/",
    ]
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    last_err = None
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for ep in endpoints:
            try:
                r = await client.post(ep, headers=headers, json={"url": url})
                if r.status_code != 200:
                    last_err = f"{ep}: HTTP {r.status_code}"
                    continue
                data = r.json()
                link = data.get("url") or data.get("audio")
                if not link:
                    last_err = f"{ep}: no url"
                    continue
                filepath = os.path.join(outdir, f"cobalt_{random.randint(1000, 9999)}.mp4")
                total = 0
                async with client.stream("GET", link) as resp:
                    resp.raise_for_status()
                    with open(filepath, "wb") as f:
                        async for chunk in resp.aiter_bytes(65536):
                            f.write(chunk)
                            total += len(chunk)
                            if total > DOWNLOAD_MAX_MB * 1024 * 1024:
                                raise RuntimeError("Файл слишком большой")
                return filepath, "Видео"
            except Exception as e:
                last_err = f"{ep}: {e}"
                log.debug("cobalt %s: %s", ep, e)
                continue
    raise RuntimeError(f"Cobalt: {last_err}")


async def _tikwm_download(url: str, outdir: str) -> tuple[str, str]:
    api = "https://www.tikwm.com/api/"
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        r = await client.get(api, params={"url": url, "hd": 1})
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            raise RuntimeError(data.get("msg", "tikwm error"))
        info = data.get("data", {})
        link = info.get("hdplay") or info.get("play")
        if not link:
            raise RuntimeError("tikwm: нет ссылки")
        if not link.startswith("http"):
            link = "https://www.tikwm.com" + link
        filepath = os.path.join(outdir, f"tt_{random.randint(1000, 9999)}.mp4")
        async with client.stream("GET", link) as resp:
            resp.raise_for_status()
            with open(filepath, "wb") as f:
                async for chunk in resp.aiter_bytes(65536):
                    f.write(chunk)
        title = info.get("title", "TikTok") or "TikTok"
        return filepath, title


async def download_video(url: str, outdir: str) -> tuple[str, str]:
    errors = []
    if YTDLP_OK:
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, _ytdlp_download, url, outdir)
        except Exception as e:
            errors.append(f"yt-dlp: {str(e)[:100]}")
            log.info("yt-dlp failed: %s", e)

    try:
        return await _cobalt_download(url, outdir)
    except Exception as e:
        errors.append(f"cobalt: {str(e)[:100]}")
        log.info("cobalt failed: %s", e)

    if "tiktok.com" in url or "vt.tiktok" in url:
        try:
            return await _tikwm_download(url, outdir)
        except Exception as e:
            errors.append(f"tikwm: {str(e)[:100]}")
            log.info("tikwm failed: %s", e)

    raise RuntimeError("Не вышло скачать. " + " | ".join(errors)[:300])


# ============================================================
# BUSINESS HANDLERS
# ============================================================

@router.business_message(F.text.regexp(r"^\.save\s*$"))
async def business_save(message: Message, bot: Bot):
    await save_business_message(message)
    if not message.from_user or message.from_user.id != owner_id:
        return
    if not message.business_connection_id:
        return

    target_text = ""
    if message.reply_to_message and message.reply_to_message.text:
        target_text = message.reply_to_message.text
    else:
        target_text = message.text or ""
    match = URL_RE.search(target_text)
    if not match:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text="⚠️ Ответь <code>.save</code> на сообщение со ссылкой.",
            business_connection_id=message.business_connection_id
        )
        return

    url = match.group(0).rstrip(".,)")

    await bot.edit_message_text(
        chat_id=message.chat.id, message_id=message.message_id,
        text="📥 <i>Качаю видос…</i>",
        business_connection_id=message.business_connection_id
    )

    tmpdir = tempfile.mkdtemp(prefix="save_")
    try:
        filepath, title = await download_video(url, tmpdir)
        if not filepath or not os.path.exists(filepath):
            raise RuntimeError("Файл куда-то пропал")

        size_mb = os.path.getsize(filepath) / 1024 / 1024
        if size_mb > DOWNLOAD_MAX_MB:
            raise RuntimeError(f"Жирный слишком ({size_mb:.1f} МБ)")

        await bot.send_video(
            chat_id=message.chat.id,
            video=FSInputFile(filepath),
            caption=f"🎬 <b>{esc(title) or 'Видео'}</b>",
            business_connection_id=message.business_connection_id
        )
        try:
            await bot.delete_business_messages(
                business_connection_id=message.business_connection_id,
                message_ids=[message.message_id]
            )
        except Exception:
            pass
    except Exception as e:
        log.error("save failed: %s", e)
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text=f"⚠️ Не вышло: <code>{esc(str(e)[:200])}</code>",
            business_connection_id=message.business_connection_id
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@router.business_message(F.text.regexp(r"^\.anim(\s+.+)?$"))
async def business_anim(message: Message, bot: Bot):
    await save_business_message(message)
    if not message.from_user or message.from_user.id != owner_id:
        return
    if not message.business_connection_id:
        return

    text = (message.text or "")[5:].strip()
    if not text:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text="⚠️ Пиши так: <code>.anim Твой текст</code>",
            business_connection_id=message.business_connection_id
        )
        return

    if len(text) > ANIM_MAX_CHARS:
        text = text[:ANIM_MAX_CHARS]

    bc = message.business_connection_id
    chat_id = message.chat.id

    try:
        sent = await bot.send_message(chat_id=chat_id, text="▱", business_connection_id=bc)
    except Exception as e:
        log.warning("anim send failed: %s", e)
        return

    try:
        await bot.delete_business_messages(business_connection_id=bc, message_ids=[message.message_id])
    except Exception:
        pass

    n = len(text)
    for i in range(1, n + 1):
        partial = text[:i]
        ratio = i / n
        if ratio < 0.3:
            styled = f"<i>{esc(partial)}</i>"
        elif ratio < 0.55:
            styled = f"<b>{esc(partial)}</b>"
        elif ratio < 0.75:
            styled = f"<s>{esc(partial)}</s>"
        elif ratio < 0.9:
            styled = f"<b><i>{esc(partial)}</i></b>"
        else:
            styled = f"<code>{esc(partial)}</code>"
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=sent.message_id,
                text=styled, business_connection_id=bc
            )
        except TelegramBadRequest:
            pass
        except Exception as e:
            log.debug("anim edit: %s", e)
        await asyncio.sleep(ANIM_DELAY)

    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=sent.message_id,
            text=esc(text), business_connection_id=bc
        )
    except Exception:
        pass


@router.business_message(F.text.regexp(r"^\.ai(\s+.+)?$"))
async def business_ai(message: Message, bot: Bot):
    await save_business_message(message)
    if not message.from_user or message.from_user.id != owner_id:
        return
    if not message.business_connection_id:
        return

    bc = message.business_connection_id
    chat_id = message.chat.id

    can, used, limit, is_premium = await check_limit(owner_id)
    if not can:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"⭐ Взять Premium ({PREMIUM_PRICE_STARS}⭐)",
                callback_data="buy_premium"
            )]
        ])
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message.message_id,
            text=(f"⚠️ <b>Всё, лимит</b>\n"
                  f"Юзанул <b>{used}/{limit}</b> за сегодня.\n\n"
                  f"Возьми Premium у меня в личке → /start → 🤖 Bob AI"),
            business_connection_id=bc, reply_markup=kb
        )
        return

    prompt = (message.text or "")[3:].strip()
    if not prompt:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message.message_id,
            text=f"🤖 Пиши: <code>.ai Твой вопрос</code>\n"
                 f"Осталось: <b>{limit - used}</b>",
            business_connection_id=bc
        )
        return

    try:
        thinking = await bot.send_message(
            chat_id=chat_id, text="🤖 <i>Ща…</i>",
            business_connection_id=bc
        )
    except Exception as e:
        log.warning("ai send failed: %s", e)
        return

    try:
        await bot.delete_business_messages(business_connection_id=bc, message_ids=[message.message_id])
    except Exception:
        pass

    try:
        answer = await ask_ai(prompt)
        answer = answer[:4000]
        await bot.edit_message_text(
            chat_id=chat_id, message_id=thinking.message_id,
            text=f"🤖 {esc(answer)}",
            business_connection_id=bc
        )
        await increment_usage(owner_id)
    except Exception as e:
        log.error("AI error: %s", e)
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=thinking.message_id,
                text=f"⚠️ Косяк: <code>{esc(str(e)[:200])}</code>",
                business_connection_id=bc
            )
        except Exception:
            pass


@router.business_message(F.text.regexp(r"^\.qr(\s+.+)?$"))
async def business_qr(message: Message, bot: Bot):
    await save_business_message(message)
    if not message.from_user or message.from_user.id != owner_id:
        return
    if not message.business_connection_id:
        return

    bc = message.business_connection_id
    text = (message.text or "")[3:].strip()
    if not text:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text="⚠️ Пиши: <code>.qr текст или ссылка</code>",
            business_connection_id=bc
        )
        return

    try:
        import qrcode
        buf = BytesIO()
        qrcode.make(text).save(buf, format="PNG")
        buf.seek(0)
        await bot.send_photo(
            chat_id=message.chat.id,
            photo=BufferedInputFile(buf.read(), filename="qr.png"),
            caption=f"🔳 QR: <code>{esc(text[:200])}</code>",
            business_connection_id=bc
        )
        try:
            await bot.delete_business_messages(business_connection_id=bc, message_ids=[message.message_id])
        except Exception:
            pass
    except ImportError:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text="⚠️ Поставь <code>qrcode[pil]</code>",
            business_connection_id=bc
        )
    except Exception as e:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text=f"⚠️ {esc(str(e)[:200])}",
            business_connection_id=bc
        )


@router.business_message(F.text.regexp(r"^\.(help|whois|typing|mute(?:\s+\d+)?|unmute|muted|id|ping|note(?:\s+.+)?|game)$"))
async def business_commands(message: Message, bot: Bot):
    await save_business_message(message)
    if not message.from_user or message.from_user.id != owner_id:
        return
    if not message.business_connection_id:
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    chat_id = message.chat.id
    bc = message.business_connection_id
    mid = message.message_id

    async def edit(text, kb=None):
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=mid, text=text,
                business_connection_id=bc, reply_markup=kb
            )
        except TelegramBadRequest as e:
            log.warning("edit: %s", e)

    try:
        if cmd == ".help":
            await edit(
                "🤖 <b>Что умею</b>\n"
                "<code>.ai вопрос</code> · Bob AI\n"
                "<code>.save</code> (ответом) · качнуть видос\n"
                "<code>.anim текст</code> · анимация\n"
                "<code>.qr текст</code> · QR\n"
                "<code>.whois</code> · инфа о собеседнике\n"
                "<code>.typing</code> · печатаю\n"
                "<code>.mute 60</code> · <code>.unmute</code> · <code>.muted</code>\n"
                "<code>.id</code> · <code>.ping</code> · <code>.note</code> · <code>.game</code>"
            )
        elif cmd == ".ping":
            await edit("🏓 <b>Живой!</b>")
        elif cmd == ".id":
            await edit(
                f"🆔 Чат: <code>{chat_id}</code>\n"
                f"👤 Ты: <code>{message.from_user.id}</code>\n"
                f"🔗 BC: <code>{esc(bc)}</code>"
            )
        elif cmd == ".whois":
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute("SELECT * FROM contacts WHERE chat_id=?", (chat_id,))
                row = await cur.fetchone()
            if not row:
                await edit("👤 Ничего не знаю про этого чела.")
            else:
                first = "—"
                try:
                    first = datetime.fromisoformat(row["first_seen"]).astimezone().strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pass
                uname = f"@{esc(row['username'])}" if row["username"] else "—"
                await edit(
                    "👤 <b>Инфа</b>\n"
                    f"📛 <b>{esc(row['user_name'])}</b>\n"
                    f"🆔 <code>{row['user_id']}</code>\n"
                    f"🔗 {uname}\n"
                    f"📅 Первый раз: {esc(first)}\n"
                    f"💬 Сообщений: <b>{row['message_count']}</b>"
                )
        elif cmd == ".typing":
            try:
                await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING, business_connection_id=bc)
            except Exception:
                pass
            await edit("⌨️ <i>Печатаю…</i>")
        elif cmd == ".note":
            if not arg:
                row = await get_note(chat_id)
                if row and row[0]:
                    await edit(f"📝 {esc(row[0])}")
                else:
                    await edit("📝 Заметок нет.")
            else:
                await set_note(chat_id, arg)
                await edit(f"📝 Записал: {esc(arg)}")
        elif cmd.startswith(".mute"):
            tokens = arg.split()
            seconds = 60
            target = None
            if tokens:
                try:
                    seconds = max(1, min(int(tokens[0]), 86400))
                except ValueError:
                    pass
            if len(tokens) > 1:
                try:
                    target = int(tokens[1])
                except ValueError:
                    pass
            if target is None:
                async with aiosqlite.connect(DB_PATH) as db:
                    cur = await db.execute(
                        "SELECT user_id FROM messages WHERE chat_id=? AND user_id IS NOT NULL "
                        "AND user_id!=? ORDER BY id DESC LIMIT 1",
                        (chat_id, owner_id)
                    )
                    row = await cur.fetchone()
                if not row:
                    await edit("⚠️ Укажи ID: <code>.mute 60 123456</code>")
                    return
                target = row[0]
            until = utcnow() + timedelta(seconds=seconds)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO mutes(chat_id,target_user_id,mute_until) VALUES(?,?,?)",
                    (chat_id, target, until.isoformat())
                )
                await db.commit()
            await edit(f"🔇 Заткнул <code>{target}</code> на {seconds} сек")
        elif cmd == ".unmute":
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT user_id FROM messages WHERE chat_id=? AND user_id IS NOT NULL "
                    "AND user_id!=? ORDER BY id DESC LIMIT 1",
                    (chat_id, owner_id)
                )
                row = await cur.fetchone()
                if row:
                    await db.execute("DELETE FROM mutes WHERE chat_id=? AND target_user_id=?",
                                     (chat_id, row[0]))
                    await db.commit()
            await edit("🔊 Размутил")
        elif cmd == ".muted":
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT target_user_id, mute_until FROM mutes WHERE chat_id=? AND mute_until > ?",
                    (chat_id, utcnow().isoformat())
                )
                rows = await cur.fetchall()
            if not rows:
                await edit("🔊 Никого не мьютил")
            else:
                lines = []
                for uid, until in rows:
                    try:
                        t = datetime.fromisoformat(until).astimezone().strftime("%H:%M:%S")
                    except Exception:
                        t = "—"
                    lines.append(f"• <code>{uid}</code> до {t}")
                await edit("🔇 <b>В муте</b>\n" + "\n".join(lines))
        elif cmd == ".game":
            await edit(
                "🎮 <b>Во что играем?</b>",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔫 Рулетка",
                                          callback_data=f"game:roulette:{chat_id}:{bc}")],
                    [InlineKeyboardButton(text="⭕❌ Крестики-нолики",
                                          callback_data=f"game:ttt:{chat_id}:{bc}")],
                    [InlineKeyboardButton(text="🎲 Угадай число",
                                          callback_data=f"game:guess:{chat_id}:{bc}")],
                ])
            )
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.warning("command failed: %s", e)


@router.business_message(F.text.regexp(r"^\d{1,3}$"))
async def guess_input(message: Message):
    if not message.from_user:
        return
    chat_id = message.chat.id
    active = [(k, v) for k, v in guess_games.items() if k[0] == chat_id]
    if not active:
        return
    key, game = active[-1]
    try:
        guess = int(message.text)
    except ValueError:
        return
    if not 1 <= guess <= 100:
        return
    game["tries"] += 1
    if guess == game["number"]:
        await message.answer(f"🎉 Угадал! Это {guess}. Попыток: {game['tries']}.")
        guess_games.pop(key, None)
    elif guess < game["number"]:
        await message.answer("⬆️ Больше")
    else:
        await message.answer("⬇️ Меньше")


@router.business_message()
async def business_message_handler(message: Message, bot: Bot):
    await save_business_message(message)
    if not message.business_connection_id:
        return
    sender = message.from_user
    if not sender or sender.id == owner_id:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT mute_until FROM mutes WHERE chat_id=? AND target_user_id=?",
            (message.chat.id, sender.id)
        )
        row = await cur.fetchone()
    if row:
        try:
            until = datetime.fromisoformat(row[0])
            if until > utcnow():
                try:
                    await bot.delete_business_messages(
                        business_connection_id=message.business_connection_id,
                        message_ids=[message.message_id]
                    )
                except Exception:
                    pass
                try:
                    notice = await bot.send_message(
                        chat_id=message.chat.id,
                        text=f"🔇 <i>Ты в муте до {until.astimezone().strftime('%H:%M:%S')}</i>",
                        business_connection_id=message.business_connection_id
                    )
                    async def _del():
                        await asyncio.sleep(MUTE_NOTICE_SECONDS)
                        try:
                            await bot.delete_business_messages(
                                business_connection_id=message.business_connection_id,
                                message_ids=[notice.message_id]
                            )
                        except Exception:
                            pass
                    asyncio.create_task(_del())
                except Exception:
                    pass
                return
        except Exception:
            pass

    s = await get_settings()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT message_count FROM contacts WHERE chat_id=?", (message.chat.id,))
        cnt_row = await cur.fetchone()
    count = cnt_row[0] if cnt_row else 1
    if count == 1 and s["answer_first_enabled"] and s["answer_first_text"]:
        try:
            await bot.send_message(
                message.chat.id, s["answer_first_text"],
                business_connection_id=message.business_connection_id
            )
        except Exception as e:
            log.info("first reply: %s", e)

    if s["off_hours_enabled"]:
        now = local_now(s["timezone"]).strftime("%H:%M")
        start, end = s["off_hours_start"], s["off_hours_end"]
        is_off = (start <= now < end) if start <= end else (now >= start or now < end)
        if is_off:
            try:
                await bot.send_message(
                    message.chat.id, s["off_hours_text"],
                    business_connection_id=message.business_connection_id
                )
            except Exception as e:
                log.info("off-hours: %s", e)

    if s["forward_to_owner_enabled"]:
        try:
            head = f"📥 <b>{esc(sender.full_name)}</b> <code>{sender.id}</code>"
            if message.text or message.caption:
                await bot.send_message(owner_id, f"{head}\n\n{esc(message.text or message.caption)}")
            else:
                await bot.send_message(owner_id, head)
        except Exception as e:
            log.info("forward: %s", e)


@router.deleted_business_messages()
async def business_deleted_handler(event: BusinessMessagesDeleted, bot: Bot):
    for mid in event.message_ids:
        row = await find_message(event.chat.id, mid)
        if not row:
            continue
        stamp = row["date"]
        try:
            stamp = datetime.fromisoformat(stamp).astimezone().strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            pass
        direction = "исходящее" if row["is_outgoing"] else "входящее"
        caption = (
            "🗑 <b>Снёс сообщение</b>\n"
            f"💬 <code>{event.chat.id}</code>\n"
            f"👤 <b>{esc(row['user_name'])}</b> <code>{row['user_id']}</code>\n"
            f"🕐 {esc(stamp)}\n"
            f"📎 <i>{esc(row['content_type'])}</i> ({direction})\n\n"
            f"{esc(row['text'] or '—')}"
        )
        try:
            ctype = row["content_type"]
            fid = row["media_file_id"]
            if fid and ctype == "photo":
                await bot.send_photo(owner_id, fid, caption=caption[:1024])
            elif fid and ctype == "video":
                await bot.send_video(owner_id, fid, caption=caption[:1024])
            elif fid and ctype == "animation":
                await bot.send_animation(owner_id, fid, caption=caption[:1024])
            elif fid and ctype == "document":
                await bot.send_document(owner_id, fid, caption=caption[:1024])
            elif fid and ctype == "voice":
                await bot.send_voice(owner_id, fid, caption=caption[:1024])
            elif fid and ctype == "audio":
                await bot.send_audio(owner_id, fid, caption=caption[:1024])
            elif fid and ctype == "sticker":
                await bot.send_message(owner_id, caption)
                await bot.send_sticker(owner_id, fid)
            else:
                await bot.send_message(owner_id, caption)
        except Exception as e:
            log.warning("deleted notify: %s", e)


# --- Игры ---
@router.callback_query(F.data.startswith("game:"))
async def game_callback(call: CallbackQuery, bot: Bot):
    if not call.message:
        await call.answer()
        return
    parts = call.data.split(":")
    kind = parts[1]
    chat_id = int(parts[2])
    bc = ":".join(parts[3:])
    if kind == "roulette":
        roulette_games[(chat_id, call.message.message_id)] = 0
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="🔄 Крутить",
            callback_data=f"roulette:spin:{chat_id}:{call.message.message_id}:{bc}")]])
        await call.message.edit_text("🔫 <b>Рулетка</b>\n6 камер, 1 патрон. Жми.", reply_markup=kb)
    elif kind == "guess":
        guess_games[(chat_id, call.message.message_id)] = {
            "number": random.randint(1, 100), "tries": 0, "bc": bc
        }
        await call.message.edit_text("🎲 <b>Угадай число</b> от 1 до 100. Пиши в чат.")
    elif kind == "ttt":
        ttt_games[(chat_id, call.message.message_id)] = {
            "board": [""] * 9, "turn": "❌", "owner": owner_id,
            "opponent": None, "bc": bc, "ended": False
        }
        await render_ttt(call.message, chat_id, call.message.message_id)
    await call.answer()


async def render_ttt(message, chat_id, mid):
    game = ttt_games[(chat_id, mid)]
    kb = []
    for r in range(3):
        kb.append([InlineKeyboardButton(
            text=game["board"][r*3+c] or "·",
            callback_data=f"ttt:move:{chat_id}:{mid}:{r*3+c}"
        ) for c in range(3)])
    await message.edit_text(f"⭕❌ Ход: <b>{game['turn']}</b>",
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("roulette:spin:"))
async def roulette_spin(call: CallbackQuery):
    p = call.data.split(":")
    chat_id, mid = int(p[2]), int(p[3])
    key = (chat_id, mid)
    if key not in roulette_games:
        await call.answer("Всё, игра кончилась", show_alert=True)
        return
    if random.randint(1, 6) == 1:
        roulette_games.pop(key, None)
        await call.message.edit_text("💥 <b>Бах!</b> Всё.")
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="🔄 Крутить", callback_data=call.data)]])
        await call.message.edit_text("😮‍💨 Щёлк. Ещё?", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("ttt:move:"))
async def ttt_move(call: CallbackQuery):
    p = call.data.split(":")
    chat_id, mid, idx = int(p[2]), int(p[3]), int(p[4])
    key = (chat_id, mid)
    game = ttt_games.get(key)
    if not game or game["ended"]:
        await call.answer("Всё, конец", show_alert=True)
        return
    uid = call.from_user.id
    if game["opponent"] is None:
        if uid == owner_id:
            await call.answer("Ждём соперника", show_alert=True)
            return
        game["opponent"] = uid
    elif uid not in {owner_id, game["opponent"]}:
        await call.answer("Ты не в игре", show_alert=True)
        return
    expected = owner_id if game["turn"] == "❌" else game["opponent"]
    if uid != expected:
        await call.answer("Не твой ход", show_alert=True)
        return
    if game["board"][idx]:
        await call.answer("Занято", show_alert=True)
        return
    game["board"][idx] = game["turn"]
    b = game["board"]
    wins = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]
    winner = next((b[a] for a,c,d in wins if b[a] and b[a]==b[c]==b[d]), None)
    if winner:
        game["ended"] = True
        await call.message.edit_text(f"🏆 <b>{winner}</b> победил!", reply_markup=None)
    elif all(b):
        game["ended"] = True
        await call.message.edit_text("🤝 Ничья!", reply_markup=None)
    else:
        game["turn"] = "⭕" if game["turn"] == "❌" else "❌"
        await render_ttt(call.message, chat_id, mid)
    await call.answer()


@router.business_connection()
async def business_connection_handler(connection: BusinessConnection, bot: Bot):
    log.info("Business connection %s enabled=%s", connection.id, connection.is_enabled)


async def update_business_name(bot: Bot):
    while True:
        try:
            s = await get_settings()
            if s["time_in_nick_enabled"]:
                me = await bot.get_me()
                clock = local_now(s["timezone"]).strftime("%H:%M")
                desired = f"{me.first_name or BOT_NAME} [{clock}]"[:64]
                method = getattr(bot, "set_business_account_name", None)
                if method:
                    async with name_update_lock:
                        await method(name=desired)
        except Exception:
            pass
        await asyncio.sleep(60)


async def main():
    if not api_token:
        raise RuntimeError("Нет API_TOKEN в .env")
    if owner_id <= 0:
        raise RuntimeError("Корявый OWNER_ID в .env")
    await init_db()
    bot = Bot(api_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    asyncio.create_task(update_business_name(bot))
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types() + [
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages"
    ])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
