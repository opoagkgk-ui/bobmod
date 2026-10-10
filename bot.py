import asyncio
import logging
import os
import random
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, BaseFilter
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BusinessConnection, BusinessMessagesDeleted,
)

# ---------- Config ----------
load_dotenv()
api_token = os.getenv("API_TOKEN", "").strip()
try:
    owner_id = int(os.getenv("OWNER_ID", "0"))
except ValueError:
    owner_id = 0

DB_PATH = "bot.db"
KEEP_PER_CHAT = 100
MUTE_NOTICE_SECONDS = 4

router = Router()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("secretary")

# ---------- In-memory state ----------
guess_games = {}          # (chat_id, mid) -> {"number":int,"tries":int,"bc":str}
roulette_games = {}       # (chat_id, mid) -> spins
ttt_games = {}            # (chat_id, mid) -> board/turn/...
pending_inputs = {}       # owner_id -> action
name_update_lock = asyncio.Lock()


# ---------- Utilities ----------
def utcnow():
    return datetime.now(timezone.utc)


def local_now(tz_name="UTC"):
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        return datetime.now(timezone.utc)


def esc(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def code(s):
    return f"<code>{esc(s)}</code>"


# ---------- Custom filters ----------
class HasPendingInput(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user is not None and message.from_user.id in pending_inputs


class GuessGameActive(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        if not message.text:
            return False
        if not re.fullmatch(r"\d{1,3}", message.text.strip()):
            return False
        return any(k[0] == message.chat.id for k in guess_games)


# ---------- Database ----------
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
        CREATE INDEX IF NOT EXISTS idx_messages_chat_date ON messages(chat_id, date);

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
        row = await cur.fetchone()
        return row


async def set_note(chat_id, text):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO notes(chat_id,note,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(chat_id) DO UPDATE SET note=excluded.note, updated_at=excluded.updated_at",
            (chat_id, text, utcnow().isoformat())
        )
        await db.commit()


# ---------- Message payload helpers ----------
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


# ---------- Menus ----------
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings")],
        [InlineKeyboardButton(text="📖 Помощь", callback_data="menu:help")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="menu:stats")],
    ])


def back_button(target="menu:main"):
    return [InlineKeyboardButton(text="⬅️ Назад", callback_data=target)]


def settings_menu(s):
    def flag(v): return "✅" if v else "❌"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Ответы на первое сообщение", callback_data="menu:sec:first")],
        [InlineKeyboardButton(text="🕐 Имя и расписание", callback_data="menu:sec:schedule")],
        [InlineKeyboardButton(text="🚫 Приватность", callback_data="menu:sec:privacy")],
        [InlineKeyboardButton(text="📥 Пересылка владельцу", callback_data="menu:sec:forward")],
        back_button(),
    ])


def settings_first(s):
    def flag(v): return "✅ ВКЛ" if v else "❌ ВЫКЛ"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💬 Автоответ на первое: {flag(s['answer_first_enabled'])}",
                              callback_data="toggle:answer_first_enabled")],
        [InlineKeyboardButton(text="✏️ Изменить текст", callback_data="prompt:answer_first_text")],
        [InlineKeyboardButton(text=f"📄 Текущий: {(s['answer_first_text'] or '—')[:40]}",
                              callback_data="noop")],
        back_button("menu:settings"),
    ])


def settings_schedule(s):
    def flag(v): return "✅ ВКЛ" if v else "❌ ВЫКЛ"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🕐 Время в имени: {flag(s['time_in_nick_enabled'])}",
                              callback_data="toggle:time_in_nick_enabled")],
        [InlineKeyboardButton(text=f"🌍 Часовой пояс: {s['timezone']}",
                              callback_data="prompt:timezone")],
        [InlineKeyboardButton(text=f"🌙 Автоответ вне часов: {flag(s['off_hours_enabled'])}",
                              callback_data="toggle:off_hours_enabled")],
        [InlineKeyboardButton(text=f"🕒 Часы: {s['off_hours_start']}–{s['off_hours_end']}",
                              callback_data="prompt:off_hours")],
        [InlineKeyboardButton(text="✏️ Текст автоответа", callback_data="prompt:off_hours_text")],
        back_button("menu:settings"),
    ])


def settings_privacy(s):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Чёрный список", callback_data="menu:blacklist")],
        back_button("menu:settings"),
    ])


def settings_forward(s):
    def flag(v): return "✅ ВКЛ" if v else "❌ ВЫКЛ"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📥 Пересылать сообщения владельцу: {flag(s['forward_to_owner_enabled'])}",
                              callback_data="toggle:forward_to_owner_enabled")],
        [InlineKeyboardButton(text="ℹ️ Куда приходят уведомления",
                              callback_data="noop")],
        back_button("menu:settings"),
    ])


async def render_settings(msg_or_call, edit=False):
    s = await get_settings()
    text = (
        "⚙️ <b>Настройки секретаря</b>\n"
        "<i>Выбери раздел для настройки</i>\n\n"
        f"💬 Первый ответ: {'✅' if s['answer_first_enabled'] else '❌'}\n"
        f"🕐 Время в имени: {'✅' if s['time_in_nick_enabled'] else '❌'}\n"
        f"🌍 Пояс: <code>{esc(s['timezone'])}</code>\n"
        f"🌙 Автоответ: {'✅' if s['off_hours_enabled'] else '❌'} "
        f"({esc(s['off_hours_start'])}–{esc(s['off_hours_end'])})\n"
        f"📥 Пересылка владельцу: {'✅' if s['forward_to_owner_enabled'] else '❌'}"
    )
    kb = settings_menu(s)
    if edit:
        try:
            await msg_or_call.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            await msg_or_call.answer(text, reply_markup=kb)
    else:
        await msg_or_call.answer(text, reply_markup=kb)


# ============================================================
# PRIVATE HANDLERS (owner)
# ============================================================

@router.message(Command("start"))
async def cmd_start(message: Message):
    await get_settings()
    await message.answer(
        "👋 <b>Привет! Я твой бизнес-секретарь.</b>\n\n"
        "Я работаю в <b>Telegram Business</b> чатах:\n"
        "• отвечаю первым сообщением\n"
        "• предупреждаю о нерабочих часах\n"
        "• сохраняю удалённые сообщения\n"
        "• выполняю команды через точку\n\n"
        "<i>Подключи меня в Настройки → Telegram Business → Чат-автоматизация</i>",
        reply_markup=main_menu()
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "<b>📖 Помощь</b>\n\n"
        "<b>Владельцу в личке:</b>\n"
        "/start — меню\n"
        "/help — эта справка\n"
        "/settings — настройки\n"
        "/stats — статистика\n\n"
        "<b>Команды в Business-чатах</b> (пишутся с точки):\n"
        "<code>.help</code> — список\n"
        "<code>.whois</code> — карточка собеседника\n"
        "<code>.typing</code> — показать «печатает»\n"
        "<code>.mute 60</code> — мут на 60 сек\n"
        "<code>.unmute</code> — снять мут\n"
        "<code>.muted</code> — список мутов\n"
        "<code>.id</code> — технические ID\n"
        "<code>.note текст</code> — заметка о чате\n"
        "<code>.note</code> — показать заметку\n"
        "<code>.ping</code> — проверить бота\n"
        "<code>.game</code> — игры",
        reply_markup=main_menu()
    )


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    if message.from_user and message.from_user.id != owner_id:
        await message.answer("⛔ Настройки доступны только владельцу.")
        return
    await render_settings(message)


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
        blacklisted = await count("SELECT COUNT(*) FROM blacklist")
        mutes = await count("SELECT COUNT(*) FROM mutes")
        notes = await count("SELECT COUNT(*) FROM notes")
    await message.answer(
        "📊 <b>Статистика</b>\n"
        "─────────────────\n"
        f"💾 Сообщений в архиве: <b>{messages}</b>\n"
        f"👥 Контактов: <b>{chats}</b>\n"
        f"🚫 В чёрном списке: <b>{blacklisted}</b>\n"
        f"🔇 Активных мутов: <b>{mutes}</b>\n"
        f"📝 Заметок: <b>{notes}</b>"
    )


@router.message(Command("ping"))
async def cmd_ping(message: Message):
    if not message.from_user or message.from_user.id != owner_id:
        return
    await message.answer("🏓 Pong! Бот на связи.")


# ----- Menu callbacks (private) -----
@router.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@router.callback_query(F.data == "menu:main")
async def cb_menu_main(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    await call.message.edit_text(
        "🏠 <b>Главное меню</b>\n<i>Выбери действие</i>",
        reply_markup=main_menu()
    )
    await call.answer()


@router.callback_query(F.data == "menu:help")
async def cb_menu_help(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    await call.message.edit_text(
        "<b>📖 Помощь</b>\n\n"
        "В Business-чатах используй команды с точкой:\n"
        "<code>.help</code> · <code>.whois</code> · <code>.typing</code>\n"
        "<code>.mute 60</code> · <code>.unmute</code> · <code>.muted</code>\n"
        "<code>.id</code> · <code>.note</code> · <code>.ping</code> · <code>.game</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[back_button()])
    )
    await call.answer()


@router.callback_query(F.data == "menu:stats")
async def cb_menu_stats(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async def count(q):
            cur = await db.execute(q)
            return (await cur.fetchone())[0]
        messages = await count("SELECT COUNT(*) FROM messages")
        chats = await count("SELECT COUNT(*) FROM contacts")
        blacklisted = await count("SELECT COUNT(*) FROM blacklist")
        mutes = await count("SELECT COUNT(*) FROM mutes")
    await call.message.edit_text(
        "📊 <b>Статистика</b>\n"
        "─────────────────\n"
        f"💾 Сообщений: <b>{messages}</b>\n"
        f"👥 Контактов: <b>{chats}</b>\n"
        f"🚫 В ЧС: <b>{blacklisted}</b>\n"
        f"🔇 Мутов: <b>{mutes}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[back_button()])
    )
    await call.answer()


@router.callback_query(F.data == "menu:settings")
async def cb_menu_settings(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    await render_settings(call.message, edit=True)
    await call.answer()


@router.callback_query(F.data.startswith("menu:sec:"))
async def cb_section(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    section = call.data.split(":", 2)[2]
    s = await get_settings()
    if section == "first":
        text = "💬 <b>Первый ответ</b>\n<i>Бот отвечает на самое первое сообщение в новом чате</i>"
        kb = settings_first(s)
    elif section == "schedule":
        text = "🕐 <b>Расписание</b>\n<i>Время в имени и автоответ вне рабочих часов</i>"
        kb = settings_schedule(s)
    elif section == "privacy":
        text = "🚫 <b>Приватность</b>\n<i>Чёрный список пользователей</i>"
        kb = settings_privacy(s)
    elif section == "forward":
        text = "📥 <b>Пересылка владельцу</b>\n<i>Дублировать сообщения клиентов тебе в личку</i>"
        kb = settings_forward(s)
    else:
        await call.answer("Неизвестный раздел", show_alert=True)
        return
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "menu:blacklist")
async def cb_blacklist(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM blacklist ORDER BY user_id LIMIT 30")
        rows = await cur.fetchall()
    listing = "\n".join(f"• <code>{r[0]}</code>" for r in rows) if rows else "<i>Пусто</i>"
    await call.message.edit_text(
        f"🚫 <b>Чёрный список</b>\n{listing}\n\n"
        "Отправь <code>ID</code> чтобы добавить, или <code>-ID</code> чтобы удалить.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить", callback_data="prompt:blacklist_add")],
            [InlineKeyboardButton(text="➖ Удалить", callback_data="prompt:blacklist_remove")],
            back_button("menu:settings"),
        ])
    )
    await call.answer()


@router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    key = call.data.split(":", 1)[1]
    s = await get_settings()
    if key not in s:
        await call.answer("Неизвестный параметр", show_alert=True)
        return
    new_val = 0 if s[key] else 1
    await set_setting(key, new_val)
    s = await get_settings()
    # Re-render current section
    if key in {"answer_first_enabled", "answer_first_text"}:
        await call.message.edit_text("💬 <b>Первый ответ</b>", reply_markup=settings_first(s))
    elif key in {"time_in_nick_enabled", "off_hours_enabled", "off_hours_start", "off_hours_end", "off_hours_text", "timezone"}:
        await call.message.edit_text("🕐 <b>Расписание</b>", reply_markup=settings_schedule(s))
    elif key == "forward_to_owner_enabled":
        await call.message.edit_text("📥 <b>Пересылка владельцу</b>", reply_markup=settings_forward(s))
    await call.answer("✅ Сохранено")


@router.callback_query(F.data.startswith("prompt:"))
async def cb_prompt(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    action = call.data.split(":", 1)[1]
    prompts = {
        "answer_first_text": "✏️ Отправь <b>текст первого ответа</b> следующим сообщением.",
        "off_hours_text": "✏️ Отправь <b>текст автоответа</b> следующим сообщением.",
        "off_hours": "🕒 Отправь часы в формате <code>ЧЧ:ММ-ЧЧ:ММ</code>, например <code>22:00-08:00</code>.",
        "timezone": "🌍 Отправь часовой пояс IANA, например <code>Europe/Moscow</code>.",
        "blacklist_add": "🚫 Отправь <code>user_id</code> для добавления в чёрный список.",
        "blacklist_remove": "➖ Отправь <code>user_id</code> для удаления из чёрного списка.",
    }
    if action not in prompts:
        await call.answer()
        return
    pending_inputs[owner_id] = action
    await call.message.answer(prompts[action])
    await call.answer()


@router.message(F.chat.type == "private", F.from_user.id == owner_id, HasPendingInput())
async def receive_setting_value(message: Message):
    action = pending_inputs.pop(owner_id, None)
    if not action:
        return
    value = (message.text or "").strip()
    try:
        if action == "answer_first_text":
            await set_setting("answer_first_text", value)
        elif action == "off_hours_text":
            await set_setting("off_hours_text", value)
        elif action == "off_hours":
            m = re.fullmatch(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})", value)
            if not m:
                raise ValueError("Нужен формат ЧЧ:ММ-ЧЧ:ММ")
            for t in m.groups():
                datetime.strptime(t, "%H:%M")
            await set_setting("off_hours_start", m.group(1))
            await set_setting("off_hours_end", m.group(2))
        elif action == "timezone":
            ZoneInfo(value)
            await set_setting("timezone", value)
        elif action in {"blacklist_add", "blacklist_remove"}:
            target = int(value)
            if action == "blacklist_add":
                await add_blacklist(target)
                await message.answer(f"🚫 ID <code>{target}</code> добавлен.")
            else:
                await remove_blacklist(target)
                await message.answer(f"✅ ID <code>{target}</code> удалён.")
            return
        await message.answer("✅ <b>Настройка сохранена</b>")
    except Exception as e:
        await message.answer(f"⚠️ Не удалось сохранить: {esc(e)}")
    await render_settings(message)


# ============================================================
# BUSINESS HANDLERS (order matters!)
# ============================================================

# 1) Dot-commands — должно быть ДО общего business_message
@router.business_message(F.text.regexp(r"^\."))
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
            log.warning("edit failed: %s", e)

    try:
        if cmd == ".help":
            await edit(
                "🤖 <b>Секретарь — команды</b>\n"
                "─────────────────\n"
                "<code>.help</code> · список команд\n"
                "<code>.whois</code> · карточка собеседника\n"
                "<code>.typing</code> · эффект «печатает»\n"
                "<code>.mute 60</code> · мут на 60 сек\n"
                "<code>.unmute</code> · снять мут\n"
                "<code>.muted</code> · список мутов\n"
                "<code>.id</code> · техническая информация\n"
                "<code>.note текст</code> · сохранить заметку\n"
                "<code>.note</code> · показать заметку\n"
                "<code>.ping</code> · проверка связи\n"
                "<code>.game</code> · игры"
            )
        elif cmd == ".ping":
            await edit("🏓 <b>Pong!</b> Бот на связи.")
        elif cmd == ".id":
            await edit(
                "🆔 <b>Технические ID</b>\n"
                "─────────────────\n"
                f"💬 Чат: <code>{chat_id}</code>\n"
                f"👤 Ты: <code>{message.from_user.id}</code>\n"
                f"🔗 Соединение: <code>{esc(bc)}</code>"
            )
        elif cmd == ".whois":
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute("SELECT * FROM contacts WHERE chat_id=?", (chat_id,))
                row = await cur.fetchone()
            if not row:
                await edit("👤 Карточка пока пуста — нет сохранённой истории.")
            else:
                first = "—"
                try:
                    first = datetime.fromisoformat(row["first_seen"]).astimezone().strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pass
                uname = f"@{esc(row['username'])}" if row["username"] else "—"
                await edit(
                    "👤 <b>Карточка собеседника</b>\n"
                    "─────────────────\n"
                    f"📛 Имя: <b>{esc(row['user_name'])}</b>\n"
                    f"🆔 ID: <code>{row['user_id']}</code>\n"
                    f"🔗 Username: {uname}\n"
                    f"📅 Первый контакт: {esc(first)}\n"
                    f"💬 Сообщений: <b>{row['message_count']}</b>"
                )
        elif cmd == ".typing":
            try:
                await bot.send_chat_action(
                    chat_id=chat_id, action=ChatAction.TYPING, business_connection_id=bc
                )
            except Exception:
                pass
            await edit("⌨️ <i>Печатает…</i>")
        elif cmd == ".note":
            if not arg:
                row = await get_note(chat_id)
                if row and row[0]:
                    await edit(f"📝 <b>Заметка о чате</b>\n\n{esc(row[0])}")
                else:
                    await edit("📝 Заметки пока нет.\nИспользуй <code>.note текст</code> для сохранения.")
            else:
                await set_note(chat_id, arg)
                await edit(f"📝 <b>Заметка сохранена:</b>\n\n{esc(arg)}")
        elif cmd.startswith(".mute"):
            seconds = 60
            target = None
            tokens = arg.split()
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
                    await edit("⚠️ Не удалось определить собеседника. Укажи ID: <code>.mute 60 123456</code>")
                    return
                target = row[0]
            until = utcnow() + timedelta(seconds=seconds)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO mutes(chat_id,target_user_id,mute_until) VALUES(?,?,?)",
                    (chat_id, target, until.isoformat())
                )
                await db.commit()
            await edit(f"🔇 <b>Мут установлен</b>\nПользователь: <code>{target}</code>\nДо: {until.astimezone().strftime('%H:%M:%S')} ({seconds} сек)")
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
            await edit("🔊 <b>Мут снят</b>")
        elif cmd == ".muted":
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT target_user_id, mute_until FROM mutes WHERE chat_id=? "
                    "AND mute_until > ? ORDER BY mute_until",
                    (chat_id, utcnow().isoformat())
                )
                rows = await cur.fetchall()
            if not rows:
                await edit("🔊 Активных мутов нет.")
            else:
                lines = []
                for uid, until in rows:
                    try:
                        t = datetime.fromisoformat(until).astimezone().strftime("%H:%M:%S")
                    except Exception:
                        t = "—"
                    lines.append(f"• <code>{uid}</code> до {t}")
                await edit("🔇 <b>Активные муты</b>\n\n" + "\n".join(lines))
        elif cmd == ".game":
            await edit(
                "🎮 <b>Выбери игру</b>",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔫 Русская рулетка",
                                          callback_data=f"game:roulette:{chat_id}:{bc}")],
                    [InlineKeyboardButton(text="⭕❌ Крестики-нолики",
                                          callback_data=f"game:ttt:{chat_id}:{bc}")],
                    [InlineKeyboardButton(text="🎲 Угадай число",
                                          callback_data=f"game:guess:{chat_id}:{bc}")],
                ])
            )
        else:
            await edit("❓ Неизвестная команда. Напиши <code>.help</code>")

    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.warning("Business command failed: %s", e)
        try:
            await bot.send_message(owner_id, f"⚠️ Команда <code>{esc(cmd)}</code> не выполнена: {esc(e)}")
        except Exception:
            pass


# 2) Guess number input — до общего
@router.business_message(GuessGameActive())
async def guess_input(message: Message):
    await save_business_message(message)
    if not message.from_user:
        return
    chat_id = message.chat.id
    active = [(k, v) for k, v in guess_games.items() if k[0] == chat_id]
    if not active:
        return
    key, game = active[-1]
    try:
        guess = int(message.text)
    except (ValueError, TypeError):
        return
    if not 1 <= guess <= 100:
        return
    game["tries"] += 1
    if guess == game["number"]:
        await message.answer(f"🎉 <b>Верно!</b> Число <b>{guess}</b>. Попыток: {game['tries']}.")
        guess_games.pop(key, None)
    elif guess < game["number"]:
        await message.answer("⬆️ Больше")
    else:
        await message.answer("⬇️ Меньше")


# 3) Общий обработчик — ПОСЛЕДНИМ
@router.business_message()
async def business_message_handler(message: Message, bot: Bot):
    await save_business_message(message)
    if not message.business_connection_id:
        return
    sender = message.from_user
    if not sender or sender.id == owner_id:
        return

    # Mute: удаление и уведомление
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
                        text=f"🔇 <i>Пользователь в муте до {until.astimezone().strftime('%H:%M:%S')}</i>",
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
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM mutes WHERE chat_id=? AND target_user_id=?",
                                 (message.chat.id, sender.id))
                await db.commit()
        except Exception:
            pass

    s = await get_settings()

    # Первый ответ
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
            log.info("First reply unavailable: %s", e)

    # Автоответ вне часов
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
                log.info("Off-hours reply unavailable: %s", e)

    # Пересылка владельцу
    if s["forward_to_owner_enabled"]:
        try:
            head = (
                f"📥 <b>Новое сообщение</b>\n"
                f"👤 {esc(sender.full_name)} <code>{sender.id}</code>\n"
                f"💬 Чат: <code>{message.chat.id}</code>"
            )
            if message.text or message.caption:
                await bot.send_message(
                    owner_id,
                    f"{head}\n\n{esc(message.text or message.caption)}"
                )
            else:
                await bot.send_message(owner_id, head)
                try:
                    await bot.forward_message(owner_id, message.chat.id, message.message_id)
                except Exception:
                    pass
        except Exception as e:
            log.info("Forward to owner unavailable: %s", e)


# ---------- Deleted messages ----------
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
            "🗑 <b>Удалённое сообщение</b>\n"
            "─────────────────\n"
            f"💬 Чат: <code>{event.chat.id}</code>\n"
            f"👤 Кто: <b>{esc(row['user_name'])}</b> <code>{row['user_id']}</code>\n"
            f"🕐 Когда: {esc(stamp)}\n"
            f"📎 Тип: <i>{esc(row['content_type'])}</i> ({direction})\n\n"
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
            log.warning("Could not notify deleted message %s: %s", mid, e)


# ---------- Games ----------
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
            callback_data=f"roulette:spin:{chat_id}:{call.message.message_id}:{bc}"
        )]])
        await call.message.edit_text(
            "🔫 <b>Русская рулетка</b>\n6 камер, 1 патрон. Нажми «Крутить».",
            reply_markup=kb
        )
    elif kind == "guess":
        number = random.randint(1, 100)
        guess_games[(chat_id, call.message.message_id)] = {"number": number, "tries": 0, "bc": bc}
        await call.message.edit_text("🎲 <b>Угадай число</b>\nЯ загадал от 1 до 100. Отправляй догадки в чат.")
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
        kb.append([
            InlineKeyboardButton(
                text=game["board"][r*3+c] or "·",
                callback_data=f"ttt:move:{chat_id}:{mid}:{r*3+c}"
            ) for c in range(3)
        ])
    await message.edit_text(
        f"⭕❌ <b>Крестики-нолики</b>\nХод: <b>{game['turn']}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


@router.callback_query(F.data.startswith("roulette:spin:"))
async def roulette_spin(call: CallbackQuery):
    p = call.data.split(":")
    chat_id, mid = int(p[2]), int(p[3])
    key = (chat_id, mid)
    if key not in roulette_games:
        await call.answer("Игра уже закончена", show_alert=True)
        return
    if random.randint(1, 6) == 1:
        roulette_games.pop(key, None)
        await call.message.edit_text("💥 <b>Бах!</b> Игра окончена.")
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="🔄 Крутить", callback_data=call.data
        )]])
        await call.message.edit_text("😮‍💨 Щёлк! Повезло. Продолжить?", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("ttt:move:"))
async def ttt_move(call: CallbackQuery):
    p = call.data.split(":")
    chat_id, mid, idx = int(p[2]), int(p[3]), int(p[4])
    key = (chat_id, mid)
    game = ttt_games.get(key)
    if not game or game["ended"]:
        await call.answer("Игра завершена", show_alert=True)
        return
    uid = call.from_user.id
    if game["opponent"] is None:
        if uid == owner_id:
            await call.answer("Ждём ход собеседника", show_alert=True)
            return
        game["opponent"] = uid
    elif uid not in {owner_id, game["opponent"]}:
        await call.answer("Ты не участвуешь в этой игре", show_alert=True)
        return
    expected_uid = owner_id if game["turn"] == "❌" else game["opponent"]
    if uid != expected_uid:
        await call.answer("Сейчас ход другого игрока", show_alert=True)
        return
    if game["board"][idx]:
        await call.answer("Клетка занята", show_alert=True)
        return
    game["board"][idx] = game["turn"]
    b = game["board"]
    wins = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]
    winner = next((b[a] for a, c, d in wins if b[a] and b[a] == b[c] == b[d]), None)
    if winner:
        game["ended"] = True
        await call.message.edit_text(f"🏆 Победили <b>{winner}</b>!", reply_markup=None)
    elif all(b):
        game["ended"] = True
        await call.message.edit_text("🤝 <b>Ничья!</b>", reply_markup=None)
    else:
        game["turn"] = "⭕" if game["turn"] == "❌" else "❌"
        await render_ttt(call.message, chat_id, mid)
    await call.answer()


# ---------- Business connection hook ----------
@router.business_connection()
async def business_connections_handler(connection: BusinessConnection, bot: Bot):
    log.info("Business connection %s, enabled=%s", connection.id, connection.is_enabled)
    try:
        if connection.is_enabled:
            await bot.send_message(
                owner_id,
                f"✅ <b>Business-подключение активно</b>\nID: <code>{esc(connection.id)}</code>"
            )
        else:
            await bot.send_message(
                owner_id,
                f"⚠️ <b>Business-подключение отключено</b>\nID: <code>{esc(connection.id)}</code>"
            )
    except Exception:
        pass


# ---------- Background task: name with time ----------
async def update_business_name(bot: Bot):
    while True:
        try:
            s = await get_settings()
            if s["time_in_nick_enabled"]:
                me = await bot.get_me()
                base = me.first_name or "Secretary"
                clock = local_now(s["timezone"]).strftime("%H:%M")
                desired = f"{base} [{clock}]"[:64]
                method = getattr(bot, "set_business_account_name", None)
                if method:
                    async with name_update_lock:
                        await method(name=desired)
        except Exception as e:
            log.info("Business account name update unavailable: %s", e)
        await asyncio.sleep(60)


# ---------- Entry point ----------
async def main():
    if not api_token:
        raise RuntimeError("Не найден API_TOKEN в .env")
    if not isinstance(owner_id, int) or owner_id <= 0:
        raise RuntimeError("Укажи корректный числовой OWNER_ID в .env")
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
