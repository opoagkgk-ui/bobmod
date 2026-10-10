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
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BusinessConnection, BusinessMessagesDeleted, InputMediaPhoto, InputMediaVideo,
)

# Читаем существующий .env пользователя: API_TOKEN и OWNER_ID.
load_dotenv()
api_token = os.getenv("API_TOKEN", "").strip()
try:
    owner_id = int(os.getenv("OWNER_ID", "0"))
except ValueError:
    owner_id = 0

DB_PATH = "bot.db"
KEEP_PER_CHAT = 100
MUTE_NOTICE_SECONDS = 3
router = Router()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("secretary")

# In-memory state for simple interactive games; persisted settings/messages live in SQLite.
guess_games = {}
roulette_games = {}
ttt_games = {}
pending_settings = set()
name_update_lock = asyncio.Lock()


def utcnow():
    return datetime.now(timezone.utc)


def local_now(tz_name="UTC"):
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        return datetime.now(timezone.utc)


def fmt_dt(dt):
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def esc(s):
    # HTML escaping for user-controlled content.
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


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
            autotranslate_enabled INTEGER DEFAULT 0,
            translate_lang TEXT DEFAULT 'en'
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
        """)
        await db.commit()


async def get_settings(user=owner_id):
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


async def set_setting(key, value, user=owner_id):
    allowed = {
        "answer_first_enabled", "answer_first_text", "time_in_nick_enabled", "timezone",
        "off_hours_enabled", "off_hours_start", "off_hours_end", "off_hours_text",
        "autotranslate_enabled", "translate_lang"
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


def message_payload(message: Message):
    text = message.text or message.caption or ""
    ctype = "text"
    file_id = None
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
            """INSERT INTO messages(chat_id,message_id,user_id,user_name,text,content_type,media_file_id,date,is_outgoing)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (chat_id, message.message_id, uid, display, text, ctype, file_id,
             message.date.isoformat() if message.date else utcnow().isoformat(), int(outgoing))
        )
        # Keep last 100 by insertion order per chat.
        await db.execute("""DELETE FROM messages WHERE chat_id=? AND id NOT IN
                          (SELECT id FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT ?)""",
                         (chat_id, chat_id, KEEP_PER_CHAT))
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


def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
        [InlineKeyboardButton(text="📖 Помощь", callback_data="help")],
    ])


def settings_menu(s):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💬 Ответ на первое: {'ВКЛ' if s['answer_first_enabled'] else 'ВЫКЛ'}", callback_data="set:first_toggle")],
        [InlineKeyboardButton(text="✏️ Текст первого ответа", callback_data="set:first_text")],
        [InlineKeyboardButton(text=f"🕐 Время в имени: {'ВКЛ' if s['time_in_nick_enabled'] else 'ВЫКЛ'}", callback_data="set:name_toggle")],
        [InlineKeyboardButton(text=f"🌍 Часовой пояс: {s['timezone']}", callback_data="set:timezone")],
        [InlineKeyboardButton(text=f"🌙 Автоответ вне часов: {'ВКЛ' if s['off_hours_enabled'] else 'ВЫКЛ'}", callback_data="set:off_toggle")],
        [InlineKeyboardButton(text="🕒 Часы автоответа", callback_data="set:off_hours")],
        [InlineKeyboardButton(text="✏️ Текст автоответа", callback_data="set:off_text")],
        [InlineKeyboardButton(text=f"🌐 Автоперевод: {'ВКЛ' if s['autotranslate_enabled'] else 'ВЫКЛ'}", callback_data="set:translate_toggle")],
        [InlineKeyboardButton(text="🌐 Язык перевода", callback_data="set:lang")],
        [InlineKeyboardButton(text="🚷 Чёрный список", callback_data="set:blacklist")],
    ])


@router.message(Command("start"))
async def cmd_start(message: Message):
    await get_settings()
    await message.answer(
        "Привет! Я бизнес-бот «Секретарь».\nИспользуй меню ниже для настроек.\n\n"
        "Команды в бизнес-чатах начинаются с точки: .help, .whois, .typing, .mute, .unmute, .game.",
        reply_markup=main_menu()
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "<b>Справка</b>\n/start — меню\n/help — справка\n/settings — настройки\n"
        "/stats — статистика владельцу\n\n"
        "В подключённых Business-чатах: .help, .whois, .typing, .mute 60, .unmute, .game."
    )


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    if message.from_user and message.from_user.id != owner_id:
        await message.answer("Настройки доступны только владельцу.")
        return
    s = await get_settings()
    await message.answer("⚙️ Настройки секретаря:", reply_markup=settings_menu(s))


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
    await message.answer(f"📊 <b>Статистика</b>\nСообщений в архиве: {messages}\nКонтактов: {chats}\n"
                         f"В чёрном списке: {blacklisted}\nАктивных/сохранённых мутов: {mutes}")


@router.callback_query(F.data == "help")
async def cb_help(call: CallbackQuery):
    await call.message.edit_text(
        "<b>Команды</b>\n.help — список\n.whois — карточка контакта\n.typing — эффект печати\n"
        ".mute 60 — мут на 60 секунд\n.unmute — снять мут\n.game — меню игр",
        reply_markup=main_menu()
    )
    await call.answer()


@router.callback_query(F.data == "settings")
async def cb_settings(call: CallbackQuery):
    if not call.from_user or call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    await call.message.edit_text("⚙️ Настройки секретаря:", reply_markup=settings_menu(await get_settings()))
    await call.answer()


@router.callback_query(F.data.startswith("set:"))
async def cb_setting(call: CallbackQuery):
    if not call.from_user or call.from_user.id != owner_id:
        await call.answer("Только владелец", show_alert=True)
        return
    action = call.data.split(":", 1)[1]
    s = await get_settings()
    if action == "first_toggle":
        await set_setting("answer_first_enabled", 0 if s["answer_first_enabled"] else 1)
    elif action == "name_toggle":
        await set_setting("time_in_nick_enabled", 0 if s["time_in_nick_enabled"] else 1)
    elif action == "off_toggle":
        await set_setting("off_hours_enabled", 0 if s["off_hours_enabled"] else 1)
    elif action == "translate_toggle":
        await set_setting("autotranslate_enabled", 0 if s["autotranslate_enabled"] else 1)
    elif action in {"first_text", "off_text", "off_hours", "timezone", "lang", "blacklist"}:
        prompts = {
            "first_text": "Отправь следующим сообщением текст первого ответа.",
            "off_text": "Отправь следующим сообщением текст автоответа.",
            "off_hours": "Отправь часы в формате ЧЧ:ММ-ЧЧ:ММ, например 22:00-08:00.",
            "timezone": "Отправь часовой пояс из IANA, например Europe/Moscow или Europe/Stockholm.",
            "lang": "Отправь код языка перевода, например en, ru, de, fr.",
            "blacklist": "Отправь user_id чтобы добавить в чёрный список, либо -ID чтобы удалить. Оттуда сообщения не сохраняются."
        }
        pending_settings.add(action)
        await call.message.answer(prompts[action])
        await call.answer()
        return
    await call.message.edit_reply_markup(reply_markup=settings_menu(await get_settings()))
    await call.answer("Сохранено")


@router.message(F.chat.type == "private", F.from_user.id == owner_id, ~F.text.startswith("/"))
async def receive_setting_value(message: Message):
    if not pending_settings:
        return
    # A single owner can have only one pending prompt at a time.
    action = next(iter(pending_settings))
    pending_settings.discard(action)
    value = (message.text or "").strip()
    try:
        if action == "first_text":
            await set_setting("answer_first_text", value)
        elif action == "off_text":
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
        elif action == "lang":
            if not re.fullmatch(r"[a-zA-Z-]{2,12}", value):
                raise ValueError("Укажи код языка, например en")
            await set_setting("translate_lang", value.lower())
        elif action == "blacklist":
            target = int(value)
            if target < 0:
                await remove_blacklist(abs(target))
                await message.answer(f"ID {abs(target)} удалён из чёрного списка.")
            else:
                await add_blacklist(target)
                await message.answer(f"ID {target} добавлен в чёрный список.")
            return
        await message.answer("Настройка сохранена.")
    except Exception as e:
        await message.answer(f"Не удалось сохранить: {esc(e)}")
    await message.answer("⚙️ Настройки:", reply_markup=settings_menu(await get_settings()))


@router.business_message()
async def business_message_handler(message: Message, bot: Bot):
    # Business updates are available only for connections whose owner granted relevant rights.
    await save_business_message(message)
    if not message.business_connection_id:
        return
    sender = message.from_user
    if not sender:
        return
    # Don't react to owner's outgoing messages.
    if sender.id == owner_id:
        return

    # Mute handling: delete incoming message if API rights allow it.
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT mute_until FROM mutes WHERE chat_id=? AND target_user_id=?",
                               (message.chat.id, sender.id))
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
                notice = await bot.send_message(
                    chat_id=message.chat.id,
                    text=f"Вы замучены до {until.astimezone().strftime('%H:%M:%S')}",
                    business_connection_id=message.business_connection_id
                )
                async def delete_notice():
                    await asyncio.sleep(MUTE_NOTICE_SECONDS)
                    try:
                        await bot.delete_business_messages(
                            business_connection_id=message.business_connection_id,
                            message_ids=[notice.message_id]
                        )
                    except Exception:
                        pass
                asyncio.create_task(delete_notice())
                return
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM mutes WHERE chat_id=? AND target_user_id=?", (message.chat.id, sender.id))
                await db.commit()
        except Exception:
            pass

    s = await get_settings()
    # First incoming message in this chat.
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT message_count FROM contacts WHERE chat_id=?", (message.chat.id,))
        row = await cur.fetchone()
    count = row[0] if row else 1
    if count == 1 and s["answer_first_enabled"] and s["answer_first_text"]:
        try:
            await bot.send_message(message.chat.id, s["answer_first_text"],
                                   business_connection_id=message.business_connection_id)
        except Exception as e:
            log.info("First reply unavailable: %s", e)

    # Off-hours auto-reply.
    if s["off_hours_enabled"]:
        now = local_now(s["timezone"]).strftime("%H:%M")
        start, end = s["off_hours_start"], s["off_hours_end"]
        is_off = (start <= now < end) if start <= end else (now >= start or now < end)
        if is_off:
            try:
                await bot.send_message(message.chat.id, s["off_hours_text"],
                                       business_connection_id=message.business_connection_id)
            except Exception as e:
                log.info("Off-hours reply unavailable: %s", e)

    # Translation is optional; this implementation uses Telegram's available text only and
    # intentionally does not pretend to translate without a translation provider/API.
    if s["autotranslate_enabled"] and (message.text or message.caption):
        await bot.send_message(
            owner_id,
            f"🌐 Сообщение для перевода ({s['translate_lang']})\n"
            f"От: {esc(sender.full_name)} (ID {sender.id})\n\n{esc(message.text or message.caption)}\n\n"
            "Автоматический перевод не настроен: добавь API переводчика для настоящего перевода."
        )


@router.business_message(F.text.regexp(r"^\.(help|whois|typing|mute(?:\s+\d+)?|unmute|game)$"))
async def business_commands(message: Message, bot: Bot):
    if not message.from_user or message.from_user.id != owner_id or not message.business_connection_id:
        return
    command = (message.text or "").strip()
    chat_id = message.chat.id
    bc = message.business_connection_id
    try:
        if command == ".help":
            text = "Команды: .help, .whois, .typing, .mute <сек>, .unmute, .game"
            await bot.edit_message_text(chat_id=chat_id, message_id=message.message_id,
                                        text=text, business_connection_id=bc)
        elif command == ".whois":
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute("SELECT * FROM contacts WHERE chat_id=?", (chat_id,))
                row = await cur.fetchone()
            if row:
                first = datetime.fromisoformat(row["first_seen"]).astimezone().strftime("%Y-%m-%d %H:%M")
                text = (f"👤 Карточка собеседника\nИмя: {esc(row['user_name'])}\nID: {row['user_id']}\n"
                        f"Username: @{esc(row['username']) if row['username'] else '—'}\n"
                        f"Первый контакт: {first}\nСообщений: {row['message_count']}")
            else:
                text = "Карточка пока недоступна: нет сохранённой истории."
            await bot.edit_message_text(chat_id=chat_id, message_id=message.message_id,
                                        text=text, business_connection_id=bc)
        elif command == ".typing":
            await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING,
                                       business_connection_id=bc)
            await bot.edit_message_text(chat_id=chat_id, message_id=message.message_id,
                                        text="⌨️ Печатает…", business_connection_id=bc)
        elif command.startswith(".mute"):
            parts = command.split()
            seconds = max(1, min(int(parts[1]) if len(parts) > 1 else 60, 86400))
            # Target is the latest known non-owner participant for this chat.
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT user_id FROM messages WHERE chat_id=? AND user_id IS NOT NULL AND user_id!=? ORDER BY id DESC LIMIT 1",
                    (chat_id, owner_id))
                row = await cur.fetchone()
                if not row:
                    await bot.edit_message_text(chat_id=chat_id, message_id=message.message_id,
                                                text="Не удалось определить собеседника.", business_connection_id=bc)
                    return
                target = row[0]
                until = utcnow() + timedelta(seconds=seconds)
                await db.execute("INSERT OR REPLACE INTO mutes(chat_id,target_user_id,mute_until) VALUES(?,?,?)",
                                 (chat_id, target, until.isoformat()))
                await db.commit()
            await bot.edit_message_text(chat_id=chat_id, message_id=message.message_id,
                                        text=f"🔇 Мут установлен на {seconds} сек.", business_connection_id=bc)
        elif command == ".unmute":
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT user_id FROM messages WHERE chat_id=? AND user_id IS NOT NULL AND user_id!=? ORDER BY id DESC LIMIT 1",
                    (chat_id, owner_id))
                row = await cur.fetchone()
                if row:
                    await db.execute("DELETE FROM mutes WHERE chat_id=? AND target_user_id=?", (chat_id, row[0]))
                    await db.commit()
            await bot.edit_message_text(chat_id=chat_id, message_id=message.message_id,
                                        text="🔊 Мут снят.", business_connection_id=bc)
        elif command == ".game":
            await bot.edit_message_text(
                chat_id=chat_id, message_id=message.message_id, business_connection_id=bc,
                text="🎮 Выбери игру:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔫 Русская рулетка", callback_data=f"game:roulette:{chat_id}:{bc}")],
                    [InlineKeyboardButton(text="⭕❌ Крестики-нолики", callback_data=f"game:ttt:{chat_id}:{bc}")],
                    [InlineKeyboardButton(text="🎲 Угадай число", callback_data=f"game:guess:{chat_id}:{bc}")],
                ])
            )
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.warning("Business command failed: %s", e)
        try:
            await bot.send_message(owner_id, f"Команда {esc(command)} не выполнена: {esc(e)}")
        except Exception:
            pass


@router.business_messages_deleted()
async def business_deleted_handler(event: BusinessMessagesDeleted, bot: Bot):
    for mid in event.message_ids:
        row = await find_message(event.chat.id, mid)
        if not row:
            continue
        stamp = row["date"]
        try:
            stamp = datetime.fromisoformat(stamp).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        direction = "исходящее" if row["is_outgoing"] else "входящее"
        caption = (f"🗑 <b>Удалённое сообщение</b>\nЧат: {event.chat.id}\n"
                   f"Кто: {esc(row['user_name'])} (ID {row['user_id']})\n"
                   f"Когда: {esc(stamp)}\nТип: {esc(row['content_type'])} / {direction}\n"
                   f"Текст: {esc(row['text'] or '—')}")
        try:
            if row["media_file_id"] and row["content_type"] in {"photo", "video", "animation", "document", "voice", "audio", "sticker"}:
                if row["content_type"] == "photo":
                    await bot.send_photo(owner_id, row["media_file_id"], caption=caption[:1024])
                elif row["content_type"] == "video":
                    await bot.send_video(owner_id, row["media_file_id"], caption=caption[:1024])
                elif row["content_type"] == "animation":
                    await bot.send_animation(owner_id, row["media_file_id"], caption=caption[:1024])
                elif row["content_type"] == "document":
                    await bot.send_document(owner_id, row["media_file_id"], caption=caption[:1024])
                elif row["content_type"] == "voice":
                    await bot.send_voice(owner_id, row["media_file_id"], caption=caption[:1024])
                elif row["content_type"] == "audio":
                    await bot.send_audio(owner_id, row["media_file_id"], caption=caption[:1024])
                elif row["content_type"] == "sticker":
                    await bot.send_message(owner_id, caption)
                    await bot.send_sticker(owner_id, row["media_file_id"])
            else:
                await bot.send_message(owner_id, caption)
        except Exception as e:
            log.warning("Could not notify deleted message %s: %s", mid, e)


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
            text="🔄 Крутить", callback_data=f"roulette:spin:{chat_id}:{call.message.message_id}:{bc}")]])
        await call.message.edit_text("🔫 Русская рулетка: 6 камер, 1 патрон. Нажми «Крутить».", reply_markup=kb)
    elif kind == "guess":
        number = random.randint(1, 100)
        guess_games[(chat_id, call.message.message_id)] = {"number": number, "tries": 0, "bc": bc}
        await call.message.edit_text("🎲 Угадай число от 1 до 100. Отправляй число в этот чат.")
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
        kb.append([InlineKeyboardButton(text=game["board"][r*3+c] or "·",
                                        callback_data=f"ttt:move:{chat_id}:{mid}:{r*3+c}")
                   for c in range(3)])
    await message.edit_text(f"⭕❌ Крестики-нолики\nХод: {game['turn']}", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


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
        await call.message.edit_text("💥 Бах! Игра окончена.")
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="🔄 Крутить", callback_data=call.data)]])
        await call.message.edit_text("😮💨 Щёлк! Повезло. Продолжить?", reply_markup=kb)
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
    winner = next((b[a] for a,c,d in wins if b[a] and b[a] == b[c] == b[d]), None)
    if winner:
        game["ended"] = True
        await call.message.edit_text(f"⭕❌ Победили {winner}!", reply_markup=None)
    elif all(b):
        game["ended"] = True
        await call.message.edit_text("⭕❌ Ничья!", reply_markup=None)
    else:
        game["turn"] = "⭕" if game["turn"] == "❌" else "❌"
        await render_ttt(call.message, chat_id, mid)
    await call.answer()


@router.business_message(F.text.regexp(r"^\d{1,3}$"))
async def guess_input(message: Message):
    # Accept guesses only in chats with an active number game.
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
        await message.answer(f"🎉 Верно! Число {guess}. Попыток: {game['tries']}.")
        guess_games.pop(key, None)
    elif guess < game["number"]:
        await message.answer("Больше ⬆️")
    else:
        await message.answer("Меньше ⬇️")


async def business_connections_handler(connection: BusinessConnection, bot: Bot):
    # Placeholder hook: permissions are supplied by Telegram; commands fail gracefully if missing.
    log.info("Business connection %s, enabled=%s", connection.id, connection.is_enabled)


async def update_business_name(bot: Bot):
    # Telegram method is exposed in some Bot API/aiogram versions; not all deployments support it.
    while True:
        try:
            s = await get_settings()
            if s["time_in_nick_enabled"]:
                me = await bot.get_me()
                name = me.first_name or "Secretary"
                clock = local_now(s["timezone"]).strftime("%H:%M")
                desired = f"{name} [{clock}]"[:64]
                method = getattr(bot, "set_business_account_name", None)
                if method:
                    async with name_update_lock:
                        await method(name=desired)
        except Exception as e:
            log.info("Business account name update unavailable: %s", e)
        await asyncio.sleep(60)


async def main():
    if not api_token:
        raise RuntimeError("Не найден API_TOKEN в .env")
    if not isinstance(owner_id, int) or owner_id <= 0:
        raise RuntimeError("Укажи корректный числовой OWNER_ID в .env")
    await init_db()
    bot = Bot(api_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    # Keep the optional account-name task isolated so a missing API method won't stop polling.
    asyncio.create_task(update_business_name(bot))
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types() + [
        "business_connection", "business_message", "edited_business_message", "deleted_business_messages"
    ])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass 
