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
BOT_USERNAME = "bobmod_robot"
CHANNEL_USERNAME = "bobmodinfo"
CHANNEL_LINK = f"https://t.me/{CHANNEL_USERNAME}"

SYSTEM_PROMPT = (
    "Ты BobMod — дружелюбный ассистент в Telegram-чатах. "
    "Отвечай на русском, спокойно и по делу, без официоза и без панибратства. "
    "Кратко — 2-4 предложения. Без 'конечно!' и 'отличный вопрос!'. "
    "Если не знаешь ответа — скажи прямо, не выдумывай."
)

DB_PATH = "bot.db"
BACKUP_DIR = "backups"
KEEP_PER_CHAT = 100
MUTE_NOTICE_SECONDS = 4
FREE_DAILY_LIMIT = 5
PREMIUM_DAILY_LIMIT = 25
PREMIUM_PRICE_STARS = 50
PREMIUM_DAYS = 30
AI_COOLDOWN_FREE = 10
AI_COOLDOWN_PREMIUM = 5
DOWNLOAD_MAX_MB = 45
ANIM_MAX_CHARS = 150
ANIM_DELAY = 0.25
REF_BONUS_HOURS = 12
REF_BONUS_PURCHASE_DAYS = 5
SUB_CACHE_TTL = 60
BACKUP_INTERVAL = 3600

TIMEZONES = [
    ("🇷🇺 Москва (UTC+3)", "Europe/Moscow"),
    ("🇷🇺 Калининград (UTC+2)", "Europe/Kaliningrad"),
    ("🇷🇺 Екатеринбург (UTC+5)", "Asia/Yekaterinburg"),
    ("🇷🇺 Новосибирск (UTC+7)", "Asia/Novosibirsk"),
    ("🇺🇦 Киев (UTC+2)", "Europe/Kyiv"),
    ("🇧🇾 Минск (UTC+3)", "Europe/Minsk"),
    ("🇰🇿 Алматы (UTC+5)", "Asia/Almaty"),
    ("🇬🇧 Лондон (UTC+0)", "Europe/London"),
    ("🇩🇪 Берлин (UTC+1)", "Europe/Berlin"),
    ("🇫🇮 Хельсинки (UTC+2)", "Europe/Helsinki"),
    ("🇹🇷 Стамбул (UTC+3)", "Europe/Istanbul"),
    ("🇦🇪 Дубай (UTC+4)", "Asia/Dubai"),
    ("🇮🇳 Дели (UTC+5:30)", "Asia/Kolkata"),
    ("🇨🇳 Пекин (UTC+8)", "Asia/Shanghai"),
    ("🇯🇵 Токио (UTC+9)", "Asia/Tokyo"),
    ("🇺🇸 Нью-Йорк (UTC-5)", "America/New_York"),
    ("🇺🇸 Лос-Анджелес (UTC-8)", "America/Los_Angeles"),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bobmod")
router = Router()

# ---------------- State ----------------
guess_games = {}
roulette_games = {}
ttt_games = {}
pending_inputs = {}      # owner_id -> action
pending_admin = {}       # owner_id -> admin action
sub_cache = {}           # user_id -> (is_subscribed, ts)
name_update_lock = asyncio.Lock()
bot_ref: Bot = None


# ---------------- AI providers ----------------
AI_PROVIDERS = [
    {
        "name": "groq",
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "key_env": "GROQ_API_KEY",
        "model": "openai/gpt-oss-120b",
    },
    {
        "name": "groq-20b",
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "key_env": "GROQ_API_KEY",
        "model": "openai/gpt-oss-20b",
    },
    {
        "name": "openrouter",
        "url": "https://openrouter.ai/api/v1/chat/completions",
        "key_env": "OPENROUTER_API_KEY",
        "model": "openai/gpt-oss-120b:free",
    },
    {
        "name": "mistral",
        "url": "https://api.mistral.ai/v1/chat/completions",
        "key_env": "MISTRAL_API_KEY",
        "model": "mistral-small-latest",
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


def watermark():
    return f"\n\n<i>via @{BOT_USERNAME}</i>"


def with_watermark(text, is_premium):
    return text if is_premium else text + watermark()


URL_RE = re.compile(r"https?://[^\s]+")


def week_start():
    now = utcnow()
    return (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


# ---------------- DB ----------------
async def init_db():
    os.makedirs(BACKUP_DIR, exist_ok=True)
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
            premium_until TEXT,
            last_ai_request TEXT
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            username TEXT,
            joined_at TEXT NOT NULL,
            is_banned INTEGER DEFAULT 0,
            banned_at TEXT,
            banned_reason TEXT,
            banned_ref INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS referrals (
            user_id INTEGER PRIMARY KEY,
            referrer_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            rewarded_join INTEGER DEFAULT 0,
            rewarded_purchase INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_ref_referrer ON referrals(referrer_id);

        CREATE TABLE IF NOT EXISTS business_connections (
            id TEXT PRIMARY KEY,
            user_id INTEGER,
            enabled INTEGER DEFAULT 1,
            added_at TEXT
        );

        CREATE TABLE IF NOT EXISTS subscribers_log (
            user_id INTEGER PRIMARY KEY,
            first_seen TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS admin_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            target_id INTEGER,
            details TEXT,
            ts TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS payments_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            stars INTEGER NOT NULL,
            days INTEGER NOT NULL,
            ts TEXT NOT NULL
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


async def log_admin(action, target_id=None, details=""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO admin_log(admin_id, action, target_id, details, ts) VALUES(?,?,?,?,?)",
            (owner_id, action, target_id, details, utcnow().isoformat())
        )
        await db.commit()


# ---------------- Users ----------------
async def ensure_user(user_id, first_name=None, username=None):
    today = utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO user_limits(user_id, date, requests_count) VALUES(?,?,0)",
            (user_id, today)
        )
        await db.execute(
            "INSERT INTO users(user_id, first_name, username, joined_at) VALUES(?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET first_name=excluded.first_name, username=excluded.username",
            (user_id, first_name or "", username or "", utcnow().isoformat())
        )
        await db.execute(
            "INSERT OR IGNORE INTO subscribers_log(user_id, first_seen) VALUES(?,?)",
            (user_id, utcnow().isoformat())
        )
        await db.commit()


async def is_banned(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT is_banned FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return bool(row and row[0])


async def is_ref_banned(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT banned_ref FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return bool(row and row[0])


async def ban_user(user_id, reason=""):
    await ensure_user(user_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET is_banned=1, banned_at=?, banned_reason=? WHERE user_id=?",
            (utcnow().isoformat(), reason, user_id)
        )
        await db.commit()
    await log_admin("ban", user_id, reason)


async def unban_user(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET is_banned=0, banned_at=NULL, banned_reason=NULL WHERE user_id=?",
            (user_id,)
        )
        await db.commit()
    await log_admin("unban", user_id)


async def ban_ref(user_id, flag=True):
    await ensure_user(user_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET banned_ref=? WHERE user_id=?", (int(flag), user_id))
        await db.commit()
    await log_admin("ban_ref" if flag else "unban_ref", user_id)


async def get_user_card(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        u = await cur.fetchone()
        cur = await db.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (user_id,)
        )
        refs = (await cur.fetchone())[0]
        cur = await db.execute(
            "SELECT COUNT(*) FROM messages WHERE user_id=?", (user_id,)
        )
        msgs = (await cur.fetchone())[0]
    return dict(u) if u else None, refs, msgs


# ---------------- Premium ----------------
async def is_premium(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT is_premium, premium_until FROM user_limits WHERE user_id=?",
            (user_id,)
        )
        row = await cur.fetchone()
        if not row or not row["is_premium"] or not row["premium_until"]:
            return False
        try:
            if datetime.fromisoformat(row["premium_until"]) < utcnow():
                await db.execute(
                    "UPDATE user_limits SET is_premium=0, premium_until=NULL WHERE user_id=?",
                    (user_id,)
                )
                await db.commit()
                return False
            return True
        except Exception:
            return False


async def grant_premium(user_id, hours=0, days=0):
    await ensure_user(user_id)
    delta = timedelta(hours=hours, days=days)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT premium_until FROM user_limits WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        base = utcnow()
        if row and row["premium_until"]:
            try:
                cur_until = datetime.fromisoformat(row["premium_until"])
                if cur_until > base:
                    base = cur_until
            except Exception:
                pass
        new_until = base + delta
        await db.execute(
            "UPDATE user_limits SET is_premium=1, premium_until=? WHERE user_id=?",
            (new_until.isoformat(), user_id)
        )
        await db.commit()
        return new_until


async def revoke_premium(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE user_limits SET is_premium=0, premium_until=NULL WHERE user_id=?",
            (user_id,)
        )
        await db.commit()
    await log_admin("revoke_premium", user_id)


# ---------------- Limits + cooldown ----------------
async def check_limit(user_id):
    today = utcnow().strftime("%Y-%m-%d")
    premium = await is_premium(user_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM user_limits WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row or row["date"] != today:
            await db.execute(
                "INSERT OR REPLACE INTO user_limits(user_id, date, requests_count, is_premium, premium_until, last_ai_request) "
                "VALUES(?,?,0,?,?,?)",
                (user_id, today,
                 row["is_premium"] if row else 0,
                 row["premium_until"] if row else None,
                 None)
            )
            await db.commit()
            used = 0
        else:
            used = row["requests_count"]
        limit = PREMIUM_DAILY_LIMIT if premium else FREE_DAILY_LIMIT
        return used < limit, used, limit, premium


async def check_cooldown(user_id):
    premium = await is_premium(user_id)
    cooldown = AI_COOLDOWN_PREMIUM if premium else AI_COOLDOWN_FREE
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT last_ai_request FROM user_limits WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row or not row["last_ai_request"]:
            return True, 0
        try:
            last = datetime.fromisoformat(row["last_ai_request"])
            elapsed = (utcnow() - last).total_seconds()
            if elapsed < cooldown:
                return False, int(cooldown - elapsed) + 1
            return True, 0
        except Exception:
            return True, 0


async def increment_usage(user_id):
    today = utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE user_limits SET requests_count = requests_count + 1, last_ai_request=? "
            "WHERE user_id=? AND date=?",
            (utcnow().isoformat(), user_id, today)
        )
        await db.commit()


# ---------------- Referrals ----------------
async def register_referral(new_user_id, referrer_id):
    if new_user_id == referrer_id or referrer_id == 0:
        return False
    if await is_ref_banned(referrer_id):
        return False
    if await is_banned(referrer_id):
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM referrals WHERE user_id=?", (new_user_id,))
        if await cur.fetchone():
            return False
        await db.execute(
            "INSERT INTO referrals(user_id, referrer_id, joined_at, rewarded_join) VALUES(?,?,?,0)",
            (new_user_id, referrer_id, utcnow().isoformat())
        )
        await db.commit()
    return True


async def mark_referral_rewarded(new_user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE referrals SET rewarded_join=1 WHERE user_id=?", (new_user_id,))
        await db.commit()


async def referral_stats(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*), SUM(rewarded_purchase) FROM referrals WHERE referrer_id=?",
            (user_id,)
        )
        row = await cur.fetchone()
        return (row[0] or 0), (row[1] or 0)


async def referral_leaderboard(limit=5, week_only=True):
    if week_only:
        start = week_start().isoformat()
        q = """SELECT r.referrer_id, COUNT(*) as cnt
               FROM referrals r
               WHERE r.joined_at >= ?
               GROUP BY r.referrer_id
               ORDER BY cnt DESC LIMIT ?"""
        args = (start, limit)
    else:
        q = """SELECT referrer_id, COUNT(*) as cnt
               FROM referrals
               GROUP BY referrer_id
               ORDER BY cnt DESC LIMIT ?"""
        args = (limit,)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(q, args)
        rows = await cur.fetchall()
    result = []
    for row in rows:
        async with aiosqlite.connect(DB_PATH) as db:
            cur2 = await db.execute("SELECT first_name, username FROM users WHERE user_id=?", (row["referrer_id"],))
            r2 = await cur2.fetchone()
            if r2:
                name = r2[1] or r2[0] or f"id{row['referrer_id']}"
            else:
                name = f"id{row['referrer_id']}"
        result.append((row["referrer_id"], name, row["cnt"]))
    return result


# ---------------- Message payload / save ----------------
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


# ---------------- Subscription ----------------
async def check_subscription(user_id):
    now_ts = utcnow().timestamp()
    if user_id in sub_cache:
        cached, ts = sub_cache[user_id]
        if now_ts - ts < SUB_CACHE_TTL:
            return cached
    try:
        member = await bot_ref.get_chat_member(f"@{CHANNEL_USERNAME}", user_id)
        ok = member.status not in ("left", "kicked")
    except Exception as e:
        log.warning("sub check failed: %s", e)
        ok = True
    sub_cache[user_id] = (ok, now_ts)
    return ok


def sub_screen_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться", url=CHANNEL_LINK)],
        [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")],
    ])


async def show_sub_screen(message: Message):
    await message.answer(
        "🔒 <b>Доступ закрыт</b>\n"
        "─────────────\n"
        "Чтобы пользоваться <b>BobMod</b>, подпишитесь на канал:\n\n"
        f"📢 <b>@{CHANNEL_USERNAME}</b>\n"
        "<i>Обновления и новости бота</i>",
        reply_markup=sub_screen_kb()
    )


async def require_sub(message_or_call) -> bool:
    user = message_or_call.from_user
    if not user:
        return False
    if user.id == owner_id:
        return True
    if await is_banned(user.id):
        if isinstance(message_or_call, Message):
            await message_or_call.answer("⛔ Вы заблокированы в BobMod.")
        else:
            await message_or_call.answer("⛔ Вы заблокированы", show_alert=True)
        return False
    if not await check_subscription(user.id):
        if isinstance(message_or_call, Message):
            await show_sub_screen(message_or_call)
        else:
            await message_or_call.answer("❌ Сначала подпишитесь на канал!", show_alert=True)
        return False
    return True


@router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    user_id = call.from_user.id
    sub_cache.pop(user_id, None)
    try:
        member = await bot_ref.get_chat_member(f"@{CHANNEL_USERNAME}", user_id)
        ok = member.status not in ("left", "kicked")
    except Exception:
        ok = True
    if not ok:
        await call.answer("❌ Вы ещё не подписаны!", show_alert=True)
        return
    await ensure_user(user_id, call.from_user.first_name, call.from_user.username)
    await call.answer("✅ Спасибо за подписку!", show_alert=True)
    try:
        await call.message.edit_text(
            f"🎉 <b>Добро пожаловать в {BOT_NAME}!</b>\n\n"
            "Нажмите /start, чтобы начать.",
        )
    except Exception:
        pass


# ---------------- Menus ----------------
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings")],
        [InlineKeyboardButton(text="🎁 Рефералка", callback_data="menu:ref")],
        [InlineKeyboardButton(text="🤖 Bob AI", callback_data="menu:ai")],
        [InlineKeyboardButton(text="📖 Возможности", callback_data="menu:help")],
    ])


def back_button(target="menu:main"):
    return [InlineKeyboardButton(text="⬅️ Назад", callback_data=target)]


def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats")],
        [InlineKeyboardButton(text="🎁 Рефералы", callback_data="adm:refs")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm:users")],
        [InlineKeyboardButton(text="🚫 Баны", callback_data="adm:bans")],
        [InlineKeyboardButton(text="⚙️ Система", callback_data="adm:sys")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])

# ============================================================
# PRIVATE HANDLERS
# ============================================================

@router.message(Command("start"))
async def cmd_start(message: Message, command: Command):
    user = message.from_user
    if not user:
        return

    if user.id != owner_id and not await check_subscription(user.id):
        await show_sub_screen(message)
        return

    await ensure_user(user.id, user.first_name, user.username)

    if command.args and command.args.startswith("ref_"):
        try:
            referrer_id = int(command.args[4:])
            if await register_referral(user.id, referrer_id):
                until = await grant_premium(referrer_id, hours=REF_BONUS_HOURS)
                await mark_referral_rewarded(user.id)
                log.info("Referral: %s → %s (+%dh)", user.id, referrer_id, REF_BONUS_HOURS)
                try:
                    await bot_ref.send_message(
                        referrer_id,
                        f"🎉 <b>+{REF_BONUS_HOURS} часов Premium</b>\n"
                        f"По вашей ссылке пришёл новый пользователь.\n"
                        f"Premium активен до: <b>{until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>"
                    )
                except Exception:
                    pass
        except (ValueError, IndexError):
            pass

    await get_settings()
    premium = await is_premium(user.id)
    status = "💎 Premium" if premium else "🆓 Бесплатный тариф"

    await message.answer(
        f"👋 <b>Привет! Я {BOT_NAME}.</b>\n"
        f"Статус: {status}\n\n"
        "Помогаю в ваших Telegram-чатах:\n"
        "• автоматические ответы\n"
        "• сохранение удалённых сообщений\n"
        "• ИИ-помощник через <code>.ai</code>\n"
        "• и другие команды\n\n"
        "<i>Подключение: Настройки → Telegram Business → Чат-автоматизация</i>",
        reply_markup=main_menu()
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    if not await require_sub(message):
        return
    await message.answer(
        "<b>📖 Возможности бота</b>\n"
        "─────────────\n"
        "<b>Команды в бизнес-чатах:</b>\n"
        "<code>.ai вопрос</code> — запрос к ИИ\n"
        "<code>.save</code> (ответом на ссылку) — загрузка видео\n"
        "<code>.anim текст</code> — анимация текста\n"
        "<code>.qr текст</code> — QR-код\n"
        "<code>.whois</code> — информация о собеседнике\n"
        "<code>.typing</code> — показать «печатает»\n"
        "<code>.mute 60</code> · <code>.unmute</code> · <code>.muted</code>\n"
        "<code>.note</code> · <code>.id</code> · <code>.ping</code> · <code>.game</code>\n\n"
        "<b>В личных сообщениях:</b>\n"
        "/start · /settings · /ref · /stats",
        reply_markup=main_menu()
    )


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    if not await require_sub(message):
        return
    if message.from_user and message.from_user.id != owner_id:
        await message.answer("⛔ Доступ только для владельца.")
        return
    await render_settings(message)


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not message.from_user or message.from_user.id != owner_id:
        return
    await message.answer(
        "🔐 <b>Админ-панель BobMod</b>\n"
        "─────────────\n"
        "Выберите раздел:",
        reply_markup=admin_menu()
    )


async def render_settings(msg_or_call, edit=False):
    s = await get_settings()
    text = (
        "⚙️ <b>Настройки BobMod</b>\n"
        "─────────────\n"
        "Выберите раздел для настройки:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💬 Первый ответ: {'✅' if s['answer_first_enabled'] else '❌'}",
                              callback_data="toggle:answer_first_enabled")],
        [InlineKeyboardButton(text="✏️ Текст первого ответа", callback_data="prompt:answer_first_text")],
        [InlineKeyboardButton(text=f"🌙 Автоответ: {'✅' if s['off_hours_enabled'] else '❌'}",
                              callback_data="toggle:off_hours_enabled")],
        [InlineKeyboardButton(text=f"🕒 Часы: {s['off_hours_start']}–{s['off_hours_end']}",
                              callback_data="prompt:off_hours")],
        [InlineKeyboardButton(text="✏️ Текст автоответа", callback_data="prompt:off_hours_text")],
        [InlineKeyboardButton(text=f"📥 Пересылка вам: {'✅' if s['forward_to_owner_enabled'] else '❌'}",
                              callback_data="toggle:forward_to_owner_enabled")],
        [InlineKeyboardButton(text=f"🌍 Часовой пояс: {s['timezone']}",
                              callback_data="tz:open")],
        [InlineKeyboardButton(text=f"🕐 Время в имени: {'✅' if s['time_in_nick_enabled'] else '❌'}",
                              callback_data="toggle:time_in_nick_enabled")],
        back_button(),
    ])
    if edit:
        try:
            await msg_or_call.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            await msg_or_call.answer(text, reply_markup=kb)
    else:
        await msg_or_call.answer(text, reply_markup=kb)


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not await require_sub(message):
        return
    if not message.from_user or message.from_user.id != owner_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async def count(q, args=()):
            cur = await db.execute(q, args)
            return (await cur.fetchone())[0]
        messages = await count("SELECT COUNT(*) FROM messages")
        chats = await count("SELECT COUNT(*) FROM contacts")
        bl = await count("SELECT COUNT(*) FROM blacklist")
        mutes = await count("SELECT COUNT(*) FROM mutes")
        subs = await count("SELECT COUNT(*) FROM users")
        refs_total = await count("SELECT COUNT(*) FROM referrals")
    await message.answer(
        "📊 <b>Статистика BobMod</b>\n"
        "─────────────\n"
        f"💾 Сообщений: <b>{messages}</b>\n"
        f"👥 Чатов: <b>{chats}</b>\n"
        f"👤 Пользователей бота: <b>{subs}</b>\n"
        f"🎁 Рефералов: <b>{refs_total}</b>\n"
        f"🚫 В чёрном списке: <b>{bl}</b>\n"
        f"🔇 Активных мутов: <b>{mutes}</b>"
    )


# ---------------- Рефералка ----------------
@router.message(Command("ref"))
async def cmd_ref(message: Message):
    if not await require_sub(message):
        return
    user = message.from_user
    if not user:
        return
    await ensure_user(user.id, user.first_name, user.username)

    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    total, purchases = await referral_stats(user.id)
    earned_hours = total * REF_BONUS_HOURS
    earned_days = purchases * REF_BONUS_PURCHASE_DAYS
    premium = await is_premium(user.id)

    text = (
        "🎁 <b>Реферальная программа</b>\n"
        "─────────────\n"
        f"🔗 <b>Ваша ссылка:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"👥 Приглашено: <b>{total}</b>\n"
        f"💎 Часов за друзей: <b>{earned_hours}</b>\n"
        f"💰 Дней за покупки: <b>{earned_days}</b>\n"
        f"Статус: {'💎 Premium' if premium else '🆓 Бесплатный тариф'}\n\n"
        f"<i>За каждого приглашённого: +{REF_BONUS_HOURS} ч Premium</i>\n"
        f"<i>За покупку приглашённого: +{REF_BONUS_PURCHASE_DAYS} дн Premium</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Поделиться ссылкой",
                              url=f"https://t.me/share/url?url={ref_link}&text=Попробуй BobMod!")],
        [InlineKeyboardButton(text="🏆 Битва рефералов", callback_data="ref:battle")],
        back_button(),
    ])
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "menu:ref")
async def cb_menu_ref(call: CallbackQuery):
    if not await require_sub(call):
        return
    user = call.from_user
    await ensure_user(user.id, user.first_name, user.username)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    total, purchases = await referral_stats(user.id)
    earned_hours = total * REF_BONUS_HOURS
    earned_days = purchases * REF_BONUS_PURCHASE_DAYS
    premium = await is_premium(user.id)

    text = (
        "🎁 <b>Реферальная программа</b>\n"
        "─────────────\n"
        f"🔗 <b>Ваша ссылка:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"👥 Приглашено: <b>{total}</b>\n"
        f"💎 Часов за друзей: <b>{earned_hours}</b>\n"
        f"💰 Дней за покупки: <b>{earned_days}</b>\n"
        f"Статус: {'💎 Premium' if premium else '🆓 Бесплатный тариф'}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Поделиться",
                              url=f"https://t.me/share/url?url={ref_link}&text=Попробуй BobMod!")],
        [InlineKeyboardButton(text="🏆 Битва рефералов", callback_data="ref:battle")],
        back_button(),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "ref:battle")
async def cb_ref_battle(call: CallbackQuery):
    if not await require_sub(call):
        return
    top = await referral_leaderboard(5, week_only=True)
    if not top:
        text = (
            "🏆 <b>Битва рефералов</b>\n"
            "─────────────\n"
            "На этой неделе пока нет участников.\n"
            "Станьте первым!"
        )
    else:
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        lines = []
        for i, (uid, name, cnt) in enumerate(top):
            lines.append(f"{medals[i]} <b>{esc(name)}</b> — {cnt}")
        text = (
            "🏆 <b>Битва рефералов</b>\n"
            "─────────────\n"
            "Топ за эту неделю:\n\n"
            + "\n".join(lines)
            + "\n\n🎁 Победитель получает <b>месяц Premium</b>.\n"
            "<i>Итоги в понедельник в @bobmodinfo</i>"
        )
    kb = InlineKeyboardMarkup(inline_keyboard=[back_button("menu:ref")])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


# ---------------- AI-меню ----------------
@router.callback_query(F.data == "menu:ai")
async def cb_ai_menu(call: CallbackQuery):
    if not await require_sub(call):
        return
    user_id = call.from_user.id
    premium = await is_premium(user_id)
    _, used, limit, _ = await check_limit(user_id)
    cooldown = AI_COOLDOWN_PREMIUM if premium else AI_COOLDOWN_FREE

    filled = int((used / limit) * 10) if limit else 0
    bar = "█" * filled + "░" * (10 - filled)

    status = "💎 <b>Premium</b>" if premium else "🆓 <b>Бесплатный тариф</b>"
    text = (
        "🤖 <b>Bob AI</b>\n"
        "─────────────\n"
        f"Статус: {status}\n"
        f"Использовано сегодня: <b>{used}/{limit}</b>\n"
        f"<code>{bar}</code>\n"
        f"⏱ Задержка между запросами: <b>{cooldown} сек</b>\n\n"
        f"💎 Premium: <b>{PREMIUM_DAILY_LIMIT} запросов в день</b>\n"
        f"Цена: <b>{PREMIUM_PRICE_STARS} ⭐</b> на {PREMIUM_DAYS} дней\n\n"
        f"Использование: <code>.ai вопрос</code> в бизнес-чате"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⭐ Купить Premium ({PREMIUM_PRICE_STARS}⭐)",
                              callback_data="buy_premium")],
        back_button(),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "menu:main")
async def cb_main(call: CallbackQuery):
    if not await require_sub(call):
        return
    try:
        await call.message.edit_text("🏠 <b>Главное меню</b>", reply_markup=main_menu())
    except TelegramBadRequest:
        await call.message.answer("🏠 <b>Главное меню</b>", reply_markup=main_menu())
    await call.answer()


@router.callback_query(F.data == "menu:help")
async def cb_help(call: CallbackQuery):
    if not await require_sub(call):
        return
    text = (
        "<b>📖 Возможности бота</b>\n"
        "─────────────\n"
        "<b>В бизнес-чатах:</b>\n"
        "<code>.ai</code> · <code>.save</code> · <code>.anim</code> · <code>.qr</code>\n"
        "<code>.whois</code> · <code>.typing</code> · <code>.mute</code>\n"
        "<code>.note</code> · <code>.id</code> · <code>.ping</code> · <code>.game</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[back_button()])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "menu:settings")
async def cb_settings(call: CallbackQuery):
    if not await require_sub(call):
        return
    if call.from_user.id != owner_id:
        await call.answer("Доступ только для владельца", show_alert=True)
        return
    await render_settings(call.message, edit=True)
    await call.answer()


# ---------------- Toggles / prompts ----------------
@router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Доступ только для владельца", show_alert=True)
        return
    key = call.data.split(":", 1)[1]
    s = await get_settings()
    if key not in s:
        await call.answer("Неизвестный параметр", show_alert=True)
        return
    await set_setting(key, 0 if s[key] else 1)
    await render_settings(call.message, edit=True)
    await call.answer("Сохранено")


@router.callback_query(F.data.startswith("prompt:"))
async def cb_prompt(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Доступ только для владельца", show_alert=True)
        return
    action = call.data.split(":", 1)[1]
    prompts = {
        "answer_first_text": "Отправьте текст первого ответа.",
        "off_hours_text": "Отправьте текст автоответа.",
        "off_hours": "Отправьте часы в формате <code>ЧЧ:ММ-ЧЧ:ММ</code>.",
    }
    if action not in prompts:
        await call.answer()
        return
    pending_inputs[owner_id] = action
    await call.message.answer(prompts[action])
    await call.answer()


@router.message(F.chat.type == "private", F.from_user.id == owner_id, ~F.text.startswith("/"))
async def receive_setting_value(message: Message):
    # Сначала админские вводы
    if owner_id in pending_admin:
        action = pending_admin.pop(owner_id)
        value = (message.text or "").strip()
        await handle_admin_input(message, action, value)
        return

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
                raise ValueError("Неверный формат. Ожидается ЧЧ:ММ-ЧЧ:ММ")
            for t in m.groups():
                datetime.strptime(t, "%H:%M")
            await set_setting("off_hours_start", m.group(1))
            await set_setting("off_hours_end", m.group(2))
        elif action == "timezone":
            ZoneInfo(value)
            await set_setting("timezone", value)
        await message.answer("✅ Настройка сохранена")
    except Exception as e:
        await message.answer(f"⚠️ Не удалось сохранить: {esc(e)}")
    await render_settings(message)


# ---------------- Timezones ----------------
@router.callback_query(F.data == "tz:open")
async def cb_tz_open(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Доступ только для владельца", show_alert=True)
        return
    rows = []
    for i in range(0, len(TIMEZONES), 2):
        row = []
        for j in range(2):
            if i + j < len(TIMEZONES):
                label, tz = TIMEZONES[i + j]
                row.append(InlineKeyboardButton(text=label, callback_data=f"tz:set:{tz}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="tz:manual")])
    rows.append(back_button("menu:settings"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text("🌍 <b>Выберите часовой пояс:</b>", reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer("🌍 <b>Выберите часовой пояс:</b>", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("tz:set:"))
async def cb_tz_set(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Доступ только для владельца", show_alert=True)
        return
    tz = call.data.split(":", 2)[2]
    try:
        ZoneInfo(tz)
    except Exception:
        await call.answer("Неизвестный часовой пояс", show_alert=True)
        return
    await set_setting("timezone", tz)
    await call.answer(f"Сохранено: {tz}")
    await render_settings(call.message, edit=True)


@router.callback_query(F.data == "tz:manual")
async def cb_tz_manual(call: CallbackQuery):
    if call.from_user.id != owner_id:
        await call.answer("Доступ только для владельца", show_alert=True)
        return
    pending_inputs[owner_id] = "timezone"
    await call.message.answer("Отправьте IANA-часовой пояс, например <code>Europe/Moscow</code>.")
    await call.answer()


# ---------------- Payments ----------------
@router.callback_query(F.data == "buy_premium")
async def cb_buy_premium(call: CallbackQuery, bot: Bot):
    if not await require_sub(call):
        return
    try:
        await bot.send_invoice(
            chat_id=call.from_user.id,
            title="Bob AI Premium",
            description=f"{PREMIUM_DAILY_LIMIT} запросов в день на {PREMIUM_DAYS} дней",
            payload="premium_30d",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"Premium на {PREMIUM_DAYS} дней",
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
    user_id = message.from_user.id
    p = message.successful_payment
    if p.invoice_payload == "premium_30d":
        until = await grant_premium(user_id, days=PREMIUM_DAYS)

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO payments_log(user_id, stars, days, ts) VALUES(?,?,?,?)",
                (user_id, PREMIUM_PRICE_STARS, PREMIUM_DAYS, utcnow().isoformat())
            )
            await db.commit()

        await message.answer(
            f"✅ <b>Premium активирован</b>\n\n"
            f"Теперь вам доступно <b>{PREMIUM_DAILY_LIMIT} запросов в день</b>.\n"
            f"Действует до: <b>{until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"Спасибо за поддержку!"
        )

        # Уведомление админу
        try:
            name = message.from_user.full_name or f"id{user_id}"
            await bot_ref.send_message(
                owner_id,
                f"💰 <b>Новая покупка</b>\n"
                f"👤 {esc(name)} <code>{user_id}</code>\n"
                f"⭐ {PREMIUM_PRICE_STARS} звёзд · {PREMIUM_DAYS} дней"
            )
        except Exception:
            pass

        # Бонус рефереру
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT referrer_id, rewarded_purchase FROM referrals WHERE user_id=?",
                (user_id,)
            )
            row = await cur.fetchone()
        if row and not row[1]:
            referrer_id = row[0]
            new_until = await grant_premium(referrer_id, days=REF_BONUS_PURCHASE_DAYS)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE referrals SET rewarded_purchase=1 WHERE user_id=?",
                    (user_id,)
                )
                await db.commit()
            try:
                await bot_ref.send_message(
                    referrer_id,
                    f"💰 <b>+{REF_BONUS_PURCHASE_DAYS} дней Premium</b>\n"
                    f"Приглашённый вами пользователь оформил подписку.\n"
                    f"Premium активен до: <b>{new_until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>"
                )
            except Exception:
                pass


# ============================================================
# ADMIN PANEL
# ============================================================

@router.callback_query(F.data == "adm:menu")
async def cb_adm_menu(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    try:
        await call.message.edit_text(
            "🔐 <b>Админ-панель BobMod</b>\n─────────────\nВыберите раздел:",
            reply_markup=admin_menu()
        )
    except TelegramBadRequest:
        await call.message.answer(
            "🔐 <b>Админ-панель BobMod</b>\n─────────────\nВыберите раздел:",
            reply_markup=admin_menu()
        )
    await call.answer()


@router.callback_query(F.data == "adm:stats")
async def cb_adm_stats(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    now = utcnow()
    day_ago = (now - timedelta(days=1)).isoformat()
    week_ago = (now - timedelta(days=7)).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        async def count(q, args=()):
            cur = await db.execute(q, args)
            return (await cur.fetchone())[0]

        users_total = await count("SELECT COUNT(*) FROM users")
        users_24h = await count("SELECT COUNT(*) FROM users WHERE joined_at >= ?", (day_ago,))
        users_7d = await count("SELECT COUNT(*) FROM users WHERE joined_at >= ?", (week_ago,))
        users_banned = await count("SELECT COUNT(*) FROM users WHERE is_banned=1")
        premium_active = await count(
            "SELECT COUNT(*) FROM user_limits WHERE is_premium=1 AND premium_until > ?",
            (now.isoformat(),)
        )
        chats = await count("SELECT COUNT(*) FROM contacts")
        msgs = await count("SELECT COUNT(*) FROM messages")
        mutes = await count("SELECT COUNT(*) FROM mutes")
        bl = await count("SELECT COUNT(*) FROM blacklist")
        refs_total = await count("SELECT COUNT(*) FROM referrals")
        refs_week = await count(
            "SELECT COUNT(*) FROM referrals WHERE joined_at >= ?",
            (week_ago,)
        )
        stars_total = await count("SELECT COALESCE(SUM(stars),0) FROM payments_log")
        purchases = await count("SELECT COUNT(*) FROM payments_log")

    text = (
        "📊 <b>Статистика</b>\n"
        "─────────────\n"
        f"👤 Юзеров: <b>{users_total}</b>\n"
        f"  · за 24ч: {users_24h}\n"
        f"  · за 7д: {users_7d}\n"
        f"  · в бане: {users_banned}\n"
        f"💎 Premium активных: <b>{premium_active}</b>\n\n"
        f"💬 Чатов: <b>{chats}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>\n"
        f"🔇 Мутов: <b>{mutes}</b>\n"
        f"🚫 В ЧС: <b>{bl}</b>\n\n"
        f"🎁 Рефералов всего: <b>{refs_total}</b>\n"
        f"  · за 7д: {refs_week}\n\n"
        f"💰 Продаж Premium: <b>{purchases}</b>\n"
        f"⭐ Звёзд всего: <b>{stars_total}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm:stats")],
        back_button("adm:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "adm:refs")
async def cb_adm_refs(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    top_all = await referral_leaderboard(20, week_only=False)
    top_week = await referral_leaderboard(10, week_only=True)

    lines_all = []
    for i, (uid, name, cnt) in enumerate(top_all, 1):
        lines_all.append(f"{i}. <b>{esc(name)}</b> — {cnt}")
    lines_week = []
    for i, (uid, name, cnt) in enumerate(top_week, 1):
        lines_week.append(f"{i}. <b>{esc(name)}</b> — {cnt}")

    text = (
        "🎁 <b>Рефералы</b>\n"
        "─────────────\n"
        "<b>За всё время:</b>\n"
        + ("\n".join(lines_all) if lines_all else "<i>пусто</i>")
        + "\n\n<b>За эту неделю:</b>\n"
        + ("\n".join(lines_week) if lines_week else "<i>пусто</i>")
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Отправить топ в канал", callback_data="adm:refs_publish")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm:refs")],
        back_button("adm:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "adm:refs_publish")
async def cb_adm_refs_publish(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    ok = await publish_weekly_top(manual=True)
    if ok:
        await call.answer("✅ Отправлено в канал", show_alert=True)
    else:
        await call.answer("❌ Не удалось отправить", show_alert=True)


@router.callback_query(F.data == "adm:users")
async def cb_adm_users(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users ORDER BY joined_at DESC LIMIT 20")
        rows = await cur.fetchall()

    lines = []
    for u in rows:
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        try:
            d = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m")
        except Exception:
            d = "?"
        flag = "🚫" if u["is_banned"] else ""
        lines.append(f"• <b>{esc(name)}</b> <code>{u['user_id']}</code> {d} {flag}")

    text = (
        "👥 <b>Последние 20 юзеров</b>\n"
        "─────────────\n"
        + ("\n".join(lines) if lines else "<i>пусто</i>")
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по ID", callback_data="adm:user_find")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm:users")],
        back_button("adm:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "adm:user_find")
async def cb_adm_user_find(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    pending_admin[owner_id] = "find_user"
    await call.message.answer("Отправьте ID пользователя.")
    await call.answer()


@router.callback_query(F.data.startswith("adm:user_card:"))
async def cb_adm_user_card(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    u, refs, msgs = await get_user_card(uid)
    if not u:
        await call.answer("Юзер не найден", show_alert=True)
        return
    name = u["username"] or u["first_name"] or "—"
    premium = await is_premium(uid)
    try:
        joined = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        joined = "—"

    text = (
        f"👤 <b>Карточка</b>\n"
        "─────────────\n"
        f"📛 <b>{esc(name)}</b>\n"
        f"🆔 <code>{uid}</code>\n"
        f"📅 Регистрация: {esc(joined)}\n"
        f"💎 Premium: {'✅' if premium else '❌'}\n"
        f"🎁 Рефералов: <b>{refs}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>\n"
        f"🚫 Бан: {'✅' if u['is_banned'] else '❌'}\n"
        f"🔗 Реф-бан: {'✅' if u['banned_ref'] else '❌'}"
    )
    rows = [
        [InlineKeyboardButton(text="➕ Выдать Premium", callback_data=f"adm:grant:{uid}")],
        [InlineKeyboardButton(text="➖ Снять Premium", callback_data=f"adm:revoke:{uid}")],
    ]
    if u["is_banned"]:
        rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"adm:unban:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🚫 Забанить", callback_data=f"adm:ban:{uid}")])
    if u["banned_ref"]:
        rows.append([InlineKeyboardButton(text="🔓 Разбанить реф-ссылку", callback_data=f"adm:unban_ref:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🔒 Забанить реф-ссылку", callback_data=f"adm:ban_ref:{uid}")])
    rows.append(back_button("adm:users"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("adm:grant:"))
async def cb_adm_grant(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 день", callback_data=f"adm:grant_do:{uid}:1")],
        [InlineKeyboardButton(text="7 дней", callback_data=f"adm:grant_do:{uid}:7")],
        [InlineKeyboardButton(text="30 дней", callback_data=f"adm:grant_do:{uid}:30")],
        [InlineKeyboardButton(text="365 дней", callback_data=f"adm:grant_do:{uid}:365")],
        back_button(f"adm:user_card_show:{uid}"),
    ])
    try:
        await call.message.edit_text(f"💎 Выдать Premium <code>{uid}</code> на:", reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(f"💎 Выдать Premium <code>{uid}</code> на:", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("adm:grant_do:"))
async def cb_adm_grant_do(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    _, _, uid, days = call.data.split(":")
    uid = int(uid)
    days = int(days)
    until = await grant_premium(uid, days=days)
    await log_admin("grant_premium", uid, f"{days} дней")
    await call.answer(f"✅ Выдано на {days} дн.", show_alert=True)
    try:
        await bot_ref.send_message(
            uid,
            f"💎 <b>Вам выдан Premium!</b>\n"
            f"Срок: {days} дней\n"
            f"До: <b>{until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>"
        )
    except Exception:
        pass
    # Вернуть карточку
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


@router.callback_query(F.data.startswith("adm:revoke:"))
async def cb_adm_revoke(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    await revoke_premium(uid)
    await call.answer("✅ Premium снят", show_alert=True)
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


@router.callback_query(F.data.startswith("adm:ban:"))
async def cb_adm_ban(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    pending_admin[owner_id] = f"ban_reason:{uid}"
    await call.message.answer(f"Отправьте причину бана для <code>{uid}</code> (или «-» без причины).")
    await call.answer()


@router.callback_query(F.data.startswith("adm:unban:"))
async def cb_adm_unban(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    await unban_user(uid)
    await call.answer("✅ Разбанен", show_alert=True)
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


@router.callback_query(F.data.startswith("adm:ban_ref:"))
async def cb_adm_ban_ref(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    await ban_ref(uid, True)
    await call.answer("🔒 Реф-ссылка забанена", show_alert=True)
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


@router.callback_query(F.data.startswith("adm:unban_ref:"))
async def cb_adm_unban_ref(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    await ban_ref(uid, False)
    await call.answer("🔓 Реф-ссылка разбанена", show_alert=True)
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


@router.callback_query(F.data.startswith("adm:user_card_show:"))
async def cb_adm_user_card_show(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


@router.callback_query(F.data == "adm:bans")
async def cb_adm_bans(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE is_banned=1 ORDER BY banned_at DESC LIMIT 30")
        rows = await cur.fetchall()
        cur = await db.execute("SELECT * FROM users WHERE banned_ref=1 LIMIT 30")
        ref_rows = await cur.fetchall()

    lines = []
    for u in rows:
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        reason = u["banned_reason"] or "—"
        lines.append(f"🚫 <b>{esc(name)}</b> <code>{u['user_id']}</code>\n<i>{esc(reason)[:60]}</i>")
    ref_lines = []
    for u in ref_rows:
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        ref_lines.append(f"🔒 <b>{esc(name)}</b> <code>{u['user_id']}</code>")

    text = (
        "🚫 <b>Баны</b>\n"
        "─────────────\n"
        "<b>Юзеры:</b>\n"
        + ("\n".join(lines) if lines else "<i>пусто</i>")
        + "\n\n<b>Реф-ссылки:</b>\n"
        + ("\n".join(ref_lines) if ref_lines else "<i>пусто</i>")
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm:bans")],
        back_button("adm:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "adm:sys")
async def cb_adm_sys(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    try:
        db_size = os.path.getsize(DB_PATH) / 1024 / 1024
    except Exception:
        db_size = 0
    # Последний бэкап
    last_backup = "—"
    try:
        files = [f for f in os.listdir(BACKUP_DIR) if f.startswith("bot_") and f.endswith(".db")]
        if files:
            files.sort()
            last = files[-1]
            ts_str = last[4:19]
            dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
            last_backup = dt.astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        pass

    text = (
        "⚙️ <b>Система</b>\n"
        "─────────────\n"
        f"💾 Размер БД: <b>{db_size:.2f} МБ</b>\n"
        f"📦 Последний бэкап: <b>{esc(last_backup)}</b>\n"
        f"⏱ Интервал бэкапа: <b>{BACKUP_INTERVAL // 60} мин</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Бэкап сейчас", callback_data="adm:backup_now")],
        [InlineKeyboardButton(text="🧹 Очистить старые сообщения", callback_data="adm:cleanup")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm:sys")],
        back_button("adm:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "adm:backup_now")
async def cb_adm_backup_now(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    ok = await do_backup(send_to_owner=True)
    if ok:
        await call.answer("✅ Бэкап отправлен в личку", show_alert=True)
    else:
        await call.answer("❌ Ошибка бэкапа", show_alert=True)


@router.callback_query(F.data == "adm:cleanup")
async def cb_adm_cleanup(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    cutoff = (utcnow() - timedelta(days=30)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM messages WHERE date < ?", (cutoff,))
        deleted = cur.rowcount
        await db.commit()
    await call.answer(f"✅ Удалено: {deleted}", show_alert=True)
    await cb_adm_sys(call)


async def handle_admin_input(message: Message, action: str, value: str):
    if action.startswith("find_user:"):
        pass
    if action == "find_user":
        try:
            uid = int(value)
        except ValueError:
            await message.answer("Нужен числовой ID.")
            return
        u, refs, msgs = await get_user_card(uid)
        if not u:
            await message.answer(f"Юзер <code>{uid}</code> не найден.")
            return
        # Показываем карточку через callback-хендлер
        name = u["username"] or u["first_name"] or "—"
        premium = await is_premium(uid)
        try:
            joined = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m.%Y %H:%M")
        except Exception:
            joined = "—"
        text = (
            f"👤 <b>Карточка</b>\n"
            "─────────────\n"
            f"📛 <b>{esc(name)}</b>\n"
            f"🆔 <code>{uid}</code>\n"
            f"📅 Регистрация: {esc(joined)}\n"
            f"💎 Premium: {'✅' if premium else '❌'}\n"
            f"🎁 Рефералов: <b>{refs}</b>\n"
            f"📝 Сообщений: <b>{msgs}</b>\n"
            f"🚫 Бан: {'✅' if u['is_banned'] else '❌'}\n"
            f"🔗 Реф-бан: {'✅' if u['banned_ref'] else '❌'}"
        )
        rows = [
            [InlineKeyboardButton(text="➕ Выдать Premium", callback_data=f"adm:grant:{uid}")],
            [InlineKeyboardButton(text="➖ Снять Premium", callback_data=f"adm:revoke:{uid}")],
        ]
        if u["is_banned"]:
            rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"adm:unban:{uid}")])
        else:
            rows.append([InlineKeyboardButton(text="🚫 Забанить", callback_data=f"adm:ban:{uid}")])
        if u["banned_ref"]:
            rows.append([InlineKeyboardButton(text="🔓 Разбанить реф-ссылку", callback_data=f"adm:unban_ref:{uid}")])
        else:
            rows.append([InlineKeyboardButton(text="🔒 Забанить реф-ссылку", callback_data=f"adm:ban_ref:{uid}")])
        rows.append(back_button("adm:menu"))
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        return

    if action.startswith("ban_reason:"):
        uid = int(action.split(":")[1])
        reason = value if value != "-" else ""
        await ban_user(uid, reason)
        await message.answer(f"✅ Пользователь <code>{uid}</code> забанен.")
        return


async def publish_weekly_top(manual=False):
    """Публикует топ недели в канал."""
    top = await referral_leaderboard(5, week_only=True)
    if not top:
        if manual:
            try:
                await bot_ref.send_message(
                    f"@{CHANNEL_USERNAME}",
                    "🏆 <b>Битва рефералов</b>\n\nНа этой неделе пока нет участников."
                )
                return True
            except Exception:
                return False
        return False

    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    lines = []
    for i, (uid, name, cnt) in enumerate(top):
        lines.append(f"{medals[i]} <b>{esc(name)}</b> — {cnt}")

    winner_name = top[0][1]
    text = (
        "🏆 <b>Битва рефералов — итоги недели</b>\n"
        "─────────────\n\n"
        + "\n".join(lines)
        + f"\n\n🎁 Победитель: <b>{esc(winner_name)}</b> — месяц Premium!"
    )
    try:
        await bot_ref.send_message(f"@{CHANNEL_USERNAME}", text)
        return True
    except Exception as e:
        log.warning("publish top failed: %s", e)
        return False


# ---------------- Backup ----------------
async def do_backup(send_to_owner=False):
    try:
        ts = utcnow().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(BACKUP_DIR, f"bot_{ts}.db")
        async with aiosqlite.connect(DB_PATH) as src:
            async with aiosqlite.connect(backup_path) as dst:
                await src.backup(dst)
        # Чистим старые бэкапы (оставляем 10 последних)
        try:
            files = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith(".db")])
            for old in files[:-10]:
                os.remove(os.path.join(BACKUP_DIR, old))
        except Exception:
            pass
        if send_to_owner:
            try:
                await bot_ref.send_document(
                    owner_id,
                    FSInputFile(backup_path),
                    caption=f"💾 Бэкап БД · {utcnow().astimezone().strftime('%d.%m.%Y %H:%M')}"
                )
            except Exception as e:
                log.warning("send backup failed: %s", e)
        return True
    except Exception as e:
        log.error("backup failed: %s", e)
        return False


async def backup_loop():
    while True:
        try:
            await asyncio.sleep(BACKUP_INTERVAL)
            await do_backup(send_to_owner=True)
        except Exception as e:
            log.warning("backup loop err: %s", e)


# ---------------- Name update ----------------
async def update_business_name():
    while True:
        try:
            s = await get_settings()
            if s["time_in_nick_enabled"]:
                async with aiosqlite.connect(DB_PATH) as db:
                    cur = await db.execute(
                        "SELECT id FROM business_connections WHERE enabled=1 LIMIT 1"
                    )
                    row = await cur.fetchone()
                if row:
                    connection_id = row[0]
                    clock = local_now(s["timezone"]).strftime("%H:%M")
                    try:
                        me = await bot_ref.get_me()
                        base = me.first_name or BOT_NAME
                    except Exception:
                        base = BOT_NAME
                    desired = f"{base} [{clock}]"[:64]
                    try:
                        async with name_update_lock:
                            await bot_ref.set_business_account_name(
                                business_connection_id=connection_id,
                                first_name=desired
                            )
                        log.info("Name updated: %s", desired)
                    except Exception as e:
                        log.warning("set_business_account_name failed: %s", e)
        except Exception as e:
            log.warning("name loop err: %s", e)
        await asyncio.sleep(60)


# ---------------- Weekly publish ----------------
async def weekly_publish_loop():
    """Публикует топ каждый понедельник в 10:00 UTC."""
    while True:
        try:
            now = utcnow()
            # Ближайший понедельник 10:00
            days_ahead = (7 - now.weekday()) % 7
            if days_ahead == 0 and now.hour >= 10:
                days_ahead = 7
            target = (now + timedelta(days=days_ahead)).replace(hour=10, minute=0, second=0, microsecond=0)
            wait = (target - now).total_seconds()
            if wait < 0:
                wait = 60
            await asyncio.sleep(wait)
            await publish_weekly_top(manual=False)
        except Exception as e:
            log.warning("weekly publish err: %s", e)
            await asyncio.sleep(3600)

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
                    last_err = f"{ep}: нет ссылки"
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
                                raise RuntimeError("Файл превышает допустимый размер")
                return filepath, "Видео"
            except Exception as e:
                last_err = f"{ep}: {e}"
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
            raise RuntimeError("Ссылка не найдена")
        if not link.startswith("http"):
            link = "https://www.tikwm.com" + link
        filepath = os.path.join(outdir, f"tt_{random.randint(1000, 9999)}.mp4")
        async with client.stream("GET", link) as resp:
            resp.raise_for_status()
            with open(filepath, "wb") as f:
                async for chunk in resp.aiter_bytes(65536):
                    f.write(chunk)
        return filepath, info.get("title", "TikTok") or "TikTok"


async def download_video(url: str, outdir: str) -> tuple[str, str]:
    errors = []
    if YTDLP_OK:
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, _ytdlp_download, url, outdir)
        except Exception as e:
            errors.append(f"yt-dlp: {str(e)[:100]}")
    try:
        return await _cobalt_download(url, outdir)
    except Exception as e:
        errors.append(f"cobalt: {str(e)[:100]}")
    if "tiktok.com" in url or "vt.tiktok" in url:
        try:
            return await _tikwm_download(url, outdir)
        except Exception as e:
            errors.append(f"tikwm: {str(e)[:100]}")
    raise RuntimeError("Не удалось загрузить. " + " | ".join(errors)[:300])


# ============================================================
# BUSINESS HANDLERS
# ============================================================

async def _check_owner_business(message: Message) -> bool:
    """Проверка: владелец + business-контекст."""
    if not message.from_user or message.from_user.id != owner_id:
        return False
    if not message.business_connection_id:
        return False
    return True


# ---------- .save ----------
@router.business_message(F.text.regexp(r"^\.save\s*$"))
async def business_save(message: Message, bot: Bot):
    if not await _check_owner_business(message):
        return
    await save_business_message(message)

    target_text = ""
    if message.reply_to_message and message.reply_to_message.text:
        target_text = message.reply_to_message.text
    else:
        target_text = message.text or ""
    match = URL_RE.search(target_text)
    if not match:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text="⚠️ Ответьте командой <code>.save</code> на сообщение со ссылкой.",
            business_connection_id=message.business_connection_id
        )
        return

    url = match.group(0).rstrip(".,)")
    await bot.edit_message_text(
        chat_id=message.chat.id, message_id=message.message_id,
        text="📥 <i>Загружаю видео…</i>",
        business_connection_id=message.business_connection_id
    )

    tmpdir = tempfile.mkdtemp(prefix="save_")
    try:
        filepath, title = await download_video(url, tmpdir)
        if not filepath or not os.path.exists(filepath):
            raise RuntimeError("Файл не найден")
        size_mb = os.path.getsize(filepath) / 1024 / 1024
        if size_mb > DOWNLOAD_MAX_MB:
            raise RuntimeError(f"Файл слишком большой ({size_mb:.1f} МБ)")
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
            text=f"⚠️ Не удалось загрузить: <code>{esc(str(e)[:200])}</code>",
            business_connection_id=message.business_connection_id
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------- .anim ----------
@router.business_message(F.text.regexp(r"^\.anim(\s+.+)?$"))
async def business_anim(message: Message, bot: Bot):
    if not await _check_owner_business(message):
        return
    await save_business_message(message)

    text = (message.text or "")[5:].strip()
    if not text:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text="⚠️ Использование: <code>.anim текст</code>",
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
        except Exception:
            pass
        await asyncio.sleep(ANIM_DELAY)

    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=sent.message_id,
            text=esc(text), business_connection_id=bc
        )
    except Exception:
        pass


# ---------- .ai ----------
@router.business_message(F.text.regexp(r"^\.ai(\s+.+)?$"))
async def business_ai(message: Message, bot: Bot):
    if not await _check_owner_business(message):
        return
    await save_business_message(message)

    bc = message.business_connection_id
    chat_id = message.chat.id

    ok, seconds = await check_cooldown(owner_id)
    if not ok:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message.message_id,
            text=f"⏱ <b>Подождите {seconds} сек.</b>\nЗадержка между запросами.",
            business_connection_id=bc
        )
        return

    can, used, limit, premium = await check_limit(owner_id)
    if not can:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⭐ Купить Premium ({PREMIUM_PRICE_STARS}⭐)",
                                  callback_data="buy_premium")]
        ])
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message.message_id,
            text=(f"⚠️ <b>Лимит исчерпан</b>\n"
                  f"Использовано: <b>{used}/{limit}</b>\n\n"
                  f"Оформить Premium: /start → 🤖 Bob AI"),
            business_connection_id=bc, reply_markup=kb
        )
        return

    prompt = (message.text or "")[3:].strip()
    if not prompt:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message.message_id,
            text=f"🤖 Использование: <code>.ai вопрос</code>\n"
                 f"Осталось запросов: <b>{limit - used}</b>",
            business_connection_id=bc
        )
        return

    try:
        thinking = await bot.send_message(
            chat_id=chat_id, text="🤖 <i>Обрабатываю запрос…</i>",
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
        text = f"🤖 {esc(answer)}"
        text = with_watermark(text, premium)
        await bot.edit_message_text(
            chat_id=chat_id, message_id=thinking.message_id,
            text=text, business_connection_id=bc
        )
        await increment_usage(owner_id)
    except Exception as e:
        log.error("AI error: %s", e)
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=thinking.message_id,
                text=f"⚠️ Ошибка: <code>{esc(str(e)[:200])}</code>",
                business_connection_id=bc
            )
        except Exception:
            pass


# ---------- .qr ----------
@router.business_message(F.text.regexp(r"^\.qr(\s+.+)?$"))
async def business_qr(message: Message, bot: Bot):
    if not await _check_owner_business(message):
        return
    await save_business_message(message)

    bc = message.business_connection_id
    text = (message.text or "")[3:].strip()
    if not text:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text="⚠️ Использование: <code>.qr текст или ссылка</code>",
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
            caption=f"🔳 QR-код: <code>{esc(text[:200])}</code>",
            business_connection_id=bc
        )
        try:
            await bot.delete_business_messages(business_connection_id=bc, message_ids=[message.message_id])
        except Exception:
            pass
    except ImportError:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text="⚠️ Требуется пакет <code>qrcode[pil]</code>.",
            business_connection_id=bc
        )
    except Exception as e:
        await bot.edit_message_text(
            chat_id=message.chat.id, message_id=message.message_id,
            text=f"⚠️ Ошибка: <code>{esc(str(e)[:200])}</code>",
            business_connection_id=bc
        )


# ---------- .mute (с иммунитетом) ----------
async def _find_target_user(chat_id):
    """Находит последнего не-владельца в чате."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id FROM messages WHERE chat_id=? AND user_id IS NOT NULL "
            "AND user_id!=? ORDER BY id DESC LIMIT 1",
            (chat_id, owner_id)
        )
        row = await cur.fetchone()
    return row[0] if row else None


# ---------- .game ----------
@router.business_message(F.text.regexp(r"^\.(help|whois|typing|mute(?:\s+\d+(?:\s+\d+)?)?|unmute|muted|id|ping|note(?:\s+.+)?|game)$"))
async def business_commands(message: Message, bot: Bot):
    if not await _check_owner_business(message):
        return
    await save_business_message(message)

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=2)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    arg2 = parts[2].strip() if len(parts) > 2 else ""
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
                "🤖 <b>BobMod команды</b>\n"
                "─────────────\n"
                "<code>.ai вопрос</code> · запрос к ИИ\n"
                "<code>.save</code> (ответом) · загрузка видео\n"
                "<code>.anim текст</code> · анимация\n"
                "<code>.qr текст</code> · QR-код\n"
                "<code>.whois</code> · инфо о собеседнике\n"
                "<code>.typing</code> · показать «печатает»\n"
                "<code>.mute 60</code> · <code>.unmute</code> · <code>.muted</code>\n"
                "<code>.note</code> · <code>.id</code> · <code>.ping</code> · <code>.game</code>"
            )
        elif cmd == ".ping":
            await edit("🏓 <b>Бот работает</b>")
        elif cmd == ".id":
            await edit(
                f"🆔 ID чата: <code>{chat_id}</code>\n"
                f"👤 Ваш ID: <code>{message.from_user.id}</code>\n"
                f"🔗 Соединение: <code>{esc(bc)}</code>"
            )
        elif cmd == ".whois":
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute("SELECT * FROM contacts WHERE chat_id=?", (chat_id,))
                row = await cur.fetchone()
            if not row:
                await edit("👤 Информации пока нет.")
            else:
                first = "—"
                try:
                    first = datetime.fromisoformat(row["first_seen"]).astimezone().strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pass
                uname = f"@{esc(row['username'])}" if row["username"] else "—"
                await edit(
                    "👤 <b>Информация о собеседнике</b>\n"
                    "─────────────\n"
                    f"📛 Имя: <b>{esc(row['user_name'])}</b>\n"
                    f"🆔 ID: <code>{row['user_id']}</code>\n"
                    f"🔗 Username: {uname}\n"
                    f"📅 Первый контакт: {esc(first)}\n"
                    f"💬 Сообщений: <b>{row['message_count']}</b>"
                )
        elif cmd == ".typing":
            try:
                await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING, business_connection_id=bc)
            except Exception:
                pass
            await edit("⌨️ <i>Печатает…</i>")
        elif cmd == ".note":
            if not arg:
                row = await get_note(chat_id)
                if row and row[0]:
                    await edit(f"📝 {esc(row[0])}")
                else:
                    await edit("📝 Заметок нет.")
            else:
                full_note = (arg + " " + arg2).strip() if arg2 else arg
                await set_note(chat_id, full_note)
                await edit(f"📝 Заметка сохранена: {esc(full_note)}")
        elif cmd.startswith(".mute"):
            # Парсим аргументы: .mute 60 [user_id]
            tokens = (arg + " " + arg2).split()
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
                target = await _find_target_user(chat_id)
                if not target:
                    await edit("⚠️ Укажите ID: <code>.mute 60 123456</code>")
                    return

            # === ИММУНИТЕТ PREMIUM ===
            if await is_premium(target):
                await edit("⛔ Пользователь защищён Premium-иммунитетом")
                return

            until = utcnow() + timedelta(seconds=seconds)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO mutes(chat_id,target_user_id,mute_until) VALUES(?,?,?)",
                    (chat_id, target, until.isoformat())
                )
                await db.commit()
            await edit(f"🔇 Пользователь заглушён на {seconds} сек.")
        elif cmd == ".unmute":
            target = await _find_target_user(chat_id)
            if target:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("DELETE FROM mutes WHERE chat_id=? AND target_user_id=?",
                                     (chat_id, target))
                    await db.commit()
                await edit("🔊 Мут снят")
            else:
                await edit("🔊 Активных мутов нет")
        elif cmd == ".muted":
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT target_user_id, mute_until FROM mutes WHERE chat_id=? AND mute_until > ?",
                    (chat_id, utcnow().isoformat())
                )
                rows = await cur.fetchall()
            if not rows:
                await edit("🔊 Активных мутов нет")
            else:
                lines = []
                for uid, until in rows:
                    try:
                        t = datetime.fromisoformat(until).astimezone().strftime("%H:%M:%S")
                    except Exception:
                        t = "—"
                    lines.append(f"• <code>{uid}</code> до {t}")
                await edit("🔇 <b>Активные муты</b>\n" + "\n".join(lines))
        elif cmd == ".game":
            await edit(
                "🎮 <b>Выберите игру</b>",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔫 Русская рулетка",
                                          callback_data=f"game:roulette:{chat_id}:{bc}")],
                    [InlineKeyboardButton(text="⭕❌ Крестики-нолики",
                                          callback_data=f"game:ttt:{chat_id}:{bc}")],
                    [InlineKeyboardButton(text="🎲 Угадай число",
                                          callback_data=f"game:guess:{chat_id}:{bc}")],
                ])
            )
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.warning("command failed: %s", e)


# ---------- Guess game ----------
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
        await message.answer(f"🎉 Верно! Это {guess}. Попыток: {game['tries']}.")
        guess_games.pop(key, None)
    elif guess < game["number"]:
        await message.answer("⬆️ Больше")
    else:
        await message.answer("⬇️ Меньше")


# ---------- Общий обработчик бизнес-сообщений ----------
@router.business_message()
async def business_message_handler(message: Message, bot: Bot):
    await save_business_message(message)
    if not message.business_connection_id:
        return
    sender = message.from_user
    if not sender or sender.id == owner_id:
        return

    # Mute
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
                        text=f"🔇 <i>Вы в муте до {until.astimezone().strftime('%H:%M:%S')}</i>",
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
    premium_owner = await is_premium(owner_id)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT message_count FROM contacts WHERE chat_id=?", (message.chat.id,))
        cnt_row = await cur.fetchone()
    count = cnt_row[0] if cnt_row else 1
    if count == 1 and s["answer_first_enabled"] and s["answer_first_text"]:
        try:
            text = with_watermark(s["answer_first_text"], premium_owner)
            await bot.send_message(
                message.chat.id, text,
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
                text = with_watermark(s["off_hours_text"], premium_owner)
                await bot.send_message(
                    message.chat.id, text,
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


# ---------- Удалённые сообщения ----------
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
            "─────────────\n"
            f"💬 Чат: <code>{event.chat.id}</code>\n"
            f"👤 От: <b>{esc(row['user_name'])}</b> <code>{row['user_id']}</code>\n"
            f"🕐 Время: {esc(stamp)}\n"
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
            log.warning("deleted notify: %s", e)


# ---------- Игры ----------
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
        await call.message.edit_text("🔫 <b>Русская рулетка</b>\n6 камер, 1 патрон.", reply_markup=kb)
    elif kind == "guess":
        guess_games[(chat_id, call.message.message_id)] = {
            "number": random.randint(1, 100), "tries": 0, "bc": bc
        }
        await call.message.edit_text("🎲 <b>Угадай число</b> от 1 до 100. Пишите в чат.")
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
        await call.answer("Игра завершена", show_alert=True)
        return
    if random.randint(1, 6) == 1:
        roulette_games.pop(key, None)
        await call.message.edit_text("💥 <b>Бах!</b> Игра окончена.")
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="🔄 Крутить", callback_data=call.data)]])
        await call.message.edit_text("😮‍💨 Щёлк. Продолжить?", reply_markup=kb)
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
            await call.answer("Ожидается ход собеседника", show_alert=True)
            return
        game["opponent"] = uid
    elif uid not in {owner_id, game["opponent"]}:
        await call.answer("Вы не участвуете в игре", show_alert=True)
        return
    expected = owner_id if game["turn"] == "❌" else game["opponent"]
    if uid != expected:
        await call.answer("Сейчас ход другого игрока", show_alert=True)
        return
    if game["board"][idx]:
        await call.answer("Клетка занята", show_alert=True)
        return
    game["board"][idx] = game["turn"]
    b = game["board"]
    wins = [(0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6)]
    winner = next((b[a] for a,c,d in wins if b[a] and b[a]==b[c]==b[d]), None)
    if winner:
        game["ended"] = True
        await call.message.edit_text(f"🏆 Победили <b>{winner}</b>!", reply_markup=None)
    elif all(b):
        game["ended"] = True
        await call.message.edit_text("🤝 Ничья!", reply_markup=None)
    else:
        game["turn"] = "⭕" if game["turn"] == "❌" else "❌"
        await render_ttt(call.message, chat_id, mid)
    await call.answer()


# ---------- Business connection ----------
@router.business_connection()
async def business_connection_handler(connection: BusinessConnection, bot: Bot):
    log.info("Business connection %s enabled=%s", connection.id, connection.is_enabled)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO business_connections(id, user_id, enabled, added_at) VALUES(?,?,?,?)",
            (connection.id, connection.user.id if connection.user else owner_id,
             int(connection.is_enabled), utcnow().isoformat())
        )
        await db.commit()


# ============================================================
# ENTRY POINT
# ============================================================

async def main():
    global bot_ref
    if not api_token:
        raise RuntimeError("Не найден API_TOKEN в .env")
    if owner_id <= 0:
        raise RuntimeError("Некорректный OWNER_ID в .env")

    await init_db()

    bot_ref = Bot(api_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)

    # Фоновые задачи
    asyncio.create_task(backup_loop())
    asyncio.create_task(update_business_name())
    asyncio.create_task(weekly_publish_loop())

    log.info("BobMod started. Owner: %s", owner_id)
    await dp.start_polling(bot_ref, allowed_updates=dp.resolve_used_update_types() + [
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages"
    ])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass 
