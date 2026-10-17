import asyncio
import json
import logging
import os
import random
import re
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from zoneinfo import ZoneInfo

import httpx
import libsql_client
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BusinessConnection, BusinessMessagesDeleted,
    FSInputFile, BufferedInputFile, LabeledPrice, PreCheckoutQuery,
    WebAppInfo,
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

TURSO_URL = os.getenv("TURSO_DATABASE_URL", "").strip()
TURSO_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "").strip()

BOT_NAME = "BobMod"
BOT_USERNAME = "bobmod_robot"
CHANNEL_USERNAME = "bobmodinfo"
CHANNEL_LINK = f"https://t.me/{CHANNEL_USERNAME}"

DEFAULT_PROMPT = (
    "Ты BobMod — дружелюбный ассистент в Telegram-чатах. "
    "Отвечай на русском, спокойно и по делу, без официоза и без панибратства. "
    "Кратко — 2-4 предложения. Без 'конечно!' и 'отличный вопрос!'. "
    "Если не знаешь ответа — скажи прямо, не выдумывай."
)

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
CUSTOM_PROMPT_MAX = 500
CUSTOM_CATEGORY_MAX = 200

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

CONSTRUCTOR_CATEGORIES = {
    "c1": {"title": "🎭 Характер", "options": {
        "friendly": "Дружелюбный", "business": "Деловой", "funny": "С юмором",
        "sarcastic": "Саркастичный", "caring": "Заботливый"}},
    "c2": {"title": "📏 Длина ответа", "options": {
        "short": "Кратко (1-2 предложения)", "medium": "Средне (3-5 предложений)", "long": "Подробно"}},
    "c3": {"title": "🗣 Стиль общения", "options": {
        "ty": "На «ты»", "vy": "На «вы»", "formal": "Официально", "informal": "Без официоза"}},
    "c4": {"title": "🌐 Язык ответа", "options": {
        "ru": "Русский", "en": "Английский", "auto": "Авто (по языку вопроса)"}},
    "c5": {"title": "😊 Эмодзи", "options": {
        "much": "Много эмодзи", "little": "Немного эмодзи", "none": "Без эмодзи"}},
    "c6": {"title": "🎯 Специализация", "options": {
        "universal": "Универсальный", "study": "Помощник по учёбе",
        "business": "Бизнес-ассистент", "coder": "Помощник программиста"}},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bobmod")
router = Router()

# ---------------- State ----------------
guess_games = {}
roulette_games = {}
ttt_games = {}
pending_inputs = {}
pending_admin = {}
pending_ai_input = {}
sub_cache = {}
connection_cache = {}
name_update_lock = asyncio.Lock()
bot_ref: Bot = None
db_client: libsql_client.Client = None


# ---------------- Turso DB (с защитой от падений) ----------------
async def db_execute(sql: str, args: tuple = ()):
    try:
        return await db_client.execute(sql, args)
    except Exception as e:
        log.error(f"SQL Execute Error: {e} | Query: {sql[:100]}")
        # Возвращаем заглушку, чтобы код не падал при ожидании ответа
        class DummyResult:
            rows = []
        return DummyResult()

async def db_fetchone(sql: str, args: tuple = ()):
    try:
        result = await db_client.execute(sql, args)
        return result.rows[0] if result.rows else None
    except Exception as e:
        log.error(f"SQL FetchOne Error: {e} | Query: {sql[:100]}")
        return None


async def db_fetchall(sql: str, args: tuple = ()):
    try:
        result = await db_client.execute(sql, args)
        return list(result.rows)
    except Exception as e:
        log.error(f"SQL FetchAll Error: {e} | Query: {sql[:100]}")
        return []


async def db_fetchval(sql: str, args: tuple = (), default=None):
    row = await db_fetchone(sql, args)
    if not row:
        return default
    return row[0] if row else default


async def init_db():
    statements = [
        """CREATE TABLE IF NOT EXISTS messages (
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
        )""",
        "CREATE INDEX IF NOT EXISTS idx_messages_chat_mid ON messages(chat_id, message_id)",
        """CREATE TABLE IF NOT EXISTS settings (
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
        )""",
        """CREATE TABLE IF NOT EXISTS blacklist (
            owner_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            PRIMARY KEY(owner_id, user_id)
        )""",
        """CREATE TABLE IF NOT EXISTS mutes (
            chat_id INTEGER NOT NULL, target_user_id INTEGER NOT NULL,
            mute_until TEXT NOT NULL, PRIMARY KEY(chat_id, target_user_id)
        )""",
        """CREATE TABLE IF NOT EXISTS contacts (
            chat_id INTEGER PRIMARY KEY, first_seen TEXT NOT NULL,
            message_count INTEGER DEFAULT 0, user_id INTEGER,
            user_name TEXT, username TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS notes (
            chat_id INTEGER PRIMARY KEY, note TEXT, updated_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS user_limits (
            user_id INTEGER PRIMARY KEY, date TEXT NOT NULL,
            requests_count INTEGER DEFAULT 0, is_premium INTEGER DEFAULT 0,
            premium_until TEXT, last_ai_request TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT,
            joined_at TEXT NOT NULL, is_banned INTEGER DEFAULT 0,
            banned_at TEXT, banned_reason TEXT, banned_ref INTEGER DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS referrals (
            user_id INTEGER PRIMARY KEY, referrer_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL, rewarded_join INTEGER DEFAULT 0,
            rewarded_purchase INTEGER DEFAULT 0
        )""",
        "CREATE INDEX IF NOT EXISTS idx_ref_referrer ON referrals(referrer_id)",
        """CREATE TABLE IF NOT EXISTS pending_referrals (
            user_id INTEGER PRIMARY KEY, referrer_id INTEGER NOT NULL, ts TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS business_connections (
            id TEXT PRIMARY KEY, user_id INTEGER,
            enabled INTEGER DEFAULT 1, added_at TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_bc_user ON business_connections(user_id)",
        """CREATE TABLE IF NOT EXISTS business_names (
            connection_id TEXT PRIMARY KEY, base_name TEXT NOT NULL, saved_at TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS admin_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER NOT NULL,
            action TEXT NOT NULL, target_id INTEGER, details TEXT, ts TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS payments_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            stars INTEGER NOT NULL, days INTEGER NOT NULL, ts TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS ai_settings (
            user_id INTEGER PRIMARY KEY, mode TEXT DEFAULT 'default',
            custom_prompt TEXT, constructor_data TEXT, updated_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS spin_cooldowns (
            user_id INTEGER PRIMARY KEY, last_spin TEXT NOT NULL, extra_spins INTEGER DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS nomute_chats (
            chat_id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL, created_at TEXT
        )""",
    ]
    for stmt in statements:
        try:
            await db_client.execute(stmt)
        except Exception as e:
            log.warning("DDL error: %s — %s", stmt[:60], e)
    log.info("Turso DB initialized")


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


def with_watermark(text, is_premium_flag):
    return text if is_premium_flag else text + watermark()


URL_RE = re.compile(r"https?://[^\s]+")


def week_start():
    now = utcnow()
    return (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def row_to_dict(row, columns):
    if not row:
        return None
    return {col: row[i] for i, col in enumerate(columns)}


AI_PROVIDERS = [
    {"name": "groq", "url": "https://api.groq.com/openai/v1/chat/completions",
     "key_env": "GROQ_API_KEY", "model": "openai/gpt-oss-120b"},
    {"name": "groq-20b", "url": "https://api.groq.com/openai/v1/chat/completions",
     "key_env": "GROQ_API_KEY", "model": "openai/gpt-oss-20b"},
    {"name": "openrouter", "url": "https://openrouter.ai/api/v1/chat/completions",
     "key_env": "OPENROUTER_API_KEY", "model": "openai/gpt-oss-120b:free"},
    {"name": "mistral", "url": "https://api.mistral.ai/v1/chat/completions",
     "key_env": "MISTRAL_API_KEY", "model": "mistral-small-latest"},
]


async def get_user_prompt(user_id: int) -> str:
    premium = await is_premium(user_id)
    if not premium:
        return DEFAULT_PROMPT
    row = await db_fetchone(
        "SELECT mode, custom_prompt, constructor_data FROM ai_settings WHERE user_id=?",
        (user_id,)
    )
    if not row:
        return DEFAULT_PROMPT
    mode = row[0] or "default"
    custom_prompt = row[1]
    constructor_data = row[2]
    if mode == "custom" and custom_prompt:
        return custom_prompt
    if mode == "constructor" and constructor_data:
        try:
            data = json.loads(constructor_data)
            parts = []
            for cat_key, cat in CONSTRUCTOR_CATEGORIES.items():
                if cat_key in data and data[cat_key]:
                    val = data[cat_key]
                    title = cat["title"].split(" ", 1)
                    cat_name = title[1] if len(title) > 1 else cat["title"]
                    parts.append(f"{cat_name}: {val}")
            if parts:
                return DEFAULT_PROMPT + "\n\nДополнительные настройки:\n" + "\n".join(parts)
        except Exception as e:
            log.warning("constructor parse error: %s", e)
    return DEFAULT_PROMPT


async def ask_ai(prompt: str, user_id: int = None) -> str:
    system_prompt = DEFAULT_PROMPT
    if user_id:
        try:
            system_prompt = await get_user_prompt(user_id)
        except Exception as e:
            log.warning("get_user_prompt failed: %s", e)
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
                        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                        json={
                            "model": p["model"],
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": prompt},
                            ],
                            "max_tokens": 1024,
                            "temperature": 0.8,
                        },
                    )
                if r.status_code == 200:
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
    raise RuntimeError("ИИ недоступен. " + " | ".join(errors_log)[:300])


SETTINGS_COLS = ["user_id", "answer_first_enabled", "answer_first_text",
                 "time_in_nick_enabled", "timezone", "off_hours_enabled",
                 "off_hours_start", "off_hours_end", "off_hours_text",
                 "forward_to_owner_enabled"]


async def get_settings(user):
    row = await db_fetchone("SELECT * FROM settings WHERE user_id=?", (user,))
    if row:
        return row_to_dict(row, SETTINGS_COLS)
    await db_execute("INSERT OR IGNORE INTO settings(user_id) VALUES(?)", (user,))
    row = await db_fetchone("SELECT * FROM settings WHERE user_id=?", (user,))
    return row_to_dict(row, SETTINGS_COLS)


async def set_setting(user, key, value):
    allowed = {
        "answer_first_enabled", "answer_first_text", "time_in_nick_enabled", "timezone",
        "off_hours_enabled", "off_hours_start", "off_hours_end", "off_hours_text",
        "forward_to_owner_enabled",
    }
    if key not in allowed:
        return
    await get_settings(user)
    await db_execute(f"UPDATE settings SET {key}=? WHERE user_id=?", (value, user))


async def get_ai_settings(user_id):
    row = await db_fetchone(
        "SELECT mode, custom_prompt, constructor_data, updated_at FROM ai_settings WHERE user_id=?",
        (user_id,)
    )
    if not row:
        return {"mode": "default", "custom_prompt": None, "constructor_data": None, "updated_at": None}
    return {"mode": row[0] or "default", "custom_prompt": row[1],
            "constructor_data": row[2], "updated_at": row[3]}


async def save_ai_settings(user_id, mode=None, custom_prompt=None, constructor_data=None):
    await db_execute("INSERT OR IGNORE INTO ai_settings(user_id) VALUES(?)", (user_id,))
    updates = []
    args = []
    if mode is not None:
        updates.append("mode=?")
        args.append(mode)
    if custom_prompt is not None:
        updates.append("custom_prompt=?")
        args.append(custom_prompt)
    if constructor_data is not None:
        updates.append("constructor_data=?")
        args.append(constructor_data)
    updates.append("updated_at=?")
    args.append(utcnow().isoformat())
    args.append(user_id)
    await db_execute(f"UPDATE ai_settings SET {', '.join(updates)} WHERE user_id=?", tuple(args))


async def reset_ai_settings(user_id):
    await db_execute(
        "UPDATE ai_settings SET mode='default', custom_prompt=NULL, constructor_data=NULL, updated_at=? WHERE user_id=?",
        (utcnow().isoformat(), user_id)
    )


async def is_blacklisted(owner, user_id):
    row = await db_fetchone("SELECT 1 FROM blacklist WHERE owner_id=? AND user_id=?", (owner, user_id))
    return row is not None


async def add_blacklist(owner, user_id):
    await db_execute("INSERT OR IGNORE INTO blacklist(owner_id, user_id) VALUES(?,?)", (owner, user_id))


async def remove_blacklist(owner, user_id):
    await db_execute("DELETE FROM blacklist WHERE owner_id=? AND user_id=?", (owner, user_id))


async def get_note(chat_id):
    return await db_fetchone("SELECT note, updated_at FROM notes WHERE chat_id=?", (chat_id,))


async def set_note(chat_id, text):
    await db_execute(
        "INSERT INTO notes(chat_id,note,updated_at) VALUES(?,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET note=excluded.note, updated_at=excluded.updated_at",
        (chat_id, text, utcnow().isoformat())
    )


async def log_admin(action, target_id=None, details=""):
    await db_execute(
        "INSERT INTO admin_log(admin_id, action, target_id, details, ts) VALUES(?,?,?,?,?)",
        (owner_id, action, target_id, details, utcnow().isoformat())
    )


async def ensure_user(user_id, first_name=None, username=None):
    today = utcnow().strftime("%Y-%m-%d")
    await db_execute(
        "INSERT OR IGNORE INTO user_limits(user_id, date, requests_count) VALUES(?,?,0)",
        (user_id, today)
    )
    await db_execute(
        "INSERT INTO users(user_id, first_name, username, joined_at) VALUES(?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET first_name=excluded.first_name, username=excluded.username",
        (user_id, first_name or "", username or "", utcnow().isoformat())
    )


async def is_banned(user_id):
    return bool(await db_fetchval("SELECT is_banned FROM users WHERE user_id=?", (user_id,), 0))


async def is_ref_banned(user_id):
    return bool(await db_fetchval("SELECT banned_ref FROM users WHERE user_id=?", (user_id,), 0))


async def ban_user(user_id, reason=""):
    await ensure_user(user_id)
    await db_execute(
        "UPDATE users SET is_banned=1, banned_at=?, banned_reason=? WHERE user_id=?",
        (utcnow().isoformat(), reason, user_id)
    )
    await log_admin("ban", user_id, reason)


async def unban_user(user_id):
    await db_execute(
        "UPDATE users SET is_banned=0, banned_at=NULL, banned_reason=NULL WHERE user_id=?",
        (user_id,)
    )
    await log_admin("unban", user_id)


async def ban_ref(user_id, flag=True):
    await ensure_user(user_id)
    await db_execute("UPDATE users SET banned_ref=? WHERE user_id=?", (int(flag), user_id))
    await log_admin("ban_ref" if flag else "unban_ref", user_id)


USERS_COLS = ["user_id", "first_name", "username", "joined_at",
              "is_banned", "banned_at", "banned_reason", "banned_ref"]


async def get_user_card(user_id):
    row = await db_fetchone("SELECT * FROM users WHERE user_id=?", (user_id,))
    if not row:
        return None, 0, 0
    refs = await db_fetchval("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (user_id,), 0)
    msgs = await db_fetchval("SELECT COUNT(*) FROM messages WHERE user_id=?", (user_id,), 0)
    return row_to_dict(row, USERS_COLS), refs, msgs


async def is_premium(user_id):
    row = await db_fetchone(
        "SELECT is_premium, premium_until FROM user_limits WHERE user_id=?", (user_id,)
    )
    if not row or not row[0] or not row[1]:
        return False
    try:
        until = datetime.fromisoformat(row[1])
        if until < utcnow():
            await db_execute(
                "UPDATE user_limits SET is_premium=0, premium_until=NULL WHERE user_id=?",
                (user_id,)
            )
            return False
        return True
    except Exception:
        return False


async def get_premium_until(user_id):
    row = await db_fetchone("SELECT premium_until FROM user_limits WHERE user_id=?", (user_id,))
    if not row or not row[0]:
        return None
    try:
        return datetime.fromisoformat(row[0])
    except Exception:
        return None


async def grant_premium(user_id, hours=0, days=0):
    await ensure_user(user_id)
    delta = timedelta(hours=hours, days=days)
    row = await db_fetchone("SELECT premium_until FROM user_limits WHERE user_id=?", (user_id,))
    base = utcnow()
    if row and row[0]:
        try:
            cur_until = datetime.fromisoformat(row[0])
            if cur_until > base:
                base = cur_until
        except Exception:
            pass
    new_until = base + delta
    await db_execute(
        "UPDATE user_limits SET is_premium=1, premium_until=? WHERE user_id=?",
        (new_until.isoformat(), user_id)
    )
    return new_until


async def revoke_premium(user_id):
    await db_execute(
        "UPDATE user_limits SET is_premium=0, premium_until=NULL WHERE user_id=?", (user_id,)
    )
    await log_admin("revoke_premium", user_id)


async def check_limit(user_id):
    today = utcnow().strftime("%Y-%m-%d")
    premium = await is_premium(user_id)
    row = await db_fetchone(
        "SELECT date, requests_count FROM user_limits WHERE user_id=?", (user_id,)
    )
    if not row:
        await db_execute(
            "INSERT INTO user_limits(user_id, date, requests_count) VALUES(?,?,0)",
            (user_id, today)
        )
        used = 0
    elif row[0] != today:
        await db_execute(
            "UPDATE user_limits SET date=?, requests_count=0, last_ai_request=NULL WHERE user_id=?",
            (today, user_id)
        )
        used = 0
    else:
        used = row[1]
    limit = PREMIUM_DAILY_LIMIT if premium else FREE_DAILY_LIMIT
    return used < limit, used, limit, premium


async def check_cooldown(user_id):
    premium = await is_premium(user_id)
    cooldown = AI_COOLDOWN_PREMIUM if premium else AI_COOLDOWN_FREE
    row = await db_fetchone("SELECT last_ai_request FROM user_limits WHERE user_id=?", (user_id,))
    if not row or not row[0]:
        return True, 0
    try:
        last = datetime.fromisoformat(row[0])
        elapsed = (utcnow() - last).total_seconds()
        if elapsed < cooldown:
            return False, int(cooldown - elapsed) + 1
        return True, 0
    except Exception:
        return True, 0


async def increment_usage(user_id):
    today = utcnow().strftime("%Y-%m-%d")
    await db_execute(
        "UPDATE user_limits SET requests_count = requests_count + 1, last_ai_request=? "
        "WHERE user_id=? AND date=?",
        (utcnow().isoformat(), user_id, today)
    )


async def register_referral(new_user_id, referrer_id):
    if new_user_id == referrer_id or referrer_id == 0:
        return False
    if await is_ref_banned(referrer_id):
        return False
    if await is_banned(referrer_id):
        return False
    existing = await db_fetchone("SELECT 1 FROM referrals WHERE user_id=?", (new_user_id,))
    if existing:
        return False
    await db_execute(
        "INSERT INTO referrals(user_id, referrer_id, joined_at, rewarded_join) VALUES(?,?,?,0)",
        (new_user_id, referrer_id, utcnow().isoformat())
    )
    return True


async def mark_referral_rewarded(new_user_id):
    await db_execute("UPDATE referrals SET rewarded_join=1 WHERE user_id=?", (new_user_id,))


async def referral_stats(user_id):
    row = await db_fetchone(
        "SELECT COUNT(*), SUM(rewarded_purchase) FROM referrals WHERE referrer_id=?", (user_id,)
    )
    if not row:
        return 0, 0
    return (row[0] or 0), (row[1] or 0)


async def referral_leaderboard(limit=5, week_only=True):
    if week_only:
        start = week_start().isoformat()
        rows = await db_fetchall(
            """SELECT r.referrer_id, COUNT(*) as cnt FROM referrals r
               WHERE r.joined_at >= ? AND r.referrer_id != ?
               GROUP BY r.referrer_id ORDER BY cnt DESC LIMIT ?""",
            (start, owner_id, limit)
        )
    else:
        rows = await db_fetchall(
            """SELECT referrer_id, COUNT(*) as cnt FROM referrals
               WHERE referrer_id != ? GROUP BY referrer_id ORDER BY cnt DESC LIMIT ?""",
            (owner_id, limit)
        )
    result = []
    for row in rows:
        referrer_id = row[0]
        cnt = row[1]
        urow = await db_fetchone("SELECT first_name, username FROM users WHERE user_id=?", (referrer_id,))
        name = (urow[1] or urow[0] or f"id{referrer_id}") if urow else f"id{referrer_id}"
        result.append((referrer_id, name, cnt))
    return result


async def get_connection_owner(connection_id):
    return await db_fetchval("SELECT user_id FROM business_connections WHERE id=?", (connection_id,))


async def get_user_connection(user_id):
    return await db_fetchval(
        "SELECT id FROM business_connections WHERE user_id=? AND enabled=1 LIMIT 1", (user_id,)
    )


async def save_connection(connection_id, user_id, enabled):
    await db_execute(
        "INSERT OR REPLACE INTO business_connections(id, user_id, enabled, added_at) VALUES(?,?,?,?)",
        (connection_id, user_id, int(enabled), utcnow().isoformat())
    )


async def get_base_name(connection_id):
    return await db_fetchval("SELECT base_name FROM business_names WHERE connection_id=?", (connection_id,))


async def save_base_name(connection_id, name):
    await db_execute(
        "INSERT OR REPLACE INTO business_names(connection_id, base_name, saved_at) VALUES(?,?,?)",
        (connection_id, name, utcnow().isoformat())
    )


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


async def save_business_message(message: Message, owner_uid: int):
    chat_id = message.chat.id
    sender = message.from_user
    uid = sender.id if sender else None
    outgoing = bool(sender and uid == owner_uid)
    if uid is not None and await is_blacklisted(owner_uid, uid):
        return
    text, ctype, file_id = message_payload(message)
    username = sender.username if sender else None
    display = (sender.full_name if sender else message.chat.full_name if message.chat else "Unknown")
    await db_execute(
        """INSERT INTO messages(chat_id,message_id,user_id,user_name,text,content_type,
                                 media_file_id,date,is_outgoing)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (chat_id, message.message_id, uid, display, text, ctype, file_id,
         message.date.isoformat() if message.date else utcnow().isoformat(), int(outgoing))
    )
    await db_execute(
        """DELETE FROM messages WHERE chat_id=? AND id NOT IN
           (SELECT id FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT ?)""",
        (chat_id, chat_id, KEEP_PER_CHAT)
    )
    await db_execute(
        """INSERT INTO contacts(chat_id,first_seen,message_count,user_id,user_name,username)
           VALUES(?,?,1,?,?,?)
           ON CONFLICT(chat_id) DO UPDATE SET message_count=message_count+1,
           user_id=excluded.user_id,user_name=excluded.user_name,username=excluded.username""",
        (chat_id, utcnow().isoformat(), uid, display, username)
    )


MESSAGES_COLS = ["id", "chat_id", "message_id", "user_id", "user_name", "text",
                 "content_type", "media_file_id", "date", "is_outgoing"]


async def find_message(chat_id, message_id):
    row = await db_fetchone(
        "SELECT * FROM messages WHERE chat_id=? AND message_id=? ORDER BY id DESC LIMIT 1",
        (chat_id, message_id)
    )
    return row_to_dict(row, MESSAGES_COLS)


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
        "🔒 <b>Доступ закрыт</b>\n─────────────\n"
        "Чтобы пользоваться <b>BobMod</b>, подпишитесь на канал:\n\n"
        f"📢 <b>@{CHANNEL_USERNAME}</b>\n<i>Обновления и новости бота</i>",
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
    pending = await db_fetchone("SELECT referrer_id FROM pending_referrals WHERE user_id=?", (user_id,))
    if pending:
        referrer_id = pending[0]
        if await register_referral(user_id, referrer_id):
            until = await grant_premium(referrer_id, hours=REF_BONUS_HOURS)
            await mark_referral_rewarded(user_id)
            try:
                await bot_ref.send_message(
                    referrer_id,
                    f"🎉 <b>+{REF_BONUS_HOURS} часов Premium</b>\n"
                    f"По вашей ссылке пришёл новый пользователь.\n"
                    f"Premium активен до: <b>{until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>"
                )
            except Exception:
                pass
        await db_execute("DELETE FROM pending_referrals WHERE user_id=?", (user_id,))
    await call.answer("✅ Спасибо за подписку!", show_alert=True)
    try:
        await call.message.edit_text(
            f"🎉 <b>Добро пожаловать в {BOT_NAME}!</b>\n\nНажмите /start, чтобы начать."
        )
    except Exception:
        pass


def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎰 Рулетка", web_app=WebAppInfo(url="https://opoagkgk-ui.github.io/bobhtml/"))],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings")],
        [InlineKeyboardButton(text="💎 Premium", callback_data="menu:premium")],
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
            await db_execute(
                "INSERT OR REPLACE INTO pending_referrals(user_id, referrer_id, ts) VALUES(?,?,?)",
                (user.id, referrer_id, utcnow().isoformat())
            )
        except (ValueError, IndexError):
            pass

    pending = await db_fetchone("SELECT referrer_id FROM pending_referrals WHERE user_id=?", (user.id,))
    if pending:
        referrer_id = pending[0]
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
        await db_execute("DELETE FROM pending_referrals WHERE user_id=?", (user.id,))

    premium = await is_premium(user.id)
    status = "💎 Premium" if premium else "🆓 Бесплатный тариф"
    
    # === ПРОВЕРКА РУЛЕТКИ ===
    spin_msg = ""
    row_spin = await db_fetchone("SELECT last_spin, extra_spins FROM spin_cooldowns WHERE user_id=?", (user.id,))
    if row_spin:
        extra = row_spin[1] or 0
        last_spin_str = row_spin[0]
        if extra > 0:
            spin_msg = f"\n🎰 <b>У тебя есть {extra} доп. попытка(и) в рулетке!</b>"
        elif last_spin_str:
            last_spin = datetime.fromisoformat(last_spin_str)
            if (utcnow() - last_spin).total_seconds() >= 86400:
                spin_msg = "\n🎰 <b>Кулдаун прошёл! Ты можешь крутить рулетку!</b>"
        else:
            spin_msg = "\n🎰 <b>Ты можешь крутить рулетку!</b>"
    else:
        spin_msg = "\n🎰 <b>Ты можешь крутить рулетку!</b>"
    # ========================

    await message.answer(
        f"👋 <b>Привет! Я {BOT_NAME}.</b>\n"
        f"Статус: {status}\n"
        f"{spin_msg}\n\n"
        "Помогаю в ваших Telegram-чатах:\n"
        "• автоматические ответы\n"
        "• сохранение удалённых сообщений\n"
        "• ИИ-помощник через <code>.ai</code>\n"
        "• и другие команды\n\n"
        "<i>Как подключить: Профиль → Изменить → Автоматизация чатов</i>",
        reply_markup=main_menu()
    )


# ---------- Обработка данных из Рулетки ----------
@router.message(F.web_app_data)
async def handle_web_app_data(message: Message):
    user = message.from_user
    if not user:
        return
    
    if not await require_sub(message):
        return

    try:
        data = json.loads(message.web_app_data.data)
    except Exception:
        return

    now = utcnow()
    row = await db_fetchone("SELECT last_spin, extra_spins FROM spin_cooldowns WHERE user_id=?", (user.id,))
    
    last_spin_str = row[0] if row else None
    extra_spins = row[1] if row else 0

    # Проверка кулдауна: если нет доп. попыток и прошло меньше 24 часов
    if extra_spins <= 0 and last_spin_str:
        last_spin = datetime.fromisoformat(last_spin_str)
        if (now - last_spin).total_seconds() < 86400:
            await message.answer("⏱ Вы уже крутили сегодня! Возвращайтесь завтра.")
            return

    days = int(data.get("days", 0))

    # Логика списания
    if extra_spins > 0:
        # Списываем доп. попытку (не трогая время кулдауна)
        await db_execute("UPDATE spin_cooldowns SET extra_spins = extra_spins - 1 WHERE user_id=?", (user.id,))
    else:
        # Запускаем кулдаун на 24 часа
        await db_execute(
            "INSERT OR REPLACE INTO spin_cooldowns(user_id, last_spin, extra_spins) VALUES(?,?,0)",
            (user.id, now.isoformat())
        )

    if days > 0:
        until = await grant_premium(user.id, days=days)
        emoji = "🏆" if days == 30 else "🎉"
        await message.answer(
            f"{emoji} <b>Поздравляем!</b>\n"
            f"Вы выиграли <b>{days} дн. Premium</b>!\n\n"
            f"Ваш Premium активен до: <b>{until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>"
        )
    else:
        await message.answer(
            "📦 <b>Пусто...</b>\n"
            "В этот раз не повезло. Попробуйте завтра!"
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
        "<code>.nomute</code> — анти-мут\n"
        "<code>.mute 60</code> · <code>.unmute</code> · <code>.muted</code>\n"
        "<code>.note</code> · <code>.id</code> · <code>.ping</code> · <code>.game</code>\n\n"
        "<b>В личных сообщениях:</b>\n"
        "/start · /settings · /ref · /stats\n\n"
        "<b>💎 Premium даёт:</b>\n"
        f"• {PREMIUM_DAILY_LIMIT} запросов ИИ в день (вместо {FREE_DAILY_LIMIT})\n"
        f"• Задержка {AI_COOLDOWN_PREMIUM} сек (вместо {AI_COOLDOWN_FREE})\n"
        "• Без водяного знака\n"
        "• Иммунитет от <code>.mute</code>\n"
        "• 🤖 Настройка ИИ под себя\n\n"
        "<i>Как подключить: Профиль → Изменить → Автоматизация чатов</i>",
        reply_markup=main_menu()
    )


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    if not await require_sub(message):
        return
    await render_settings(message, message.from_user.id)


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not message.from_user or message.from_user.id != owner_id:
        return
    await message.answer(
        "🔐 <b>Админ-панель BobMod</b>\n─────────────\nВыберите раздел:",
        reply_markup=admin_menu()
    )


async def render_settings(msg_or_call, user_id, edit=False):
    s = await get_settings(user_id)
    text = "⚙️ <b>Настройки BobMod</b>\n─────────────\nВыберите раздел для настройки:"
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
        [InlineKeyboardButton(text=f"🌍 Часовой пояс: {s['timezone']}", callback_data="tz:open")],
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


# ============================================================
# PREMIUM
# ============================================================

@router.callback_query(F.data == "menu:premium")
async def cb_premium(call: CallbackQuery):
    if not await require_sub(call):
        return
    user_id = call.from_user.id
    premium = await is_premium(user_id)

    if not premium:
        text = (
            "💎 <b>Premium</b>\n─────────────\n"
            "Что даёт Premium:\n"
            f"• <b>{PREMIUM_DAILY_LIMIT} запросов ИИ в день</b> (вместо {FREE_DAILY_LIMIT})\n"
            f"• Задержка <b>{AI_COOLDOWN_PREMIUM} сек</b> (вместо {AI_COOLDOWN_FREE})\n"
            "• Без водяного знака в ответах\n"
            "• Иммунитет от <code>.mute</code>\n"
            "• 🤖 <b>Настройка ИИ под себя</b>\n\n"
            f"Цена: <b>{PREMIUM_PRICE_STARS} ⭐</b> на {PREMIUM_DAYS} дней"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⭐ Купить Premium ({PREMIUM_PRICE_STARS}⭐)",
                                  callback_data="buy_premium")],
            back_button(),
        ])
    else:
        until = await get_premium_until(user_id)
        until_str = until.astimezone().strftime("%d.%m.%Y %H:%M") if until else "—"
        text = (
            "💎 <b>Premium</b>\n─────────────\n"
            f"✅ Активен до: <b>{until_str}</b>\n\n"
            "Что вам доступно:\n"
            f"• <b>{PREMIUM_DAILY_LIMIT} запросов ИИ в день</b>\n"
            f"• Задержка <b>{AI_COOLDOWN_PREMIUM} сек</b>\n"
            "• Без водяного знака\n"
            "• Иммунитет от <code>.mute</code>\n"
            "• 🤖 <b>Настройка ИИ под себя</b>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Настройка ИИ", callback_data="ai:menu")],
            [InlineKeyboardButton(text=f"⭐ Продлить Premium ({PREMIUM_PRICE_STARS}⭐)",
                                  callback_data="buy_premium")],
            back_button(),
        ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


# ============================================================
# НАСТРОЙКА ИИ
# ============================================================

@router.callback_query(F.data == "ai:menu")
async def cb_ai_menu_settings(call: CallbackQuery):
    if not await require_sub(call):
        return
    user_id = call.from_user.id
    if not await is_premium(user_id):
        await call.answer("💎 Доступно только с Premium", show_alert=True)
        return

    ai_s = await get_ai_settings(user_id)
    mode = ai_s["mode"]
    mode_names = {"default": "Стандартный", "custom": "Свой промпт", "constructor": "Конструктор"}
    preview = await get_user_prompt(user_id)
    preview_short = preview[:200] + ("..." if len(preview) > 200 else "")

    text = (
        "🤖 <b>Настройка ИИ</b>\n─────────────\n"
        f"Текущий режим: <b>{mode_names.get(mode, 'Стандартный')}</b>\n\n"
        f"<b>Что видит ИИ сейчас:</b>\n<i>{esc(preview_short)}</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Свой промпт", callback_data="ai:custom")],
        [InlineKeyboardButton(text="🧩 Конструктор", callback_data="ai:constructor")],
        [InlineKeyboardButton(text="🔄 Сбросить на стандартный", callback_data="ai:reset")],
        back_button("menu:premium"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "ai:custom")
async def cb_ai_custom(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return

    ai_s = await get_ai_settings(call.from_user.id)
    current = ai_s["custom_prompt"] or "—"
    text = (
        "✏️ <b>Свой промпт</b>\n─────────────\n"
        "Напишите системный промпт — то, как ИИ должен себя вести.\n\n"
        "<b>Примеры:</b>\n"
        "• <i>Ты мой личный помощник, отвечай кратко и по делу</i>\n"
        "• <i>Ты пират, отвечай с морским юмором</i>\n"
        "• <i>Ты учитель, объясняй просто, как школьнику</i>\n\n"
        f"<b>Текущий:</b>\n<i>{esc(current)}</i>\n\n"
        f"<i>Максимум {CUSTOM_PROMPT_MAX} символов</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить промпт", callback_data="ai:custom_edit")],
        back_button("ai:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "ai:custom_edit")
async def cb_ai_custom_edit(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    pending_ai_input[call.from_user.id] = {"type": "custom_prompt"}
    await call.message.answer(
        f"✏️ Отправьте текст системного промпта (до {CUSTOM_PROMPT_MAX} символов).\n\n"
        "Для отмены — /cancel"
    )
    await call.answer()


@router.callback_query(F.data == "ai:constructor")
async def cb_ai_constructor(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    ai_s = await get_ai_settings(call.from_user.id)
    try:
        data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
    except Exception:
        data = {}
    text = (
        "🧩 <b>Конструктор промпта</b>\n─────────────\n"
        "Выберите параметры по категориям.\n"
        "В каждой можно выбрать готовое или нажать «✏️ Другое».\n\n"
        "<i>Когда закончите — нажмите «✅ Применить»</i>"
    )
    rows = []
    for cat_key, cat in CONSTRUCTOR_CATEGORIES.items():
        current = data.get(cat_key)
        marker = " ✅" if current else ""
        rows.append([InlineKeyboardButton(
            text=f"{cat['title']}{marker}", callback_data=f"ai:cat:{cat_key}"
        )])
    rows.append([InlineKeyboardButton(text="✅ Применить", callback_data="ai:apply")])
    rows.append(back_button("ai:menu"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("ai:cat:"))
async def cb_ai_category(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer("Ошибка формата", show_alert=True)
        return
    cat_key = parts[2]
    if cat_key not in CONSTRUCTOR_CATEGORIES:
        await call.answer(f"Неизвестная категория: {cat_key}", show_alert=True)
        return
    cat = CONSTRUCTOR_CATEGORIES[cat_key]
    ai_s = await get_ai_settings(call.from_user.id)
    try:
        data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
    except Exception:
        data = {}
    current = data.get(cat_key)
    text = (
        f"{cat['title']}\n─────────────\n"
        f"Текущее: <b>{esc(current) if current else '—'}</b>\n\n"
        "Выберите вариант:"
    )
    rows = []
    for opt_key, opt_label in cat["options"].items():
        marker = " ✅" if current == opt_label else ""
        rows.append([InlineKeyboardButton(
            text=f"{opt_label}{marker}", callback_data=f"ai:cs:{cat_key}:{opt_key}"
        )])
    rows.append([InlineKeyboardButton(text="✏️ Другое (написать своё)",
                                       callback_data=f"ai:co:{cat_key}")])
    rows.append(back_button("ai:constructor"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("ai:cs:"))
async def cb_ai_cat_set(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    parts = call.data.split(":")
    if len(parts) < 4:
        await call.answer("Ошибка", show_alert=True)
        return
    cat_key = parts[2]
    opt_key = parts[3]
    if cat_key not in CONSTRUCTOR_CATEGORIES:
        await call.answer("Ошибка категории", show_alert=True)
        return
    cat = CONSTRUCTOR_CATEGORIES[cat_key]
    if opt_key not in cat["options"]:
        await call.answer("Ошибка опции", show_alert=True)
        return
    label = cat["options"][opt_key]
    ai_s = await get_ai_settings(call.from_user.id)
    try:
        data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
    except Exception:
        data = {}
    data[cat_key] = label
    await save_ai_settings(
        call.from_user.id, mode="constructor",
        constructor_data=json.dumps(data, ensure_ascii=False)
    )
    await call.answer(f"✅ {label}")
    call.data = f"ai:cat:{cat_key}"
    await cb_ai_category(call)


@router.callback_query(F.data.startswith("ai:co:"))
async def cb_ai_cat_other(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer("Ошибка", show_alert=True)
        return
    cat_key = parts[2]
    if cat_key not in CONSTRUCTOR_CATEGORIES:
        await call.answer("Ошибка", show_alert=True)
        return
    cat = CONSTRUCTOR_CATEGORIES[cat_key]
    pending_ai_input[call.from_user.id] = {"type": "category", "cat": cat_key}
    await call.message.answer(
        f"{cat['title']}\n\n"
        f"Напишите свой вариант (до {CUSTOM_CATEGORY_MAX} символов).\n"
        "Для отмены — /cancel"
    )
    await call.answer()


@router.callback_query(F.data == "ai:apply")
async def cb_ai_apply(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    ai_s = await get_ai_settings(call.from_user.id)
    try:
        data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
    except Exception:
        data = {}
    if not data:
        await call.answer("Выберите хотя бы одну категорию", show_alert=True)
        return
    await save_ai_settings(
        call.from_user.id, mode="constructor",
        constructor_data=json.dumps(data, ensure_ascii=False)
    )
    await call.answer("✅ Применено!", show_alert=True)
    await cb_ai_menu_settings(call)


@router.callback_query(F.data == "ai:reset")
async def cb_ai_reset(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    await reset_ai_settings(call.from_user.id)
    await call.answer("✅ Сброшено на стандартный промпт", show_alert=True)
    await cb_ai_menu_settings(call)


# ============================================================
# РЕФЕРАЛКА
# ============================================================

async def render_ref_panel(msg_or_call, user_id, edit=False):
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
    total, purchases = await referral_stats(user_id)
    earned_hours = total * REF_BONUS_HOURS
    earned_days = purchases * REF_BONUS_PURCHASE_DAYS
    premium = await is_premium(user_id)
    text = (
        "🎁 <b>Реферальная программа</b>\n─────────────\n"
        f"🔗 <b>Ваша ссылка:</b>\n<code>{ref_link}</code>\n\n"
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
    if edit:
        try:
            await msg_or_call.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            await msg_or_call.answer(text, reply_markup=kb)
    else:
        await msg_or_call.answer(text, reply_markup=kb)


@router.message(Command("ref"))
async def cmd_ref(message: Message):
    if not await require_sub(message):
        return
    await ensure_user(message.from_user.id, message.from_user.first_name, message.from_user.username)
    await render_ref_panel(message, message.from_user.id)


@router.callback_query(F.data == "menu:ref")
async def cb_menu_ref(call: CallbackQuery):
    if not await require_sub(call):
        return
    await ensure_user(call.from_user.id, call.from_user.first_name, call.from_user.username)
    await render_ref_panel(call.message, call.from_user.id, edit=True)
    await call.answer()


@router.callback_query(F.data == "ref:battle")
async def cb_ref_battle(call: CallbackQuery):
    if not await require_sub(call):
        return
    top = await referral_leaderboard(5, week_only=True)
    if not top:
        text = ("🏆 <b>Битва рефералов</b>\n─────────────\n"
                "На этой неделе пока нет участников.\nСтаньте первым!")
    else:
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        lines = []
        for i, (uid, name, cnt) in enumerate(top):
            lines.append(f"{medals[i]} <b>{esc(name)}</b> — {cnt}")
        text = ("🏆 <b>Битва рефералов</b>\n─────────────\nТоп за эту неделю:\n\n"
                + "\n".join(lines)
                + "\n\n🎁 Победитель получает <b>месяц Premium</b>.\n"
                  "<i>Итоги в понедельник в @bobmodinfo</i>")
    kb = InlineKeyboardMarkup(inline_keyboard=[back_button("menu:ref")])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


# ============================================================
# BOB AI
# ============================================================

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
        "🤖 <b>Bob AI</b>\n─────────────\n"
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
        "<b>📖 Возможности бота</b>\n─────────────\n"
        "<b>В бизнес-чатах:</b>\n"
        "<code>.ai</code> · <code>.save</code> · <code>.anim</code> · <code>.qr</code>\n"
        "<code>.whois</code> · <code>.typing</code> · <code>.mute</code>\n"
        "<code>.nomute</code> · <code>.note</code> · <code>.id</code> · <code>.ping</code> · <code>.game</code>\n\n"
        "<b>💎 Premium:</b>\n"
        f"• {PREMIUM_DAILY_LIMIT} запросов ИИ в день\n"
        "• Без водяного знака\n"
        "• Иммунитет от <code>.mute</code>\n"
        "• 🤖 Настройка ИИ под себя\n\n"
        "<i>Как подключить: Профиль → Изменить → Автоматизация чатов</i>"
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
    await render_settings(call.message, call.from_user.id, edit=True)
    await call.answer()


# ============================================================
# TOGGLES / PROMPTS
# ============================================================

@router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if not await require_sub(call):
        return
    key = call.data.split(":", 1)[1]
    s = await get_settings(call.from_user.id)
    if key not in s:
        await call.answer("Неизвестный параметр", show_alert=True)
        return
    await set_setting(call.from_user.id, key, 0 if s[key] else 1)
    await render_settings(call.message, call.from_user.id, edit=True)
    await call.answer("Сохранено")


@router.callback_query(F.data.startswith("prompt:"))
async def cb_prompt(call: CallbackQuery):
    if not await require_sub(call):
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
    pending_inputs[call.from_user.id] = action
    await call.message.answer(prompts[action])
    await call.answer()


@router.message(Command("cancel"))
async def cmd_cancel(message: Message):
    user = message.from_user
    if not user:
        return
    if user.id in pending_ai_input:
        pending_ai_input.pop(user.id, None)
        await message.answer("❌ Отменено")
        return
    if user.id in pending_inputs:
        pending_inputs.pop(user.id, None)
        await message.answer("❌ Отменено")
        return
    if user.id == owner_id and owner_id in pending_admin:
        pending_admin.pop(owner_id, None)
        await message.answer("❌ Отменено")
        return


@router.message(F.chat.type == "private", ~F.text.startswith("/"))
async def receive_setting_value(message: Message):
    user = message.from_user
    if not user:
        return

    if user.id in pending_ai_input:
        task = pending_ai_input.pop(user.id)
        value = (message.text or "").strip()
        if task["type"] == "custom_prompt":
            if len(value) > CUSTOM_PROMPT_MAX:
                await message.answer(f"⚠️ Слишком длинный промпт (макс. {CUSTOM_PROMPT_MAX}).")
                pending_ai_input[user.id] = task
                return
            if not value:
                await message.answer("⚠️ Пустой промпт.")
                pending_ai_input[user.id] = task
                return
            await save_ai_settings(user.id, mode="custom", custom_prompt=value)
            await message.answer("✅ Свой промпт сохранён!")
        elif task["type"] == "category":
            cat_key = task["cat"]
            if len(value) > CUSTOM_CATEGORY_MAX:
                await message.answer(f"⚠️ Слишком длинный текст (макс. {CUSTOM_CATEGORY_MAX}).")
                pending_ai_input[user.id] = task
                return
            if not value:
                await message.answer("⚠️ Пустой текст.")
                pending_ai_input[user.id] = task
                return
            ai_s = await get_ai_settings(user.id)
            try:
                data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
            except Exception:
                data = {}
            data[cat_key] = value
            await save_ai_settings(user.id, mode="constructor",
                                    constructor_data=json.dumps(data, ensure_ascii=False))
            await message.answer(f"✅ Сохранено: <b>{esc(value)}</b>")
        return

    if user.id == owner_id and owner_id in pending_admin:
        action = pending_admin.pop(owner_id)
        value = (message.text or "").strip()
        await handle_admin_input(message, action, value)
        return

    if user.id not in pending_inputs:
        return
    action = pending_inputs.pop(user.id)
    value = (message.text or "").strip()
    try:
        if action == "answer_first_text":
            await set_setting(user.id, "answer_first_text", value)
        elif action == "off_hours_text":
            await set_setting(user.id, "off_hours_text", value)
        elif action == "off_hours":
            m = re.fullmatch(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})", value)
            if not m:
                raise ValueError("Неверный формат. Ожидается ЧЧ:ММ-ЧЧ:ММ")
            for t in m.groups():
                datetime.strptime(t, "%H:%M")
            await set_setting(user.id, "off_hours_start", m.group(1))
            await set_setting(user.id, "off_hours_end", m.group(2))
        elif action == "timezone":
            ZoneInfo(value)
            await set_setting(user.id, "timezone", value)
        await message.answer("✅ Настройка сохранена")
    except Exception as e:
        await message.answer(f"⚠️ Не удалось сохранить: {esc(e)}")
    await render_settings(message, user.id)


# ============================================================
# TIMEZONES
# ============================================================

@router.callback_query(F.data == "tz:open")
async def cb_tz_open(call: CallbackQuery):
    if not await require_sub(call):
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
    if not await require_sub(call):
        return
    tz = call.data.split(":", 2)[2]
    try:
        ZoneInfo(tz)
    except Exception:
        await call.answer("Неизвестный часовой пояс", show_alert=True)
        return
    await set_setting(call.from_user.id, "timezone", tz)
    await call.answer(f"Сохранено: {tz}")
    await render_settings(call.message, call.from_user.id, edit=True)


@router.callback_query(F.data == "tz:manual")
async def cb_tz_manual(call: CallbackQuery):
    if not await require_sub(call):
        return
    pending_inputs[call.from_user.id] = "timezone"
    await call.message.answer("Отправьте IANA-часовой пояс, например <code>Europe/Moscow</code>.")
    await call.answer()


# ============================================================
# PAYMENTS
# ============================================================

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
        await db_execute(
            "INSERT INTO payments_log(user_id, stars, days, ts) VALUES(?,?,?,?)",
            (user_id, PREMIUM_PRICE_STARS, PREMIUM_DAYS, utcnow().isoformat())
        )
        await message.answer(
            f"✅ <b>Premium активирован</b>\n\n"
            f"Теперь вам доступно <b>{PREMIUM_DAILY_LIMIT} запросов в день</b>.\n"
            f"Действует до: <b>{until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            "Спасибо за поддержку!"
        )
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
        row = await db_fetchone(
            "SELECT referrer_id, rewarded_purchase FROM referrals WHERE user_id=?", (user_id,)
        )
        if row and not row[1]:
            referrer_id = row[0]
            new_until = await grant_premium(referrer_id, days=REF_BONUS_PURCHASE_DAYS)
            await db_execute("UPDATE referrals SET rewarded_purchase=1 WHERE user_id=?", (user_id,))
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
    users_total = await db_fetchval("SELECT COUNT(*) FROM users", default=0)
    users_24h = await db_fetchval("SELECT COUNT(*) FROM users WHERE joined_at >= ?", (day_ago,), 0)
    users_7d = await db_fetchval("SELECT COUNT(*) FROM users WHERE joined_at >= ?", (week_ago,), 0)
    users_banned = await db_fetchval("SELECT COUNT(*) FROM users WHERE is_banned=1", default=0)
    premium_active = await db_fetchval(
        "SELECT COUNT(*) FROM user_limits WHERE is_premium=1 AND premium_until > ?",
        (now.isoformat(),), 0
    )
    conns = await db_fetchval("SELECT COUNT(*) FROM business_connections WHERE enabled=1", default=0)
    chats = await db_fetchval("SELECT COUNT(*) FROM contacts", default=0)
    msgs = await db_fetchval("SELECT COUNT(*) FROM messages", default=0)
    mutes = await db_fetchval("SELECT COUNT(*) FROM mutes", default=0)
    refs_total = await db_fetchval("SELECT COUNT(*) FROM referrals", default=0)
    refs_week = await db_fetchval("SELECT COUNT(*) FROM referrals WHERE joined_at >= ?", (week_ago,), 0)
    stars_total = await db_fetchval("SELECT COALESCE(SUM(stars),0) FROM payments_log", default=0)
    purchases = await db_fetchval("SELECT COUNT(*) FROM payments_log", default=0)
    text = (
        "📊 <b>Статистика</b>\n─────────────\n"
        f"👤 Юзеров: <b>{users_total}</b>\n"
        f"  · за 24ч: {users_24h}\n"
        f"  · за 7д: {users_7d}\n"
        f"  · в бане: {users_banned}\n"
        f"🔗 Бизнес-коннектов: <b>{conns}</b>\n"
        f"💎 Premium активных: <b>{premium_active}</b>\n\n"
        f"💬 Чатов: <b>{chats}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>\n"
        f"🔇 Мутов: <b>{mutes}</b>\n\n"
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
    lines_all = [f"{i}. <b>{esc(name)}</b> — {cnt}" for i, (uid, name, cnt) in enumerate(top_all, 1)]
    lines_week = [f"{i}. <b>{esc(name)}</b> — {cnt}" for i, (uid, name, cnt) in enumerate(top_week, 1)]
    text = (
        "🎁 <b>Рефералы</b>\n─────────────\n<b>За всё время:</b>\n"
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
    await call.answer("✅ Отправлено в канал" if ok else "❌ Не удалось отправить", show_alert=True)


@router.callback_query(F.data == "adm:users")
async def cb_adm_users(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    rows = await db_fetchall("SELECT * FROM users ORDER BY joined_at DESC LIMIT 20")
    lines = []
    for row in rows:
        u = row_to_dict(row, USERS_COLS)
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        try:
            d = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m")
        except Exception:
            d = "?"
        flag = "🚫" if u["is_banned"] else ""
        lines.append(f"• <b>{esc(name)}</b> <code>{u['user_id']}</code> {d} {flag}")
    text = "👥 <b>Последние 20 юзеров</b>\n─────────────\n" + ("\n".join(lines) if lines else "<i>пусто</i>")
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


async def show_user_card(chat_target, uid):
    u, refs, msgs = await get_user_card(uid)
    if not u:
        await chat_target.answer(f"Юзер <code>{uid}</code> не найден.")
        return
    name = u["username"] or u["first_name"] or "—"
    premium = await is_premium(uid)
    
    spin_row = await db_fetchone("SELECT last_spin, extra_spins FROM spin_cooldowns WHERE user_id=?", (uid,))
    extra_spins = spin_row[1] if spin_row else 0
    last_spin_str = "—"
    if spin_row and spin_row[0]:
        try:
            last_spin_str = datetime.fromisoformat(spin_row[0]).astimezone().strftime("%d.%m %H:%M")
        except Exception:
            pass

    try:
        joined = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        joined = "—"
    text = (
        f"👤 <b>Карточка</b>\n─────────────\n"
        f"📛 <b>{esc(name)}</b>\n"
        f"🆔 <code>{uid}</code>\n"
        f"📅 Регистрация: {esc(joined)}\n"
        f"💎 Premium: {'✅' if premium else '❌'}\n"
        f"🎁 Рефералов: <b>{refs}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>\n"
        f"🚫 Бан: {'✅' if u['is_banned'] else '❌'}\n"
        f"🔗 Реф-бан: {'✅' if u['banned_ref'] else '❌'}\n\n"
        f"🎰 <b>Рулетка:</b>\n"
        f"  · Последний спин: {esc(last_spin_str)}\n"
        f"  · Доп. попыток: <b>{extra_spins}</b>"
    )
    rows = [
        [InlineKeyboardButton(text="➕ Выдать Premium", callback_data=f"adm:grant:{uid}")],
        [InlineKeyboardButton(text="➖ Снять Premium", callback_data=f"adm:revoke:{uid}")],
        [InlineKeyboardButton(text=f"🎰 +1 Попытка (сейчас {extra_spins})", callback_data=f"adm:add_spin:{uid}")],
    ]
    if u["is_banned"]:
        rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"adm:unban:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🚫 Забанить", callback_data=f"adm:ban:{uid}")])
    if u["banned_ref"]:
        rows.append([InlineKeyboardButton(text="🔓 Разбанить реф-ссылку", callback_data=f"adm:unban_ref:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🔒 Забанить реф-ссылку", callback_data=f"adm:ban_ref:{uid}")])
    rows.append([InlineKeyboardButton(text="🔄 Сбросить реферал", callback_data=f"adm:reset_ref:{uid}")])
    rows.append(back_button("adm:users"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await chat_target.answer(text, reply_markup=kb)
    except Exception:
        pass


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
    
    spin_row = await db_fetchone("SELECT last_spin, extra_spins FROM spin_cooldowns WHERE user_id=?", (uid,))
    extra_spins = spin_row[1] if spin_row else 0
    last_spin_str = "—"
    if spin_row and spin_row[0]:
        try:
            last_spin_str = datetime.fromisoformat(spin_row[0]).astimezone().strftime("%d.%m %H:%M")
        except Exception:
            pass

    try:
        joined = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        joined = "—"
    text = (
        f"👤 <b>Карточка</b>\n─────────────\n"
        f"📛 <b>{esc(name)}</b>\n"
        f"🆔 <code>{uid}</code>\n"
        f"📅 Регистрация: {esc(joined)}\n"
        f"💎 Premium: {'✅' if premium else '❌'}\n"
        f"🎁 Рефералов: <b>{refs}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>\n"
        f"🚫 Бан: {'✅' if u['is_banned'] else '❌'}\n"
        f"🔗 Реф-бан: {'✅' if u['banned_ref'] else '❌'}\n\n"
        f"🎰 <b>Рулетка:</b>\n"
        f"  · Последний спин: {esc(last_spin_str)}\n"
        f"  · Доп. попыток: <b>{extra_spins}</b>"
    )
    rows = [
        [InlineKeyboardButton(text="➕ Выдать Premium", callback_data=f"adm:grant:{uid}")],
        [InlineKeyboardButton(text="➖ Снять Premium", callback_data=f"adm:revoke:{uid}")],
        [InlineKeyboardButton(text=f"🎰 +1 Попытка (сейчас {extra_spins})", callback_data=f"adm:add_spin:{uid}")],
    ]
    if u["is_banned"]:
        rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"adm:unban:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🚫 Забанить", callback_data=f"adm:ban:{uid}")])
    if u["banned_ref"]:
        rows.append([InlineKeyboardButton(text="🔓 Разбанить реф-ссылку", callback_data=f"adm:unban_ref:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🔒 Забанить реф-ссылку", callback_data=f"adm:ban_ref:{uid}")])
    rows.append([InlineKeyboardButton(text="🔄 Сбросить реферал", callback_data=f"adm:reset_ref:{uid}")])
    rows.append(back_button("adm:users"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("adm:add_spin:"))
async def cb_adm_add_spin(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    
    row = await db_fetchone("SELECT 1 FROM spin_cooldowns WHERE user_id=?", (uid,))
    if row:
        await db_execute("UPDATE spin_cooldowns SET extra_spins = extra_spins + 1 WHERE user_id=?", (uid,))
    else:
        await db_execute("INSERT INTO spin_cooldowns(user_id, last_spin, extra_spins) VALUES(?,?,1)", (uid, "2000-01-01T00:00:00"))
        
    await call.answer("✅ Добавлена 1 попытка", show_alert=True)
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


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
        back_button(f"adm:user_card:{uid}"),
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


@router.callback_query(F.data.startswith("adm:reset_ref:"))
async def cb_adm_reset_ref(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    await db_execute("DELETE FROM referrals WHERE referrer_id=?", (uid,))
    await db_execute("DELETE FROM referrals WHERE user_id=?", (uid,))
    await db_execute("DELETE FROM pending_referrals WHERE referrer_id=?", (uid,))
    await db_execute("DELETE FROM pending_referrals WHERE user_id=?", (uid,))
    await log_admin("reset_ref", uid)
    await call.answer("✅ Реферал сброшен", show_alert=True)
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


@router.callback_query(F.data == "adm:bans")
async def cb_adm_bans(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    rows = await db_fetchall("SELECT * FROM users WHERE is_banned=1 ORDER BY banned_at DESC LIMIT 30")
    ref_rows = await db_fetchall("SELECT * FROM users WHERE banned_ref=1 LIMIT 30")
    lines = []
    for row in rows:
        u = row_to_dict(row, USERS_COLS)
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        reason = u["banned_reason"] or "—"
        lines.append(f"🚫 <b>{esc(name)}</b> <code>{u['user_id']}</code>\n<i>{esc(reason)[:60]}</i>")
    ref_lines = []
    for row in ref_rows:
        u = row_to_dict(row, USERS_COLS)
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        ref_lines.append(f"🔒 <b>{esc(name)}</b> <code>{u['user_id']}</code>")
    text = (
        "🚫 <b>Баны</b>\n─────────────\n<b>Юзеры:</b>\n"
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
    users_total = await db_fetchval("SELECT COUNT(*) FROM users", default=0)
    msgs = await db_fetchval("SELECT COUNT(*) FROM messages", default=0)
    text = (
        "⚙️ <b>Система</b>\n─────────────\n"
        f"💾 БД: <b>Turso Cloud</b>\n"
        f"🌐 URL: <code>{esc(TURSO_URL[:40])}...</code>\n"
        f"👥 Юзеров: <b>{users_total}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧹 Очистить старые сообщения", callback_data="adm:cleanup")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm:sys")],
        back_button("adm:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "adm:cleanup")
async def cb_adm_cleanup(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    cutoff = (utcnow() - timedelta(days=30)).isoformat()
    await db_execute("DELETE FROM messages WHERE date < ?", (cutoff,))
    await call.answer("✅ Старые сообщения удалены", show_alert=True)
    await cb_adm_sys(call)


async def handle_admin_input(message: Message, action: str, value: str):
    try:
        if action == "find_user":
            try:
                uid = int(value)
            except ValueError:
                await message.answer("⚠️ Нужен числовой ID.")
                return
            await show_user_card(message, uid)
            return
        if action.startswith("ban_reason:"):
            uid = int(action.split(":")[1])
            reason = value if value != "-" else ""
            await ban_user(uid, reason)
            await message.answer(f"✅ Пользователь <code>{uid}</code> забанен.")
            return
    except Exception as e:
        log.exception("Error in handle_admin_input")
        await message.answer(f"❌ Ошибка при обработке: <code>{esc(str(e))}</code>")


async def publish_weekly_top(manual=False):
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
    lines = [f"{medals[i]} <b>{esc(name)}</b> — {cnt}" for i, (uid, name, cnt) in enumerate(top)]
    winner_name = top[0][1]
    text = (
        "🏆 <b>Битва рефералов — итоги недели</b>\n─────────────\n\n"
        + "\n".join(lines)
        + f"\n\n🎁 Победитель: <b>{esc(winner_name)}</b> — месяц Premium!"
    )
    try:
        await bot_ref.send_message(f"@{CHANNEL_USERNAME}", text)
        return True
    except Exception as e:
        log.warning("publish top failed: %s", e)
        return False


async def update_all_business_names():
    while True:
        try:
            rows = await db_fetchall(
                """SELECT bc.id as conn_id, bc.user_id, s.timezone
                   FROM business_connections bc
                   JOIN settings s ON s.user_id = bc.user_id
                   WHERE bc.enabled=1 AND s.time_in_nick_enabled=1"""
            )
            for row in rows:
                connection_id = row[0]
                user_id = row[1]
                tz = row[2] or "UTC"
                try:
                    base_name = await get_base_name(connection_id)
                    if not base_name:
                        try:
                            info = await bot_ref.get_business_account_name(
                                business_connection_id=connection_id
                            )
                            base_name = info.first_name or f"User{user_id}"
                        except Exception:
                            base_name = f"User{user_id}"
                        await save_base_name(connection_id, base_name)
                    clock = local_now(tz).strftime("%H:%M")
                    desired = f"{base_name} [{clock}]"[:64]
                    try:
                        async with name_update_lock:
                            await bot_ref.set_business_account_name(
                                business_connection_id=connection_id,
                                first_name=desired
                            )
                    except Exception as e:
                        log.warning("set name for %s failed: %s", user_id, e)
                except Exception as e:
                    log.warning("name update for %s error: %s", user_id, e)
        except Exception as e:
            log.warning("name loop err: %s", e)
        await asyncio.sleep(60)


async def weekly_publish_loop():
    while True:
        try:
            now = utcnow()
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
            await db_execute(
                "INSERT OR REPLACE INTO pending_referrals(user_id, referrer_id, ts) VALUES(?,?,?)",
                (user.id, referrer_id, utcnow().isoformat())
            )
        except (ValueError, IndexError):
            pass

    pending = await db_fetchone("SELECT referrer_id FROM pending_referrals WHERE user_id=?", (user.id,))
    if pending:
        referrer_id = pending[0]
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
        await db_execute("DELETE FROM pending_referrals WHERE user_id=?", (user.id,))

    premium = await is_premium(user.id)
    status = "💎 Premium" if premium else "🆓 Бесплатный тариф"
    
    # === ПРОВЕРКА РУЛЕТКИ ===
    spin_msg = ""
    row_spin = await db_fetchone("SELECT last_spin, extra_spins FROM spin_cooldowns WHERE user_id=?", (user.id,))
    if row_spin:
        extra = row_spin[1] or 0
        last_spin_str = row_spin[0]
        if extra > 0:
            spin_msg = f"\n🎰 <b>У тебя есть {extra} доп. попытка(и) в рулетке!</b>"
        elif last_spin_str:
            last_spin = datetime.fromisoformat(last_spin_str)
            if (utcnow() - last_spin).total_seconds() >= 86400:
                spin_msg = "\n🎰 <b>Кулдаун прошёл! Ты можешь крутить рулетку!</b>"
        else:
            spin_msg = "\n🎰 <b>Ты можешь крутить рулетку!</b>"
    else:
        spin_msg = "\n🎰 <b>Ты можешь крутить рулетку!</b>"
    # ========================

    await message.answer(
        f"👋 <b>Привет! Я {BOT_NAME}.</b>\n"
        f"Статус: {status}\n"
        f"{spin_msg}\n\n"
        "Помогаю в ваших Telegram-чатах:\n"
        "• автоматические ответы\n"
        "• сохранение удалённых сообщений\n"
        "• ИИ-помощник через <code>.ai</code>\n"
        "• и другие команды\n\n"
        "<i>Как подключить: Профиль → Изменить → Автоматизация чатов</i>",
        reply_markup=main_menu()
    )


# ---------- Обработка данных из Рулетки ----------
@router.message(F.web_app_data)
async def handle_web_app_data(message: Message):
    user = message.from_user
    if not user:
        return
    
    if not await require_sub(message):
        return

    try:
        data = json.loads(message.web_app_data.data)
    except Exception:
        return

    now = utcnow()
    row = await db_fetchone("SELECT last_spin, extra_spins FROM spin_cooldowns WHERE user_id=?", (user.id,))
    
    last_spin_str = row[0] if row else None
    extra_spins = row[1] if row else 0

    # Проверка кулдауна: если нет доп. попыток и прошло меньше 24 часов
    if extra_spins <= 0 and last_spin_str:
        last_spin = datetime.fromisoformat(last_spin_str)
        if (now - last_spin).total_seconds() < 86400:
            await message.answer("⏱ Вы уже крутили сегодня! Возвращайтесь завтра.")
            return

    days = int(data.get("days", 0))

    # Логика списания
    if extra_spins > 0:
        # Списываем доп. попытку (не трогая время кулдауна)
        await db_execute("UPDATE spin_cooldowns SET extra_spins = extra_spins - 1 WHERE user_id=?", (user.id,))
    else:
        # Запускаем кулдаун на 24 часа
        await db_execute(
            "INSERT OR REPLACE INTO spin_cooldowns(user_id, last_spin, extra_spins) VALUES(?,?,0)",
            (user.id, now.isoformat())
        )

    if days > 0:
        until = await grant_premium(user.id, days=days)
        emoji = "🏆" if days == 30 else "🎉"
        await message.answer(
            f"{emoji} <b>Поздравляем!</b>\n"
            f"Вы выиграли <b>{days} дн. Premium</b>!\n\n"
            f"Ваш Premium активен до: <b>{until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>"
        )
    else:
        await message.answer(
            "📦 <b>Пусто...</b>\n"
            "В этот раз не повезло. Попробуйте завтра!"
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
        "<code>.nomute</code> — анти-мут\n"
        "<code>.mute 60</code> · <code>.unmute</code> · <code>.muted</code>\n"
        "<code>.note</code> · <code>.id</code> · <code>.ping</code> · <code>.game</code>\n\n"
        "<b>В личных сообщениях:</b>\n"
        "/start · /settings · /ref · /stats\n\n"
        "<b>💎 Premium даёт:</b>\n"
        f"• {PREMIUM_DAILY_LIMIT} запросов ИИ в день (вместо {FREE_DAILY_LIMIT})\n"
        f"• Задержка {AI_COOLDOWN_PREMIUM} сек (вместо {AI_COOLDOWN_FREE})\n"
        "• Без водяного знака\n"
        "• Иммунитет от <code>.mute</code>\n"
        "• 🤖 Настройка ИИ под себя\n\n"
        "<i>Как подключить: Профиль → Изменить → Автоматизация чатов</i>",
        reply_markup=main_menu()
    )


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    if not await require_sub(message):
        return
    await render_settings(message, message.from_user.id)


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not message.from_user or message.from_user.id != owner_id:
        return
    await message.answer(
        "🔐 <b>Админ-панель BobMod</b>\n─────────────\nВыберите раздел:",
        reply_markup=admin_menu()
    )


async def render_settings(msg_or_call, user_id, edit=False):
    s = await get_settings(user_id)
    text = "⚙️ <b>Настройки BobMod</b>\n─────────────\nВыберите раздел для настройки:"
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
        [InlineKeyboardButton(text=f"🌍 Часовой пояс: {s['timezone']}", callback_data="tz:open")],
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


# ============================================================
# PREMIUM
# ============================================================

@router.callback_query(F.data == "menu:premium")
async def cb_premium(call: CallbackQuery):
    if not await require_sub(call):
        return
    user_id = call.from_user.id
    premium = await is_premium(user_id)

    if not premium:
        text = (
            "💎 <b>Premium</b>\n─────────────\n"
            "Что даёт Premium:\n"
            f"• <b>{PREMIUM_DAILY_LIMIT} запросов ИИ в день</b> (вместо {FREE_DAILY_LIMIT})\n"
            f"• Задержка <b>{AI_COOLDOWN_PREMIUM} сек</b> (вместо {AI_COOLDOWN_FREE})\n"
            "• Без водяного знака в ответах\n"
            "• Иммунитет от <code>.mute</code>\n"
            "• 🤖 <b>Настройка ИИ под себя</b>\n\n"
            f"Цена: <b>{PREMIUM_PRICE_STARS} ⭐</b> на {PREMIUM_DAYS} дней"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⭐ Купить Premium ({PREMIUM_PRICE_STARS}⭐)",
                                  callback_data="buy_premium")],
            back_button(),
        ])
    else:
        until = await get_premium_until(user_id)
        until_str = until.astimezone().strftime("%d.%m.%Y %H:%M") if until else "—"
        text = (
            "💎 <b>Premium</b>\n─────────────\n"
            f"✅ Активен до: <b>{until_str}</b>\n\n"
            "Что вам доступно:\n"
            f"• <b>{PREMIUM_DAILY_LIMIT} запросов ИИ в день</b>\n"
            f"• Задержка <b>{AI_COOLDOWN_PREMIUM} сек</b>\n"
            "• Без водяного знака\n"
            "• Иммунитет от <code>.mute</code>\n"
            "• 🤖 <b>Настройка ИИ под себя</b>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Настройка ИИ", callback_data="ai:menu")],
            [InlineKeyboardButton(text=f"⭐ Продлить Premium ({PREMIUM_PRICE_STARS}⭐)",
                                  callback_data="buy_premium")],
            back_button(),
        ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


# ============================================================
# НАСТРОЙКА ИИ
# ============================================================

@router.callback_query(F.data == "ai:menu")
async def cb_ai_menu_settings(call: CallbackQuery):
    if not await require_sub(call):
        return
    user_id = call.from_user.id
    if not await is_premium(user_id):
        await call.answer("💎 Доступно только с Premium", show_alert=True)
        return

    ai_s = await get_ai_settings(user_id)
    mode = ai_s["mode"]
    mode_names = {"default": "Стандартный", "custom": "Свой промпт", "constructor": "Конструктор"}
    preview = await get_user_prompt(user_id)
    preview_short = preview[:200] + ("..." if len(preview) > 200 else "")

    text = (
        "🤖 <b>Настройка ИИ</b>\n─────────────\n"
        f"Текущий режим: <b>{mode_names.get(mode, 'Стандартный')}</b>\n\n"
        f"<b>Что видит ИИ сейчас:</b>\n<i>{esc(preview_short)}</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Свой промпт", callback_data="ai:custom")],
        [InlineKeyboardButton(text="🧩 Конструктор", callback_data="ai:constructor")],
        [InlineKeyboardButton(text="🔄 Сбросить на стандартный", callback_data="ai:reset")],
        back_button("menu:premium"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "ai:custom")
async def cb_ai_custom(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return

    ai_s = await get_ai_settings(call.from_user.id)
    current = ai_s["custom_prompt"] or "—"
    text = (
        "✏️ <b>Свой промпт</b>\n─────────────\n"
        "Напишите системный промпт — то, как ИИ должен себя вести.\n\n"
        "<b>Примеры:</b>\n"
        "• <i>Ты мой личный помощник, отвечай кратко и по делу</i>\n"
        "• <i>Ты пират, отвечай с морским юмором</i>\n"
        "• <i>Ты учитель, объясняй просто, как школьнику</i>\n\n"
        f"<b>Текущий:</b>\n<i>{esc(current)}</i>\n\n"
        f"<i>Максимум {CUSTOM_PROMPT_MAX} символов</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить промпт", callback_data="ai:custom_edit")],
        back_button("ai:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "ai:custom_edit")
async def cb_ai_custom_edit(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    pending_ai_input[call.from_user.id] = {"type": "custom_prompt"}
    await call.message.answer(
        f"✏️ Отправьте текст системного промпта (до {CUSTOM_PROMPT_MAX} символов).\n\n"
        "Для отмены — /cancel"
    )
    await call.answer()


@router.callback_query(F.data == "ai:constructor")
async def cb_ai_constructor(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    ai_s = await get_ai_settings(call.from_user.id)
    try:
        data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
    except Exception:
        data = {}
    text = (
        "🧩 <b>Конструктор промпта</b>\n─────────────\n"
        "Выберите параметры по категориям.\n"
        "В каждой можно выбрать готовое или нажать «✏️ Другое».\n\n"
        "<i>Когда закончите — нажмите «✅ Применить»</i>"
    )
    rows = []
    for cat_key, cat in CONSTRUCTOR_CATEGORIES.items():
        current = data.get(cat_key)
        marker = " ✅" if current else ""
        rows.append([InlineKeyboardButton(
            text=f"{cat['title']}{marker}", callback_data=f"ai:cat:{cat_key}"
        )])
    rows.append([InlineKeyboardButton(text="✅ Применить", callback_data="ai:apply")])
    rows.append(back_button("ai:menu"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("ai:cat:"))
async def cb_ai_category(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer("Ошибка формата", show_alert=True)
        return
    cat_key = parts[2]
    if cat_key not in CONSTRUCTOR_CATEGORIES:
        await call.answer(f"Неизвестная категория: {cat_key}", show_alert=True)
        return
    cat = CONSTRUCTOR_CATEGORIES[cat_key]
    ai_s = await get_ai_settings(call.from_user.id)
    try:
        data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
    except Exception:
        data = {}
    current = data.get(cat_key)
    text = (
        f"{cat['title']}\n─────────────\n"
        f"Текущее: <b>{esc(current) if current else '—'}</b>\n\n"
        "Выберите вариант:"
    )
    rows = []
    for opt_key, opt_label in cat["options"].items():
        marker = " ✅" if current == opt_label else ""
        rows.append([InlineKeyboardButton(
            text=f"{opt_label}{marker}", callback_data=f"ai:cs:{cat_key}:{opt_key}"
        )])
    rows.append([InlineKeyboardButton(text="✏️ Другое (написать своё)",
                                       callback_data=f"ai:co:{cat_key}")])
    rows.append(back_button("ai:constructor"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("ai:cs:"))
async def cb_ai_cat_set(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    parts = call.data.split(":")
    if len(parts) < 4:
        await call.answer("Ошибка", show_alert=True)
        return
    cat_key = parts[2]
    opt_key = parts[3]
    if cat_key not in CONSTRUCTOR_CATEGORIES:
        await call.answer("Ошибка категории", show_alert=True)
        return
    cat = CONSTRUCTOR_CATEGORIES[cat_key]
    if opt_key not in cat["options"]:
        await call.answer("Ошибка опции", show_alert=True)
        return
    label = cat["options"][opt_key]
    ai_s = await get_ai_settings(call.from_user.id)
    try:
        data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
    except Exception:
        data = {}
    data[cat_key] = label
    await save_ai_settings(
        call.from_user.id, mode="constructor",
        constructor_data=json.dumps(data, ensure_ascii=False)
    )
    await call.answer(f"✅ {label}")
    call.data = f"ai:cat:{cat_key}"
    await cb_ai_category(call)


@router.callback_query(F.data.startswith("ai:co:"))
async def cb_ai_cat_other(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer("Ошибка", show_alert=True)
        return
    cat_key = parts[2]
    if cat_key not in CONSTRUCTOR_CATEGORIES:
        await call.answer("Ошибка", show_alert=True)
        return
    cat = CONSTRUCTOR_CATEGORIES[cat_key]
    pending_ai_input[call.from_user.id] = {"type": "category", "cat": cat_key}
    await call.message.answer(
        f"{cat['title']}\n\n"
        f"Напишите свой вариант (до {CUSTOM_CATEGORY_MAX} символов).\n"
        "Для отмены — /cancel"
    )
    await call.answer()


@router.callback_query(F.data == "ai:apply")
async def cb_ai_apply(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    ai_s = await get_ai_settings(call.from_user.id)
    try:
        data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
    except Exception:
        data = {}
    if not data:
        await call.answer("Выберите хотя бы одну категорию", show_alert=True)
        return
    await save_ai_settings(
        call.from_user.id, mode="constructor",
        constructor_data=json.dumps(data, ensure_ascii=False)
    )
    await call.answer("✅ Применено!", show_alert=True)
    await cb_ai_menu_settings(call)


@router.callback_query(F.data == "ai:reset")
async def cb_ai_reset(call: CallbackQuery):
    if not await require_sub(call):
        return
    if not await is_premium(call.from_user.id):
        await call.answer("💎 Только для Premium", show_alert=True)
        return
    await reset_ai_settings(call.from_user.id)
    await call.answer("✅ Сброшено на стандартный промпт", show_alert=True)
    await cb_ai_menu_settings(call)


# ============================================================
# РЕФЕРАЛКА
# ============================================================

async def render_ref_panel(msg_or_call, user_id, edit=False):
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
    total, purchases = await referral_stats(user_id)
    earned_hours = total * REF_BONUS_HOURS
    earned_days = purchases * REF_BONUS_PURCHASE_DAYS
    premium = await is_premium(user_id)
    text = (
        "🎁 <b>Реферальная программа</b>\n─────────────\n"
        f"🔗 <b>Ваша ссылка:</b>\n<code>{ref_link}</code>\n\n"
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
    if edit:
        try:
            await msg_or_call.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            await msg_or_call.answer(text, reply_markup=kb)
    else:
        await msg_or_call.answer(text, reply_markup=kb)


@router.message(Command("ref"))
async def cmd_ref(message: Message):
    if not await require_sub(message):
        return
    await ensure_user(message.from_user.id, message.from_user.first_name, message.from_user.username)
    await render_ref_panel(message, message.from_user.id)


@router.callback_query(F.data == "menu:ref")
async def cb_menu_ref(call: CallbackQuery):
    if not await require_sub(call):
        return
    await ensure_user(call.from_user.id, call.from_user.first_name, call.from_user.username)
    await render_ref_panel(call.message, call.from_user.id, edit=True)
    await call.answer()


@router.callback_query(F.data == "ref:battle")
async def cb_ref_battle(call: CallbackQuery):
    if not await require_sub(call):
        return
    top = await referral_leaderboard(5, week_only=True)
    if not top:
        text = ("🏆 <b>Битва рефералов</b>\n─────────────\n"
                "На этой неделе пока нет участников.\nСтаньте первым!")
    else:
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        lines = []
        for i, (uid, name, cnt) in enumerate(top):
            lines.append(f"{medals[i]} <b>{esc(name)}</b> — {cnt}")
        text = ("🏆 <b>Битва рефералов</b>\n─────────────\nТоп за эту неделю:\n\n"
                + "\n".join(lines)
                + "\n\n🎁 Победитель получает <b>месяц Premium</b>.\n"
                  "<i>Итоги в понедельник в @bobmodinfo</i>")
    kb = InlineKeyboardMarkup(inline_keyboard=[back_button("menu:ref")])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


# ============================================================
# BOB AI
# ============================================================

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
        "🤖 <b>Bob AI</b>\n─────────────\n"
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
        "<b>📖 Возможности бота</b>\n─────────────\n"
        "<b>В бизнес-чатах:</b>\n"
        "<code>.ai</code> · <code>.save</code> · <code>.anim</code> · <code>.qr</code>\n"
        "<code>.whois</code> · <code>.typing</code> · <code>.mute</code>\n"
        "<code>.nomute</code> · <code>.note</code> · <code>.id</code> · <code>.ping</code> · <code>.game</code>\n\n"
        "<b>💎 Premium:</b>\n"
        f"• {PREMIUM_DAILY_LIMIT} запросов ИИ в день\n"
        "• Без водяного знака\n"
        "• Иммунитет от <code>.mute</code>\n"
        "• 🤖 Настройка ИИ под себя\n\n"
        "<i>Как подключить: Профиль → Изменить → Автоматизация чатов</i>"
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
    await render_settings(call.message, call.from_user.id, edit=True)
    await call.answer()


# ============================================================
# TOGGLES / PROMPTS
# ============================================================

@router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    if not await require_sub(call):
        return
    key = call.data.split(":", 1)[1]
    s = await get_settings(call.from_user.id)
    if key not in s:
        await call.answer("Неизвестный параметр", show_alert=True)
        return
    await set_setting(call.from_user.id, key, 0 if s[key] else 1)
    await render_settings(call.message, call.from_user.id, edit=True)
    await call.answer("Сохранено")


@router.callback_query(F.data.startswith("prompt:"))
async def cb_prompt(call: CallbackQuery):
    if not await require_sub(call):
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
    pending_inputs[call.from_user.id] = action
    await call.message.answer(prompts[action])
    await call.answer()


@router.message(Command("cancel"))
async def cmd_cancel(message: Message):
    user = message.from_user
    if not user:
        return
    if user.id in pending_ai_input:
        pending_ai_input.pop(user.id, None)
        await message.answer("❌ Отменено")
        return
    if user.id in pending_inputs:
        pending_inputs.pop(user.id, None)
        await message.answer("❌ Отменено")
        return
    if user.id == owner_id and owner_id in pending_admin:
        pending_admin.pop(owner_id, None)
        await message.answer("❌ Отменено")
        return


@router.message(F.chat.type == "private", ~F.text.startswith("/"))
async def receive_setting_value(message: Message):
    user = message.from_user
    if not user:
        return

    if user.id in pending_ai_input:
        task = pending_ai_input.pop(user.id)
        value = (message.text or "").strip()
        if task["type"] == "custom_prompt":
            if len(value) > CUSTOM_PROMPT_MAX:
                await message.answer(f"⚠️ Слишком длинный промпт (макс. {CUSTOM_PROMPT_MAX}).")
                pending_ai_input[user.id] = task
                return
            if not value:
                await message.answer("⚠️ Пустой промпт.")
                pending_ai_input[user.id] = task
                return
            await save_ai_settings(user.id, mode="custom", custom_prompt=value)
            await message.answer("✅ Свой промпт сохранён!")
        elif task["type"] == "category":
            cat_key = task["cat"]
            if len(value) > CUSTOM_CATEGORY_MAX:
                await message.answer(f"⚠️ Слишком длинный текст (макс. {CUSTOM_CATEGORY_MAX}).")
                pending_ai_input[user.id] = task
                return
            if not value:
                await message.answer("⚠️ Пустой текст.")
                pending_ai_input[user.id] = task
                return
            ai_s = await get_ai_settings(user.id)
            try:
                data = json.loads(ai_s["constructor_data"]) if ai_s["constructor_data"] else {}
            except Exception:
                data = {}
            data[cat_key] = value
            await save_ai_settings(user.id, mode="constructor",
                                    constructor_data=json.dumps(data, ensure_ascii=False))
            await message.answer(f"✅ Сохранено: <b>{esc(value)}</b>")
        return

    if user.id == owner_id and owner_id in pending_admin:
        action = pending_admin.pop(owner_id)
        value = (message.text or "").strip()
        await handle_admin_input(message, action, value)
        return

    if user.id not in pending_inputs:
        return
    action = pending_inputs.pop(user.id)
    value = (message.text or "").strip()
    try:
        if action == "answer_first_text":
            await set_setting(user.id, "answer_first_text", value)
        elif action == "off_hours_text":
            await set_setting(user.id, "off_hours_text", value)
        elif action == "off_hours":
            m = re.fullmatch(r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})", value)
            if not m:
                raise ValueError("Неверный формат. Ожидается ЧЧ:ММ-ЧЧ:ММ")
            for t in m.groups():
                datetime.strptime(t, "%H:%M")
            await set_setting(user.id, "off_hours_start", m.group(1))
            await set_setting(user.id, "off_hours_end", m.group(2))
        elif action == "timezone":
            ZoneInfo(value)
            await set_setting(user.id, "timezone", value)
        await message.answer("✅ Настройка сохранена")
    except Exception as e:
        await message.answer(f"⚠️ Не удалось сохранить: {esc(e)}")
    await render_settings(message, user.id)


# ============================================================
# TIMEZONES
# ============================================================

@router.callback_query(F.data == "tz:open")
async def cb_tz_open(call: CallbackQuery):
    if not await require_sub(call):
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
    if not await require_sub(call):
        return
    tz = call.data.split(":", 2)[2]
    try:
        ZoneInfo(tz)
    except Exception:
        await call.answer("Неизвестный часовой пояс", show_alert=True)
        return
    await set_setting(call.from_user.id, "timezone", tz)
    await call.answer(f"Сохранено: {tz}")
    await render_settings(call.message, call.from_user.id, edit=True)


@router.callback_query(F.data == "tz:manual")
async def cb_tz_manual(call: CallbackQuery):
    if not await require_sub(call):
        return
    pending_inputs[call.from_user.id] = "timezone"
    await call.message.answer("Отправьте IANA-часовой пояс, например <code>Europe/Moscow</code>.")
    await call.answer()


# ============================================================
# PAYMENTS
# ============================================================

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
        await db_execute(
            "INSERT INTO payments_log(user_id, stars, days, ts) VALUES(?,?,?,?)",
            (user_id, PREMIUM_PRICE_STARS, PREMIUM_DAYS, utcnow().isoformat())
        )
        await message.answer(
            f"✅ <b>Premium активирован</b>\n\n"
            f"Теперь вам доступно <b>{PREMIUM_DAILY_LIMIT} запросов в день</b>.\n"
            f"Действует до: <b>{until.astimezone().strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            "Спасибо за поддержку!"
        )
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
        row = await db_fetchone(
            "SELECT referrer_id, rewarded_purchase FROM referrals WHERE user_id=?", (user_id,)
        )
        if row and not row[1]:
            referrer_id = row[0]
            new_until = await grant_premium(referrer_id, days=REF_BONUS_PURCHASE_DAYS)
            await db_execute("UPDATE referrals SET rewarded_purchase=1 WHERE user_id=?", (user_id,))
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
    users_total = await db_fetchval("SELECT COUNT(*) FROM users", default=0)
    users_24h = await db_fetchval("SELECT COUNT(*) FROM users WHERE joined_at >= ?", (day_ago,), 0)
    users_7d = await db_fetchval("SELECT COUNT(*) FROM users WHERE joined_at >= ?", (week_ago,), 0)
    users_banned = await db_fetchval("SELECT COUNT(*) FROM users WHERE is_banned=1", default=0)
    premium_active = await db_fetchval(
        "SELECT COUNT(*) FROM user_limits WHERE is_premium=1 AND premium_until > ?",
        (now.isoformat(),), 0
    )
    conns = await db_fetchval("SELECT COUNT(*) FROM business_connections WHERE enabled=1", default=0)
    chats = await db_fetchval("SELECT COUNT(*) FROM contacts", default=0)
    msgs = await db_fetchval("SELECT COUNT(*) FROM messages", default=0)
    mutes = await db_fetchval("SELECT COUNT(*) FROM mutes", default=0)
    refs_total = await db_fetchval("SELECT COUNT(*) FROM referrals", default=0)
    refs_week = await db_fetchval("SELECT COUNT(*) FROM referrals WHERE joined_at >= ?", (week_ago,), 0)
    stars_total = await db_fetchval("SELECT COALESCE(SUM(stars),0) FROM payments_log", default=0)
    purchases = await db_fetchval("SELECT COUNT(*) FROM payments_log", default=0)
    text = (
        "📊 <b>Статистика</b>\n─────────────\n"
        f"👤 Юзеров: <b>{users_total}</b>\n"
        f"  · за 24ч: {users_24h}\n"
        f"  · за 7д: {users_7d}\n"
        f"  · в бане: {users_banned}\n"
        f"🔗 Бизнес-коннектов: <b>{conns}</b>\n"
        f"💎 Premium активных: <b>{premium_active}</b>\n\n"
        f"💬 Чатов: <b>{chats}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>\n"
        f"🔇 Мутов: <b>{mutes}</b>\n\n"
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
    lines_all = [f"{i}. <b>{esc(name)}</b> — {cnt}" for i, (uid, name, cnt) in enumerate(top_all, 1)]
    lines_week = [f"{i}. <b>{esc(name)}</b> — {cnt}" for i, (uid, name, cnt) in enumerate(top_week, 1)]
    text = (
        "🎁 <b>Рефералы</b>\n─────────────\n<b>За всё время:</b>\n"
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
    await call.answer("✅ Отправлено в канал" if ok else "❌ Не удалось отправить", show_alert=True)


@router.callback_query(F.data == "adm:users")
async def cb_adm_users(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    rows = await db_fetchall("SELECT * FROM users ORDER BY joined_at DESC LIMIT 20")
    lines = []
    for row in rows:
        u = row_to_dict(row, USERS_COLS)
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        try:
            d = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m")
        except Exception:
            d = "?"
        flag = "🚫" if u["is_banned"] else ""
        lines.append(f"• <b>{esc(name)}</b> <code>{u['user_id']}</code> {d} {flag}")
    text = "👥 <b>Последние 20 юзеров</b>\n─────────────\n" + ("\n".join(lines) if lines else "<i>пусто</i>")
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


async def show_user_card(chat_target, uid):
    u, refs, msgs = await get_user_card(uid)
    if not u:
        await chat_target.answer(f"Юзер <code>{uid}</code> не найден.")
        return
    name = u["username"] or u["first_name"] or "—"
    premium = await is_premium(uid)
    
    spin_row = await db_fetchone("SELECT last_spin, extra_spins FROM spin_cooldowns WHERE user_id=?", (uid,))
    extra_spins = spin_row[1] if spin_row else 0
    last_spin_str = "—"
    if spin_row and spin_row[0]:
        try:
            last_spin_str = datetime.fromisoformat(spin_row[0]).astimezone().strftime("%d.%m %H:%M")
        except Exception:
            pass

    try:
        joined = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        joined = "—"
    text = (
        f"👤 <b>Карточка</b>\n─────────────\n"
        f"📛 <b>{esc(name)}</b>\n"
        f"🆔 <code>{uid}</code>\n"
        f"📅 Регистрация: {esc(joined)}\n"
        f"💎 Premium: {'✅' if premium else '❌'}\n"
        f"🎁 Рефералов: <b>{refs}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>\n"
        f"🚫 Бан: {'✅' if u['is_banned'] else '❌'}\n"
        f"🔗 Реф-бан: {'✅' if u['banned_ref'] else '❌'}\n\n"
        f"🎰 <b>Рулетка:</b>\n"
        f"  · Последний спин: {esc(last_spin_str)}\n"
        f"  · Доп. попыток: <b>{extra_spins}</b>"
    )
    rows = [
        [InlineKeyboardButton(text="➕ Выдать Premium", callback_data=f"adm:grant:{uid}")],
        [InlineKeyboardButton(text="➖ Снять Premium", callback_data=f"adm:revoke:{uid}")],
        [InlineKeyboardButton(text=f"🎰 +1 Попытка (сейчас {extra_spins})", callback_data=f"adm:add_spin:{uid}")],
    ]
    if u["is_banned"]:
        rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"adm:unban:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🚫 Забанить", callback_data=f"adm:ban:{uid}")])
    if u["banned_ref"]:
        rows.append([InlineKeyboardButton(text="🔓 Разбанить реф-ссылку", callback_data=f"adm:unban_ref:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🔒 Забанить реф-ссылку", callback_data=f"adm:ban_ref:{uid}")])
    rows.append([InlineKeyboardButton(text="🔄 Сбросить реферал", callback_data=f"adm:reset_ref:{uid}")])
    rows.append(back_button("adm:users"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await chat_target.answer(text, reply_markup=kb)
    except Exception:
        pass


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
    
    spin_row = await db_fetchone("SELECT last_spin, extra_spins FROM spin_cooldowns WHERE user_id=?", (uid,))
    extra_spins = spin_row[1] if spin_row else 0
    last_spin_str = "—"
    if spin_row and spin_row[0]:
        try:
            last_spin_str = datetime.fromisoformat(spin_row[0]).astimezone().strftime("%d.%m %H:%M")
        except Exception:
            pass

    try:
        joined = datetime.fromisoformat(u["joined_at"]).astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        joined = "—"
    text = (
        f"👤 <b>Карточка</b>\n─────────────\n"
        f"📛 <b>{esc(name)}</b>\n"
        f"🆔 <code>{uid}</code>\n"
        f"📅 Регистрация: {esc(joined)}\n"
        f"💎 Premium: {'✅' if premium else '❌'}\n"
        f"🎁 Рефералов: <b>{refs}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>\n"
        f"🚫 Бан: {'✅' if u['is_banned'] else '❌'}\n"
        f"🔗 Реф-бан: {'✅' if u['banned_ref'] else '❌'}\n\n"
        f"🎰 <b>Рулетка:</b>\n"
        f"  · Последний спин: {esc(last_spin_str)}\n"
        f"  · Доп. попыток: <b>{extra_spins}</b>"
    )
    rows = [
        [InlineKeyboardButton(text="➕ Выдать Premium", callback_data=f"adm:grant:{uid}")],
        [InlineKeyboardButton(text="➖ Снять Premium", callback_data=f"adm:revoke:{uid}")],
        [InlineKeyboardButton(text=f"🎰 +1 Попытка (сейчас {extra_spins})", callback_data=f"adm:add_spin:{uid}")],
    ]
    if u["is_banned"]:
        rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"adm:unban:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🚫 Забанить", callback_data=f"adm:ban:{uid}")])
    if u["banned_ref"]:
        rows.append([InlineKeyboardButton(text="🔓 Разбанить реф-ссылку", callback_data=f"adm:unban_ref:{uid}")])
    else:
        rows.append([InlineKeyboardButton(text="🔒 Забанить реф-ссылку", callback_data=f"adm:ban_ref:{uid}")])
    rows.append([InlineKeyboardButton(text="🔄 Сбросить реферал", callback_data=f"adm:reset_ref:{uid}")])
    rows.append(back_button("adm:users"))
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("adm:add_spin:"))
async def cb_adm_add_spin(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    
    row = await db_fetchone("SELECT 1 FROM spin_cooldowns WHERE user_id=?", (uid,))
    if row:
        await db_execute("UPDATE spin_cooldowns SET extra_spins = extra_spins + 1 WHERE user_id=?", (uid,))
    else:
        await db_execute("INSERT INTO spin_cooldowns(user_id, last_spin, extra_spins) VALUES(?,?,1)", (uid, "2000-01-01T00:00:00"))
        
    await call.answer("✅ Добавлена 1 попытка", show_alert=True)
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


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
        back_button(f"adm:user_card:{uid}"),
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


@router.callback_query(F.data.startswith("adm:reset_ref:"))
async def cb_adm_reset_ref(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    uid = int(call.data.split(":")[2])
    await db_execute("DELETE FROM referrals WHERE referrer_id=?", (uid,))
    await db_execute("DELETE FROM referrals WHERE user_id=?", (uid,))
    await db_execute("DELETE FROM pending_referrals WHERE referrer_id=?", (uid,))
    await db_execute("DELETE FROM pending_referrals WHERE user_id=?", (uid,))
    await log_admin("reset_ref", uid)
    await call.answer("✅ Реферал сброшен", show_alert=True)
    call.data = f"adm:user_card:{uid}"
    await cb_adm_user_card(call)


@router.callback_query(F.data == "adm:bans")
async def cb_adm_bans(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    rows = await db_fetchall("SELECT * FROM users WHERE is_banned=1 ORDER BY banned_at DESC LIMIT 30")
    ref_rows = await db_fetchall("SELECT * FROM users WHERE banned_ref=1 LIMIT 30")
    lines = []
    for row in rows:
        u = row_to_dict(row, USERS_COLS)
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        reason = u["banned_reason"] or "—"
        lines.append(f"🚫 <b>{esc(name)}</b> <code>{u['user_id']}</code>\n<i>{esc(reason)[:60]}</i>")
    ref_lines = []
    for row in ref_rows:
        u = row_to_dict(row, USERS_COLS)
        name = u["username"] or u["first_name"] or f"id{u['user_id']}"
        ref_lines.append(f"🔒 <b>{esc(name)}</b> <code>{u['user_id']}</code>")
    text = (
        "🚫 <b>Баны</b>\n─────────────\n<b>Юзеры:</b>\n"
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
    users_total = await db_fetchval("SELECT COUNT(*) FROM users", default=0)
    msgs = await db_fetchval("SELECT COUNT(*) FROM messages", default=0)
    text = (
        "⚙️ <b>Система</b>\n─────────────\n"
        f"💾 БД: <b>Turso Cloud</b>\n"
        f"🌐 URL: <code>{esc(TURSO_URL[:40])}...</code>\n"
        f"👥 Юзеров: <b>{users_total}</b>\n"
        f"📝 Сообщений: <b>{msgs}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧹 Очистить старые сообщения", callback_data="adm:cleanup")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm:sys")],
        back_button("adm:menu"),
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "adm:cleanup")
async def cb_adm_cleanup(call: CallbackQuery):
    if call.from_user.id != owner_id:
        return
    cutoff = (utcnow() - timedelta(days=30)).isoformat()
    await db_execute("DELETE FROM messages WHERE date < ?", (cutoff,))
    await call.answer("✅ Старые сообщения удалены", show_alert=True)
    await cb_adm_sys(call)


async def handle_admin_input(message: Message, action: str, value: str):
    try:
        if action == "find_user":
            try:
                uid = int(value)
            except ValueError:
                await message.answer("⚠️ Нужен числовой ID.")
                return
            await show_user_card(message, uid)
            return
        if action.startswith("ban_reason:"):
            uid = int(action.split(":")[1])
            reason = value if value != "-" else ""
            await ban_user(uid, reason)
            await message.answer(f"✅ Пользователь <code>{uid}</code> забанен.")
            return
    except Exception as e:
        log.exception("Error in handle_admin_input")
        await message.answer(f"❌ Ошибка при обработке: <code>{esc(str(e))}</code>")


async def publish_weekly_top(manual=False):
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
    lines = [f"{medals[i]} <b>{esc(name)}</b> — {cnt}" for i, (uid, name, cnt) in enumerate(top)]
    winner_name = top[0][1]
    text = (
        "🏆 <b>Битва рефералов — итоги недели</b>\n─────────────\n\n"
        + "\n".join(lines)
        + f"\n\n🎁 Победитель: <b>{esc(winner_name)}</b> — месяц Premium!"
    )
    try:
        await bot_ref.send_message(f"@{CHANNEL_USERNAME}", text)
        return True
    except Exception as e:
        log.warning("publish top failed: %s", e)
        return False


async def update_all_business_names():
    while True:
        try:
            rows = await db_fetchall(
                """SELECT bc.id as conn_id, bc.user_id, s.timezone
                   FROM business_connections bc
                   JOIN settings s ON s.user_id = bc.user_id
                   WHERE bc.enabled=1 AND s.time_in_nick_enabled=1"""
            )
            for row in rows:
                connection_id = row[0]
                user_id = row[1]
                tz = row[2] or "UTC"
                try:
                    base_name = await get_base_name(connection_id)
                    if not base_name:
                        try:
                            info = await bot_ref.get_business_account_name(
                                business_connection_id=connection_id
                            )
                            base_name = info.first_name or f"User{user_id}"
                        except Exception:
                            base_name = f"User{user_id}"
                        await save_base_name(connection_id, base_name)
                    clock = local_now(tz).strftime("%H:%M")
                    desired = f"{base_name} [{clock}]"[:64]
                    try:
                        async with name_update_lock:
                            await bot_ref.set_business_account_name(
                                business_connection_id=connection_id,
                                first_name=desired
                            )
                    except Exception as e:
                        log.warning("set name for %s failed: %s", user_id, e)
                except Exception as e:
                    log.warning("name update for %s error: %s", user_id, e)
        except Exception as e:
            log.warning("name loop err: %s", e)
        await asyncio.sleep(60)


async def weekly_publish_loop():
    while True:
        try:
            now = utcnow()
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
