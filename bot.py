"""
╔══════════════════════════════════════════════════════╗
║  SLUR Refer Bot — Production Ready                  ║
║  aiogram 3 + aiosqlite + uvloop                     ║
║  Anti-fake | No crash | 20k+ users safe             ║
╚══════════════════════════════════════════════════════╝
Install:
    pip install aiogram aiosqlite uvloop
"""
# ─────────────────────────────────────────────────────
#  STDLIB
# ─────────────────────────────────────────────────────
import asyncio, logging, logging.handlers, os, io, csv
import random, shutil, signal, sys, time
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional

# ─────────────────────────────────────────────────────
#  UVLOOP — must be set BEFORE any asyncio usage
# ─────────────────────────────────────────────────────
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    _UVLOOP = True
except ImportError:
    _UVLOOP = False

# ─────────────────────────────────────────────────────
#  AIOGRAM
# ─────────────────────────────────────────────────────
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, TelegramObject,
    InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated,
)
from aiogram.filters import CommandStart, Command, ChatMemberUpdatedFilter, LEAVE_TRANSITION
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import (
    TelegramForbiddenError, TelegramBadRequest,
    TelegramRetryAfter, TelegramNotFound,
)

# ═══════════════════════════════════════════════════════
#  CONFIG  ← sirf yahan badlo
# ═══════════════════════════════════════════════════════
BOT_TOKEN          = "8675364583:AAHTEeOezGCTtPbZ5DGoIIXXiqsO_jwOFQ8"
OWNER_IDS          = [7582584348]       # permanent owners
DB_PATH            = "refer_bot.db"
BACKUP_EVERY_HOURS = 2
LOG_FILE           = "refer_bot.log"

# Anti-spam: ek user ko /start ke beech kitne seconds wait karna hoga
# New users ke liye nahi — sirf existing users jo spam kar rahe ho
USER_COOLDOWN_SEC  = 3

# ═══════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════
def setup_logging() -> None:
    fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt); root.addHandler(fh)
    sh = logging.StreamHandler(); sh.setFormatter(fmt); root.addHandler(sh)

log = logging.getLogger("slur")

# ═══════════════════════════════════════════════════════
#  DATABASE  (aiosqlite — fully async, no thread pool)
# ═══════════════════════════════════════════════════════
_db:         Optional[aiosqlite.Connection] = None
_write_lock: Optional[asyncio.Lock]         = None   # serialise writes

async def init_db() -> None:
    global _db, _write_lock
    _write_lock = asyncio.Lock()
    _db = await aiosqlite.connect(DB_PATH)
    _db.row_factory = aiosqlite.Row
    await _db.executescript("""
        PRAGMA journal_mode   = WAL;
        PRAGMA synchronous    = NORMAL;
        PRAGMA cache_size     = -32000;
        PRAGMA temp_store     = MEMORY;
        PRAGMA busy_timeout   = 10000;
        PRAGMA foreign_keys   = ON;
        PRAGMA wal_autocheckpoint = 1000;

        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT    DEFAULT '',
            full_name   TEXT    DEFAULT '',
            referred_by INTEGER,
            points      INTEGER DEFAULT 0,
            join_date   TEXT    DEFAULT (datetime('now')),
            is_banned   INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_pts ON users(points DESC);

        CREATE TABLE IF NOT EXISTS admins (
            user_id  INTEGER PRIMARY KEY,
            username TEXT DEFAULT '',
            added_by INTEGER,
            added_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS referrals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER NOT NULL,
            referred_id INTEGER NOT NULL UNIQUE,
            is_valid    INTEGER DEFAULT 1,
            credited    INTEGER DEFAULT 0,
            created_at  TEXT    DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_ref ON referrals(referrer_id);

        CREATE TABLE IF NOT EXISTS channels (
            channel_id   TEXT PRIMARY KEY,
            channel_name TEXT NOT NULL,
            channel_link TEXT DEFAULT '',
            added_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS coupons (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            code            TEXT UNIQUE NOT NULL,
            description     TEXT NOT NULL,
            points_required INTEGER NOT NULL,
            is_used         INTEGER DEFAULT 0,
            used_by         INTEGER,
            used_at         TEXT,
            added_at        TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS withdrawals (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER,
            coupon_id    INTEGER,
            points_spent INTEGER,
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT
        );
        INSERT OR IGNORE INTO settings VALUES ('min_withdraw_points','10');
        INSERT OR IGNORE INTO settings VALUES ('points_per_refer','1');
        INSERT OR IGNORE INTO settings VALUES ('support_bot','');
        INSERT OR IGNORE INTO settings VALUES ('support_text','🆘 Support');
    """)
    await _db.commit()
    log.info("Database initialised (aiosqlite + WAL)")

# ─── DB helpers ─────────────────────────────────────
async def db_fetch(sql: str, params: tuple = ()) -> list:
    async with _db.execute(sql, params) as cur:
        return await cur.fetchall()

async def db_one(sql: str, params: tuple = ()):
    async with _db.execute(sql, params) as cur:
        return await cur.fetchone()

async def db_write(sql: str, params: tuple = ()) -> None:
    async with _write_lock:
        await _db.execute(sql, params)
        await _db.commit()

async def db_write_many(stmts: list[tuple]) -> None:
    """Multiple writes in one transaction — atomic."""
    async with _write_lock:
        for sql, params in stmts:
            await _db.execute(sql, params)
        await _db.commit()

# ─── Setting helpers ─────────────────────────────────
async def get_setting(k: str) -> str:
    row = await db_one("SELECT value FROM settings WHERE key=?", (k,))
    return row["value"] if row else ""

async def set_setting(k: str, v) -> None:
    await db_write("INSERT OR REPLACE INTO settings VALUES (?,?)", (k, str(v)))

# ─── Channel helpers ─────────────────────────────────
async def get_channels() -> list:
    return await db_fetch("SELECT * FROM channels")

# ─── Admin helpers ───────────────────────────────────
async def all_admin_ids() -> list[int]:
    rows = await db_fetch("SELECT user_id FROM admins")
    return list(set(OWNER_IDS + [r["user_id"] for r in rows]))

async def is_admin(uid: int) -> bool:
    if uid in OWNER_IDS:
        return True
    return bool(await db_one("SELECT 1 FROM admins WHERE user_id=?", (uid,)))

# ─── User helpers ────────────────────────────────────
async def get_user(uid: int):
    return await db_one("SELECT * FROM users WHERE user_id=?", (uid,))

async def register_user(uid: int, uname: str, name: str, ref: Optional[int]) -> bool:
    if await db_one("SELECT 1 FROM users WHERE user_id=?", (uid,)):
        return False
    await db_write(
        "INSERT INTO users(user_id,username,full_name,referred_by) VALUES(?,?,?,?)",
        (uid, uname or "", name or "", ref))
    return True

# ─── Referral helpers ────────────────────────────────
async def credit_referrer(referred_id: int) -> Optional[int]:
    u = await db_one("SELECT referred_by FROM users WHERE user_id=?", (referred_id,))
    if not u or not u["referred_by"]:
        return None
    referrer = u["referred_by"]
    # idempotency check
    done = await db_one("SELECT credited FROM referrals WHERE referred_id=?", (referred_id,))
    if done and done["credited"]:
        return None
    pts = int(await get_setting("points_per_refer") or 1)
    await db_write_many([
        (
            "INSERT INTO referrals(referrer_id,referred_id,credited) VALUES(?,?,1)"
            " ON CONFLICT(referred_id) DO UPDATE SET credited=1",
            (referrer, referred_id)
        ),
        ("UPDATE users SET points=points+? WHERE user_id=?", (pts, referrer)),
    ])
    return referrer

async def deduct_on_leave(referred_id: int) -> tuple[Optional[int], int]:
    row = await db_one(
        "SELECT referrer_id, credited FROM referrals WHERE referred_id=? AND is_valid=1",
        (referred_id,))
    if not row or not row["credited"]:
        return None, 0
    referrer = row["referrer_id"]
    pts = int(await get_setting("points_per_refer") or 1)
    await db_write_many([
        ("UPDATE referrals SET is_valid=0 WHERE referred_id=?", (referred_id,)),
        ("UPDATE users SET points=MAX(0,points-?) WHERE user_id=?", (pts, referrer)),
    ])
    return referrer, pts

async def ref_count(uid: int) -> int:
    r = await db_one(
        "SELECT COUNT(*) AS c FROM referrals WHERE referrer_id=? AND credited=1", (uid,))
    return r["c"] if r else 0

async def user_rank(uid: int) -> int:
    r = await db_one(
        "SELECT COUNT(*)+1 AS r FROM users WHERE points>"
        "(SELECT points FROM users WHERE user_id=?)", (uid,))
    return r["r"] if r else 1

async def leaderboard() -> list:
    return await db_fetch(
        "SELECT u.user_id, u.full_name, COUNT(r.id) AS refs, u.points"
        " FROM users u"
        " LEFT JOIN referrals r ON u.user_id=r.referrer_id AND r.credited=1"
        " GROUP BY u.user_id ORDER BY refs DESC, u.points DESC LIMIT 10")

async def bot_stats() -> dict:
    return {
        "users":  (await db_one("SELECT COUNT(*) AS n FROM users"))["n"],
        "refs":   (await db_one("SELECT COUNT(*) AS n FROM referrals WHERE credited=1"))["n"],
        "pts":    (await db_one("SELECT COALESCE(SUM(points),0) AS n FROM users"))["n"],
        "avail":  (await db_one("SELECT COUNT(*) AS n FROM coupons WHERE is_used=0"))["n"],
        "used":   (await db_one("SELECT COUNT(*) AS n FROM coupons WHERE is_used=1"))["n"],
        "banned": (await db_one("SELECT COUNT(*) AS n FROM users WHERE is_banned=1"))["n"],
        "admins": (await db_one("SELECT COUNT(*) AS n FROM admins"))["n"],
        "top5":   await db_fetch(
            "SELECT u.full_name, COUNT(r.id) AS rc FROM users u"
            " JOIN referrals r ON u.user_id=r.referrer_id AND r.credited=1"
            " GROUP BY u.user_id ORDER BY rc DESC LIMIT 5"),
    }

# ═══════════════════════════════════════════════════════
#  ANTI-SPAM MIDDLEWARE  (per-user cooldown, not global)
# ═══════════════════════════════════════════════════════
_last_action: dict[int, float] = {}

class AntiSpamMiddleware(BaseMiddleware):
    """
    Ek user ke consecutive messages ke beech minimum delay.
    New joins/callbacks are NOT blocked — only rapid repeated messages.
    """
    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict], Awaitable[Any]],
        event: TelegramObject,
        data: dict,
    ) -> Any:
        if isinstance(event, Message):
            uid = event.from_user.id
            now = time.monotonic()
            last = _last_action.get(uid, 0)
            if now - last < USER_COOLDOWN_SEC and uid not in OWNER_IDS:
                try:
                    await event.answer(
                        f"⏳ Thoda ruko ({USER_COOLDOWN_SEC}s)...",
                        cache_time=USER_COOLDOWN_SEC)
                except Exception:
                    pass
                return
            _last_action[uid] = now
        return await handler(event, data)

# ═══════════════════════════════════════════════════════
#  TELEGRAM HELPERS
# ═══════════════════════════════════════════════════════
async def safe_send(bot: Bot, uid: int, **kw) -> bool:
    """Retry on flood, silently skip blocked/deleted users."""
    for attempt in range(3):
        try:
            await bot.send_message(uid, **kw)
            return True
        except TelegramRetryAfter as e:
            wait = e.retry_after + 1
            log.warning(f"RetryAfter {wait}s → uid={uid} (attempt {attempt+1})")
            await asyncio.sleep(wait)
        except (TelegramForbiddenError, TelegramNotFound):
            return False   # user ne bot block kiya / account deleted
        except TelegramBadRequest as e:
            log.warning(f"BadRequest uid={uid}: {e}")
            return False
        except Exception as e:
            log.error(f"safe_send uid={uid} attempt={attempt+1}: {e}")
            if attempt < 2:
                await asyncio.sleep(1)
    return False

async def check_all_joined(bot: Bot, uid: int) -> bool:
    """Parallel channel check — all channels at once, fast."""
    chans = await get_channels()
    if not chans:
        return True

    async def _check(ch) -> bool:
        for attempt in range(2):
            try:
                m = await bot.get_chat_member(ch["channel_id"], uid)
                return m.status not in ("left", "kicked", "banned")
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except Exception:
                return False
        return False

    results = await asyncio.gather(*[_check(ch) for ch in chans],
                                   return_exceptions=True)
    return all(r is True for r in results)

# ═══════════════════════════════════════════════════════
#  CAPTCHA
# ═══════════════════════════════════════════════════════
def gen_captcha() -> tuple[str, int]:
    ops = [("+", lambda a, b: a+b), ("-", lambda a, b: a-b), ("×", lambda a, b: a*b)]
    a, b = random.randint(2, 9), random.randint(2, 9)
    sym, fn = random.choice(ops)
    if sym == "-" and b > a:
        a, b = b, a
    return f"{a} {sym} {b}", fn(a, b)

# ═══════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════
def main_kb(is_admin: bool = False, sup: str = "", suptxt: str = "🆘 Support"):
    rows = [
        [InlineKeyboardButton(text="👤 Profile",       callback_data="profile"),
         InlineKeyboardButton(text="🔗 Referral Link", callback_data="refer_link")],
        [InlineKeyboardButton(text="🏆 Leaderboard",   callback_data="leaderboard"),
         InlineKeyboardButton(text="🎁 Redeem Points", callback_data="redeem")],
        [InlineKeyboardButton(text="📋 How It Works",  callback_data="how_it_works")],
    ]
    if sup:
        rows.append([InlineKeyboardButton(text=suptxt or "🆘 Support", url=sup)])
    if is_admin:
        rows.append([InlineKeyboardButton(text="⚙️ Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def join_kb(chans: list):
    rows = []
    for ch in chans:
        link = ch["channel_link"] or ("https://t.me/" + ch["channel_id"].lstrip("@"))
        rows.append([InlineKeyboardButton(text="📢 " + ch["channel_name"], url=link)])
    rows.append([InlineKeyboardButton(text="✅ Verify Karo!", callback_data="verify_join")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def back_kb(dest: str, label: str = "🔙 Back"):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=label, callback_data=dest)]])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Stats",         callback_data="a_stats"),
         InlineKeyboardButton(text="📢 Channels",      callback_data="a_channels")],
        [InlineKeyboardButton(text="🎟 Coupons",       callback_data="a_coupons"),
         InlineKeyboardButton(text="📋 Withdrawals",   callback_data="a_wdraw")],
        [InlineKeyboardButton(text="⚙️ Settings",      callback_data="a_settings"),
         InlineKeyboardButton(text="📣 Broadcast",     callback_data="a_bcast")],
        [InlineKeyboardButton(text="👮 Manage Admins", callback_data="a_admins")],
        [InlineKeyboardButton(text="🔙 Main Menu",     callback_data="main_menu")],
    ])

# ═══════════════════════════════════════════════════════
#  FSM STATES
# ═══════════════════════════════════════════════════════
class S(StatesGroup):
    captcha  = State()
    ch_id    = State()
    ch_name  = State()
    ch_link  = State()
    cp_code  = State()
    cp_desc  = State()
    cp_pts   = State()
    min_pts  = State()
    ref_pts  = State()
    sup_link = State()
    sup_text = State()
    bcast    = State()
    add_adm  = State()

# ═══════════════════════════════════════════════════════
#  ROUTER + GUARD
# ═══════════════════════════════════════════════════════
router = Router()

async def guard(cq: CallbackQuery) -> bool:
    if not await is_admin(cq.from_user.id):
        await cq.answer("❌ Unauthorized!", show_alert=True)
        return False
    return True

async def guard_m(msg: Message) -> bool:
    return await is_admin(msg.from_user.id)

# ═══════════════════════════════════════════════════════
#  /start  +  CAPTCHA
# ═══════════════════════════════════════════════════════
@router.message(CommandStart())
async def cmd_start(msg: Message, bot: Bot, state: FSMContext):
    try:
        uid  = msg.from_user.id
        name = msg.from_user.full_name or "User"

        # ── parse referral arg ──
        ref_id: Optional[int] = None
        parts = msg.text.split()
        if len(parts) > 1:
            try:
                r = int(parts[1])
                if r != uid:          # self-refer block
                    # check referrer actually exists
                    if await db_one("SELECT 1 FROM users WHERE user_id=?", (r,)):
                        ref_id = r
            except ValueError:
                pass

        u = await get_user(uid)

        if u and u["is_banned"]:
            await msg.answer("🚫 Aapko ban kar diya gaya hai.")
            return

        # ── returning user ──
        if u:
            await state.clear()   # koi purani state clear karo
            chans = await get_channels()
            if chans and not await check_all_joined(bot, uid):
                await msg.answer(
                    f"👋 <b>{name}</b>, pehle sare channels join karo:",
                    parse_mode="HTML", reply_markup=join_kb(chans))
                return
            sup    = await get_setting("support_bot")
            suptxt = await get_setting("support_text")
            adm    = await is_admin(uid)
            await msg.answer(
                f"👋 Welcome back, <b>{name}</b>!",
                parse_mode="HTML", reply_markup=main_kb(adm, sup, suptxt))
            return

        # ── new user → captcha ──
        q, ans = gen_captcha()
        await state.set_state(S.captcha)
        await state.update_data(ans=ans, tries=0, ref_id=ref_id,
                                name=name, uname=msg.from_user.username or "")
        await msg.answer(
            f"👋 Welcome <b>{name}</b>!\n\n"
            "🤖 <b>Verification</b> — prove karo ki tum bot nahi ho:\n\n"
            f"<b>🔢  {q}  = ?</b>\n\nSirf number type karo:",
            parse_mode="HTML")
    except Exception as e:
        log.error(f"cmd_start uid={msg.from_user.id}: {e}", exc_info=True)


@router.message(S.captcha)
async def captcha_ans(msg: Message, bot: Bot, state: FSMContext):
    try:
        data    = await state.get_data()
        correct = data.get("ans")
        tries   =
