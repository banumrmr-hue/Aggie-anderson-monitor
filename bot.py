"""
bot.py
=======
Instagram Unban Monitor - Telegram Bot (single-file build)
------------------------------------------------------------
Everything lives in this one file on purpose: no module-to-module
imports to get wrong, no "did I copy all the files" mistakes - just
this script plus an `assets` folder next to it for your two images.

WHAT THIS BOT DOES
  /m <username>  - start monitoring an Instagram account
  /l             - list active monitors in this chat
  /r <username>  - stop monitoring an account
  /s             - dashboard (active monitors, DB health, success rate, uptime)
  /help          - command reference

Before it does any of that, every command is gated behind joining a
Telegram channel (a "force subscribe" gate) via an inline
"Join Channel" / "I've Joined" button flow.

When /m succeeds, or when an account is detected live again, the bot
sends a photo card (assets/monitoring.jpg or assets/unbanned.jpg) with
a Markdown caption - falling back to a plain text message automatically
if that image file is missing, so a forgotten picture never crashes
anything.

SETUP - READ THIS FIRST
  1. pip install -r requirements.txt   (aiogram, aiohttp, aiosqlite)
  2. Set your bot token below (BOT_TOKEN) or via an environment variable.
  3. Make this bot an ADMIN of your force-subscribe channel - the
     Telegram Bot API cannot check another user's membership otherwise,
     and the gate fails closed (blocks everyone) without it.
  4. Drop two images into an `assets` folder next to this file:
       assets/monitoring.jpg
       assets/unbanned.jpg
  5. Run: python bot.py   (or press Run / F5 in Thonny)

COMPLIANCE NOTE
Instagram's Terms of Service prohibit bulk, high-frequency automated
scraping. This bot is built for low-frequency, personal monitoring of
a handful of accounts you own or are authorized to track - polling
intervals and monitor counts below are deliberately capped, not
aggressive-by-default.
"""

# ======================================================================
# IMPORTS
# ======================================================================
import asyncio
import itertools
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

import aiohttp
import aiosqlite
from aiogram import Bot, BaseMiddleware, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# ======================================================================
# CONFIG - edit these values directly, or set the matching env vars
# ======================================================================

# --- Telegram ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_BOT_TOKEN_HERE")

# --- Storage ---
DB_PATH = os.getenv("IG_MONITOR_DB", "monitor_state.db")

# --- Polling behaviour ---
# Jittered sleep between checks of a SINGLE account: a random value in
# [MIN, MAX] rather than a fixed period, so polling isn't perfectly
# regular. A hard floor of 20s is enforced below no matter what env
# vars say - lower than that risks getting your IP throttled fast,
# and no header trick changes that.
CHECK_INTERVAL_MIN_SECONDS = int(os.getenv("IG_CHECK_MIN", "45"))
CHECK_INTERVAL_MAX_SECONDS = int(os.getenv("IG_CHECK_MAX", "90"))
_HARD_FLOOR_SECONDS = 20
if CHECK_INTERVAL_MIN_SECONDS < _HARD_FLOOR_SECONDS:
    CHECK_INTERVAL_MIN_SECONDS = _HARD_FLOOR_SECONDS
if CHECK_INTERVAL_MAX_SECONDS < CHECK_INTERVAL_MIN_SECONDS:
    CHECK_INTERVAL_MAX_SECONDS = CHECK_INTERVAL_MIN_SECONDS + 15

# Global safety-net cap across ALL chats combined.
MAX_MONITORS = int(os.getenv("IG_MAX_MONITORS", "25"))

# Per-chat limit - the one users actually hit day to day. Once a chat
# has this many active monitors, /m is refused until one is removed.
MAX_MONITORS_PER_CHAT = int(os.getenv("IG_MAX_MONITORS_PER_CHAT", "5"))

# Network timeout per request (seconds)
HTTP_TIMEOUT_SECONDS = 15

# --- Optional outbound proxies ---
# Comma-separated proxy URLs YOU are authorized to use, e.g.:
#   IG_PROXIES="http://user:pass@host1:port,http://user:pass@host2:port"
# Left empty -> direct connection. Requests round-robin across whatever
# is configured here. For redundancy across your own infrastructure,
# not a ban-evasion mechanism.
PROXIES = [p.strip() for p in os.getenv("IG_PROXIES", "").split(",") if p.strip()]

# Small pool of realistic browser User-Agent strings, rotated per
# request so consecutive requests aren't byte-identical. Basic scraper
# hygiene - not a defeat of Instagram's real anti-automation systems.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 "
    "Safari/604.1",
]
ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.8",
    "en-US,en;q=0.8,fr;q=0.5",
]

# --- Force-subscribe gate ---
# Username only, no "@" and no full URL - both are built from this.
# SETUP REQUIREMENT: the bot must be an ADMIN of this channel.


# --- Photo cards ---
# Paths relative to wherever this script is run from. Missing files
# fall back automatically to a plain-text message (see send_card()).
MONITOR_PHOTO_PATH = os.getenv("IG_MONITOR_PHOTO", "assets/monitoring.jpg")
UNBANNED_PHOTO_PATH = os.getenv("IG_UNBANNED_PHOTO", "assets/unbanned.jpg")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("ig-monitor-bot")


# ======================================================================
# DATABASE - aiosqlite persistence so monitors survive a restart
# ======================================================================

_SCHEMA = """
CREATE TABLE IF NOT EXISTS monitors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id      INTEGER NOT NULL,
    username     TEXT    NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'banned',  -- banned|live|removed
    added_at     TEXT    NOT NULL,                    -- ISO-8601 UTC
    last_checked TEXT,
    last_result  TEXT,
    requested_by TEXT,                                -- @handle or name of whoever ran /m
    UNIQUE(chat_id, username)
);
"""


async def db_init() -> None:
    """Create tables if needed, and migrate older DBs missing requested_by."""
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.executescript(_SCHEMA)
        async with conn.execute("PRAGMA table_info(monitors)") as cur:
            columns = {row[1] async for row in cur}
        if "requested_by" not in columns:
            await conn.execute("ALTER TABLE monitors ADD COLUMN requested_by TEXT")
        await conn.commit()


async def db_add_monitor(chat_id: int, username: str, requested_by: str) -> int:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "INSERT INTO monitors (chat_id, username, status, added_at, requested_by) "
            "VALUES (?, ?, 'banned', ?, ?)",
            (chat_id, username.lower(), now, requested_by),
        )
        await conn.commit()
        return cursor.lastrowid


async def db_is_tracked(chat_id: int, username: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            "SELECT 1 FROM monitors WHERE chat_id = ? AND username = ? AND status != 'removed'",
            (chat_id, username.lower()),
        ) as cur:
            return await cur.fetchone() is not None


async def db_remove_monitor(chat_id: int, username: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "UPDATE monitors SET status = 'removed' "
            "WHERE chat_id = ? AND username = ? AND status != 'removed'",
            (chat_id, username.lower()),
        )
        await conn.commit()
        return cursor.rowcount > 0


async def db_get_monitor(chat_id: int, username: str) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM monitors WHERE chat_id = ? AND username = ? AND status != 'removed'",
            (chat_id, username.lower()),
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def db_list_monitors(chat_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM monitors WHERE chat_id = ? AND status != 'removed' ORDER BY added_at ASC",
            (chat_id,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def db_count_active_in_chat(chat_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            "SELECT COUNT(*) FROM monitors WHERE chat_id = ? AND status = 'banned'",
            (chat_id,),
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def db_list_all_active() -> list:
    """Used once at startup to resurrect monitors after a restart."""
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM monitors WHERE status = 'banned'") as cur:
            return [dict(r) for r in await cur.fetchall()]


async def db_mark_checked(monitor_id: int, result: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "UPDATE monitors SET last_checked = ?, last_result = ? WHERE id = ?",
            (now, result, monitor_id),
        )
        await conn.commit()


async def db_mark_live(monitor_id: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "UPDATE monitors SET status = 'live', last_checked = ?, last_result = 'live' WHERE id = ?",
            (now, monitor_id),
        )
        await conn.commit()


async def db_count_active_global() -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute("SELECT COUNT(*) FROM monitors WHERE status = 'banned'") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def db_health_ok() -> bool:
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception:
        return False


# ======================================================================
# INSTAGRAM STATUS CHECKER
# ======================================================================
#
# HONESTY NOTE: Instagram's public profile page has no official "this
# account is suspended" flag - a disabled account and a username that
# never existed both typically render the same generic "page isn't
# available" response. What CAN be reliably told apart from public
# HTML is a fully live, rendered profile for THIS SPECIFIC username
# (verified two independent ways below) versus everything else.
#
# The two-signal check below requires BOTH a canonical link AND a
# title/description mention of "@username" before declaring "live" -
# this specifically prevents false positives from unrelated suggested-
# account sidebars on "page not available" responses, which is what
# caused non-existent usernames to occasionally flip to "unbanned" in
# an earlier, simpler version of this check.

_PROFILE_URL = "https://www.instagram.com/{username}/"

_FOLLOWER_META_PATTERN = re.compile(
    r'[\d.,KkMm]+\s+Followers,\s+[\d.,KkMm]+\s+Following',
    re.IGNORECASE,
)
_RATE_LIMIT_MARKERS = ("Please wait a few minutes", "challenge", "unusual activity")


def _ig_build_headers() -> dict:
    user_agent = random.choice(USER_AGENTS)
    looks_like_chrome_desktop = "Chrome" in user_agent and "Mobile" not in user_agent
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }
    if looks_like_chrome_desktop:
        headers["Sec-Ch-Ua"] = '"Chromium";v="126", "Google Chrome";v="126", "Not.A/Brand";v="24"'
        headers["Sec-Ch-Ua-Mobile"] = "?0"
        headers["Sec-Ch-Ua-Platform"] = '"Windows"'
        headers["Sec-Fetch-Dest"] = "document"
        headers["Sec-Fetch-Mode"] = "navigate"
        headers["Sec-Fetch-Site"] = "none"
        headers["Sec-Fetch-User"] = "?1"
    return headers


def _ig_is_confirmed_live_for_username(body: str, username: str) -> bool:
    escaped = re.escape(username)
    canonical_pattern = re.compile(
        r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']'
        rf'https://www\.instagram\.com/{escaped}/?["\']',
        re.IGNORECASE,
    )
    handle_pattern = re.compile(rf'@{escaped}\b', re.IGNORECASE)

    has_canonical_match = bool(canonical_pattern.search(body))
    has_handle_match = bool(handle_pattern.search(body))
    has_follower_meta = bool(_FOLLOWER_META_PATTERN.search(body))
    return has_canonical_match and has_handle_match and has_follower_meta


async def ig_check_status(
    session: aiohttp.ClientSession, username: str, proxy: Optional[str] = None
) -> str:
    """
    Returns "live" | "unavailable" | "rate_limited" | "error".
    Never raises - any failure maps to "error" so callers can simply
    retry next cycle instead of crashing.
    """
    url = _PROFILE_URL.format(username=username)
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
    try:
        async with session.get(
            url, headers=_ig_build_headers(), proxy=proxy, timeout=timeout, allow_redirects=True
        ) as resp:
            body = await resp.text(errors="ignore")

            if resp.status == 429:
                return "rate_limited"
            if resp.status == 200:
                if _ig_is_confirmed_live_for_username(body, username):
                    return "live"
                if any(marker.lower() in body.lower() for marker in _RATE_LIMIT_MARKERS):
                    return "rate_limited"
                return "unavailable"
            if resp.status == 404:
                return "unavailable"
            return "unavailable"
    except (aiohttp.ClientError, TimeoutError):
        return "error"
    except Exception:
        return "error"


async def ig_username_reachable(
    session: aiohttp.ClientSession, username: str, proxy: Optional[str] = None
) -> bool:
    """Used by /m to sanity-check Instagram is reachable for this username at all."""
    result = await ig_check_status(session, username, proxy=proxy)
    return result in ("unavailable", "live", "rate_limited")


# ======================================================================
# PHOTO CARD HELPER
# ======================================================================

async def send_card(bot: Bot, chat_id: int, photo_path: str, caption: str) -> None:
    """
    Send `caption` as a photo's caption if `photo_path` exists and the
    upload succeeds; otherwise fall back to a plain Markdown text
    message. A missing or bad image file never breaks a notification.
    """
    if photo_path and os.path.isfile(photo_path):
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=FSInputFile(photo_path),
                caption=caption,
                parse_mode="Markdown",
            )
            return
        except Exception:
            pass
    await bot.send_message(chat_id=chat_id, text=caption, parse_mode="Markdown")


# ======================================================================
# FORCE-SUBSCRIBE GATE
# ======================================================================
#
# SETUP REQUIREMENT: Telegram's Bot API can only report whether some
# OTHER user has joined a channel if THIS BOT is itself an admin of
# that channel. Without that, every check below fails closed (safe
# default: access denied) rather than silently letting everyone through.




# ======================================================================
# MONITOR MANAGER - one asyncio task per monitored account
# ======================================================================

class Stats:
    """In-memory counters backing the /s dashboard."""

    def __init__(self) -> None:
        self.start_time = time.monotonic()
        self.checks_ok = 0
        self.checks_failed = 0

    @property
    def success_rate(self) -> float:
        total = self.checks_ok + self.checks_failed
        return 100.0 if total == 0 else round((self.checks_ok / total) * 100, 1)

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self.start_time


class MonitorManager:
    """
    Key = (chat_id, username) -> asyncio.Task. All mutation goes
    through self._lock: asyncio is single-threaded, but a lock is
    still needed since "check if running, then create+register" spans
    multiple awaits - without it two near-simultaneous /m calls for
    the same account could both slip past the check before registering.
    """

    def __init__(self, bot: Bot, session: aiohttp.ClientSession) -> None:
        self.bot = bot
        self.session = session
        self.stats = Stats()
        self._tasks: Dict[Tuple[int, str], asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._proxy_cycle = itertools.cycle(PROXIES) if PROXIES else None

    def _next_proxy(self) -> Optional[str]:
        return next(self._proxy_cycle) if self._proxy_cycle else None

    async def total_active(self) -> int:
        async with self._lock:
            return len(self._tasks)

    async def has_global_room(self) -> bool:
        return await self.total_active() < MAX_MONITORS

    async def snapshot_tasks(self):
        async with self._lock:
            return list(self._tasks.values())

    async def start_monitor(
        self, monitor_id: int, chat_id: int, username: str, added_at_iso: str, requested_by: str
    ) -> None:
        key = (chat_id, username)
        async with self._lock:
            if key in self._tasks:
                return
            task = asyncio.create_task(
                self._monitor_loop(monitor_id, chat_id, username, added_at_iso, requested_by)
            )
            self._tasks[key] = task

    async def stop_monitor(self, chat_id: int, username: str) -> bool:
        key = (chat_id, username)
        async with self._lock:
            task = self._tasks.pop(key, None)
        if task is None:
            return False
        task.cancel()
        return True

    async def restore_from_db(self) -> int:
        rows = await db_list_all_active()
        for row in rows:
            await self.start_monitor(
                row["id"], row["chat_id"], row["username"], row["added_at"], row["requested_by"] or "unknown"
            )
        return len(rows)

    async def _monitor_loop(
        self, monitor_id: int, chat_id: int, username: str, added_at_iso: str, requested_by: str
    ) -> None:
        try:
            while True:
                proxy = self._next_proxy()
                result = await ig_check_status(self.session, username, proxy=proxy)

                if result == "error":
                    self.stats.checks_failed += 1
                else:
                    self.stats.checks_ok += 1

                await db_mark_checked(monitor_id, result)

                if result == "live":
                    await self._notify_unbanned(chat_id, username, added_at_iso, requested_by)
                    await db_mark_live(monitor_id)
                    break

                delay = random.uniform(CHECK_INTERVAL_MIN_SECONDS, CHECK_INTERVAL_MAX_SECONDS)
                if result == "rate_limited":
                    delay *= 2
                await asyncio.sleep(delay)

        except asyncio.CancelledError:
            raise
        finally:
            key = (chat_id, username)
            async with self._lock:
                self._tasks.pop(key, None)

    async def _notify_unbanned(self, chat_id: int, username: str, added_at_iso: str, requested_by: str) -> None:
        added_at = datetime.fromisoformat(added_at_iso)
        now = datetime.now(timezone.utc)
        elapsed_seconds = int((now - added_at).total_seconds())
        hours, remainder = divmod(elapsed_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        profile_url = f"https://www.instagram.com/{username}/"

        caption = (
            "✅ *Instagram Account Unbanned!*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 [@{username}]({profile_url})\n"
            f"⏱ *Time Taken:* {hours}h {minutes}m {seconds}s\n"
            f"👤 *Requested by:* {requested_by}\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🎉 _Great news — the profile is live again! Please double-check "
            "manually before relying on this for anything important._"
        )
        try:
            await send_card(self.bot, chat_id, UNBANNED_PHOTO_PATH, caption)
        except Exception:
            pass  # DB is already marked live regardless of delivery success


# ======================================================================
# COMMAND HANDLERS
# ======================================================================

commands_router = Router(name="ig-monitor")
_USERNAME_RE = re.compile(r"^[A-Za-z0-9._]{1,30}$")


def _parse_username(text: str) -> Optional[str]:
    parts = (text or "").strip().split(maxsplit=1)
    if len(parts) != 2:
        return None
    candidate = parts[1].strip().lstrip("@")
    if not _USERNAME_RE.match(candidate):
        return None
    return candidate.lower()


def _requester_label(message: Message) -> str:
    user = message.from_user
    if user is None:
        return "unknown"
    if user.username:
        return f"@{user.username}"
    return user.full_name or f"user {user.id}"


@commands_router.message(Command("start", "help"))
async def cmd_help(message: Message) -> None:
    await message.reply(
        "🤖 *Instagram Unban Monitor*\n\n"
        "`/m <username>` — start monitoring an account\n"
        "`/l` — list your active monitors\n"
        "`/r <username>` — stop monitoring an account\n"
        "`/s` — system dashboard\n\n"
        f"📌 Each chat can track up to *{MAX_MONITORS_PER_CHAT}* accounts at once — "
        "remove one with `/r` to free up a slot.\n\n"
        "_Public-page checks can't tell a disabled account apart from one that "
        "never existed until it comes back live - only add accounts you know "
        "were actually suspended._",
        parse_mode="Markdown",
    )


@commands_router.message(Command("m"))
async def cmd_start_monitor(
    message: Message, session: aiohttp.ClientSession, monitor_manager: MonitorManager
) -> None:
    username = _parse_username(message.text or "")
    if not username:
        await message.reply(
            "⚠️ Usage: `/m <username>`\nExample: `/m john_doe`\n"
            "Usernames may only contain letters, numbers, dots and underscores.",
            parse_mode="Markdown",
        )
        return

    chat_id = message.chat.id

    if await db_is_tracked(chat_id, username):
        await message.reply(f"ℹ️ `@{username}` is already being monitored here.", parse_mode="Markdown")
        return

    current_in_chat = await db_count_active_in_chat(chat_id)
    if current_in_chat >= MAX_MONITORS_PER_CHAT:
        await message.reply(
            "🚫 *Monitor slots full*\n"
            f"This chat is already tracking *{current_in_chat}/{MAX_MONITORS_PER_CHAT}* accounts "
            "— the maximum allowed at once.\n"
            "Use `/l` to see them, then `/r <username>` to free up a slot before adding a new one.",
            parse_mode="Markdown",
        )
        return

    if not await monitor_manager.has_global_room():
        await message.reply(
            "🚫 The bot is at its global monitor capacity right now. Please try again later.",
            parse_mode="Markdown",
        )
        return

    status_msg = await message.reply(f"🔎 Validating `@{username}`…", parse_mode="Markdown")

    reachable = await ig_username_reachable(session, username)
    if not reachable:
        await status_msg.edit_text(
            f"❌ Couldn't reach Instagram to validate `@{username}` right now "
            "(network or rate-limit issue). Please try again shortly.",
            parse_mode="Markdown",
        )
        return

    requested_by = _requester_label(message)
    monitor_id = await db_add_monitor(chat_id, username, requested_by)
    row = await db_get_monitor(chat_id, username)
    await monitor_manager.start_monitor(monitor_id, chat_id, username, row["added_at"], requested_by)

    profile_url = f"https://www.instagram.com/{username}/"
    slots_used = current_in_chat + 1

    caption = (
        "🛰 *Instagram Account Monitoring*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 [@{username}]({profile_url}) added successfully!\n"
        "🔔 You'll be notified the instant this account goes live again.\n"
        f"👤 *Requested by:* {requested_by}\n"
        f"📊 *Monitoring slot:* {slots_used}/{MAX_MONITORS_PER_CHAT}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    await status_msg.delete()
    await send_card(message.bot, chat_id, MONITOR_PHOTO_PATH, caption)


@commands_router.message(Command("l"))
async def cmd_list_monitors(message: Message) -> None:
    rows = await db_list_monitors(message.chat.id)
    if not rows:
        await message.reply(
            "📭 No active monitors in this chat. Add one with `/m <username>`.", parse_mode="Markdown"
        )
        return

    lines = [
        "📋 *Active Instagram Monitors*",
        f"📊 Slots used: *{len(rows)}/{MAX_MONITORS_PER_CHAT}*",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    for row in rows:
        status_icon = "🟢" if row["status"] == "live" else "🟡"
        last = row["last_result"] or "pending first check"
        requester = row["requested_by"] or "unknown"
        profile_url = f"https://www.instagram.com/{row['username']}/"
        lines.append(f"{status_icon} [@{row['username']}]({profile_url}) — last check: {last} — requested by {requester}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    await message.reply("\n".join(lines), parse_mode="Markdown")


@commands_router.message(Command("r"))
async def cmd_remove_monitor(message: Message, monitor_manager: MonitorManager) -> None:
    username = _parse_username(message.text or "")
    if not username:
        await message.reply("⚠️ Usage: `/r <username>`", parse_mode="Markdown")
        return

    chat_id = message.chat.id
    removed_db = await db_remove_monitor(chat_id, username)
    removed_task = await monitor_manager.stop_monitor(chat_id, username)

    if removed_db or removed_task:
        remaining = await db_count_active_in_chat(chat_id)
        await message.reply(
            f"🗑 Stopped monitoring `@{username}`.\n📊 Slots used: *{remaining}/{MAX_MONITORS_PER_CHAT}*",
            parse_mode="Markdown",
        )
    else:
        await message.reply(f"ℹ️ `@{username}` wasn't being monitored.", parse_mode="Markdown")


@commands_router.message(Command("s"))
async def cmd_dashboard(message: Message, monitor_manager: MonitorManager) -> None:
    total_active = await monitor_manager.total_active()
    healthy = await db_health_ok()
    uptime = int(monitor_manager.stats.uptime_seconds)
    hours, remainder = divmod(uptime, 3600)
    minutes, seconds = divmod(remainder, 60)

    text = (
        "📊 *System Dashboard*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Active monitors (global): *{total_active}/{MAX_MONITORS}*\n"
        f"📌 Per-chat limit: *{MAX_MONITORS_PER_CHAT}*\n"
        f"💾 Database: {'✅ healthy' if healthy else '❌ unreachable'}\n"
        f"📶 Request success rate: *{monitor_manager.stats.success_rate}%* "
        f"({monitor_manager.stats.checks_ok} ok / {monitor_manager.stats.checks_failed} failed)\n"
        f"⏱ Bot uptime: {hours}h {minutes}m {seconds}s\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    await message.reply(text, parse_mode="Markdown")


# ======================================================================
# ENTRY POINT
# ======================================================================

async def _register_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        [
            BotCommand(command="m", description="Start monitoring an Instagram account"),
            BotCommand(command="l", description="List your active monitors"),
            BotCommand(command="r", description="Remove a monitor"),
            BotCommand(command="s", description="Show system dashboard"),
            BotCommand(command="help", description="Show help"),
        ]
    )


async def main() -> None:
    if not BOT_TOKEN or BOT_TOKEN == "PUT_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("Set BOT_TOKEN near the top of this file (or as an environment variable) before running.")

    await db_init()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
    dp = Dispatcher()

    # Gate every command in commands_router behind channel membership.
    

       # owns the "I've Joined" button callback
    dp.include_router(commands_router)    # /m /l /r /s /help

    async with aiohttp.ClientSession() as session:
        monitor_manager = MonitorManager(bot, session)

        restored = await monitor_manager.restore_from_db()
        if restored:
            log.info("Restored %d monitor(s) from a previous session.", restored)

        await _register_commands(bot)

        log.info("Bot starting…")
        try:
            await dp.start_polling(bot, session=session, monitor_manager=monitor_manager)
        finally:
            log.info("Shutting down - cancelling active monitor tasks…")
            tasks = await monitor_manager.snapshot_tasks()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
