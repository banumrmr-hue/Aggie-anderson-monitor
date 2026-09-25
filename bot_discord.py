"""
Instagram Unban Monitor - Discord Bot

Commands:
  !m <username>  - start monitoring
  !l             - list monitors
  !r <username>  - stop monitoring
  !s             - dashboard
  !help          - help

Converted from the uploaded Telegram/aiogram version to Discord/discord.py.
"""

import asyncio
import itertools
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import aiohttp
import aiosqlite
import discord
from discord.ext import commands

# ======================================================================
# CONFIG
# ======================================================================

BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "PUT_DISCORD_BOT_TOKEN_HERE")
DB_PATH = os.getenv("IG_MONITOR_DB", "monitor_state.db")

CHECK_INTERVAL_MIN_SECONDS = int(os.getenv("IG_CHECK_MIN", "45"))
CHECK_INTERVAL_MAX_SECONDS = int(os.getenv("IG_CHECK_MAX", "90"))
_HARD_FLOOR_SECONDS = 20
if CHECK_INTERVAL_MIN_SECONDS < _HARD_FLOOR_SECONDS:
    CHECK_INTERVAL_MIN_SECONDS = _HARD_FLOOR_SECONDS
if CHECK_INTERVAL_MAX_SECONDS < CHECK_INTERVAL_MIN_SECONDS:
    CHECK_INTERVAL_MAX_SECONDS = CHECK_INTERVAL_MIN_SECONDS + 15

MAX_MONITORS = int(os.getenv("IG_MAX_MONITORS", "25"))
MAX_MONITORS_PER_CHAT = int(os.getenv("IG_MAX_MONITORS_PER_CHAT", "5"))
HTTP_TIMEOUT_SECONDS = 15

PROXIES = [p.strip() for p in os.getenv("IG_PROXIES", "").split(",") if p.strip()]

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

MONITOR_PHOTO_PATH = os.getenv("IG_MONITOR_PHOTO", "assets/monitoring.jpg")
UNBANNED_PHOTO_PATH = os.getenv("IG_UNBANNED_PHOTO", "assets/unbanned.jpg")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("ig-monitor-discord")

# Customer branding - change only this line when needed.
BOT_NAME = "Aggie anderson monitor"

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

def _extract_profile_image_url(body: str) -> Optional[str]:
    """Extract Instagram's public profile image from the page metadata."""
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            return match.group(1).replace("&amp;", "&")
    return None


def _extract_followers_following(body: str) -> tuple[str, str]:
    patterns = [
        re.compile(r'([\d.,KkMm]+)\s+Followers[,\s]+([\d.,KkMm]+)\s+Following', re.I),
        re.compile(
            r'"edge_followed_by"\s*:\s*\{"count"\s*:\s*(\d+)\}.*?'
            r'"edge_follow"\s*:\s*\{"count"\s*:\s*(\d+)\}',
            re.I | re.S,
        ),
    ]
    for pattern in patterns:
        m = pattern.search(body)
        if m:
            return m.group(1), m.group(2)
    return "Unknown", "Unknown"



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

            # Cloudflare challenge is not proof that the Instagram account is banned.
            body_lower = body.lower()
            if (
                "challenge-platform" in body_lower
                or "cf-chl-" in body_lower
                or "just a moment..." in body_lower
                or "cf-error-details" in body_lower
            ):
                return "rate_limited"

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


async def ig_get_profile_image(
    session: aiohttp.ClientSession, username: str, proxy: Optional[str] = None
) -> Optional[str]:
    url = _PROFILE_URL.format(username=username)
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
    try:
        async with session.get(
            url, headers=_ig_build_headers(), proxy=proxy,
            timeout=timeout, allow_redirects=True
        ) as resp:
            body = await resp.text(errors="ignore")
            body_lower = body.lower()
            if any(x in body_lower for x in ("challenge-platform", "cf-chl-", "just a moment...", "cf-error-details")):
                return None
            if resp.status == 200:
                return _extract_profile_image_url(body)
    except Exception:
        pass
    return None


async def ig_get_profile_stats(
    session: aiohttp.ClientSession, username: str, proxy: Optional[str] = None
) -> tuple[str, str]:
    url = _PROFILE_URL.format(username=username)
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
    try:
        async with session.get(
            url, headers=_ig_build_headers(), proxy=proxy,
            timeout=timeout, allow_redirects=True
        ) as resp:
            body = await resp.text(errors="ignore")
            body_lower = body.lower()
            if any(x in body_lower for x in ("challenge-platform", "cf-chl-", "just a moment...", "cf-error-details")):
                return "Unknown", "Unknown"
            if resp.status == 200:
                return _extract_followers_following(body)
    except Exception:
        pass
    return "Unknown", "Unknown"


async def ig_username_reachable(
    session: aiohttp.ClientSession, username: str, proxy: Optional[str] = None
) -> bool:
    """Used by /m to sanity-check Instagram is reachable for this username at all."""
    result = await ig_check_status(session, username, proxy=proxy)
    return result in ("unavailable", "live", "rate_limited")



# ======================================================================
# DISCORD MESSAGE / PHOTO HELPER
# ======================================================================

async def send_card(channel: discord.abc.Messageable, photo_path: str, caption: str) -> None:
    """Send an image with a caption, or fall back to text if the image is missing."""
    try:
        if photo_path and os.path.isfile(photo_path):
            await channel.send(content=caption, file=discord.File(photo_path))
        else:
            await channel.send(caption)
    except Exception:
        try:
            await channel.send(caption)
        except Exception:
            pass


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
                    await self._notify_unbanned(
                        chat_id, username, added_at_iso, requested_by, proxy
                    )
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

    async def _notify_unbanned(
        self,
        chat_id: int,
        username: str,
        added_at_iso: str,
        requested_by: str,
        proxy: Optional[str] = None,
    ) -> None:
        added_at = datetime.fromisoformat(added_at_iso)
        now = datetime.now(timezone.utc)
        elapsed_seconds = max(0, int((now - added_at).total_seconds()))
        days, rem = divmod(elapsed_seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, seconds = divmod(rem, 60)

        followers, following = await ig_get_profile_stats(
            self.session, username, proxy=proxy
        )
        profile_image_url = await ig_get_profile_image(
            self.session, username, proxy=proxy
        )
        profile_url = f"https://www.instagram.com/{username}/"

        embed = discord.Embed(
            title="Instagram Account Unbanned",
            description=f"**[@{username}]({profile_url})** 🟢",
            color=discord.Color.green(),
        )
        if profile_image_url:
            embed.set_thumbnail(url=profile_image_url)
            embed.set_image(url=profile_image_url)

        embed.add_field(name="Followers", value=f"`{followers}`", inline=True)
        embed.add_field(name="Following", value=f"`{following}`", inline=True)

        time_text = (
            f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"
            if days else f"{hours} hours, {minutes} minutes, {seconds} seconds"
        )
        embed.add_field(name="Time taken", value=time_text, inline=False)
        embed.add_field(name="Requested by", value=requested_by, inline=False)
        embed.set_footer(text=f"Live at {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        channel = self.bot.get_channel(chat_id)
        if channel is not None:
            try:
                await channel.send(embed=embed)
            except Exception:
                pass


# ======================================================================
# DISCORD COMMAND HANDLERS
# ======================================================================

USERNAME_RE = re.compile(r"^[A-Za-z0-9._]{1,30}$")

def parse_username(argument: str) -> Optional[str]:
    candidate = (argument or "").strip().lstrip("@")
    if not USERNAME_RE.match(candidate):
        return None
    return candidate.lower()

def requester_label(ctx: commands.Context) -> str:
    return f"@{ctx.author.name}"


bot_intents = discord.Intents.default()
bot_intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=bot_intents, help_command=None)
monitor_manager: Optional[MonitorManager] = None
http_session: Optional[aiohttp.ClientSession] = None


@bot.event
async def on_ready() -> None:
    log.info("Logged in as %s (%s)", bot.user, bot.user.id if bot.user else "?")
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="Instagram monitors",
        )
    )


@bot.command(name="help")
async def cmd_help(ctx: commands.Context) -> None:
    await ctx.send(
        "🤖 **Instagram Unban Monitor**\n\n"
        "`!m <username>` — start monitoring an account\n"
        "`!l` — list active monitors\n"
        "`!r <username>` — stop monitoring an account\n"
        "`!s` — dashboard\n"
        "`!help` — show help\n\n"
        f"📌 This channel can track up to **{MAX_MONITORS_PER_CHAT}** accounts."
    )


@bot.command(name="m")
async def cmd_start_monitor(ctx: commands.Context, username_arg: str = "") -> None:
    global monitor_manager, http_session

    username = parse_username(username_arg)
    if not username:
        await ctx.send("⚠️ Usage: `!m <username>`\nExample: `!m john_doe`")
        return

    if monitor_manager is None or http_session is None:
        await ctx.send("⚠️ Monitor system is still starting. Try again in a moment.")
        return

    chat_id = ctx.channel.id

    if await db_is_tracked(chat_id, username):
        await ctx.send(f"ℹ️ `@{username}` is already being monitored here.")
        return

    current_in_chat = await db_count_active_in_chat(chat_id)
    if current_in_chat >= MAX_MONITORS_PER_CHAT:
        await ctx.send(
            "🚫 **Monitor slots full**\n"
            f"This channel is already tracking **{current_in_chat}/{MAX_MONITORS_PER_CHAT}** accounts.\n"
            "Use `!l` to see them, then `!r <username>` to free a slot."
        )
        return

    if not await monitor_manager.has_global_room():
        await ctx.send("🚫 The bot is at its global monitor capacity right now.")
        return

    status_msg = await ctx.send(f"🔎 Validating `@{username}`…")
    reachable = await ig_username_reachable(http_session, username)

    if not reachable:
        await status_msg.edit(
            content=f"❌ Couldn't reach Instagram to validate `@{username}` right now."
        )
        return

    requested_by = requester_label(ctx)
    monitor_id = await db_add_monitor(chat_id, username, requested_by)
    row = await db_get_monitor(chat_id, username)
    await monitor_manager.start_monitor(
        monitor_id, chat_id, username, row["added_at"], requested_by
    )

    profile_url = f"https://www.instagram.com/{username}/"
    slots_used = current_in_chat + 1

    embed = discord.Embed(
        title="Instagram Profile",
        description=f"**[@{username}]({profile_url})**\n🔴 **BANNED**",
        color=discord.Color.red(),
    )
    embed.add_field(name="Requested by", value=requested_by, inline=True)
    embed.add_field(name="Monitor slot", value=f"{slots_used}/{MAX_MONITORS_PER_CHAT}", inline=True)
    embed.set_footer(text=BOT_NAME)

    await status_msg.delete()
    await ctx.send(embed=embed)


@bot.command(name="l")
async def cmd_list_monitors(ctx: commands.Context) -> None:
    rows = await db_list_monitors(ctx.channel.id)

    if not rows:
        await ctx.send("📭 No active monitors in this channel. Add one with `!m <username>`.")
        return

    lines = [
        "📋 **Active Instagram Monitors**",
        f"📊 Slots used: **{len(rows)}/{MAX_MONITORS_PER_CHAT}**",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    for row in rows:
        status_icon = "🟢" if row["status"] == "live" else "🟡"
        last = row["last_result"] or "pending first check"
        requester = row["requested_by"] or "unknown"
        profile_url = f"https://www.instagram.com/{row['username']}/"
        lines.append(
            f"{status_icon} [@{row['username']}]({profile_url}) — "
            f"last check: {last} — requested by {requester}"
        )

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    await ctx.send("\n".join(lines))


@bot.command(name="r")
async def cmd_remove_monitor(ctx: commands.Context, username_arg: str = "") -> None:
    global monitor_manager

    username = parse_username(username_arg)
    if not username:
        await ctx.send("⚠️ Usage: `!r <username>`")
        return

    chat_id = ctx.channel.id
    removed_db = await db_remove_monitor(chat_id, username)
    removed_task = await monitor_manager.stop_monitor(chat_id, username) if monitor_manager else False

    if removed_db or removed_task:
        remaining = await db_count_active_in_chat(chat_id)
        await ctx.send(
            f"🗑 Stopped monitoring `@{username}`.\n"
            f"📊 Slots used: **{remaining}/{MAX_MONITORS_PER_CHAT}**"
        )
    else:
        await ctx.send(f"ℹ️ `@{username}` wasn't being monitored.")


@bot.command(name="s")
async def cmd_dashboard(ctx: commands.Context) -> None:
    if monitor_manager is None:
        await ctx.send("⚠️ Dashboard is still starting.")
        return

    total_active = await monitor_manager.total_active()
    healthy = await db_health_ok()
    uptime = int(monitor_manager.stats.uptime_seconds)
    hours, remainder = divmod(uptime, 3600)
    minutes, seconds = divmod(remainder, 60)

    await ctx.send(
        "📊 **System Dashboard**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Active monitors (global): **{total_active}/{MAX_MONITORS}**\n"
        f"📌 Per-channel limit: **{MAX_MONITORS_PER_CHAT}**\n"
        f"💾 Database: {'✅ healthy' if healthy else '❌ unreachable'}\n"
        f"📶 Request success rate: **{monitor_manager.stats.success_rate}%** "
        f"({monitor_manager.stats.checks_ok} ok / {monitor_manager.stats.checks_failed} failed)\n"
        f"⏱ Bot uptime: {hours}h {minutes}m {seconds}s\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError) -> None:
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("⚠️ Missing argument. Use `!help` for command usage.")
        return
    log.error("Command error: %s", error)


# ======================================================================
# ENTRY POINT
# ======================================================================

async def main() -> None:
    global monitor_manager, http_session

    if not BOT_TOKEN or BOT_TOKEN == "PUT_DISCORD_BOT_TOKEN_HERE":
        raise RuntimeError(
            "Set DISCORD_BOT_TOKEN near the top of this file or as an environment variable."
        )

    await db_init()
    http_session = aiohttp.ClientSession()
    monitor_manager = MonitorManager(bot, http_session)

    restored = await monitor_manager.restore_from_db()
    if restored:
        log.info("Restored %d monitor(s) from a previous session.", restored)

    try:
        log.info("Discord bot starting…")
        await bot.start(BOT_TOKEN)
    finally:
        tasks = await monitor_manager.snapshot_tasks() if monitor_manager else []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        if http_session:
            await http_session.close()


if __name__ == "__main__":
    asyncio.run(main())
