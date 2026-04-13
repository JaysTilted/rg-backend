"""Shared Mattermost utilities — posting, channel management.

Drop-in replacement for the original Slack module. Same function signatures,
Mattermost REST API underneath.
"""

from __future__ import annotations

import logging
import traceback

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

ERRORS_CHANNEL = "notifications"


def _base_url() -> str:
    return settings.mattermost_url.rstrip("/")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.mattermost_bot_token}",
        "Content-Type": "application/json",
    }


def _is_configured() -> bool:
    return bool(settings.mattermost_bot_token and settings.mattermost_url)


async def _get_team_id() -> str | None:
    """Resolve the Mattermost team ID from the configured team name."""
    if not _is_configured() or not settings.mattermost_team_name:
        return None
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{_base_url()}/api/v4/teams/name/{settings.mattermost_team_name}",
            headers=_headers(),
        )
        if resp.status_code == 200:
            return resp.json().get("id")
        logger.warning("MATTERMOST | get_team failed | status=%s", resp.status_code)
        return None


async def _resolve_channel_id(channel: str) -> str | None:
    """Resolve a channel name (with or without #) to a channel ID.

    If `channel` already looks like an ID (26-char alphanumeric), returns it as-is.
    """
    channel = channel.lstrip("#")

    # If it looks like an ID already, use it directly
    if len(channel) == 26 and channel.isalnum():
        return channel

    team_id = await _get_team_id()
    if not team_id:
        return None

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{_base_url()}/api/v4/teams/{team_id}/channels/name/{channel}",
            headers=_headers(),
        )
        if resp.status_code == 200:
            return resp.json().get("id")
        logger.warning(
            "MATTERMOST | resolve_channel failed | channel=%s status=%s",
            channel,
            resp.status_code,
        )
        return None


async def post_message(channel: str, text: str) -> None:
    """Post a message to a Mattermost channel.

    No-op if Mattermost is not configured.
    """
    if not _is_configured():
        return
    channel_id = await _resolve_channel_id(channel)
    if not channel_id:
        logger.warning("MATTERMOST | post_message | could not resolve channel=%s", channel)
        return
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{_base_url()}/api/v4/posts",
            headers=_headers(),
            json={"channel_id": channel_id, "message": text},
        )
        if resp.status_code not in (200, 201):
            logger.warning(
                "MATTERMOST | post_message failed | channel=%s status=%s",
                channel,
                resp.status_code,
            )


# Backward-compat alias for existing callsites
post_slack_message = post_message


async def notify_error(
    exc: Exception,
    *,
    source: str,
    stage: str = "",
    context: dict | None = None,
) -> None:
    """Send a structured error notification to the notifications channel.

    Captures the last 3 traceback frames for easy debugging.
    Never raises — all errors are swallowed to avoid cascading failures.
    """
    if not _is_configured():
        return
    try:
        tb_lines = traceback.format_tb(exc.__traceback__)
        last_3 = "".join(tb_lines[-3:]).strip() if tb_lines else "No traceback available"

        ctx = context or {}
        client_info = ctx.get("client_slug") or ctx.get("client_id") or ""
        contact_info = ctx.get("contact_id", "")
        trigger = ctx.get("trigger_type", "")

        ctx_parts = []
        if client_info:
            ctx_parts.append(f"client={client_info}")
        if contact_info:
            ctx_parts.append(f"contact={contact_info}")
        if trigger:
            ctx_parts.append(f"trigger={trigger}")
        ctx_str = " | ".join(ctx_parts) or "no context"

        stage_str = f"\n**Stage:** `{stage}`" if stage else ""

        text = (
            f":red_circle: **Unhandled Exception**\n"
            f"**Source:** `{source}`{stage_str}\n"
            f"**Context:** {ctx_str}\n"
            f"**Error:** `{type(exc).__name__}: {str(exc)[:200]}`\n"
            f"```\n{last_3[:1500]}\n```"
        )
        await post_message(ERRORS_CHANNEL, text)
    except Exception as e:
        logger.warning("notify_error: failed to send Mattermost alert: %s", e)


async def find_channel_by_name(name: str) -> str | None:
    """Find a Mattermost channel by name. Returns channel ID or None."""
    if not _is_configured():
        return None
    return await _resolve_channel_id(name)


async def create_channel(name: str) -> str | None:
    """Create a public Mattermost channel. Returns channel ID.

    If the channel name is already taken, looks up the existing one.
    Returns None if not configured or creation fails.
    """
    if not _is_configured():
        return None
    team_id = await _get_team_id()
    if not team_id:
        return None

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{_base_url()}/api/v4/channels",
            headers=_headers(),
            json={
                "team_id": team_id,
                "name": name,
                "display_name": name,
                "type": "O",  # Open/public channel
            },
        )
        if resp.status_code in (200, 201):
            channel_id = resp.json()["id"]
            logger.info("MATTERMOST | channel_created | name=%s id=%s", name, channel_id)
            return channel_id

        # Channel name taken — look it up
        data = resp.json()
        if data.get("id") == "store.sql_channel.save_channel.exists.app_error":
            logger.info("MATTERMOST | channel exists, looking up | name=%s", name)
            return await find_channel_by_name(name)

        logger.warning(
            "MATTERMOST | create_channel failed | name=%s status=%s error=%s",
            name,
            resp.status_code,
            data.get("message", ""),
        )
        return None


# Backward-compat alias
create_slack_channel = create_channel


async def invite_to_channel(channel_id: str, user_id: str) -> None:
    """Invite a user to a Mattermost channel. Ignores already-in-channel errors."""
    if not _is_configured():
        return
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{_base_url()}/api/v4/channels/{channel_id}/members",
            headers=_headers(),
            json={"user_id": user_id},
        )
        if resp.status_code not in (200, 201):
            data = resp.json()
            # Ignore "already in channel" errors
            if data.get("id") != "api.channel.add_members.already_member.app_error":
                logger.warning(
                    "MATTERMOST | invite failed | channel=%s user=%s error=%s",
                    channel_id,
                    user_id,
                    data.get("message", ""),
                )
