"""Shared Slack utilities — posting, channel management."""

from __future__ import annotations

import logging
import traceback

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

_SLACK_API = "https://slack.com/api"
ERRORS_CHANNEL = "#python-errors"


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.slack_bot_token}"}


async def post_slack_message(channel: str, text: str) -> None:
    """Post a message to a Slack channel.

    No-op if slack_bot_token is not configured.
    """
    if not settings.slack_bot_token:
        return
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{_SLACK_API}/chat.postMessage",
            headers=_headers(),
            json={"channel": channel, "text": text},
        )
        data = resp.json()
        if not data.get("ok"):
            logger.warning("SLACK | post_message failed | channel=%s error=%s", channel, data.get("error"))


async def notify_error(
    exc: Exception,
    *,
    source: str,
    stage: str = "",
    context: dict | None = None,
) -> None:
    """Send a structured error notification to #python-errors.

    Captures the last 3 traceback frames for easy debugging.
    Never raises — all errors are swallowed to avoid cascading failures.
    """
    if not settings.slack_bot_token:
        return
    try:
        # Last 3 frames of traceback
        tb_lines = traceback.format_tb(exc.__traceback__)
        last_3 = "".join(tb_lines[-3:]).strip() if tb_lines else "No traceback available"

        ctx = context or {}
        client_info = ctx.get("client_slug") or ctx.get("client_id") or ""
        contact_info = ctx.get("contact_id", "")
        trigger = ctx.get("trigger_type", "")

        # Build context line (only include non-empty fields)
        ctx_parts = []
        if client_info:
            ctx_parts.append(f"client={client_info}")
        if contact_info:
            ctx_parts.append(f"contact={contact_info}")
        if trigger:
            ctx_parts.append(f"trigger={trigger}")
        ctx_str = " | ".join(ctx_parts) or "no context"

        stage_str = f"\n*Stage:* `{stage}`" if stage else ""

        text = (
            f":red_circle: *Unhandled Exception*\n"
            f"*Source:* `{source}`{stage_str}\n"
            f"*Context:* {ctx_str}\n"
            f"*Error:* `{type(exc).__name__}: {str(exc)[:200]}`\n"
            f"```{last_3[:1500]}```"
        )
        await post_slack_message(ERRORS_CHANNEL, text)
    except Exception as e:
        logger.warning("notify_error: failed to send Slack alert: %s", e)


async def find_channel_by_name(name: str) -> str | None:
    """Find a public Slack channel by name (paginated). Returns channel ID or None."""
    if not settings.slack_bot_token:
        return None
    cursor = None
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            params: dict = {"types": "public_channel", "exclude_archived": "true", "limit": "200"}
            if cursor:
                params["cursor"] = cursor
            resp = await client.get(
                f"{_SLACK_API}/conversations.list",
                headers=_headers(),
                params=params,
            )
            data = resp.json()
            if not data.get("ok"):
                logger.warning("SLACK | conversations.list failed | error=%s", data.get("error"))
                return None
            for ch in data.get("channels", []):
                if ch.get("name") == name:
                    return ch["id"]
            cursor = data.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
    return None


async def create_slack_channel(name: str) -> str | None:
    """Create a public Slack channel. Returns channel ID.

    If the channel name is already taken, looks up the existing one.
    Returns None if token is missing or creation fails.
    """
    if not settings.slack_bot_token:
        return None
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{_SLACK_API}/conversations.create",
            headers=_headers(),
            json={"name": name},
        )
        data = resp.json()
        if data.get("ok"):
            channel_id = data["channel"]["id"]
            logger.info("SLACK | channel_created | name=%s id=%s", name, channel_id)
            return channel_id
        if data.get("error") == "name_taken":
            logger.info("SLACK | channel exists, looking up | name=%s", name)
            return await find_channel_by_name(name)
        logger.warning("SLACK | create_channel failed | name=%s error=%s", name, data.get("error"))
        return None


async def invite_to_channel(channel_id: str, user_id: str) -> None:
    """Invite a user to a Slack channel. Ignores already_in_channel errors."""
    if not settings.slack_bot_token:
        return
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{_SLACK_API}/conversations.invite",
            headers=_headers(),
            json={"channel": channel_id, "users": user_id},
        )
        data = resp.json()
        if not data.get("ok") and data.get("error") != "already_in_channel":
            logger.warning("SLACK | invite failed | channel=%s user=%s error=%s", channel_id, user_id, data.get("error"))
