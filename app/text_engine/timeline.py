"""Step 3.5: Chat history & timeline — fetch, merge, format.

Merges chat history messages, attachments, and call logs into a single
chronological timeline string that agents use as conversation context.

Port of n8n's "Format Unified Timeline" code node.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from prefect import task
from zoneinfo import ZoneInfo

from app.models import PipelineContext
from app.services.postgres_client import postgres
from app.services.supabase_client import supabase
from app.text_engine.utils import get_timezone, parse_datetime

logger = logging.getLogger(__name__)

# Message type legend entries (only included when type is present)
_MSG_TYPE_LEGEND: dict[str, str] = {
    "LEAD": "LEAD = Message from the lead/prospect",
    "AI": "AI = AI-generated message",
    "WORKFLOW": "WORKFLOW = Automated workflow message (drips, reminders, campaigns, etc.)",
    "MANUAL": "MANUAL = Staff member sent this message",
    "FOLLOW_UP": "FOLLOW_UP = AI follow-up message",
    "CALL": "CALL = Phone call log entry",
}

# Channel legend entries (only included when channel is present)
_CHANNEL_LEGEND: dict[str, str] = {
    "SMS": "[SMS] = Text message",
    "Email": "[Email] = Email",
    "GMB": "[GMB] = Google Business Messages",
    "IG": "[IG] = Instagram DM",
    "FB": "[FB] = Facebook Messenger",
    "WhatsApp": "[WhatsApp] = WhatsApp message",
    "Live Chat": "[Live Chat] = Website live chat",
}


def _build_legend(
    source_labels: set[str], channels: set[str], has_calls: bool, is_followup: bool,
) -> str:
    """Build a dynamic timeline legend showing only types/channels present."""
    lines = ["## CONVERSATION HISTORY", ""]

    # Message types — always include LEAD + AI, then only others that appear
    type_lines = []
    for key in ("LEAD", "AI", "WORKFLOW", "MANUAL", "FOLLOW_UP", "CALL"):
        if key in ("LEAD", "AI"):
            type_lines.append(f"- {_MSG_TYPE_LEGEND[key]}")
        elif key == "CALL" and has_calls:
            type_lines.append(f"- {_MSG_TYPE_LEGEND[key]}")
        elif key in source_labels:
            type_lines.append(f"- {_MSG_TYPE_LEGEND[key]}")
    if not is_followup:
        type_lines.append("- [CURRENT MESSAGE] = Respond to this message")

    lines.append("### Message Types:")
    lines.extend(type_lines)

    # Channels — only show channels that exist
    channel_lines = []
    for ch_key in ("SMS", "Email", "GMB", "IG", "FB", "WhatsApp", "Live Chat"):
        if ch_key in channels:
            channel_lines.append(f"- {_CHANNEL_LEGEND[ch_key]}")

    if channel_lines:
        lines.append("")
        lines.append("### Channel Tags:")
        lines.extend(channel_lines)

    lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


@task(name="build_timeline", retries=1, retry_delay_seconds=3, timeout_seconds=30)
async def build_timeline(ctx: PipelineContext, is_followup: bool = False) -> None:
    """Fetch chat history, call logs, attachments and build unified timeline.

    Populates:
    - ctx.chat_history: raw chat history rows (list of dicts)
    - ctx.call_logs: recent call log records
    - ctx.timeline: formatted timeline string for agent context
    - ctx.chat_history_text: same as ctx.timeline (backward compat)
    """
    chat_table = ctx.config.get("chat_history_table_name", "")
    entity_id = ctx.config.get("id", ctx.entity_id)
    tz = ctx.tz or get_timezone(ctx.config)

    # Fetch data in parallel
    import asyncio

    chat_history_task = (
        postgres.get_chat_history(chat_table, ctx.contact_id, limit=100)
        if chat_table
        else _empty_list()
    )
    call_logs_task = supabase.get_call_logs(
        ctx.contact_id, entity_id, limit=5
    )

    chat_history, call_logs = await asyncio.gather(
        chat_history_task, call_logs_task
    )

    ctx.chat_history = chat_history
    ctx.call_logs = call_logs

    # Attachments already loaded by Step 3.4
    attachments = ctx.attachments or []

    # Build timeline
    ctx.timeline, timeline_stats = format_timeline(chat_history, call_logs, attachments, tz, is_followup=is_followup)
    ctx.chat_history_text = ctx.timeline

    # Populate artifact diagnostics
    ctx.timeline_stats = timeline_stats

    logger.info(
        "Timeline built: %d messages, %d calls, %d attachments (followup=%s)",
        len(chat_history),
        len(call_logs),
        len(attachments),
        is_followup,
    )


async def _empty_list() -> list:
    return []


def _group_consecutive_messages(
    entries: list[dict[str, Any]],
    human_window_seconds: int = 300,
    ai_window_seconds: int = 10,
) -> list[dict[str, Any]]:
    """Group consecutive same-type messages within a time window for display.

    Entries are sorted newest-first. Content within a group is ordered
    oldest-first (prepend older content). This makes the timeline cleaner
    when a lead sends multiple bubbles in quick succession, or when the AI
    sends split messages that should appear as one response.

    Human messages group within 5 minutes (lead sending multiple texts).
    AI messages group within 10 seconds (split messages stored separately).
    """
    if not entries:
        return entries

    grouped: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    for entry in entries:
        if (
            current is not None
            and entry["type"] == "message"
            and current["type"] == "message"
            and entry["is_human"] == current["is_human"]
        ):
            window = human_window_seconds if entry["is_human"] else ai_window_seconds
            if abs((current["timestamp"] - entry["timestamp"]).total_seconds()) < window:
                # Same turn — prepend content (entry is older since list is newest-first)
                current["content"] = entry["content"] + "\n" + current["content"]
                current["attachment_refs"] = entry["attachment_refs"] + current["attachment_refs"]
                continue

        if current is not None:
            grouped.append(current)
        current = dict(entry)
        current["attachment_refs"] = list(entry["attachment_refs"])

    if current is not None:
        grouped.append(current)
    return grouped


def format_timeline(
    chat_history: list[dict[str, Any]],
    call_logs: list[dict[str, Any]],
    attachments: list[dict[str, Any]],
    tz: ZoneInfo,
    is_followup: bool = False,
) -> tuple[str, dict[str, Any]]:
    """Format all data into a unified timeline string + stats dict.

    Decoupled from PipelineContext — any workflow can call this directly
    with raw chat_history, call_logs, and attachments data.

    Returns (timeline_string, stats_dict) where stats_dict contains
    message/call/attachment counts and channels present.
    Matches n8n's Format Unified Timeline output exactly.
    When is_followup=True, uses the follow-up variant (no [CURRENT MESSAGE]
    marker, adds FOLLOW-UP ATTEMPT header).
    """
    # Build attachment lookup by timestamp (for inline refs)
    attachment_by_time: dict[int, list[str]] = {}
    for att in attachments:
        ts_str = att.get("message_timestamp") or att.get("created_at")
        if ts_str:
            ts = parse_datetime(ts_str)
            ts_ms = int(ts.timestamp() * 1000)
            type_label = (att.get("type", "image") or "image").capitalize()
            type_index = att.get("type_index", 1)
            ref = f"{type_label} {type_index}"
            attachment_by_time.setdefault(ts_ms, []).append(ref)

    # Parse chat messages
    entries: list[dict[str, Any]] = []
    for row in chat_history:
        ts = row.get("timestamp")
        if isinstance(ts, str):
            ts = parse_datetime(ts)

        role = row.get("role", "unknown")
        is_human = role == "human"
        source = row.get("source") or ("lead" if is_human else "AI")
        channel = row.get("channel") or ""
        content = row.get("content") or ""

        # Find attachment refs — three-tier matching:
        # 0. Direct attachment_ids (UUIDs referencing attachments table)
        # 1. Match by ghl_message_id (exact, from conversation sync)
        # 2. Timestamp proximity fallback (60s window, for legacy data)
        attachment_refs: list[str] = []
        ghl_id = row.get("ghl_message_id") or ""

        row_att_ids = row.get("attachment_ids") or []
        if row_att_ids:
            # Tier 0: Direct UUID lookup — build labels from matched attachment records
            for att in attachments:
                if str(att.get("id", "")) in [str(aid) for aid in row_att_ids]:
                    type_label = (att.get("type", "image") or "image").capitalize()
                    type_index = att.get("type_index", 1)
                    attachment_refs.append(f"{type_label} {type_index}")
        elif ghl_id:
            for att in attachments:
                if att.get("ghl_message_id") == ghl_id:
                    type_label = (att.get("type", "image") or "image").capitalize()
                    type_index = att.get("type_index", 1)
                    attachment_refs.append(f"{type_label} {type_index}")
        elif ts:
            msg_ms = int(ts.timestamp() * 1000)
            for att_ms, refs in attachment_by_time.items():
                if abs(msg_ms - att_ms) < 60000:
                    attachment_refs.extend(refs)

        entries.append({
            "timestamp": ts or datetime.now(timezone.utc),
            "type": "message",
            "is_human": is_human,
            "source": source,
            "channel": channel,
            "content": content,
            "attachment_refs": attachment_refs,
        })

    # Parse call logs
    for call in call_logs:
        ts = parse_datetime(call.get("created_at", ""))
        direction = (call.get("direction") or "Unknown").capitalize()
        status = (call.get("status") or "Unknown").capitalize()
        summary = call.get("summary") or "No summary available"

        entries.append({
            "timestamp": ts,
            "type": "call",
            "is_human": False,
            "source": "CALL",
            "channel": "",
            "content": f"{direction} | {status}\n{summary}",
            "attachment_refs": [],
        })

    # Sort most recent first
    entries.sort(key=lambda e: e["timestamp"], reverse=True)

    # Group consecutive same-type messages for cleaner display
    # Human: 5-min window (lead sending multiple texts)
    # AI: 10-sec window (split messages stored as separate rows)
    entries = _group_consecutive_messages(entries)

    # Cross-platform strftime: Windows uses %# for no-pad, Unix uses %-
    _no_pad = "#" if os.name == "nt" else "-"

    # Count consecutive follow-ups for the follow-up header
    followup_count = 0
    if is_followup:
        for entry in entries:
            if entry["type"] == "message" and not entry["is_human"]:
                src = entry["source"].lower()
                # Only count actual follow-ups, not regular AI replies
                if src in ("follow_up", "followup", "follow up"):
                    followup_count += 1
            elif entry["type"] == "message" and entry["is_human"]:
                break

    if is_followup:
        logger.info("TIMELINE | followup_count=%d | entries=%d", followup_count, len(entries))

    # Format each entry
    found_first_human = False
    lines: list[str] = []

    for entry in entries:
        ts: datetime = entry["timestamp"]
        date_str = ts.astimezone(tz).strftime(f"%b %{_no_pad}d")
        time_str = ts.astimezone(tz).strftime(f"%{_no_pad}I:%M %p")

        marker = ""
        if entry["type"] == "message":
            if entry["is_human"]:
                label = "LEAD"
                # Only add [CURRENT MESSAGE] marker for reply timelines, not follow-ups
                if not found_first_human and not is_followup:
                    marker = " [CURRENT MESSAGE - RESPOND TO THIS]"
                    found_first_human = True
            else:
                label = entry["source"].upper()
        else:
            label = "CALL"

        channel_tag = f" [{entry['channel']}]" if entry["channel"] else ""

        line = f"[{date_str}, {time_str}] {label}{channel_tag}{marker}: {entry['content']}"

        if entry["attachment_refs"]:
            line += f"\n  [Attachments: {', '.join(entry['attachment_refs'])}]"

        lines.append(line)

    # Build attachments section
    attachments_section = ""
    if attachments:
        att_lines = []
        for att in sorted(attachments, key=lambda a: a.get("created_at", "")):
            desc = att.get("description") or "No description available"
            type_label = (att.get("type", "image") or "image").capitalize()
            type_index = att.get("type_index", 1)
            label_prefix = f"{type_label} {type_index}: "
            # Only add label if not already present
            if not desc.startswith(("Image ", "Video ", "Audio ", "Document ", "Link ")):
                desc = label_prefix + desc

            ts = parse_datetime(att.get("message_timestamp") or att.get("created_at", ""))
            date_str = ts.astimezone(tz).strftime(f"%b %{_no_pad}d")
            time_str = ts.astimezone(tz).strftime(f"%{_no_pad}I:%M %p")
            att_lines.append(f"[{date_str}, {time_str}] {desc}")

        attachments_section = f"\n\n--- ALL LEAD ATTACHMENTS ---\n" + "\n".join(att_lines)

    # Collect source labels and channels actually present for dynamic legend
    source_labels: set[str] = set()
    channels_present: set[str] = set()
    for entry in entries:
        if entry["type"] == "message":
            src_upper = entry["source"].upper()
            if not entry["is_human"] and src_upper not in ("AI",):
                source_labels.add(src_upper)
            if entry["channel"]:
                channels_present.add(entry["channel"])

    # Assemble
    output = _build_legend(source_labels, channels_present, bool(call_logs), is_followup)

    # Add follow-up attempt header for follow-up timelines
    if is_followup:
        attempt_num = followup_count + 1
        output += f"## FOLLOW-UP ATTEMPT\n"
        output += (
            f"This is the {_ordinal(attempt_num)} follow-up in a row. "
            f"{followup_count} previous follow-up(s) were sent with no lead response.\n"
        )
        output += "*Follow-up context: The lead has not responded recently.*\n\n"

    if lines:
        output += "\n\n".join(lines)
    else:
        output += "*No conversation history.*"
    output += attachments_section

    # Build stats dict for artifact diagnostics
    stats = {
        "messages": len(chat_history),
        "calls": len(call_logs),
        "attachments": len(attachments),
        "channels": sorted(channels_present),
        "source_types": sorted(source_labels),
        "followup_count": followup_count,
    }

    return output, stats


def _ordinal(n: int) -> str:
    """Return ordinal string for a number (1st, 2nd, 3rd, etc.)."""
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


