"""Step 3.6: Booking context — fetch appointments, determine booking status.

Replaces n8n's "Get Appointments" → "Format Appointment Data" →
"Check Booking Status" → "Set Booking Data" chain.

Fetches from GHL Calendar API each request (not cached/stored).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from prefect import task
from zoneinfo import ZoneInfo

from app.models import PipelineContext
from app.services.supabase_client import supabase
from app.text_engine.utils import get_timezone

logger = logging.getLogger(__name__)

# Appointment statuses that count as "booked"
_CONFIRMED_STATUSES = {"confirmed"}


@task(name="load_booking_context", retries=1, retry_delay_seconds=3, timeout_seconds=30)
async def load_booking_context(ctx: PipelineContext) -> None:
    """Fetch GHL appointments and set booking context on pipeline.

    Populates:
    - ctx.bookings: list of upcoming confirmed appointment dicts
    - ctx.has_upcoming_booking: True if any upcoming confirmed appointment
    - ctx.upcoming_booking_text: formatted upcoming appointments (with IDs)
    - ctx.past_booking_text: formatted past 30-day appointment history
    """
    try:
        events = await ctx.ghl.get_appointments(ctx.contact_id)
    except Exception:
        logger.warning("Failed to fetch appointments", exc_info=True)
        ctx.upcoming_booking_text = "Appointment status: Unable to retrieve."
        return

    if events is None:
        events = []

    logger.info("BOOKING | contact=%s | ghl_events=%d", ctx.contact_id, len(events))

    # Parse timezone from client config
    tz = ctx.tz or get_timezone(ctx.config)

    now = datetime.now(timezone.utc)

    # Filter & format appointments
    upcoming_confirmed: list[dict[str, Any]] = []
    all_formatted: list[dict[str, Any]] = []

    for event in events:
        start_str = event.get("startTime", "")
        status = (event.get("appointmentStatus") or "").lower()
        title = event.get("title", "Appointment")

        start_dt = _parse_dt(start_str, tz)
        if not start_dt:
            continue

        # Normalized appointment shape (10.5): GHL's "startTime" → "start" (datetime)
        # Consumers: booking.py, followup.py, agent.py all access this normalized form
        formatted = {
            "id": event.get("id") or event.get("eventId", ""),
            "title": title,
            "start": start_dt,  # Parsed datetime (GHL raw field: "startTime")
            "end": _parse_dt(event.get("endTime", ""), tz),
            "status": status,
            "calendar_id": event.get("calendarId", ""),
        }
        all_formatted.append(formatted)

        # Upcoming + confirmed? (12h safety offset for timezone edge cases)
        if start_dt > (now - timedelta(hours=12)) and status in _CONFIRMED_STATUSES:
            upcoming_confirmed.append(formatted)

    # Sort upcoming by date (soonest first)
    upcoming_confirmed.sort(key=lambda a: a["start"])

    # Merge Supabase bookings — deduplicate by appointment ID
    # Matches n8n's "Set Booking Data" node which merges both sources
    existing_ids = {a["id"] for a in all_formatted if a.get("id")}
    try:
        entity_id = ctx.config.get("id", ctx.entity_id)
        sb_bookings = await supabase.get_bookings(ctx.contact_id, entity_id)
        for sb in sb_bookings:
            sb_id = sb.get("ghl_appointment_id", "")
            if sb_id and sb_id in existing_ids:
                continue  # Already in GHL list — skip duplicate
            sb_start = _parse_dt(sb.get("appointment_datetime", ""), tz)
            if not sb_start:
                continue
            sb_status = (sb.get("status") or "confirmed").lower()
            sb_appt = {
                "id": sb_id,
                "title": sb.get("ghl_calendar_name", "Appointment"),
                "start": sb_start,
                "end": None,
                "status": sb_status,
                "calendar_id": None,
                "source": "supabase_fallback",
            }
            all_formatted.append(sb_appt)
            existing_ids.add(sb_id)
            # Also check if this Supabase booking is upcoming + confirmed
            if sb_start > (now - timedelta(hours=12)) and sb_status in _CONFIRMED_STATUSES:
                upcoming_confirmed.append(sb_appt)
        if sb_bookings:
            logger.info("Supabase bookings merged: %d records", len(sb_bookings))
    except Exception as e:
        logger.warning("Supabase bookings check failed: %s", e)

    # Re-sort upcoming after merge (soonest first)
    upcoming_confirmed.sort(key=lambda a: a["start"])

    # Configurable lookback window (default 30 days)
    # Read from setter's conversation.reply post_booking and conversation.follow_up
    _setter = ctx.compiled.get("_matched_setter") or {}
    _conversation = _setter.get("conversation", {})
    _reply = _conversation.get("reply") or {}
    _agent_window = _reply.get("sections", {}).get("post_booking", {}).get("appointment_context_window", 30)
    _fu = _conversation.get("follow_up") or {}
    _fu_window = _fu.get("appointment_context_window", 30)
    lookback_days = max(_agent_window, _fu_window, 30)  # never go below 30

    lookback_ago = now - timedelta(days=lookback_days)
    all_recent = [a for a in all_formatted if a["start"] >= lookback_ago]
    all_recent.sort(key=lambda a: a["start"], reverse=True)

    ctx.bookings = upcoming_confirmed
    ctx.all_bookings = all_recent
    ctx.has_upcoming_booking = bool(upcoming_confirmed)
    ctx.upcoming_booking_text = _format_upcoming_booking_text(upcoming_confirmed, tz)
    ctx.past_booking_text = _format_past_booking_text(all_recent, tz)

    logger.info(
        "Booking context: %d total events, %d in %d-day window, %d upcoming confirmed",
        len(all_formatted),
        len(all_recent),
        lookback_days,
        len(upcoming_confirmed),
    )


def _format_upcoming_booking_text(
    upcoming: list[dict[str, Any]],
    tz: ZoneInfo,
) -> str:
    """Format upcoming confirmed appointments with full details (IDs for tools).

    Used by reply agent (needs IDs for cancel/reschedule tools) and classifiers
    (need to know booking status). Single format for all consumers.
    """
    lines: list[str] = []

    if upcoming:
        lines.append(f"Has Confirmed Upcoming Appointment: YES ({len(upcoming)})")
        lines.append("")
        lines.append("Appointments:")
        details: list[str] = []
        for appt in upcoming:
            start: datetime = appt["start"]
            end = appt.get("end")
            local_start = start.astimezone(tz)
            date_str = local_start.strftime("%A, %B %d, %Y").replace(" 0", " ")
            start_str = local_start.strftime("%I:%M %p").lstrip("0")
            end_str = ""
            if end:
                end_str = end.astimezone(tz).strftime("%I:%M %p").lstrip("0")

            detail = (
                f"Status: {appt['status']}\n"
                f"Date: {date_str}\n"
                f"Time: {start_str}"
            )
            if end_str:
                detail += f" - {end_str}"
            detail += (
                f"\nTitle: {appt['title']}\n"
                f"Calendar ID: {appt.get('calendar_id', '')}\n"
                f"Event ID: {appt.get('id', '')}  ← Use this ID for reschedule or cancel"
            )
            details.append(detail)
        lines.append("\n\n---\n\n".join(details))
    else:
        lines.append("Has Confirmed Upcoming Appointment: NO")

    return "\n".join(lines)


def _format_past_booking_text(
    all_appts: list[dict[str, Any]],
    tz: ZoneInfo,
) -> str:
    """Format past appointments from 30-day lookback (compact, no IDs).

    Used by all agents for cancel/no-show/completed visit awareness.
    Returns empty string when no past appointments exist.
    """
    now = datetime.now(timezone.utc)
    past = [a for a in all_appts if a["start"] <= now]
    past.sort(key=lambda a: a["start"], reverse=True)

    if not past:
        return ""

    lines: list[str] = ["Past Appointments (Last 30 Days):"]
    for appt in past:
        start: datetime = appt["start"]
        local = start.astimezone(tz)
        date_str = local.strftime("%B %d, %Y at %I:%M %p")
        status = appt["status"].capitalize()
        lines.append(f"- {date_str} - {appt['title']} ({status})")

    return "\n".join(lines)


def _parse_dt(dt_str: str, fallback_tz: ZoneInfo | None = None) -> datetime | None:
    """Parse ISO 8601 datetime string into timezone-aware datetime.

    GHL returns datetimes in two formats:
    - With offset (e.g. free-slots API): "2026-03-02T09:30:00-08:00"
    - Without offset (e.g. appointments API): "2026-03-02 11:30:00"

    Naive datetimes from GHL are in the GHL *location* timezone, which should
    match the client's configured timezone.  When ``fallback_tz`` is provided
    naive values are interpreted in that timezone; otherwise UTC is assumed.
    """
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=fallback_tz or timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None
