"""Core logic for running test scenarios directly against the pipeline.

Builds a PipelineContext with a MockGHLClient and calls reply_pipeline /
followup_pipeline without going through HTTP or Prefect.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable
from uuid import uuid4

from app.config import settings
from app.models import PipelineContext, TokenUsage
from app.services.ai_client import set_ai_context, clear_ai_context, chat
from app.services.supabase_client import supabase
from app.services.postgres_client import postgres
from app.text_engine.pipeline import reply_pipeline, followup_pipeline, run_pipeline_with_tracking
from app.testing.mock_ghl import MockGHLClient
from app.testing.models import (
    MockGHLConfig,
    TestScenario,
    LeadConfig,
    TestContext,
    DripConfig,
    SeedMessage,
    AppointmentConfig,
    CallLogConfig,
    Turn,
)

logger = logging.getLogger(__name__)

# Production follow-up cadence: 6h → 21h → 1d → 2d → 3d → 6d
DEFAULT_FOLLOWUP_CADENCE = [6.0, 21.0, 24.0, 48.0, 72.0, 144.0]


# ---------------------------------------------------------------------------
# Variable resolution (pattern from outreach_resolver.py)
# ---------------------------------------------------------------------------

def _build_variable_map(
    lead: LeadConfig,
    entity_config: dict[str, Any],
) -> dict[str, str]:
    """Build {{placeholder}} -> value lookup from lead config + entity config."""
    full_name = f"{lead.first_name} {lead.last_name}".strip()
    return {
        "{{contact.first_name}}": lead.first_name,
        "{{contact.last_name}}": lead.last_name,
        "{{contact.name}}": full_name,
        "{{contact.email}}": lead.email,
        "{{contact.phone}}": lead.phone,
        "{{location.name}}": entity_config.get("name", "") or entity_config.get("name", ""),
        "{{location.address}}": entity_config.get("address", ""),
        "{{location.city}}": entity_config.get("city", ""),
        "{{location.state}}": entity_config.get("state", ""),
    }


def _resolve_text(text: str, var_map: dict[str, str]) -> str:
    """Substitute {{...}} variables and strip unresolved ones."""
    result = text
    for placeholder, value in var_map.items():
        if value:
            result = result.replace(placeholder, value)
    # Strip unresolved {{...}} placeholders
    result = re.sub(r"\{\{[^}]+\}\}", "", result)
    return re.sub(r"  +", " ", result).strip()


# ---------------------------------------------------------------------------
# Dynamic lead message generation
# ---------------------------------------------------------------------------

_LEAD_SIM_SYSTEM = """\
You are simulating a real person replying to a sales rep via SMS. You are NOT the AI, \
you are NOT the sales rep. You are the LEAD — a real person texting back.

Your job: generate a realistic lead reply based on the conversation so far and the \
intent you've been given. The intent tells you WHAT to say, but you decide HOW to say it \
naturally.

## Rules
- Write like a real person texting: casual, short, lowercase, maybe abbreviations
- React to what the sales rep actually said — acknowledge their questions, respond to their points
- Stay on track with the intent — don't go off-topic or add information not in the intent
- Keep it to 1-3 short sentences max (this is SMS, not email)
- Don't be overly enthusiastic or overly formal
- If the AI asked you a question, answer it naturally while weaving in your intent
- If follow-up messages were sent before your reply, you can reference them naturally \
  (e.g., "yeah I saw your messages" or just respond to the latest one)
- Match the energy level implied by the intent — if the intent is skeptical, be skeptical; \
  if interested, be interested; if objecting, object naturally
- If the intent implies minimal effort (e.g., "just answer with your business type"), \
  give the absolute bare minimum — one word or a few words, no full sentence
- Never repeat words or stutter (e.g., "bookings. bookings." is wrong). Read your output \
  back before finalizing — if any word appears twice in a row or a phrase is repeated, fix it
- Never break character — you are a real lead, not a test script
- CRITICAL: Your output must be a realistic SMS text message. NEVER output a description of \
  what the lead does (like "Asks about pricing" or "Expresses frustration"). Output the \
  actual words the lead would type in an SMS. Wrong: "Asks if prices are similar to competitor." \
  Right: "how do your prices compare to [competitor]?"\
"""

_LEAD_SIM_SCHEMA = {
    "type": "object",
    "properties": {
        "lead_message": {
            "type": "string",
            "description": "The lead's SMS reply",
        },
    },
    "required": ["lead_message"],
}


async def _generate_dynamic_lead_message(
    intent: str,
    test_description: str,
    conversation_so_far: list[dict[str, Any]],
    contact_name: str,
    business_name: str = "",
    services_text: str = "",
) -> str:
    """Generate a dynamic lead reply based on intent and conversation context.

    Uses Gemini Flash to create a realistic lead message that reacts to the AI's
    actual reply while staying on track with the test's intent.
    """
    # Build conversation summary from prior turns + follow-ups
    convo_lines = []
    for entry in conversation_so_far:
        role = entry.get("role", "")
        if role == "turn":
            lead_msg = entry.get("lead_message", "")
            ai_resp = entry.get("ai_response", "")
            if lead_msg:
                convo_lines.append(f"LEAD: {lead_msg}")
            if ai_resp:
                convo_lines.append(f"SALES REP: {ai_resp}")
        elif role == "followup":
            fu_msg = entry.get("followup_message", "")
            fu_num = entry.get("followup_number", "?")
            if fu_msg:
                convo_lines.append(f"SALES REP (follow-up #{fu_num}): {fu_msg}")
            elif not entry.get("followup_needed"):
                pass

    convo_text = "\n".join(convo_lines) if convo_lines else "(no prior messages)"

    biz_context = ""
    if business_name:
        biz_context += f"## Business You're Contacting\n{business_name}\n\n"
    if services_text:
        biz_context += f"## Services They Offer (ONLY reference these)\n{services_text}\n\n"

    prompt = (
        f"{biz_context}"
        f"## Test Scenario\n{test_description}\n\n"
        f"## Your Name\n{contact_name}\n\n"
        f"## Conversation So Far\n{convo_text}\n\n"
        f"## Your Intent For This Reply\n{intent}\n\n"
        f"Generate your SMS reply. Stay on track with the intent while reacting "
        f"naturally to what the sales rep said. ONLY mention services this business "
        f"actually offers — NEVER invent or ask about services not listed above."
    )

    try:
        resp = await chat(
            messages=[
                {"role": "system", "content": _LEAD_SIM_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            model="google/gemini-2.5-flash",
            temperature=0.7,
            label="lead_simulator",
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "lead_sim",
                    "strict": True,
                    "schema": _LEAD_SIM_SCHEMA,
                },
            },
        )
        content = resp.choices[0].message.content or ""
        result = json.loads(content)
        generated = (result.get("lead_message") or "").strip()
        if generated:
            logger.info("Dynamic lead message generated: %s", generated[:100])
            return generated
        logger.warning("Dynamic lead generation returned empty, falling back to intent")
        return intent
    except Exception as e:
        logger.error("Dynamic lead generation failed: %s — falling back to intent", e)
        return intent


def _resolve_setter_cadence(entity_config: dict[str, Any], setter_key: str, count: int) -> list[float]:
    """Read follow-up cadence from the setter config, matching production behavior.

    Reads from: system_config.setters.{setter_key}.conversation.follow_up.sections.cadence_timing.timings
    Falls back to DEFAULT_FOLLOWUP_CADENCE if not configured.
    """
    try:
        from app.services.message_scheduler import parse_cadence_to_hours
        from app.text_engine.followup_compiler import get_cadence_timing

        sc = entity_config.get("system_config", {})
        setters = sc.get("setters", {})
        setter = setters.get(setter_key) or next(
            (s for s in setters.values() if s.get("is_default")),
            next(iter(setters.values()), {}),
        )
        fu_config = setter.get("conversation", {}).get("follow_up")
        custom_timings = get_cadence_timing(fu_config)
        cadence = parse_cadence_to_hours(custom_timings)

        # Extend if needed
        while len(cadence) < count:
            cadence.append(cadence[-1] if cadence else 24.0)
        return cadence
    except Exception as e:
        logger.warning("Failed to resolve setter cadence, using default: %s", e)
        return _resolve_cadence(None, count)


def _build_test_timeline(
    conversation: list[dict[str, Any]],
    entity_config: dict[str, Any],
    current_message: str,
    channel: str,
    backdate_hours: float = 0,
    is_followup: bool = False,
) -> tuple[str, list[dict[str, Any]]]:
    """Build a timeline string from the in-memory conversation array.

    Returns (timeline_string, fake_chat_history_rows) matching the format
    that build_timeline() and format_timeline() produce from the chat table.
    This lets the pipeline get conversation context without touching the DB.
    """
    from app.text_engine.timeline import format_timeline
    from app.text_engine.utils import get_timezone

    tz = get_timezone(entity_config)
    base_time = datetime.now(timezone.utc)
    if backdate_hours > 0:
        base_time -= timedelta(hours=backdate_hours)

    # Convert conversation entries to chat_history row format
    chat_rows: list[dict[str, Any]] = []
    msg_offset = 0

    for entry in conversation:
        role = entry.get("role", "")

        if role == "seed":
            # Pre-conversation seed messages (drips, prior exchanges)
            hours_ago = entry.get("seed_hours_ago", 24)
            ts = base_time - timedelta(hours=hours_ago)
            chat_rows.append({
                "role": entry.get("seed_role", "ai"),
                "content": entry.get("seed_content", ""),
                "source": entry.get("seed_source", "outreach_drip"),
                "channel": entry.get("seed_channel", channel),
                "timestamp": ts,
                "ghl_message_id": None,
                "attachment_ids": [],
            })

        elif role == "turn":
            # Lead message
            lead_msg = entry.get("lead_message", "")
            if lead_msg:
                ts = base_time + timedelta(seconds=msg_offset)
                chat_rows.append({
                    "role": "human",
                    "content": lead_msg,
                    "source": "lead_reply",
                    "channel": channel,
                    "timestamp": ts,
                    "ghl_message_id": None,
                    "attachment_ids": [],
                })
                msg_offset += 3

            # AI response
            ai_text = entry.get("ai_response", "")
            if ai_text:
                parts = ai_text.split(" | ") if " | " in ai_text else [ai_text]
                for part in parts:
                    ts = base_time + timedelta(seconds=msg_offset)
                    chat_rows.append({
                        "role": "ai",
                        "content": part,
                        "source": "AI",
                        "channel": channel,
                        "timestamp": ts,
                        "ghl_message_id": None,
                        "attachment_ids": [],
                    })
                    msg_offset += 1

        elif role == "followup":
            fu_msg = entry.get("followup_message", "")
            if fu_msg and entry.get("followup_needed", False):
                ts = base_time + timedelta(seconds=msg_offset)
                chat_rows.append({
                    "role": "ai",
                    "content": fu_msg,
                    "source": "follow_up",
                    "channel": channel,
                    "timestamp": ts,
                    "ghl_message_id": None,
                    "attachment_ids": [],
                })
                msg_offset += 2

    # Add the current inbound message (if not a follow-up)
    if current_message and not is_followup:
        ts = base_time + timedelta(seconds=msg_offset)
        chat_rows.append({
            "role": "human",
            "content": current_message,
            "source": "lead_reply",
            "channel": channel,
            "timestamp": ts,
            "ghl_message_id": None,
            "attachment_ids": [],
        })

    # Follow-up logic expects chat_history newest-first, matching postgres.get_chat_history().
    chat_rows.sort(key=lambda row: row["timestamp"], reverse=True)

    # Format using the same function the production pipeline uses
    timeline_str, _stats = format_timeline(chat_rows, [], [], tz, is_followup=is_followup)

    return timeline_str, chat_rows


def _resolve_cadence(cadence: list[float] | None, count: int) -> list[float]:
    """Convert followup_cadence to a per-follow-up interval list.

    - None → production cadence
    - List → use as-is, extend with last value if more follow-ups than entries
    """
    if cadence is None:
        result = list(DEFAULT_FOLLOWUP_CADENCE)
    else:
        result = [float(v) for v in cadence]

    # Extend if more follow-ups than cadence entries
    while len(result) < count:
        result.append(result[-1] if result else 24.0)

    return result[:count]


# ---------------------------------------------------------------------------
# Context setup helpers
# ---------------------------------------------------------------------------

async def _setup_appointments(
    mock_config: MockGHLConfig,
    entity_config: dict[str, Any],
    entity_id: str,
    contact_id: str,
    appointments: list[AppointmentConfig],
) -> None:
    """Inject appointments into mock GHL + Supabase."""
    for appt in appointments:
        now = datetime.now(timezone.utc)
        # Use provided start time if given, otherwise default to 2 days from now
        if appt.start:
            try:
                start_time = datetime.fromisoformat(appt.start.replace("Z", "+00:00"))
                if start_time.tzinfo is None:
                    if appt.timezone:
                        # Treat naive datetime as being in the specified timezone
                        from zoneinfo import ZoneInfo
                        tz = ZoneInfo(appt.timezone)
                        start_time = start_time.replace(tzinfo=tz)
                    else:
                        start_time = start_time.replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning("Invalid start time '%s', defaulting to +2 days", appt.start)
                start_time = now + timedelta(days=2)
        else:
            start_time = now + timedelta(days=2)
        end_time = start_time + timedelta(hours=1)

        # Resolve calendar_name against entity's booking calendars (from system_config setter)
        calendar_name = appt.calendar_name
        calendar_id = appt.calendar_id or str(uuid4())

        if not appt.calendar_id and entity_config:
            _sys = entity_config.get("system_config") or {}
            _setters = _sys.get("setters") or {}
            _first_setter = next(iter(_setters.values()), {}) if _setters else {}
            _booking = _first_setter.get("booking") or {}
            cal_ids = [c for c in _booking.get("calendars", []) if isinstance(c, dict) and c.get("enabled", True)]
            for cal in cal_ids:
                if cal.get("name") == calendar_name:
                    calendar_id = cal.get("calendar_id") or cal.get("id") or calendar_id
                    break
            else:
                if cal_ids and isinstance(cal_ids[0], dict):
                    calendar_id = cal_ids[0].get("calendar_id") or cal_ids[0].get("id") or calendar_id

        # Inject into MockGHLClient
        mock_config.appointments.append({
            "id": str(uuid4()),
            "title": calendar_name,
            "startTime": start_time.isoformat(),
            "endTime": end_time.isoformat(),
            "appointmentStatus": appt.status,
            "calendarId": calendar_id,
        })

        # Mock GHL injection above is sufficient — pipeline reads appointments from
        # MockGHLClient in test mode. No Supabase writes needed (test data isolation).


async def _setup_call_logs(
    entity_id: str,
    contact_id: str,
    call_logs: list[CallLogConfig],
) -> None:
    """Set up call logs for test context.

    No Supabase writes — test data isolation. Call log context is injected
    into the timeline by the test runner (in-memory conversation array).
    This function is kept for API compatibility but is now a no-op.
    """
    logger.info("_setup_call_logs | test mode | %d call logs (mock only, no DB writes)", len(call_logs))


async def _seed_drip_messages(
    entity_config: dict[str, Any],
    entity_id: str,
    contact_id: str,
    drip: DripConfig,
    lead: LeadConfig,
    var_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Seed outreach drip messages into chat history.

    Mode A (template): Fetch from outreach_templates table, resolve variables.
    Mode B (custom): Use provided messages with variable resolution.

    Returns list of seeded message dicts for conversation record.
    """
    chat_table = entity_config.get("chat_history_table_name", "")
    if not chat_table:
        logger.warning("seed_drip_messages: no chat_history_table_name in entity config")
        return []

    now = datetime.now(timezone.utc)
    seeded: list[dict[str, Any]] = []

    if drip.template or drip.template_ids:
        # Mode A: Fetch one or more templates from outreach_templates
        templates: list[dict[str, Any]] = []
        if drip.template_ids:
            for template_id in drip.template_ids:
                resp = await supabase._request(
                    supabase.main_client,
                    "GET",
                    "/outreach_templates",
                    params={"id": f"eq.{template_id}", "entity_id": f"eq.{entity_id}", "limit": "1"},
                    label="fetch_drip_template_by_id",
                )
                data = resp.json() if resp.status_code < 400 else []
                if data:
                    templates.append(data[0])
        else:
            resp = await supabase._request(
                supabase.main_client, "GET", "/outreach_templates",
                params={
                    "entity_id": f"eq.{entity_id}",
                    "form_service_interest": f"ilike.{drip.template}",
                    "limit": "1",
                },
                label="fetch_drip_template",
            )
            templates = resp.json() if resp.status_code < 400 else []

        if not templates:
            logger.warning("Drip templates not found for entity %s", entity_id)
            return []

        count = drip.messages_sent or 0
        drip_pool: list[tuple[str, str]] = []
        for template in templates:
            template_name = template.get("form_service_interest", "Outreach")
            for i in range(1, 10):
                sms_text = template.get(f"sms_{i}", "")
                if not sms_text:
                    continue
                resolved = _resolve_text(sms_text, var_map)
                if resolved:
                    drip_pool.append((template_name, resolved))

        if not drip_pool:
            return []

        selected_pool = drip_pool[:count] if count > 0 else drip_pool
        total_selected = len(selected_pool)
        for i, (template_name, resolved) in enumerate(selected_pool, start=1):
            hours_ago = (total_selected - i + 1) * 24.0  # Spread messages across days
            seeded.append({
                "role": "outreach_drip",
                "content": resolved,
                "hours_ago": hours_ago,
                "template": template_name,
                "message_number": i,
            })

        logger.info("Seeded %d drip messages from %d template(s) (in-memory only)", len(seeded), len(templates))

    else:
        # Mode B: Custom messages
        for msg in drip.messages:
            resolved = _resolve_text(msg.content, var_map)
            if not resolved:
                continue
            seeded.append({
                "role": "outreach_drip",
                "content": resolved,
                "hours_ago": msg.hours_ago,
                "timestamp": msg.timestamp,
            })

        logger.info("Seeded %d custom drip messages (in-memory only)", len(seeded))

    return seeded


async def _seed_messages(
    entity_config: dict[str, Any],
    entity_id: str,
    contact_id: str,
    messages: list[SeedMessage],
    var_map: dict[str, str],
) -> None:
    """Seed conversation history messages into Postgres."""
    if not messages:
        return

    chat_table = entity_config.get("chat_history_table_name", "")
    if not chat_table:
        logger.warning("seed_messages: no chat_history_table_name in entity config")
        return

    now = datetime.now(timezone.utc)
    # Sort by hours_ago descending (oldest first) for chronological insertion
    sorted_msgs = sorted(messages, key=lambda m: m.hours_ago, reverse=True)

    batch = []
    for msg in sorted_msgs:
        content = _resolve_text(msg.content, var_map)
        if not content:
            continue

        # Map role: "lead" -> "human", "ai" -> "ai"
        msg_type = "human" if msg.role == "lead" else "ai"

        # Build source value: if not specified, use "lead_reply" for human, "AI" for ai
        source = msg.source or ("lead_reply" if msg.role == "lead" else "AI")

        message_obj = {
            "type": msg_type,
            "content": content,
            "additional_kwargs": {
                "source": source,
                "channel": msg.channel,
            },
            "response_metadata": {},
        }

        batch.append({
            "session_id": contact_id,
            "message": message_obj,
            "timestamp": now - timedelta(hours=msg.hours_ago),
            "lead_id": entity_id,
        })

    if batch:
        await postgres.insert_messages_batch(chat_table, batch)
        logger.info(
            "seed_messages: inserted %d messages for contact %s (spanning %.0fh)",
            len(batch), contact_id,
            max(m.hours_ago for m in messages),
        )


async def _setup_lead(
    entity_id: str,
    contact_id: str,
    lead: LeadConfig,
    qualification_status: str = "",
    qualification_notes: dict[str, Any] | None = None,
) -> None:
    """Create a lead record in Supabase."""
    lead_data: dict[str, Any] = {
        "entity_id": entity_id,
        "ghl_contact_id": contact_id,
        "contact_name": f"{lead.first_name} {lead.last_name}".strip(),
        "contact_phone": lead.phone,
        "source": lead.source,
    }
    if qualification_status:
        lead_data["qualification_status"] = qualification_status
    if qualification_notes:
        lead_data["qualification_notes"] = qualification_notes

    try:
        await supabase._request(
            supabase.main_client,
            "POST",
            "/leads",
            json=lead_data,
            headers={"Prefer": "return=representation"},
            label="setup_lead",
        )
        logger.info("Created lead in leads with qualification_status=%s",
                     qualification_status or "undetermined")
    except Exception as e:
        logger.warning("setup_lead failed: %s", e)


def _setup_contact_data(
    mock_config: MockGHLConfig,
    tags: list[str],
    custom_fields: dict[str, Any],
) -> None:
    """Apply tags and custom fields to mock contact."""
    if tags:
        existing = mock_config.contact.get("tags", [])
        mock_config.contact["tags"] = existing + tags
        logger.info("Added tags %s to mock contact", tags)

    if custom_fields:
        if "customFields" not in mock_config.contact:
            mock_config.contact["customFields"] = []
        for key, value in custom_fields.items():
            mock_config.contact["customFields"].append({
                "key": key,
                "field_value": value if isinstance(value, str) else json.dumps(value),
            })
        logger.info("Added %d custom fields to mock contact", len(custom_fields))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run_single_test(
    scenario: TestScenario,
    entity_config: dict[str, Any],
    entity_id: str,
    tenant_keys_override: dict[str, str] | None = None,
    on_message: Callable[[list[dict[str, Any]]], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    """Run a single test scenario (possibly multi-turn) and return structured result.

    Args:
        tenant_keys_override: If provided, use these tenant API keys instead of the
            system testing key. Dict with keys: openrouter, google, openai, anthropic,
            deepseek, xai. Used by the simulator so partners pay their own API costs.
    """

    contact_id = str(uuid4())
    start = time.perf_counter()
    conversation: list[dict[str, Any]] = []
    issues: list[str] = []
    result: dict[str, Any] = {}
    test_token_usage = TokenUsage()

    lead = scenario.lead
    ctx_config = scenario.context
    channel = scenario.channel
    agent_type = scenario.agent_type

    # Build variable map for template resolution
    var_map = _build_variable_map(lead, entity_config)

    # Build fake contact
    fake_contact = {
        "id": contact_id,
        "firstName": lead.first_name,
        "lastName": lead.last_name,
        "email": lead.email,
        "phone": lead.phone,
        "tags": ["ai test mode"],
        "source": lead.source,
    }

    # Build mock GHL
    mock_config = MockGHLConfig(
        contact=fake_contact,
        timezone=entity_config.get("timezone", "America/Chicago"),
        free_slots=_generate_free_slots(entity_config.get("timezone", "America/Chicago")),
    )
    mock_ghl = MockGHLClient(mock_config)

    # ------------------------------------------------------------------
    # Context setup (declarative)
    # ------------------------------------------------------------------
    try:
        # Tags + custom fields
        if ctx_config.tags or ctx_config.custom_fields:
            _setup_contact_data(mock_config, ctx_config.tags, ctx_config.custom_fields)

        # Appointments
        if ctx_config.appointments:
            await _setup_appointments(
                mock_config, entity_config, entity_id,
                contact_id, ctx_config.appointments,
            )

        # Call logs
        if ctx_config.call_logs:
            await _setup_call_logs(entity_id, contact_id, ctx_config.call_logs)

        # Lead record — no Supabase write needed. Pipeline's _get_or_create_lead()
        # builds a mock lead in test mode, picking up qualification_status/notes from ctx.

        # Drip messages
        if ctx_config.drip_messages:
            drip_entries = await _seed_drip_messages(
                entity_config, entity_id, contact_id,
                ctx_config.drip_messages, lead, var_map,
            )
            for entry in drip_entries:
                conversation.append(entry)

        # Seed messages — inject into in-memory conversation array (no DB writes)
        # The pipeline reads ctx.timeline built from this array, not the chat DB
        if ctx_config.seed_messages:
            now = datetime.now(timezone.utc)
            sorted_seeds = sorted(ctx_config.seed_messages, key=lambda m: m.hours_ago, reverse=True)
            for msg in sorted_seeds:
                content = _resolve_text(msg.content, var_map)
                if not content:
                    continue
                role = "human" if msg.role == "lead" else "ai"
                source = msg.source or ("lead_reply" if msg.role == "lead" else "outreach_drip")
                channel = msg.channel or scenario.channel or "SMS"
                conversation.insert(0, {
                    "role": "seed",
                    "seed_role": role,
                    "seed_content": content,
                    "seed_source": source,
                    "seed_channel": channel,
                    "seed_hours_ago": msg.hours_ago,
                })

    except Exception as e:
        issues.append(f"Context setup failed: {e}")
        logger.error("Context setup failed for %s: %s", scenario.id, e, exc_info=True)

    # ------------------------------------------------------------------
    # Conversation turns
    # ------------------------------------------------------------------
    prev_qual_status = "undetermined"
    prev_qual_notes: dict | None = None

    for turn_num, turn in enumerate(scenario.conversation, 1):
        # Ghost turn: lead went silent. Skip reply pipeline entirely
        # and drive the followup pipeline directly so the scenario
        # reflects what actually happens in production when a lead
        # never replies to Scott's opener.
        if turn.no_response:
            conversation.append({
                "role": "turn",
                "turn_number": turn_num,
                "lead_message": "",
                "ai_response": None,
                "path": "followup",
                "tools_used": [],
                "issues": [],
                "no_response": True,
            })
            # Fall through to the followup loop below; no reply side effects.
            if turn.followups > 0:
                per_turn_cadence = (
                    _resolve_setter_cadence(entity_config, agent_type, turn.followups)
                    if tenant_keys_override is not None
                    else _resolve_cadence(turn.followup_cadence, turn.followups)
                )
                chat_table = entity_config.get("chat_history_table_name", "")
                cumulative_hours = 0.0
                for fu_num in range(1, turn.followups + 1):
                    interval = per_turn_cadence[fu_num - 1]
                    cumulative_hours += interval
                    _backdate_mock_appointments(mock_config, interval)
                    if tenant_keys_override is None:
                        await _backdate_chat_history(chat_table, contact_id, interval)

                    fu_ctx, fu_result = await _run_followup(
                        entity_config, entity_id, contact_id,
                        agent_type, channel, mock_ghl,
                        token_tracker=test_token_usage,
                        determination_only=scenario.determination_only,
                        media_only=scenario.media_only,
                        text_generator_only=scenario.text_generator_only,
                        mock_media=scenario.mock_media,
                        tenant_keys_override=tenant_keys_override,
                        conversation_for_timeline=conversation,
                        cumulative_backdate_hours=cumulative_hours,
                    )
                    fu_entry = _build_followup_result(fu_result, fu_num, after_turn=turn_num, ctx=fu_ctx)
                    conversation.append(fu_entry)
            continue

        # Dynamic lead generation
        if turn.intent and not turn.message:
            _tko = tenant_keys_override  # shorthand
            set_ai_context(
                api_key=(_tko or {}).get("openrouter") or settings.openrouter_testing_key,
                token_tracker=test_token_usage,
                **({"google_key": _tko["google"], "anthropic_key": _tko["anthropic"],
                    "openai_key": _tko["openai"], "deepseek_key": _tko["deepseek"],
                    "xai_key": _tko["xai"]} if _tko else {}),
            )
            try:
                # Extract business context for lead message generation
                _biz_name = entity_config.get("name", "")
                _sys_cfg = entity_config.get("system_config") or {}
                if isinstance(_sys_cfg, str):
                    import json as _jj
                    _sys_cfg = _jj.loads(_sys_cfg)
                _svc_cfg = _sys_cfg.get("service_config") or {}
                _svc_list = _svc_cfg.get("services") or []
                _svc_text = ", ".join(s.get("name", "") for s in _svc_list if s.get("name")) if _svc_list else ""

                generated = await _generate_dynamic_lead_message(
                    intent=turn.intent,
                    test_description=scenario.description,
                    conversation_so_far=conversation,
                    contact_name=lead.first_name,
                    business_name=_biz_name,
                    services_text=_svc_text,
                )
                lead_message = generated
                lead_generated = True
            finally:
                clear_ai_context()
        else:
            lead_message = _resolve_text(turn.message, var_map) if turn.message else ""
            lead_generated = False

        # Build fresh context for this turn
        ctx = _build_context(
            entity_config=entity_config,
            entity_id=entity_id,
            contact_id=contact_id,
            agent_type=agent_type,
            channel=channel,
            message=lead_message,
            mock_ghl=mock_ghl,
            attachment_urls=turn.attachments or None,
            lead_config=lead,
        )

        # Inject previous turn's qualification state
        ctx.qualification_status = prev_qual_status
        ctx.qualification_notes = prev_qual_notes

        # Test mode: build timeline from in-memory conversation array
        # so the pipeline doesn't need the chat history table.
        # Applies to BOTH simulator mode and batch-mode E2E scenarios
        # (seeded via TestContext.seed_messages and turn.message).
        ctx.timeline, ctx.chat_history = _build_test_timeline(
            conversation, entity_config, lead_message, channel,
        )

        # Set token tracker and AI context
        _tko = tenant_keys_override
        ctx.openrouter_api_key = (_tko or {}).get("openrouter") or settings.openrouter_testing_key
        ctx.token_usage = test_token_usage
        set_ai_context(
            api_key=ctx.openrouter_api_key,
            token_tracker=ctx.token_usage,
            **({"google_key": _tko["google"], "anthropic_key": _tko["anthropic"],
                "openai_key": _tko["openai"], "deepseek_key": _tko["deepseek"],
                "xai_key": _tko["xai"]} if _tko else {}),
        )

        # Run pipeline with full workflow tracking (decisions, LLM calls, system config)
        try:
            t0 = time.perf_counter()
            result = await run_pipeline_with_tracking(ctx)
            elapsed = time.perf_counter() - t0
        except Exception as e:
            logger.error("Pipeline error on %s turn %d: %s", scenario.id, turn_num, e, exc_info=True)
            issues.append(f"Turn {turn_num} pipeline error: {e}")
            conversation.append({
                "role": "turn",
                "turn_number": turn_num,
                "lead_message": lead_message,
                "ai_response": None,
                "path": "error",
                "error": str(e),
                "tools_used": [],
                "issues": [str(e)],
            })
            break
        finally:
            clear_ai_context()

        # Extract result data
        turn_data = _build_turn_result(ctx, result, turn_num, lead_message, turn, lead_generated, elapsed)
        conversation.append(turn_data)

        # Notify caller of progress (for incremental UI updates)
        if on_message:
            try:
                await on_message(conversation)
            except Exception:
                pass  # Don't let callback errors break the test

        # Carry qualification state forward
        if ctx.qualification_status and ctx.qualification_status != "undetermined":
            prev_qual_status = ctx.qualification_status
            prev_qual_notes = ctx.qualification_notes

        # Check expected path
        if turn.expect_path:
            actual_path = turn_data["path"]
            if not _paths_match(turn.expect_path, actual_path):
                issue = f"Turn {turn_num}: expected path '{turn.expect_path}', got '{actual_path}'"
                issues.append(issue)
                turn_data["issues"].append(issue)

        # If special path, conversation ends
        normalized = _PATH_CANONICAL.get(turn_data["path"].lower(), turn_data["path"].lower())
        if normalized not in ("reply", ""):
            break

        # Per-turn follow-ups
        if turn.followups > 0:
            # Resolve cadence: simulator reads from setter config, batch test uses scenario/default
            if tenant_keys_override is not None:
                per_turn_cadence = _resolve_setter_cadence(entity_config, agent_type, turn.followups)
            else:
                per_turn_cadence = _resolve_cadence(turn.followup_cadence, turn.followups)
            chat_table = entity_config.get("chat_history_table_name", "")
            cumulative_hours = 0.0
            for fu_num in range(1, turn.followups + 1):
                interval = per_turn_cadence[fu_num - 1]
                cumulative_hours += interval
                _backdate_mock_appointments(mock_config, interval)

                if tenant_keys_override is not None:
                    # Simulator: skip chat table backdate, build timeline from conversation array
                    _inject_smart_followup_timer(mock_config, result, cumulative_hours)
                else:
                    await _backdate_chat_history(chat_table, contact_id, interval)
                    _inject_smart_followup_timer(mock_config, result, cumulative_hours)

                fu_ctx, fu_result = await _run_followup(
                    entity_config, entity_id, contact_id,
                    agent_type, channel, mock_ghl,
                    token_tracker=test_token_usage,
                    determination_only=scenario.determination_only,
                    media_only=scenario.media_only,
                    text_generator_only=scenario.text_generator_only,
                    mock_media=scenario.mock_media,
                    tenant_keys_override=tenant_keys_override,
                    conversation_for_timeline=conversation,
                    cumulative_backdate_hours=cumulative_hours,
                )
                fu_entry = _build_followup_result(fu_result, fu_num, after_turn=turn_num, ctx=fu_ctx)
                conversation.append(fu_entry)

                if on_message:
                    try:
                        await on_message(conversation)
                    except Exception:
                        pass

    # ------------------------------------------------------------------
    # Build final result
    # ------------------------------------------------------------------
    duration = time.perf_counter() - start

    # Summarize GHL mock calls
    ghl_summary: dict[str, int] = {}
    for call in mock_ghl.calls_log:
        method = call.get("method", "unknown")
        ghl_summary[method] = ghl_summary.get(method, 0) + 1

    return {
        "test_id": scenario.id,
        "category": scenario.category,
        "description": scenario.description,
        "status": "failed" if issues else "passed",
        "channel": channel,
        "agent_type": agent_type,
        "turns": sum(1 for c in conversation if c.get("role") == "turn"),
        "duration_seconds": round(duration, 1),
        "issues": issues,
        "conversation": conversation,
        "contact_id": contact_id,
        "lead": lead.model_dump(),
        "test_context": ctx_config.model_dump(),
        "ghl_calls_summary": ghl_summary,
        "token_usage": test_token_usage.summary(),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PATH_CANONICAL: dict[str, str] = {
    "respond": "reply",
    "reply": "reply",
    "do_not_respond": "no_reply",
    "dont_respond": "no_reply",
    "dnr": "no_reply",
    "no reply needed": "no_reply",
    "opt_out": "opt_out",
    "stop": "opt_out",
    "opt out": "opt_out",
    "human": "human",
    "transfer": "human",
    "transfer to human": "human",
    "stop_bot": "stop_bot",
}


def _paths_match(expected: str, actual: str) -> bool:
    """Compare paths with normalization for all known forms."""
    e = _PATH_CANONICAL.get(expected.lower(), expected.lower())
    a = _PATH_CANONICAL.get(actual.lower(), actual.lower())
    return e == a


def _build_context(
    *,
    entity_config: dict[str, Any],
    entity_id: str,
    contact_id: str,
    agent_type: str,
    channel: str,
    message: str,
    mock_ghl: MockGHLClient,
    attachment_urls: list[str] | None = None,
    lead_config: LeadConfig | None = None,
) -> PipelineContext:
    """Create a PipelineContext pre-loaded with mock GHL."""
    lc = lead_config or LeadConfig()

    ctx = PipelineContext(
        entity_id=entity_id,
        contact_id=contact_id,
        trigger_type="reply",
        channel=channel,
        agent_type=agent_type,
        message=message,
        raw_payload=message,
        contact_phone=lc.phone,
        contact_email=lc.email,
        contact_name=f"{lc.first_name} {lc.last_name}".strip(),
        config=entity_config,
        slug=entity_config.get("slug", "test"),
        ghl_api_key="mock-key",
        ghl_location_id="mock-location",
        is_test_mode=True,
    )

    ctx.ghl = mock_ghl

    # Inject test attachments into mock conversation_messages
    if attachment_urls:
        mock_ghl.config.conversation_messages = [{
            "id": f"test-att-{uuid4().hex[:8]}",
            "messageType": "TYPE_SMS",
            "direction": "inbound",
            "body": message,
            "dateAdded": datetime.now(timezone.utc).isoformat(),
            "attachments": attachment_urls,
            "source": "direct_test",
            "contactId": contact_id,
        }]

    return ctx


async def _run_followup(
    entity_config: dict[str, Any],
    entity_id: str,
    contact_id: str,
    agent_type: str,
    channel: str,
    mock_ghl: MockGHLClient,
    token_tracker: TokenUsage | None = None,
    determination_only: bool = False,
    media_only: bool = False,
    text_generator_only: bool = False,
    mock_media: dict[str, Any] | None = None,
    tenant_keys_override: dict[str, str] | None = None,
    conversation_for_timeline: list[dict[str, Any]] | None = None,
    cumulative_backdate_hours: float = 0,
) -> tuple[PipelineContext, dict[str, Any]]:
    """Run the follow-up pipeline and return (ctx, result)."""
    _tko = tenant_keys_override

    ctx = PipelineContext(
        entity_id=entity_id,
        contact_id=contact_id,
        trigger_type="followup",
        channel=channel,
        agent_type=agent_type,
        message="",
        config=entity_config,
        slug=entity_config.get("slug", "test"),
        ghl_api_key="mock-key",
        ghl_location_id="mock-location",
        openrouter_api_key=(_tko or {}).get("openrouter") or settings.openrouter_testing_key,
        determination_only=determination_only,
        media_only=media_only,
        text_generator_only=text_generator_only,
        mock_media=mock_media,
        is_test_mode=True,
    )
    ctx.ghl = mock_ghl
    if token_tracker:
        ctx.token_usage = token_tracker

    scheduled_message = _extract_scheduled_message_from_contact(mock_ghl.config)
    if scheduled_message:
        ctx.scheduled_message = scheduled_message

    # Simulator: inject pre-built timeline for follow-up context
    if conversation_for_timeline is not None:
        ctx.timeline, ctx.chat_history = _build_test_timeline(
            conversation_for_timeline, entity_config, "", channel,
            backdate_hours=cumulative_backdate_hours, is_followup=True,
        )

    set_ai_context(
        api_key=ctx.openrouter_api_key,
        token_tracker=ctx.token_usage,
        **({"google_key": _tko["google"], "anthropic_key": _tko["anthropic"],
            "openai_key": _tko["openai"], "deepseek_key": _tko["deepseek"],
            "xai_key": _tko["xai"]} if _tko else {}),
    )
    try:
        result = await run_pipeline_with_tracking(ctx)
        return ctx, result
    except Exception as e:
        logger.warning("Follow-up pipeline error: %s", e, exc_info=True)
        return ctx, {"error": str(e), "followUpNeeded": False, "reason": f"Pipeline error: {e}"}
    finally:
        clear_ai_context()


def _build_turn_result(
    ctx: PipelineContext,
    result: dict[str, Any],
    turn_num: int,
    lead_message: str,
    turn: Turn,
    lead_generated: bool,
    elapsed: float,
) -> dict[str, Any]:
    """Extract structured turn data from pipeline result + context."""

    is_reply = "Message_1" in result or "Message1" in result
    path = result.get("path", "reply" if is_reply else "unknown")

    # Collect AI messages
    ai_messages = []
    for i in range(1, 6):
        for key in (f"Message_{i}", f"Message{i}"):
            msg = result.get(key)
            if msg:
                ai_messages.append(msg)
                break

    ai_text = " | ".join(ai_messages) if ai_messages else None

    tools_used = [t.get("name", "") for t in (ctx.tool_calls_log or [])]
    tool_calls_detail = list(ctx.tool_calls_log or [])
    post_proc = result.get("_post_processing", {})

    return {
        "role": "turn",
        "turn_number": turn_num,
        "lead_message": lead_message,
        "lead_intent": turn.intent or None,
        "lead_generated": lead_generated,
        "ai_response": ai_text,
        "path": path,
        "tools_used": tools_used,
        "tool_calls_log": tool_calls_detail,
        "classification": {
            "path": ctx.response_path,
            "reason": ctx.response_reason,
            "gates": ctx.classification_gates,
        },
        "post_processing": post_proc,
        "duration_seconds": round(elapsed, 1),
        "issues": [],
        # Rich context data
        "timeline": ctx.timeline,
        "upcoming_booking_text": ctx.upcoming_booking_text,
        "past_booking_text": ctx.past_booking_text,
        "attachments": ctx.attachments,
        "kb_results": getattr(ctx, "kb_results", []),
        "extraction": post_proc.get("extraction", {}),
        "qualification": {
            "status": ctx.qualification_status,
            "notes": ctx.qualification_notes,
            "form_interest": ctx.form_interest,
        },
        "followup_scheduling": post_proc.get("followup", {}),
        "ghl_calls": list(ctx.ghl.calls_log) if hasattr(ctx.ghl, "calls_log") else [],
        # Prompt log (lightweight label + variables only)
        "prompt_log": list(ctx.prompt_log) if ctx.prompt_log else [],
        # Workflow run ID (for brain icon inspector)
        "workflow_run_id": getattr(ctx, "workflow_run_id", None),
        # Reply media
        "media_url": ctx.selected_media_url,
        "media_type": ctx.selected_media_type,
        "media_name": ctx.selected_media_name,
        "media_description": ctx.selected_media_description,
        # Security
        "security": {
            "original_response": ctx.security_original_response or None,
            "replacements_applied": ctx.security_replacements_applied,
            "check_result": ctx.security_check_result or None,
        } if (ctx.security_original_response or ctx.security_check_result) else None,
    }


def _build_followup_result(
    result: dict[str, Any],
    fu_num: int,
    after_turn: int | None = None,
    ctx: PipelineContext | None = None,
) -> dict[str, Any]:
    """Build a follow-up entry from followup_pipeline result."""
    entry: dict[str, Any] = {
        "role": "followup",
        "followup_number": fu_num,
        "after_turn": after_turn,
        "followup_needed": result.get("followUpNeeded", False),
        "followup_message": result.get("suggestedFollowUp"),
        "path": result.get("path"),
        "reason": (result.get("reason") or "").strip() or "No reason provided",
        "workflow_run_id": getattr(ctx, "workflow_run_id", None) if ctx else None,
        "media_url": result.get("media_url"),
        "media_type": result.get("media_type"),
        "media_name": result.get("media_name"),
        "media_description": result.get("media_description"),
        "filtered_media_count": result.get("filtered_media_count", 0),
        "followup_mode": result.get("followup_mode", "ai_generated"),
    }
    if result.get("static_template_position"):
        entry["static_template_position"] = result["static_template_position"]
    if result.get("reschedule_timeframe"):
        entry["reschedule_timeframe"] = result["reschedule_timeframe"]
        entry["reschedule_reason"] = result.get("reschedule_reason", "")
    if result.get("determination_decision"):
        entry["determination_decision"] = result["determination_decision"]
    if result.get("path") == "media_only":
        entry["media_name"] = result.get("media_name", "")
        entry["media_description"] = result.get("media_description", "")
        entry["media_selection_index"] = result.get("media_selection_index", 0)
        entry["filtered_media_count"] = result.get("filtered_media_count", 0)
        entry["needs_text"] = result.get("needs_text", True)
    if ctx is not None:
        entry["timeline"] = ctx.timeline
        entry["upcoming_booking_text"] = ctx.upcoming_booking_text
        entry["past_booking_text"] = ctx.past_booking_text
        entry["prompt_log"] = list(ctx.prompt_log) if ctx.prompt_log else []
    return entry


async def _backdate_chat_history(
    chat_table: str, contact_id: str, hours_back: float,
) -> None:
    """Shift all chat history timestamps for a contact backward by N hours."""
    if not chat_table or hours_back <= 0:
        return
    async with postgres.chat_pool.acquire() as conn:
        tag = await conn.execute(
            f'UPDATE "{chat_table}" '
            f"SET timestamp = timestamp - interval '{hours_back} hours' "
            f"WHERE session_id = $1",
            contact_id,
        )
        logger.info("Backdated chat history by %.1fh for %s: %s", hours_back, contact_id, tag)


def _backdate_mock_appointments(
    mock_config: MockGHLConfig,
    hours_back: float,
) -> None:
    """Shift in-memory mock appointment datetimes backward by N hours."""
    if hours_back <= 0:
        return

    for appt in mock_config.appointments:
        for key in ("startTime", "endTime"):
            value = appt.get(key)
            if not value:
                continue
            try:
                dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except ValueError:
                continue
            appt[key] = (dt - timedelta(hours=hours_back)).isoformat()


def _extract_scheduled_message_from_contact(
    mock_config: MockGHLConfig,
) -> dict[str, Any] | None:
    """Rehydrate test-mode smart follow-up timer metadata into scheduled_message shape."""
    custom_fields = mock_config.contact.get("customFields", []) or []
    timer_json: str | None = None
    for cf in custom_fields:
        key = cf.get("key") or cf.get("id")
        if key == "smartfollowup_timer":
            timer_json = cf.get("field_value") or cf.get("value")
            break

    if not timer_json:
        return None

    try:
        metadata = json.loads(timer_json)
    except Exception:
        logger.warning("Invalid smartfollowup_timer JSON on mock contact")
        return None

    return {
        "source": "smart_reschedule",
        "smart_reason": metadata.get("reason", ""),
        "metadata": metadata,
    }


def _inject_smart_followup_timer(
    mock_config: MockGHLConfig,
    reply_result: dict[str, Any],
    hours_back: float,
) -> None:
    """Extract smart follow-up timer from reply result and inject into mock contact."""
    followup_data = reply_result.get("_post_processing", {}).get("followup", {})
    action = followup_data.get("action", "")

    if action != "scheduled":
        return

    due = followup_data.get("due", "")
    if due:
        try:
            due_dt = datetime.fromisoformat(str(due).replace("Z", "+00:00"))
            due = (due_dt - timedelta(hours=hours_back)).isoformat()
        except ValueError:
            pass

    timer_json = json.dumps({
        "timeframe": followup_data.get("timeframe", ""),
        "reason": followup_data.get("reason", ""),
        "due": due,
        "scheduled_at": (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat(),
    })

    contact = mock_config.contact
    if "customFields" not in contact:
        contact["customFields"] = []

    contact["customFields"] = [
        cf for cf in contact["customFields"]
        if cf.get("key") != "smartfollowup_timer"
    ]
    contact["customFields"].append({
        "key": "smartfollowup_timer",
        "field_value": timer_json,
    })

    tags = contact.get("tags", [])
    if "smartfollowup" not in [t.lower() for t in tags]:
        tags.append("smartfollowup")
        contact["tags"] = tags

    logger.info(
        "Injected smart followup timer: %s (backdated %.1fh)",
        followup_data.get("timeframe"), hours_back,
    )


def _generate_free_slots(tz_name: str = "America/Chicago") -> dict[str, Any]:
    """Generate realistic free slots for the next 7 days."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    start_date = now.date() + timedelta(days=1)

    slots: dict[str, list[str]] = {}
    for day_offset in range(7):
        d = start_date + timedelta(days=day_offset)
        if d.weekday() >= 5:
            continue
        day_slots = []
        for hour in range(9, 17):
            slot_dt = datetime(d.year, d.month, d.day, hour, 0, tzinfo=tz)
            day_slots.append(slot_dt.isoformat())
        slots[d.isoformat()] = day_slots

    return slots
