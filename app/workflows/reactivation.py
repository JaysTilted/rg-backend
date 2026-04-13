"""Auto-Reactivation Workflow — AI-personalized re-engagement message.

Generates a single high-quality SMS for leads dormant 45+ days.
Called by GHL Workflow 3 (Reactivate), returns resolved message in HTTP response.
Follow-up system handles subsequent messages with media.

Architecture:
  Phase 1: Data Loading (parallel)
  Phase 2: Context Building (deterministic, config-driven from setter)
  Phase 2.5: Qualification Gate (LLM, config-driven qual_block/qual_allow)
  Phase 3: AI Generation (single Sonnet call)
    - Banned pattern enforcement with retry
    - Sensitivity pre-scan with lead-specific warnings
    - Config-driven message_content/message_angle/constraints
  Phase 4: Security (3 layers)
  Phase 5: Create lead + Finalize

No PipelineContext — standalone workflow like outreach_resolver.py.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from prefect import flow

from app.config import settings
from app.models import ReactivateBody, TokenUsage
from app.services.ai_client import (
    classify,
    chat,
    set_ai_context,
    clear_ai_context,
)
from app.services.ghl_client import GHLClient
from app.services.postgres_client import postgres


class _MinimalModelCtx:
    """Minimal context for resolve_model/resolve_temperature in non-pipeline workflows."""
    def __init__(self, config: dict):
        self.config = config or {}
        sys_config = config.get("system_config") or {}
        setters = sys_config.get("setters") or {}
        setter = {}
        for _k, v in setters.items():
            if v.get("is_default"):
                setter = v
                break
        if not setter and setters:
            setter = next(iter(setters.values()))
        self.compiled = {
            "ai_models": setter.get("ai_models") or {},
            "ai_temperatures": setter.get("ai_temperatures") or {},
        }
from app.services.mattermost import post_message as post_slack_message
from app.services.supabase_client import supabase
from app.services.workflow_tracker import WorkflowTracker
from app.text_engine.booking import _parse_dt
from app.text_engine.offers import get_active_offers, render_offers_text
from app.text_engine.qualification import _format_criteria_for_qual_agent
from app.text_engine.security import (
    apply_term_replacements_standalone,
    strip_punctuation_dashes,
)
from app.text_engine.timeline import format_timeline
from app.text_engine.utils import get_timezone, parse_datetime
from app.text_engine.model_resolver import resolve_model, resolve_temperature

logger = logging.getLogger(__name__)

# Always P1-only — follow-up system handles subsequent messages with media.
# The 6-position drip was removed as part of the setter architecture simplification.


# ============================================================================
# PHASE 1: DATA LOADING
# ============================================================================


async def _load_all_data(
    ghl: GHLClient,
    config: dict[str, Any],
    contact_id: str,
    entity_id: str,
) -> dict[str, Any]:
    """Load all data sources in parallel. Returns a dict of loaded data."""
    chat_table = config.get("chat_history_table_name", "")
    if not chat_table:
        # Derive from company name (same convention as data_loading.py)
        slug = config.get("name", entity_id[:8])
        chat_table = slug.lower().replace(" ", "_").replace("-", "_")

    # Parallel fetch: GHL contact, chat history, lead record, appointments,
    # call logs, attachments — mirrors text engine's data loading
    ghl_contact_task = ghl.get_contact(contact_id)
    chat_task = postgres.get_chat_history(chat_table, contact_id, limit=50)
    lead_task = supabase.get_lead(contact_id, entity_id)
    appts_task = ghl.get_appointments(contact_id)
    call_logs_task = supabase.get_call_logs(contact_id, entity_id, limit=10)
    attachments_task = postgres.get_attachments(contact_id, entity_id)

    ghl_contact, chat_history, lead, appointments, call_logs, attachments = (
        await asyncio.gather(
            ghl_contact_task, chat_task, lead_task, appts_task,
            call_logs_task, attachments_task,
            return_exceptions=True,
        )
    )

    # Handle failures gracefully — any single source failing shouldn't kill the workflow
    data_loading_errors = []
    if isinstance(ghl_contact, Exception):
        logger.warning("REACTIVATE | ghl_contact load failed: %s", ghl_contact)
        data_loading_errors.append(f"ghl_contact: {ghl_contact}")
        ghl_contact = {}
    if isinstance(chat_history, Exception):
        logger.warning("REACTIVATE | chat_history load failed: %s", chat_history)
        data_loading_errors.append(f"chat_history: {chat_history}")
        chat_history = []
    if isinstance(lead, Exception):
        logger.warning("REACTIVATE | lead load failed: %s", lead)
        data_loading_errors.append(f"lead: {lead}")
        lead = None
    if isinstance(appointments, Exception):
        logger.warning("REACTIVATE | appointments load failed: %s", appointments)
        data_loading_errors.append(f"appointments: {appointments}")
        appointments = []
    if isinstance(call_logs, Exception):
        logger.warning("REACTIVATE | call_logs load failed: %s", call_logs)
        data_loading_errors.append(f"call_logs: {call_logs}")
        call_logs = []
    if isinstance(attachments, Exception):
        logger.warning("REACTIVATE | attachments load failed: %s", attachments)
        data_loading_errors.append(f"attachments: {attachments}")
        attachments = []

    return {
        "ghl_contact": ghl_contact,
        "chat_history": chat_history,
        "lead": lead,
        "appointments": appointments,
        "call_logs": call_logs,
        "attachments": attachments,
        "data_loading_errors": data_loading_errors,
    }


# ============================================================================
# PHASE 2: CONTEXT BUILDING
# ============================================================================


def _clean_chat_history(chat_history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Strip metadata markers from stored chat messages before formatting.

    The text engine's format_timeline doesn't know about these markers,
    so we clean them before passing data through.
    """
    cleaned = []
    for entry in chat_history:
        content = entry.get("content") or ""
        if content.startswith("## USER LAST UTTERANCE"):
            entry = dict(entry)
            entry["content"] = content.replace("## USER LAST UTTERANCE", "").strip()
        cleaned.append(entry)
    return cleaned


def _build_timeline(
    chat_history: list[dict[str, Any]],
    call_logs: list[dict[str, Any]],
    attachments: list[dict[str, Any]],
    tz: ZoneInfo,
) -> str:
    """Build unified timeline using the text engine's format_timeline.

    Same format the reply/follow-up agents see: messages + call logs +
    attachments merged chronologically, most-recent-first, with grouping.
    Uses is_followup=True to suppress [CURRENT MESSAGE] marker (no new
    inbound message to respond to), then strips the follow-up attempt
    header since this is reactivation, not a follow-up.
    """
    # Clean metadata markers before formatting
    cleaned = _clean_chat_history(chat_history)

    timeline_text, _stats = format_timeline(
        cleaned, call_logs, attachments, tz, is_followup=True,
    )

    # Strip the "FOLLOW-UP ATTEMPT" header — not relevant for reactivation
    lines = timeline_text.split("\n")
    filtered = []
    skip = False
    for line in lines:
        if line.startswith("## FOLLOW-UP ATTEMPT"):
            skip = True
            continue
        if skip and (line.startswith("*Follow-up context:") or line.startswith("This is the ")):
            continue
        skip = False
        filtered.append(line)
    return "\n".join(filtered)


def _clean_appointment_title(raw_title: str) -> str:
    """Extract meaningful appointment info from GHL's verbose title format.

    GHL titles look like:
    "Roselle Hernandez (702) 712-9696 | Dr. K Beauty in LV | RF Microneedling | Phone Consultation"

    We want just the treatment and type parts (after the business name), or
    fall back to the full title if it doesn't match the pattern.
    """
    if "|" not in raw_title:
        return raw_title.strip()

    parts = [p.strip() for p in raw_title.split("|")]
    # Typical GHL pattern: [Name+Phone, Business, Treatment, Type]
    # We want everything after the business name (parts[2:])
    if len(parts) >= 3:
        # Skip the first two parts (name+phone, business name)
        meaningful = [p for p in parts[2:] if p]
        if meaningful:
            return " - ".join(meaningful)
    # Fallback: return the full title
    return raw_title.strip()


def _format_booking_history(
    appointments: list[dict[str, Any]],
    tz: ZoneInfo,
) -> str:
    """Format ALL-TIME appointment history matching text engine's booking format.

    Same style as booking.py's _format_past_booking_text but without the
    30-day filter. Parses raw GHL events into clean title + date + status.

    Confirmed = attended. Cancelled = did not attend.
    """
    if not appointments:
        return "No appointment history."

    parsed: list[dict[str, Any]] = []
    for event in appointments:
        start_str = event.get("startTime", "")
        status = (event.get("appointmentStatus") or "").lower()
        raw_title = event.get("title", "Appointment")
        title = _clean_appointment_title(raw_title)

        start_dt = _parse_dt(start_str, tz)
        if not start_dt:
            continue

        parsed.append({"title": title, "start": start_dt, "status": status})

    if not parsed:
        return "No appointment history."

    # Sort most recent first (matches text engine)
    parsed.sort(key=lambda a: a["start"], reverse=True)

    lines: list[str] = ["Appointment History:"]
    for appt in parsed:
        local = appt["start"].astimezone(tz)
        date_str = local.strftime("%B %d, %Y at %I:%M %p")
        status = appt["status"].capitalize()
        lines.append(f"- {date_str} - {appt['title']} ({status})")

    return "\n".join(lines)


def _detect_lead_language(chat_history: list[dict[str, Any]]) -> str:
    """Detect the lead's language from their messages. Returns language name."""
    _SPANISH_WORDS = {
        "hola", "sí", "si", "gracias", "por", "favor", "quiero", "puedo",
        "necesito", "dónde", "cuánto", "cómo", "bueno", "bien", "disculpe",
        "okey", "cuándo", "también", "pero", "porque", "habla", "español",
        "cita", "clínica", "interesa", "quisieras", "agendar", "precio",
        "cobrar", "dónde", "verdad", "temprano", "salgo", "preguntado",
    }
    for entry in chat_history:
        if entry.get("role") != "human":
            continue
        content = (entry.get("content") or "").lower().strip()
        if not content:
            continue
        # Spanish-specific characters
        if any(c in content for c in "ñáéíóúü¿¡"):
            return "Spanish"
        # Common Spanish words
        words = set(content.replace(".", " ").replace(",", " ").split())
        if len(words & _SPANISH_WORDS) >= 2:
            return "Spanish"
    return "English"


def _build_context(
    config: dict[str, Any],
    data: dict[str, Any],
    body: ReactivateBody,
) -> dict[str, str]:
    """Build the shared context dict used by all AI generation prompts."""
    lead = data.get("lead") or {}
    ghl_contact = data.get("ghl_contact") or {}

    # Name resolution: body > GHL contact > lead
    contact_name = (body.name or "").strip()
    if not contact_name:
        contact_name = (
            ghl_contact.get("name")
            or ghl_contact.get("firstName", "")
            or ""
        ).strip()
    first_name = contact_name.split()[0] if contact_name else ""

    # Service interest from lead record
    service_interest = lead.get("form_interest", "") or ""

    # Business identity
    business_name = config.get("name") or "our office"

    # Timezone — matches text engine
    tz = get_timezone(config)

    # Format components — using text engine formatters
    timeline_text = _build_timeline(
        data.get("chat_history", []),
        data.get("call_logs", []),
        data.get("attachments", []),
        tz,
    )
    booking_text = _format_booking_history(data.get("appointments", []), tz)

    # Resolve setter from GHL contact's custom field
    system_config = config.get("system_config") or {}
    from app.text_engine.agent_compiler import resolve_setter
    custom_fields = ghl_contact.get("customFields") or []
    setter_key = ""
    for cf in custom_fields:
        if cf.get("id") == "agent_type_new" or cf.get("key") == "agent_type_new":
            setter_key = cf.get("value", "")
            break
    setter = resolve_setter(system_config, setter_key) or system_config

    # Services + offers from setter
    from app.text_engine.services_compiler import compile_all_offers, compile_services_list
    services_text = compile_services_list(setter.get("services"), names_only=False) or ""
    offers_text = compile_all_offers(setter.get("services")) or ""

    # Bot persona from setter
    from app.text_engine.bot_persona_compiler import compile_bot_persona
    bot_persona = compile_bot_persona(
        setter.get("bot_persona"),
        company=config.get("name", ""),
        section_filter="followup",
    ) or ""

    # Security / compliance from setter
    from app.text_engine.security_compiler import compile_compliance_rules
    security_prompt = compile_compliance_rules(setter.get("security")) or ""

    # Supported languages from setter
    supported_languages = setter.get("supported_languages") or config.get("supported_languages") or ["English"]
    if isinstance(supported_languages, str):
        try:
            supported_languages = json.loads(supported_languages)
        except (json.JSONDecodeError, TypeError):
            supported_languages = ["English"]
    languages_text = ", ".join(supported_languages) if supported_languages else "English"

    # Detect actual lead language from conversation
    detected_language = _detect_lead_language(data.get("chat_history", []))

    # Qualification context from lead record
    qualification_status = lead.get("qualification_status", "") or ""
    qualification_notes = lead.get("qualification_notes")  # JSONB dict or None

    # Format qualification criteria from setter's services config
    qualification_criteria = ""
    _svc_config = setter.get("services")
    if _svc_config and isinstance(_svc_config, dict):
        qualification_criteria = _format_criteria_for_qual_agent(
            _svc_config, service_interest
        )

    # Reactivation config from setter (for prompt injection)
    reactivation_config = setter.get("auto_reactivation") or {}

    return {
        "contact_name": contact_name,
        "first_name": first_name,
        "service_interest": service_interest,
        "business_name": business_name,
        "timeline": timeline_text,
        "booking_history": booking_text,
        "services": services_text,
        "offers": offers_text,
        "bot_persona": bot_persona,
        "security_prompt": security_prompt,
        "supported_languages": languages_text,
        "detected_language": detected_language,
        "channel": body.ReplyChannel,
        "qualification_status": qualification_status,
        "qualification_notes": qualification_notes,
        "qualification_criteria": qualification_criteria,
        "setter_key": setter_key,
        "reactivation_config": reactivation_config,
    }


# ============================================================================
# PHASE 2.5: QUALIFICATION GATE
# ============================================================================


_QUAL_GATE_SYSTEM = """\
You are a lead reactivation qualifier. Your job is to decide whether a dormant \
lead should receive a re-engagement drip sequence.

DEFAULT TO YES. You should reactivate the lead UNLESS there is a clear, hard \
reason not to. People change their minds. Circumstances change. The bar to \
BLOCK reactivation is very high.

BLOCK reactivation ONLY for these hard stops:
- Lead explicitly said "don't contact me", "stop", "stop texting me", \
"leave me alone", or was hostile/threatening
- Wrong number, spam, or bot (conversation makes no sense, random gibberish)
- Lead is clearly not a real person or is a competitor fishing for info
- Lead already completed a one-time, non-repeatable service AND the business \
has no other services they might be interested in (check the services list)

DO NOT block reactivation for:
- Short conversations or one-word replies
- Lead who ghosted / stopped replying (most common — always reactivate)
- "Not interested right now" or "maybe later" (circumstances change)
- Price objections ("too expensive")
- Lead who cancelled or no-showed an appointment
- Lead who already booked or completed an appointment (they can book again, \
or try other services — this is a GOOD reactivation candidate)
- Lead who was mid-conversation about scheduling (they may still want to book)
- No conversation history at all (new-ish lead who never engaged)
- Any ambiguous situation (when in doubt, reactivate)

IMPORTANT: Having booked or completed an appointment is NOT a reason to skip \
IF the service is repeatable or the business has other services to offer. \
SKIP if the lead already completed a one-time service (e.g., wedding photography, \
home purchase) AND the business has no other services they could use. There is \
nothing to reactivate them for — the messages would have no relevant offer.

## QUALIFICATION STATUS GUIDANCE

If the lead has a previous qualification status and notes, use them to inform \
your decision — but DO NOT hard-block on "disqualified" alone. Check the \
qualification notes to understand WHY they were disqualified, then judge whether \
that reason is temporary or permanent.

REACTIVATE if the disqualification reason is temporary or circumstantial:
- Budget or pricing concerns (financial situations change)
- Scheduling or timing conflicts (availability changes)
- "Not ready yet" or "thinking about it" (may be ready now)
- Needed to check with someone else (spouse, insurance, landlord)
- Seasonal or weather-dependent reasons (season may have changed)

SKIP if the disqualification reason is permanent or won't change within a reactivation cycle:
- Lives outside service area with no indication of moving
- Needs a service the business does not offer at all
- Is not the target customer type (e.g., commercial vs residential)
- Has a condition that permanently prevents the service
- Does not meet an age requirement and won't meet it for a long time (e.g., \
lead is 16, must be 18 — that's years away, not weeks)

The qualification notes contain what the lead actually said and why each \
criterion was marked confirmed, disqualified, or undetermined. Use this \
evidence to make your judgment. When in doubt, REACTIVATE.
"""

_QUAL_GATE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "reactivate": {
            "type": "boolean",
            "description": "True to proceed with reactivation, False to skip",
        },
        "reason": {
            "type": "string",
            "description": "Brief explanation of the decision",
        },
    },
    "required": ["reactivate", "reason"],
}


def _compile_qual_config(reactivation_config: dict[str, Any]) -> str:
    """Compile qual_block, qual_allow, and generous_stance into extra qual gate instructions."""
    parts: list[str] = []

    # Generous stance
    generous = reactivation_config.get("generous_stance") or {}
    if isinstance(generous, dict) and generous.get("enabled"):
        prompt = (generous.get("prompt") or "").strip()
        if prompt:
            parts.append(f"## Additional Guidance\n{prompt}")

    # Qual block rules
    qual_block = reactivation_config.get("qual_block") or {}
    block_prompts: list[str] = []
    for key, section in qual_block.items():
        if isinstance(section, dict) and section.get("enabled"):
            prompt = (section.get("prompt") or "").strip()
            if prompt:
                block_prompts.append(f"- {prompt}")
    if block_prompts:
        parts.append("## Additional BLOCK Criteria\n" + "\n".join(block_prompts))

    # Qual allow rules
    qual_allow = reactivation_config.get("qual_allow") or {}
    allow_prompts: list[str] = []
    for key, section in qual_allow.items():
        if isinstance(section, dict) and section.get("enabled"):
            prompt = (section.get("prompt") or "").strip()
            if prompt:
                allow_prompts.append(f"- {prompt}")
    if allow_prompts:
        parts.append("## Additional ALLOW Criteria (DO NOT block for these)\n" + "\n".join(allow_prompts))

    return "\n\n".join(parts)


def _compile_generation_config(reactivation_config: dict[str, Any]) -> str:
    """Compile message_content, message_angle, constraints, and custom_sections into prompt text."""
    parts: list[str] = []

    # Message content rules
    msg_content = reactivation_config.get("message_content") or {}
    content_prompts: list[str] = []
    for key, section in msg_content.items():
        if isinstance(section, dict) and section.get("enabled"):
            prompt = (section.get("prompt") or "").strip()
            if prompt:
                content_prompts.append(f"- {prompt}")
    if content_prompts:
        parts.append(
            "<message_guidelines>\n"
            "## Message Content Guidelines\n"
            + "\n".join(content_prompts)
            + "\n</message_guidelines>"
        )

    # Message angle
    msg_angle = reactivation_config.get("message_angle") or {}
    angle_prompts: list[str] = []
    for key, section in msg_angle.items():
        if isinstance(section, dict) and section.get("enabled"):
            prompt = (section.get("prompt") or "").strip()
            if prompt:
                angle_prompts.append(f"- {prompt}")
    if angle_prompts:
        parts.append(
            "<approach_style>\n"
            "## Approach Style\n"
            + "\n".join(angle_prompts)
            + "\n</approach_style>"
        )

    # Constraints
    constraints = reactivation_config.get("constraints") or {}
    constraint_prompts: list[str] = []
    for key, section in constraints.items():
        if isinstance(section, dict) and section.get("enabled"):
            prompt = (section.get("prompt") or "").strip()
            if prompt:
                constraint_prompts.append(f"- {prompt}")
    if constraint_prompts:
        parts.append(
            "<constraints>\n"
            "## Constraints\n"
            + "\n".join(constraint_prompts)
            + "\n</constraints>"
        )

    # Custom sections
    custom_sections = reactivation_config.get("custom_sections") or []
    custom_prompts: list[str] = []
    for section in custom_sections:
        if isinstance(section, dict) and section.get("enabled"):
            prompt = (section.get("prompt") or "").strip()
            label = (section.get("label") or "").strip()
            if prompt:
                custom_prompts.append(f"### {label}\n{prompt}" if label else prompt)
    if custom_prompts:
        parts.append(
            "<additional_instructions>\n"
            "## Additional Instructions\n"
            + "\n\n".join(custom_prompts)
            + "\n</additional_instructions>"
        )

    return "\n\n".join(parts)


async def _qualification_gate(context: dict[str, str], config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run the qualification gate. Returns {reactivate: bool, reason: str}."""
    services_section = ""
    if context.get("services"):
        services_section = f"\n## Available Services\n{context['services']}\n"

    offers_section = ""
    if context.get("offers"):
        offers_section = f"\n## Current Offers\n{context['offers']}\n"

    # Qualification context — only include if there's actual data
    qual_section = ""
    qual_status = context.get("qualification_status", "")
    qual_notes = context.get("qualification_notes")
    qual_criteria = context.get("qualification_criteria", "")

    if qual_status or qual_notes:
        qual_parts = ["\n## Previous Qualification"]
        if qual_status:
            qual_parts.append(f"Status: {qual_status}")
        if qual_notes and isinstance(qual_notes, dict):
            # Format the notes JSON into readable text
            notes_lines = []
            for criterion_name, criterion_data in qual_notes.items():
                status = criterion_data.get("status", "undetermined")
                evidence = criterion_data.get("evidence", "")
                notes_lines.append(f"- {criterion_name}: {status}" + (f" — {evidence}" if evidence else ""))
            if notes_lines:
                qual_parts.append("Notes:")
                qual_parts.extend(notes_lines)
        qual_section = "\n".join(qual_parts) + "\n"

    criteria_section = ""
    if qual_criteria:
        criteria_section = f"\n## Qualification Criteria\n{qual_criteria}\n"

    prompt = f"""## Lead Details
- Name: {context['contact_name'] or 'Unknown'}
- Service Interest: {context['service_interest'] or 'Unknown'}
- Channel: {context['channel']}

## Appointment History
{context['booking_history']}

{context['timeline']}
{services_section}{offers_section}{qual_section}{criteria_section}
Should this lead be reactivated? Remember: default to YES unless there is a \
hard stop reason listed in your instructions."""

    # Build qual system prompt with config-driven additions
    reactivation_config = context.get("reactivation_config") or {}
    qual_system = _QUAL_GATE_SYSTEM
    qual_config_text = _compile_qual_config(reactivation_config)
    if qual_config_text:
        qual_system += "\n\n" + qual_config_text

    try:
        result = await classify(
            prompt=prompt,
            schema=_QUAL_GATE_SCHEMA,
            system_prompt=qual_system,
            model=resolve_model(_MinimalModelCtx(config or {}), "reactivation_qual"),
            temperature=resolve_temperature(_MinimalModelCtx(config or {}), "reactivation_qual"),
            label="reactivation_qual",
        )
        logger.info(
            "REACTIVATE | qual_gate | reactivate=%s | reason=%s",
            result.get("reactivate"), result.get("reason"),
        )
        return result
    except Exception as e:
        # On failure, default to reactivating (generous stance)
        logger.warning("REACTIVATE | qual_gate FAILED, defaulting to reactivate: %s", e)
        return {"reactivate": True, "reason": f"Qualification gate failed ({e}), defaulting to reactivate"}


# ============================================================================
# PHASE 3: AI GENERATION
# ============================================================================


# ── Shared prompt section constants ──

_ROLE_SMS = """\
## YOUR ROLE

You are a real person who works at this business. You're texting someone you talked \
to over a month ago. You scrolled back through their texts, you remember what they \
said, and you have a genuine reason to reach out today.

Your job is to sound like a person, not a system. Every message should feel like \
you actually read the conversation and are responding to THIS specific person, not \
filling in a template with their name.

GOLDEN RULE: Never fabricate, never lie, never make things up. If the lead didn't \
say something, don't claim they did. If a bundle doesn't exist, don't invent one. \
If a price isn't listed, don't guess. There is always a truthful angle — use it.

SMS style: never use dashes (em dashes, en dashes, or hyphens used as dashes), \
never use bullet points, never use markdown formatting, never use emojis."""

_VOICE = """\
## YOUR VOICE

You text like someone who works at this business — front desk, coordinator, \
assistant, whoever talks to clients. Casual but professional. You're not writing \
marketing copy, you're texting someone you talked to before.

KEY PRINCIPLE: Reference something SPECIFIC from the conversation. What did the \
LEAD actually say? What were they worried about? What day did they say works? \
What question did they ask? Pull a real detail and build your message around it. \
That's what makes it feel personal instead of automated.

BAD (generic, could be sent to anyone):
"Our services are a great option for you."

GOOD (specific, clearly read the conversation):
"You mentioned your roof took a hit in the storm last month. Did insurance \
end up covering the repair?"

BAD (product catalog):
"We have teeth whitening available for $450."

GOOD (conversational):
"The whitening you asked about is $450 and honestly the results have been \
pretty impressive lately."

Your messages should be 1-3 sentences. Not too short (one flat statement), not too \
long (a paragraph). Think of how you'd actually text a client you remember. \
Use proper punctuation and grammar — match the style of the Bot Persona section \
if one is provided."""

_P1_STANDALONE_FRAMEWORK = """\
## YOUR TASK: STANDALONE RE-ENGAGEMENT MESSAGE

Write ONE high-quality re-engagement message. This is the ONLY message you're writing — \
after this, the follow-up system handles subsequent messages automatically with media.

Your message should naturally combine two things:
1. A personal callback — reference something the LEAD actually said or asked about
2. A value hook — mention a relevant service, offer, or price that gives them a reason \
to reply NOW

This is NOT two separate sentences stapled together. It should read as one natural thought \
that flows from "I remember you" to "here's why now is a good time."

HOW TO USE OFFERS:
- If Current Offers has something relevant to this lead's interest, weave it in naturally. \
Don't announce it like a sale — mention it like you just remembered it.
- If no offers are relevant, mention the service price casually instead.
- If neither fits, just ask a compelling question about their situation.

GOOD EXAMPLES (diverse industries):
- "You were asking about the whitening for the coffee stains — we actually just dropped \
the take-home kit to $149 if you wanted to try something more gradual than the in-office \
stuff. Would that feel like a better starting point?" (dental)
- "Did your insurance adjuster ever make it out about those shingles? We have a free \
inspection running right now if you want a second opinion on the damage." (roofing)
- "New year is here and you mentioned that was your timeline for getting started. The \
private training setup is exactly how we do it, just you and your trainer with no gym \
crowd. Want to get something on the calendar before the schedule fills up?" (fitness)
- "Your AC went out during that heat wave back in August — did you end up getting it \
fixed? We're running a free diagnostic right now with any repair over $500, so that fee \
you asked about would be covered." (HVAC)

BAD EXAMPLES (do NOT write like this):
- "We have a special offer on teeth whitening." (generic, no conversation reference)
- "Are you still interested in our services?" (could be sent to anyone)
- "Hi Maria! We miss you at Dr. K Beauty!" (corporate, no substance)
- "You mentioned the whitening. Also, we have a promotion." (two disconnected sentences)

IF NO OFFERS OR PRICING exist: focus entirely on the personal callback with a question. \
Do not invent offers, prices, or discounts.

CONSTRAINTS:
- 1-3 sentences (substantial but not a paragraph)
- End with a question that invites a reply (about THEM, not about booking)
- Don't start with their name
- Never start with "I was", "I remember", "I noticed", "I saw", "Checking in", \
"Looking back" — just say the thing
- The offer/price should feel like a natural aside, not the main pitch
- The conversation reference is the main reason you're texting"""



_GLOBAL_RULES = """\
## SEQUENCE RULES

These apply to every message you generate:

REJECTIONS: If the lead said "no" to a specific service in the conversation (e.g., \
"no Botox", "not interested in X"), that service must NOT appear in your message or \
any other message in the sequence. Find an alternative from the services list.

PRICING: When a service has both a regular price and a promotional/intro price in \
Current Offers, always use the promotional price.

ADAPTATION: If this position's goal doesn't fit this lead's situation (no price listed, \
no secondary service, etc.), adapt naturally. A simple honest message beats forcing a \
bad fit.

NO REPEAT PHRASES: Never use the same phrase or framing across multiple messages. \
Banned filler that makes messages sound like a drip sequence: "still available", \
"when you're ready", "whenever you're ready", "if you're interested", "let me know". \
Each message should feel like a standalone text, not part of an automated series.

SELF-CHECK: Before finalizing, ask yourself — would this sound weird if someone sent \
it to me after a month of silence? If yes, rewrite it.\""""

_BANNED_PATTERNS = """\
## BANNED PATTERNS

These patterns signal "automated sequence" instead of a real human reaching out. \
Using ANY of these will get your message thrown out.

CRUTCH OPENERS — Never start a message with any of these:
- "I was looking back..." / "I was looking at..." / "Looking back at..."
- "I was checking the schedule..." / "I was just checking..."
- "I remember you were..." / "I remember you..." / "I remembered..."
- "I was thinking about..." / "I was just thinking..."
- "I saw you had..." / "I saw you were..." / "I noticed you..."
- "Checking in on..." / "Checking in since..."
- "I wanted to see if..." / "I wanted to check..."

These all scream "CRM automation." A real person doesn't narrate how they came to text \
you. They just say the thing.

OTHER BANNED PATTERNS:
- "still available" / "still an option" — sounds like a CRM reminder, not a person.
- "when you're ready" / "whenever you're ready" — passive filler.
- "still thinking about [anything]?" — Next-day follow-up language, not 45-day language.
- Starting with "Just" — "Just checking in", "Just wanted to" — minimizes your outreach.
- "if you're still interested" / "if you're still considering" — references stale intent.
- "I hope you're doing well" / "Hope all is well" — generic filler, wastes the first line.
- "no pressure at all" / "no pressure" — creates the pressure it disclaims.
- "reaching out one more time" / "one last time" — telegraphs a sequence.
- "let me know if you change your mind" — passive and defeatist.
- "let me know" — lazy closer, gives the lead nothing to respond to.

SENSITIVITY:
- Never remind a lead that something was "too expensive" or reference their budget constraints. \
If they couldn't afford something, suggest alternatives without pointing out why.
- Never ask about a family member's health or treatments. Keep messages about the lead.
- Weight management and body image topics require extra sensitivity. Do not frame negatively \
("dealing with that issue", "struggling with"). Keep it neutral and respectful.
- Never imply a lead should be unhappy with their appearance or results. Questions like \
"are you still happy with how you look?" come across as passive-aggressive.\""""

_ANTI_HALLUCINATION = """\
## CONTEXT ACCURACY RULES

HOW TO READ THE CONVERSATION HISTORY:
- "LEAD:" = words the lead actually typed. This is the ONLY source of truth for what \
the lead said, asked about, or expressed interest in.
- "AI:", "DRIP:", "FOLLOW_UP:" = words WE sent. These are NOT things the lead said. If you \
attribute something from an AI, DRIP, or FOLLOW_UP message to the lead, that is fabrication.
- "Service Interest" field = the ad campaign they clicked. It is NOT something they said. \
Use the lead's OWN words from LEAD messages when referencing their interest.

APPOINTMENTS AND CONSULTATIONS:
- Check the Appointment History section for the definitive record of what was booked.
- "Confirmed" = the lead attended. You CAN reference this as something that happened.
- "Cancelled" = the lead did NOT attend. Do not say they came in or completed it.
- If the AI OFFERED a consultation but no appointment appears in Appointment History, \
it was never booked. Do not reference it as something that occurred.
- Call logs in the timeline are additional evidence — if a call log confirms something \
happened, you can reference it.

PRICING:
- Use prices from the Available Services and Current Offers sections — these are current
- When a service has BOTH a regular price and a promotional/intro price, always use the \
promotional price
- If a service has no listed price, do not mention any price
- Never fabricate or guess pricing
- Never combine services into bundles unless that bundle exists in the services list
- If a promotional price was mentioned in the conversation history AND it also appears in \
the services/offers list, it is valid to use. If a conversation price contradicts the \
current list, use the list price.

SERVICES:
- The service you suggest must address the same need the lead expressed. Don't suggest \
something for a different problem just because it's available or on sale.
- For health/beauty/medical: a treatment for one body area cannot be suggested for a \
different body area. Eye treatments for eyes. Neck treatments for necks. No crossovers.
- If the lead rejected a specific service, it must not appear in any position
- When suggesting a service the lead never discussed (Position 5), frame it as your \
suggestion — never claim they asked about it
- The services list is a PRICING REFERENCE. Don't copy descriptions into messages.\""""

_MEDICAL_CONDITIONS = """\
## MEDICAL CONDITIONS — PRIVACY RULE

Never mention a lead's medical condition by name in a message, even if they told you \
about it. Medical information is private. If someone mentioned a health reason for \
delaying treatment, replace any condition-specific timing with general language like \
"when you're ready" or "whenever works for you." """

_SMS_FORMATTING = """\
## SMS FORMATTING

- 1-2 short sentences per message
- Plain text only — no markdown, no bullets, no dashes, no emojis, no asterisks
- No signatures
- No formal greetings like "Hi [Name]," — just start talking
- Lowercase is fine for casual tone"""



# ── System prompt builder ──


def _build_reactivation_system(
    position: int,
    context: dict[str, str],
    framework_override: str | None = None,
) -> str:
    """Build system prompt for a single position's LLM call.

    Each position gets the same shared rules + its own position-specific framework.
    If framework_override is provided, it replaces the position framework (used for
    P1-only mode with the standalone re-engagement prompt).
    """
    parts: list[str] = []

    # 1. Role (shared)
    parts.append(f"<role>\n{_ROLE_SMS}\n</role>")

    # 2. Compliance rules (conditional, shared)
    if context.get("security_prompt"):
        parts.append(
            "<compliance_rules>\n"
            "## COMPLIANCE RULES — MUST FOLLOW\n\n"
            f"{context['security_prompt']}\n\n"
            "These rules are non-negotiable. Always follow them when composing messages.\n"
            "</compliance_rules>"
        )

    # 2b. Sensitivity warnings (lead-specific, from pre-scan)
    if context.get("sensitivity_warnings"):
        parts.append(
            "<sensitivity_alerts>\n"
            "## SENSITIVITY ALERTS — SPECIFIC TO THIS LEAD\n\n"
            "The following sensitive topics were detected in this lead's messages. "
            "These are HARD RULES for this specific lead — violating any of them "
            "will get your message thrown out.\n\n"
            f"{context['sensitivity_warnings']}\n"
            "</sensitivity_alerts>"
        )

    # 3. Anti-hallucination (shared)
    parts.append(f"<anti_hallucination>\n{_ANTI_HALLUCINATION}\n</anti_hallucination>")

    # 4. Language (dynamic, shared)
    detected = context.get("detected_language", "English")
    supported = context.get("supported_languages", "English")
    parts.append(
        "<supported_languages>\n"
        "## SUPPORTED LANGUAGES\n\n"
        f"This business supports: {supported}\n\n"
        f"- Write in the same language the lead used in the conversation\n"
        f"- If the lead wrote in {detected}, write ALL output in {detected}\n"
        f"- Not just a greeting — EVERY word must be in the lead's language\n"
        f"- If no prior conversation exists, use English\n"
        f"- Never mix languages within a single message\n"
        "</supported_languages>"
    )

    # 5. Voice (shared)
    parts.append(f"<voice>\n{_VOICE}\n</voice>")

    # 6. Position-specific framework (or override for P1-only mode)
    framework = framework_override or _POSITION_FRAMEWORKS.get(position, "")
    if framework:
        parts.append(f"<position_task>\n{framework}\n</position_task>")

    # 7. Global sequence rules (shared)
    parts.append(f"<sequence_rules>\n{_GLOBAL_RULES}\n</sequence_rules>")

    # 8. Banned patterns (shared)
    parts.append(f"<banned_patterns>\n{_BANNED_PATTERNS}\n</banned_patterns>")

    # 9. Medical conditions (shared)
    parts.append(f"<medical_conditions>\n{_MEDICAL_CONDITIONS}\n</medical_conditions>")

    # 10. SMS formatting (shared)
    parts.append(f"<formatting>\n{_SMS_FORMATTING}\n</formatting>")

    # 11. Services reference (conditional, shared)
    if context.get("services"):
        parts.append(
            "<services_reference>\n"
            "## Available Services — PRICING REFERENCE ONLY\n\n"
            "Use this list ONLY for accurate pricing. Do NOT copy service "
            "descriptions into messages.\n\n"
            f"{context['services']}\n"
            "</services_reference>"
        )

    # 12. Current offers (conditional, shared)
    if context.get("offers"):
        parts.append(
            "<current_offers>\n"
            "## Current Offers & Promotions\n\n"
            f"{context['offers']}\n"
            "</current_offers>"
        )

    # 13. Bot persona (conditional, shared)
    if context.get("bot_persona"):
        parts.append(
            "<bot_persona>\n"
            "## Your Persona\n\n"
            "Write in the style described below. This is how you text.\n\n"
            f"{context['bot_persona']}\n"
            "</bot_persona>"
        )

    # 14. Config-driven sections (message guidelines, approach, constraints, custom)
    reactivation_config = context.get("reactivation_config") or {}
    generation_config_text = _compile_generation_config(reactivation_config)
    if generation_config_text:
        parts.append(generation_config_text)

    # 15. Output format (per position)
    output = (
        "## OUTPUT FORMAT\n\n"
        f'Return valid JSON: {{"sms_{position}": "your message text"}}'
    )
    parts.append(f"<output_format>\n{output}\n</output_format>")

    return "\n\n".join(parts)


# ── User prompt builder ──


def _build_reactivation_user(
    position: int,
    context: dict[str, str],
    previous_messages: dict[str, str],
) -> str:
    """Build user prompt for a single position's LLM call.

    Mirrors text engine's agent.py _build_user_prompt structure:
    lead context → appointment status → timeline → cascading context.
    """
    parts: list[str] = []

    # Lead context (matches text engine's Contact Details section)
    parts.append("# LEAD CONTEXT")
    parts.append("")
    parts.append("## Contact Details")
    if context.get("first_name"):
        parts.append(f"Name: {context['first_name']}")
    if context.get("service_interest"):
        parts.append(f"Service Interest (from ad): {context['service_interest']}")
    parts.append(f"Business: {context['business_name']}")

    # Appointment history (matches text engine's Appointment Status section)
    parts.append("")
    parts.append("## Appointment History")
    parts.append(context["booking_history"])
    parts.append(
        f"\nThis lead has been dormant for {context.get('days_dormant', '45+')} days. "
        "All appointments above are IN THE PAST. Confirmed = they attended. "
        "Cancelled = they did not attend."
    )

    # Timeline (matches text engine's full timeline output)
    parts.append("")
    parts.append(context["timeline"])

    # Cascading context (P2+ only)
    if position > 1 and previous_messages:
        parts.append(
            "\nIMPORTANT: The messages below are ones WE generated and sent. They are NOT "
            "things the lead said. Do NOT treat topics from these messages as things the "
            "lead brought up. Your ONLY source of truth for what the lead cares about is "
            "the LEAD messages in the conversation history above."
        )
        parts.append("\n## Previously Sent Messages (no reply received)")
        for i in range(1, position):
            key = f"sms_{i}"
            if key in previous_messages:
                parts.append(f"Position {i}: \"{previous_messages[key]}\"")

        # Name usage tracking
        first_name = context.get("first_name", "")
        if first_name:
            name_count = sum(
                1 for i in range(1, position)
                if first_name.lower() in previous_messages.get(f"sms_{i}", "").lower()
            )
            parts.append(
                f"\nName usage so far: {name_count} of {position - 1} previous messages "
                f"used the lead's name. Target: 2-3 out of 6 total."
            )

        parts.append(
            "\nYOUR MESSAGE MUST BE STRUCTURALLY DIFFERENT from every message above. "
            "Check each previous message — if yours starts similarly, references "
            "the same topic the same way, or asks a similar question, rewrite it. "
            "The lead will receive all of these over multiple days. If two messages "
            "feel like the same text rephrased, the lead will stop reading."
        )

    # Generation instruction
    parts.append(
        f"\n---\n\nGenerate ONE SMS message for Position {position}. "
        "Follow the position framework in your instructions."
    )

    return "\n".join(parts)


# ── Response format schema builder ──


def _sms_response_format(position: int) -> dict[str, Any]:
    """Build structured output schema for a single position."""
    key = f"sms_{position}"
    return {
        "type": "json_schema",
        "json_schema": {
            "name": f"sms_position_{position}",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    key: {"type": "string"},
                },
                "required": [key],
            },
        },
    }



def _strip_banned_opener(text: str) -> str:
    """Last-resort: remove a banned opener prefix from a message.

    Tries to find the first sentence boundary after the banned part
    and returns the rest. If the whole message is bad, returns as-is.
    """
    stripped = text.strip()
    for regex, _desc in _BANNED_OPENER_REGEXES:
        m = regex.search(stripped)
        if m:
            # Try to find the end of the offending clause (comma, period, or dash)
            rest = stripped[m.end():]
            # Look for a natural break
            for sep in [". ", ", ", " — ", " - "]:
                idx = rest.find(sep)
                if idx != -1 and idx < 40:
                    candidate = rest[idx + len(sep):].strip()
                    if len(candidate) > 20:
                        # Capitalize first letter
                        return candidate[0].upper() + candidate[1:]
            break
    return stripped


async def _generate_message(context: dict[str, str], config: dict[str, Any] | None = None) -> tuple[dict[str, str], dict[str, Any]]:
    """Generate a single high-quality re-engagement message using Sonnet.

    Always P1-only — the follow-up system handles subsequent messages
    with media support (GIFs, voice notes, images).

    Returns (messages_dict, generation_metadata).
    """
    gen_meta: dict[str, Any] = {}

    # Pre-scan sensitivity
    sensitivity_warnings = scan_sensitivity(context.get("timeline", ""))
    gen_meta["sensitivity_warnings"] = sensitivity_warnings
    pos_context = dict(context)
    if sensitivity_warnings:
        warning_block = "\n".join(f"- {w}" for w in sensitivity_warnings)
        pos_context["sensitivity_warnings"] = warning_block

    # Build prompts using standalone framework
    system = _build_reactivation_system(
        1, pos_context, framework_override=_P1_STANDALONE_FRAMEWORK,
    )
    user = _build_reactivation_user(1, pos_context, {})

    resp = await chat(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        model=resolve_model(_MinimalModelCtx(config or {}), "reactivation_p1_only"),
        temperature=resolve_temperature(_MinimalModelCtx(config or {}), "reactivation_p1_only"),
        response_format=_sms_response_format(1),
        label="reactivation_sms",
    )
    data = json.loads(resp.choices[0].message.content)
    sms_text = data.get("sms_1", "")

    # Banned pattern check with retry
    violations = check_banned_patterns(sms_text)
    gen_meta["banned_violations"] = violations
    gen_meta["retry_needed"] = False
    gen_meta["opener_stripped"] = False
    if violations:
        gen_meta["retry_needed"] = True
        logger.warning(
            "REACTIVATE | BANNED_PATTERN | violations=%s | retrying", violations,
        )
        violation_list = ", ".join(violations)
        retry_user = (
            f"{user}\n\n"
            f"YOUR PREVIOUS ATTEMPT WAS REJECTED because it contained banned patterns: "
            f"{violation_list}. Write a completely new message that avoids these patterns."
        )
        resp = await chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": retry_user},
            ],
            model=resolve_model(_MinimalModelCtx(config or {}), "reactivation_p1_only"),
            temperature=resolve_temperature(_MinimalModelCtx(config or {}), "reactivation_p1_only"),
            response_format=_sms_response_format(1),
            label="reactivation_sms",
        )
        data = json.loads(resp.choices[0].message.content)
        sms_text = data.get("sms_1", sms_text)

        # Last resort: strip banned opener if retry still has it
        retry_violations = check_banned_patterns(sms_text)
        gen_meta["retry_violations"] = retry_violations
        if retry_violations:
            sms_text = _strip_banned_opener(sms_text)
            gen_meta["opener_stripped"] = True

    return {"sms_1": sms_text}, gen_meta


# ============================================================================
# PHASE 3.5: DETERMINISTIC ENFORCEMENT
# ============================================================================

# Banned opener patterns — compiled regexes for fast checking.
# Each tuple: (compiled_regex, human-readable description)
_BANNED_OPENER_REGEXES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^I was (looking|checking|thinking|just)", re.IGNORECASE), "I was [looking/checking/thinking]..."),
    (re.compile(r"^Looking back", re.IGNORECASE), "Looking back..."),
    (re.compile(r"^I noticed", re.IGNORECASE), "I noticed..."),
    (re.compile(r"^I saw", re.IGNORECASE), "I saw..."),
    (re.compile(r"^Checking in", re.IGNORECASE), "Checking in..."),
    (re.compile(r"^I wanted to", re.IGNORECASE), "I wanted to..."),
    (re.compile(r"^Just\b", re.IGNORECASE), "Starting with Just..."),
    (re.compile(r"^I hope you", re.IGNORECASE), "I hope you..."),
    (re.compile(r"^Hope all is", re.IGNORECASE), "Hope all is..."),
]

# Banned anywhere patterns — these are bad regardless of position in the message
_BANNED_ANYWHERE_REGEXES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"I remember(ed)? you", re.IGNORECASE), "I remember(ed) you..."),
    (re.compile(r"I remember(ed)? when", re.IGNORECASE), "I remember(ed) when..."),
    (re.compile(r"I remember(ed)? (we|your|that)", re.IGNORECASE), "I remember(ed)..."),
    (re.compile(r"still thinking about", re.IGNORECASE), "still thinking about..."),
    (re.compile(r"if you'?re still interested", re.IGNORECASE), "if you're still interested"),
    (re.compile(r"if you'?re still considering", re.IGNORECASE), "if you're still considering"),
    (re.compile(r"no pressure", re.IGNORECASE), "no pressure"),
    (re.compile(r"reaching out one (more|last) time", re.IGNORECASE), "reaching out one more/last time"),
    (re.compile(r"one last time", re.IGNORECASE), "one last time"),
    (re.compile(r"let me know if you change your mind", re.IGNORECASE), "let me know if you change your mind"),
    (re.compile(r"still available", re.IGNORECASE), "still available"),
    (re.compile(r"still an option", re.IGNORECASE), "still an option"),
    (re.compile(r"when(ever)? you'?re ready", re.IGNORECASE), "when(ever) you're ready"),
]


def check_banned_patterns(text: str) -> list[str]:
    """Check a message for banned patterns. Returns list of violation descriptions."""
    violations: list[str] = []
    stripped = text.strip()
    for regex, desc in _BANNED_OPENER_REGEXES:
        if regex.search(stripped):
            violations.append(f"opener: {desc}")
    for regex, desc in _BANNED_ANYWHERE_REGEXES:
        if regex.search(stripped):
            violations.append(f"pattern: {desc}")
    return violations


# ── Sensitivity Pre-Scan ──
# Scan LEAD messages BEFORE generation. Inject specific warnings into context
# so the model knows exactly what to avoid for THIS lead.

_SENSITIVITY_PATTERNS: list[tuple[re.Pattern, str]] = [
    # Price/budget
    (re.compile(r"(too expensive|can'?t afford|too much|out of.*budget|don'?t have.*money|costly|pricey)", re.IGNORECASE),
     "PRICE SENSITIVITY: The lead mentioned cost concerns. Do NOT reference their budget, "
     "that something was too expensive, or suggest they couldn't afford it. Present pricing "
     "naturally without acknowledging their previous objection."),
    # Family health
    (re.compile(r"(my (mom|dad|mother|father|husband|wife|sister|brother|son|daughter|parent).*(sick|cancer|surgery|treatment|hospital|ill|passed|died|health))", re.IGNORECASE),
     "FAMILY HEALTH: The lead mentioned a family member's health situation. Do NOT ask about "
     "or reference this family member's health, treatments, or condition. Keep messages focused "
     "on the lead personally."),
    # Weight / body image
    (re.compile(r"(lose weight|weight loss|too fat|overweight|my weight|belly fat|love handles|body fat)", re.IGNORECASE),
     "BODY IMAGE: The lead discussed weight or body concerns. Frame any related services "
     "positively (goals, confidence, results) — never negatively (struggling, dealing with, "
     "unhappy with). Do NOT ask if they're still unhappy with their body."),
    # Bad experience
    (re.compile(r"(bad experience|didn'?t work|made it worse|hurt|painful|scarred|botched|ruined)", re.IGNORECASE),
     "BAD EXPERIENCE: The lead referenced a negative past experience. Do NOT remind them of it. "
     "Focus on what's possible going forward, not what went wrong before."),
]


def scan_sensitivity(chat_history_text: str) -> list[str]:
    """Scan LEAD messages in timeline for sensitive topics.

    Returns list of specific warning strings to inject into generation context.
    """
    # Extract only LEAD messages
    lead_lines: list[str] = []
    for line in chat_history_text.split("\n"):
        if line.strip().startswith("LEAD:"):
            lead_lines.append(line)
    lead_text = " ".join(lead_lines)

    warnings: list[str] = []
    for regex, warning in _SENSITIVITY_PATTERNS:
        if regex.search(lead_text):
            warnings.append(warning)
    return warnings


# ============================================================================
# PHASE 3b: SERVICE MATCHER
# ============================================================================


_SERVICE_MATCHER_SYSTEM = """\
You are a service matcher for a reactivation campaign. Given a lead's conversation \
history and the business's available services, determine which service(s) to mention \
in the reactivation message.

Pick the service most relevant to the lead based on:
1. Services they previously asked about or showed interest in
2. Services related to their original inquiry
3. If no clear match, pick the business's most popular/flagship service

Return a JSON object: {{"service": "service name", "reason": "brief reason for match"}}
"""


# PHASE 4: SECURITY
# ============================================================================


_BATCH_SECURITY_SYSTEM = """\
You are a compliance reviewer for outbound messages sent by a sales assistant. \
Check each message against the compliance rules and return results for ALL messages.

<compliance_rules>
{security_prompt}
</compliance_rules>

<instructions>
For EACH message:
1. Check it against every compliance rule.
2. If compliant, mark safe=true and leave rewritten empty.
3. If it violates ANY rule, mark safe=false and provide a rewritten version that:
   - Fixes ALL violations while preserving meaning and tone
   - Keeps the same casual SMS style
   - Does NOT add disclaimers, legal language, or bot-sounding text
   - Only changes what is necessary
</instructions>
"""

_BATCH_SECURITY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "position": {
                        "type": "integer",
                        "description": "Message position number (1-6)",
                    },
                    "safe": {
                        "type": "boolean",
                        "description": "True if message passes all compliance rules",
                    },
                    "rewritten": {
                        "type": "string",
                        "description": "Rewritten message if safe=false. Empty string if safe=true.",
                    },
                },
                "required": ["position", "safe", "rewritten"],
            },
        },
    },
    "required": ["results"],
}


async def _apply_security(
    messages: dict[str, str],
    config: dict[str, Any],
    security_prompt: str,
) -> dict[str, str]:
    """Apply 3-layer security to all generated messages. Mutates and returns messages."""
    # Layer 0: Deterministic dash cleanup (always)
    for key in list(messages.keys()):
        if messages[key]:
            messages[key] = strip_punctuation_dashes(messages[key])

    # Layer 1: Deterministic term replacements (if configured)
    term_replacements = config.get("term_replacements")
    if term_replacements and isinstance(term_replacements, list) and len(term_replacements) > 0:
        for key in list(messages.keys()):
            if messages[key]:
                messages[key] = apply_term_replacements_standalone(
                    messages[key], term_replacements
                )
        logger.info("REACTIVATE | security | term_replacements applied | rules=%d", len(term_replacements))

    # Layer 2: LLM compliance check (if security_prompt configured)
    if not security_prompt:
        return messages

    try:
        sms_check = await _batch_security_check(messages, security_prompt)
        messages.update(sms_check)
        logger.info("REACTIVATE | security | llm_check complete")
    except Exception as e:
        logger.warning("REACTIVATE | security | llm_check FAILED (sending originals): %s", e)
        try:
            await post_slack_message(
                channel="#python-errors",
                text=f"Reactivation security check failed: {e}",
            )
        except Exception:
            pass  # Don't let Slack failure block delivery

    return messages


async def _batch_security_check(
    messages: dict[str, str],
    security_prompt: str,
) -> dict[str, str]:
    """Check all SMS messages in one LLM call. Returns updated messages."""
    check_items: list[str] = []
    keys_by_position: dict[int, str] = {}

    for key, text in messages.items():
        if text and key.startswith("sms_"):
            pos = int(key.split("_")[1])
            check_items.append(f"Position {pos}: {text}")
            keys_by_position[pos] = key

    if not check_items:
        return {}

    system = _BATCH_SECURITY_SYSTEM.format(security_prompt=security_prompt)
    user_prompt = (
        "Check these SMS messages against the compliance rules:\n\n"
        + "\n\n".join(check_items)
    )

    result = await classify(
        prompt=user_prompt,
        schema=_BATCH_SECURITY_SCHEMA,
        system_prompt=system,
        model=resolve_model(_MinimalModelCtx(config or {}), "reactivation_security"),
        temperature=resolve_temperature(_MinimalModelCtx(config or {}), "reactivation_security"),
        label="reactivation_security_sms",
    )

    updates: dict[str, str] = {}
    for item in result.get("results", []):
        pos = item.get("position", 0)
        if not item.get("safe", True) and item.get("rewritten"):
            key = keys_by_position.get(pos)
            if key:
                updates[key] = item["rewritten"]
                logger.info("REACTIVATE | security | rewritten position %d", pos)

    return updates


# ============================================================================
# PHASE 5: FINALIZE + RESPONSE
# ============================================================================


def _build_response(
    messages: dict[str, str],
    contact_id: str,
    qual_reason: str,
) -> dict[str, Any]:
    """Build the final response dict."""
    return {
        "success": True,
        "mode": "p1_only",
        "sms_count": 1 if messages.get("sms_1") else 0,
        "sms_1": messages.get("sms_1") or None,
        "contact_id": contact_id,
        "reactivation_reason": qual_reason,
    }


# ============================================================================
# MAIN FLOW
# ============================================================================


@flow(name="reactivation", retries=0)
async def reactivate_lead(
    entity_id: str,
    body: ReactivateBody,
) -> tuple[dict[str, Any], int]:
    """Auto-Reactivation flow — generates a single personalized re-engagement message.

    Follow-up system handles subsequent messages with media.
    Returns (response_dict, http_status_code).
    """
    t0 = time.perf_counter()
    contact_id = body.id

    # Workflow tracker
    tracker = WorkflowTracker(
        "reactivation",
        entity_id=entity_id,
        ghl_contact_id=contact_id,
        trigger_source="scheduled",
    )

    if not contact_id:
        tracker.set_error("Missing contact ID")
        await tracker.save()
        return {"success": False, "error": "Missing contact ID"}, 400

    # ── Resolve entity ──
    try:
        config = await supabase.resolve_entity(entity_id)
    except ValueError:
        tracker.set_error(f"Entity {entity_id} not found")
        await tracker.save()
        return {"success": False, "error": f"Entity {entity_id} not found"}, 404

    slug = config.get("slug") or config.get("name", entity_id[:8])

    # ── Set up GHL client ──
    ghl_api_key = config.get("ghl_api_key", "")
    ghl_location_id = config.get("ghl_location_id", "")
    if not ghl_api_key or not ghl_location_id:
        logger.warning("REACTIVATE | missing GHL credentials | entity=%s", entity_id)
        tracker.set_error("Missing GHL credentials")
        await tracker.save()
        return {"success": False, "error": "Missing GHL credentials"}, 400

    ghl = GHLClient(api_key=ghl_api_key, location_id=ghl_location_id)

    # ── Set AI context with per-tenant keys ──
    from app.main import _resolve_tenant_ai_keys
    tenant_keys = await _resolve_tenant_ai_keys(entity_id)
    token_tracker = TokenUsage()
    set_ai_context(
        api_key=tenant_keys["openrouter"],
        token_tracker=token_tracker,
        google_key=tenant_keys.get("google", ""),
        openai_key=tenant_keys.get("openai", ""),
        deepseek_key=tenant_keys.get("deepseek", ""),
        xai_key=tenant_keys.get("xai", ""),
        anthropic_key=tenant_keys.get("anthropic", ""),
    )

    try:
        # ── Phase 1: Data Loading ──
        logger.info("REACTIVATE | phase1_start | entity=%s | contact=%s", slug, contact_id)
        data = await _load_all_data(ghl, config, contact_id, entity_id)
        data_loading_errors = data.get("data_loading_errors", [])

        # ── Phase 2: Context Building ──
        context = _build_context(config, data, body)
        logger.info(
            "REACTIVATE | phase2_context | name=%s | interest=%s | history=%d | appts=%d | calls=%d | attachments=%d",
            context["contact_name"], context["service_interest"],
            len(data.get("chat_history", [])), len(data.get("appointments", [])),
            len(data.get("call_logs", [])), len(data.get("attachments", [])),
        )

        # ── Phase 2.5: Qualification Gate ──
        qual_result = await _qualification_gate(context, config)
        if not qual_result.get("reactivate", True):
            elapsed = int((time.perf_counter() - t0) * 1000)
            logger.info(
                "REACTIVATE | SKIPPED | entity=%s | contact=%s | reason=%s | elapsed_ms=%d",
                slug, contact_id, qual_result.get("reason"), elapsed,
            )
            return {
                "success": True,
                "skipped": True,
                "reason": qual_result.get("reason", "Qualification gate blocked"),
                "contact_id": contact_id,
            }, 200

        # ── Always P1-only — follow-up system handles rest ──
        logger.info("REACTIVATE | mode=p1_only | entity=%s", slug)

        # ── Phase 3: AI Generation (single message) ──
        logger.info("REACTIVATE | phase3_generation_start | entity=%s", slug)
        messages, _gen_meta = await _generate_message(context, config)
        _sensitivity_warnings = _gen_meta.get("sensitivity_warnings", [])
        _banned_violations = _gen_meta.get("banned_violations", [])
        _retry_needed = _gen_meta.get("retry_needed", False)
        _opener_stripped = _gen_meta.get("opener_stripped", False)

        # ── Phase 4: Security ──
        security_prompt = context.get("security_prompt") or ""
        _pre_security_messages = dict(messages)  # snapshot before security
        messages = await _apply_security(messages, config, security_prompt)
        _security_details = {
            "messages_changed": messages != _pre_security_messages,
            "security_prompt_present": bool(security_prompt),
        }

        # ── Phase 5: Create Reactivation Lead ──
        lead = data.get("lead") or {}
        try:
            reactivation_lead = await supabase.create_lead(
                {
                    "entity_id": entity_id,
                    "ghl_contact_id": contact_id,
                    "contact_name": context["contact_name"] or None,
                    "contact_phone": body.phone or None,
                    "contact_email": body.email or None,
                    "source": "Auto Reactivation",
                    "form_interest": lead.get("form_interest") or None,
                },
            )
            logger.info(
                "REACTIVATE | lead_created | id=%s | entity=%s | contact=%s",
                reactivation_lead.get("id", ""), slug, contact_id,
            )
            if reactivation_lead.get("id"):
                tracker.set_lead_id(reactivation_lead["id"])
        except Exception as e:
            logger.warning("REACTIVATE | lead creation failed (non-fatal): %s", e)

        # ── Phase 5b: Reactivation tags (from setter config) ──
        _react_tags_added = []
        try:
            from app.text_engine.agent_compiler import resolve_setter as _rt_resolve
            _react_sk = context.get("setter_key", "")
            _react_sc = config.get("system_config") or {}
            _react_setter = _rt_resolve(_react_sc, _react_sk)
            _react_configured = (_react_setter or {}).get("tags", {}).get("reactivation")
            if isinstance(_react_configured, list):
                for _rt in _react_configured:
                    try:
                        await ghl.add_tag(contact_id, _rt)
                        _react_tags_added.append(_rt)
                    except Exception as _rt_err:
                        logger.warning("REACTIVATE | Failed to add reactivation tag '%s': %s", _rt, _rt_err)
        except Exception as _rt_outer:
            logger.warning("REACTIVATE | reactivation tag resolution failed (non-blocking): %s", _rt_outer)

        # ── Phase 6: Finalize ──
        result = _build_response(messages, contact_id, qual_result.get("reason", ""))
        result["usage"] = token_tracker.summary()

        elapsed = int((time.perf_counter() - t0) * 1000)
        logger.info(
            "REACTIVATE | DONE | entity=%s | contact=%s | sms=%d | cost=$%.4f | elapsed_ms=%d",
            slug, contact_id, result["sms_count"],
            token_tracker.estimated_cost(), elapsed,
        )

        return result, 200

    finally:
        # Workflow tracker — full decisions + runtime context
        tracker.set_token_usage(token_tracker)
        _sk = context.get("setter_key", "") if 'context' in dir() and context else None
        tracker.set_system_config(config.get("system_config") if config else None, setter_key=_sk)
        tracker.set_metadata("channel", "SMS")

        # Rich decisions — qualification gate, generation, security, delivery
        _decisions = {}
        if 'qual_result' in dir():
            _decisions["qualification_gate"] = {
                "reactivate": qual_result.get("reactivate", True),
                "reason": qual_result.get("reason", ""),
            }
        if 'messages' in dir():
            _decisions["generation"] = {
                "message_count": len(messages) if messages else 0,
                "messages": messages if messages else [],
                "sensitivity_warnings": _sensitivity_warnings if '_sensitivity_warnings' in dir() else [],
                "banned_patterns": {
                    "first_attempt_violations": _banned_violations if '_banned_violations' in dir() else [],
                    "retry_needed": _retry_needed if '_retry_needed' in dir() else False,
                    "opener_stripped": _opener_stripped if '_opener_stripped' in dir() else False,
                },
            }
            _decisions["security"] = {
                "applied": True,
                "details": _security_details if '_security_details' in dir() else None,
            }
            _decisions["delivery"] = {
                "status": "sent" if messages else "no_message",
                "channel": "SMS",
                "messages_sent": len(messages) if messages else 0,
            }
        if 'reactivation_lead' in dir() and reactivation_lead:
            _decisions["lead_created"] = {
                "id": reactivation_lead.get("id") if isinstance(reactivation_lead, dict) else None,
            }
        if '_react_tags_added' in dir():
            _decisions["reactivation_tags_added"] = _react_tags_added
        if 'data_loading_errors' in dir():
            _decisions["data_loading_errors"] = data_loading_errors
        if not qual_result.get("reactivate", True) if 'qual_result' in dir() else False:
            tracker.set_status("skipped")
            _decisions["delivery"] = {"status": "skipped_qualification", "channel": "SMS", "messages_sent": 0}
        tracker.set_decisions(_decisions)

        # Runtime context — all variables available at generation time
        if 'context' in dir() and context:
            tracker.set_runtime_context({
                "contact": {
                    "name": context.get("contact_name", ""),
                    "phone": body.phone or "",
                    "email": body.email or "",
                },
                "setter_key": context.get("setter_key", ""),
                "timeline_length": len(context.get("timeline", "")),
                "chat_history_count": len(data.get("chat_history", [])) if 'data' in dir() else 0,
                "appointments_count": len(data.get("appointments", [])) if 'data' in dir() else 0,
                "call_logs_count": len(data.get("call_logs", [])) if 'data' in dir() else 0,
                "detected_language": context.get("detected_language", "English"),
                "services_text": context.get("services", "")[:500],
                "offers_text": context.get("offers", "")[:300],
                "bot_persona": context.get("bot_persona", "")[:300],
                "qualification_status": context.get("qualification_status", ""),
                "qualification_criteria": context.get("qualification_criteria", ""),
                "qualification_notes": context.get("qualification_notes", ""),
                "service_interest": context.get("service_interest", ""),
                "booking_history": context.get("booking_history", "")[:500],
                "first_name": context.get("first_name", ""),
                "business_name": context.get("business_name", ""),
                "supported_languages": context.get("supported_languages", ["English"]),
                "reactivation_config": context.get("reactivation_config", {}),
                "generated_messages": messages if 'messages' in dir() else [],
            })

        await tracker.save()
        clear_ai_context()
