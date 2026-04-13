"""Step 3.2: Data loading — fetch GHL contact, load prompts, get/create lead.

Each function is a Prefect @task so it appears as its own step in the dashboard.
All tasks mutate the PipelineContext in place rather than returning values,
since the context is the shared state bag for the entire pipeline.
"""

from __future__ import annotations

import asyncio
import copy
import logging
from datetime import datetime, timezone
from typing import Any

from prefect import task

from app.models import PipelineContext
from app.services.supabase_client import supabase
from app.text_engine.bot_persona_compiler import compile_bot_persona
from app.text_engine.agent_compiler import (
    resolve_setter,
    compile_agent_prompt,
    compile_agent_goals_summary,
    compile_agent_capabilities,
    get_agent_lead_source,
    get_agent_urgency_scarcity,
)
from app.text_engine.booking_compiler import compile_booking_config, compile_booking_rules
from app.text_engine.transfer_compiler import compile_transfer_prompt, compile_opt_out_sections
from app.text_engine.security_compiler import (
    compile_security_protections,
    compile_compliance_rules,
    get_term_replacements,
)
from app.text_engine.case_studies_compiler import compile_case_studies, get_case_study_media
from app.text_engine.services_compiler import (
    compile_services_list,
    compile_all_offers,
    compile_offers_deployment,
)
from app.text_engine.followup_compiler import (
    compile_followup_preferences,
    compile_followup_banned_phrases,
    compile_followup_positions_for_determination,
)
from app.text_engine.utils import extract_business_hours

logger = logging.getLogger(__name__)

# Legacy prompt fields from clients table columns (kept for business_hours only)
# All agent/persona/security prompts now live in system_config.setters[key]
_PROMPT_FIELDS = [
    "bot_persona",
    "transfer_to_human_prompt",
    "security_prompt",
]


@task(name="load_data", retries=3, retry_delay_seconds=3, timeout_seconds=45)
async def load_data(ctx: PipelineContext) -> None:
    """Load all data needed before the pipeline processes the message.

    Runs in this order (some steps parallelized):
    1. Fetch GHL contact (needs api_key from config, already in ctx)
    2. Extract user details + determine test mode
    3. Load prompts (with test mode override)
    4. Get or create lead in Supabase
    5. Update lead's last_reply_datetime and last_reply_channel
    """
    # 1. Fetch GHL contact + location timezone
    # If webhook_contact_data is available (from standard webhook), skip the
    # get_contact() API call — the webhook already sent all contact data + custom fields.
    # Still fetch location timezone (lightweight, needed for scheduling).
    if ctx.webhook_contact_data:
        from app.webhooks.standard_parser import build_contact_data_from_webhook
        contact = build_contact_data_from_webhook(ctx.webhook_contact_data)
        ghl_tz = await ctx.ghl.get_location_timezone()
        logger.info("DATA_LOADING | using webhook contact data (skipped get_contact API call)")
    else:
        contact_task = ctx.ghl.get_contact(ctx.contact_id)
        tz_task = ctx.ghl.get_location_timezone()
        contact, ghl_tz = await asyncio.gather(contact_task, tz_task)

    ctx.contact = contact or {}
    if not contact:
        logger.warning("GHL contact not found: %s", ctx.contact_id)

    # Auto-detect timezone from GHL location (source of truth).
    # Overrides whatever is in Supabase config so we never rely on manual config.
    if ghl_tz:
        ctx.config["timezone"] = ghl_tz
        logger.info("Timezone auto-detected from GHL location: %s", ghl_tz)
    else:
        logger.warning(
            "Could not fetch GHL location timezone, falling back to config: %s",
            ctx.config.get("timezone", "America/Chicago"),
        )

    # 2. Extract user details
    ctx.contact_tags = contact.get("tags", []) or []
    ctx.contact_name = (
        ctx.contact_name
        or f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
    )
    ctx.contact_email = ctx.contact_email or contact.get("email", "")
    ctx.contact_phone = ctx.contact_phone or contact.get("phone", "")
    ctx.contact_source = contact.get("source", "")

    # 3. Determine test mode
    tags_lower = [t.lower() for t in ctx.contact_tags]
    ctx.is_test_mode = "ai test mode" in tags_lower

    # 4. Load prompts + media library (with test mode override)
    ctx.prompts = _build_prompts(ctx.config, ctx.is_test_mode)
    ctx.media_library = _resolve_media_library(ctx.config, ctx.is_test_mode)
    logger.info(
        "DATA_LOADING | prompts_loaded=%d | media_library=%d | test_mode=%s",
        len(ctx.prompts), len(ctx.media_library), ctx.is_test_mode,
    )

    # 5. Get or create lead
    await _get_or_create_lead(ctx)

    # 5b. Load previous qualification status from lead record so extraction
    # and the reply agent start with the correct status instead of "undetermined".
    if ctx.lead:
        prev_qual = ctx.lead.get("qualification_status") or "undetermined"
        prev_notes = ctx.lead.get("qualification_notes")
        if prev_qual != "undetermined":
            ctx.qualification_status = prev_qual
            # Handle JSONB notes (dict) or legacy text (str) or None
            if isinstance(prev_notes, dict):
                ctx.qualification_notes = prev_notes
            else:
                ctx.qualification_notes = None  # Old text format or None, ignore
            logger.info("Loaded previous qualification: %s", prev_qual)

        # Load form_interest from lead record
        ctx.form_interest = ctx.lead.get("form_interest") or ""

    # 5c. Apply JSONB config test overrides if in test mode
    # test_prompt_overrides column was dropped — overrides now come from
    # system_config.test_config_overrides (handled in _compile_system_config)
    if ctx.is_test_mode:
        test_overrides = (ctx.config.get("system_config") or {}).get("test_prompt_overrides") or {}
        if "service_config" in test_overrides:
            ctx.config["service_config"] = test_overrides["service_config"]
        if "offers_config" in test_overrides:
            ctx.config["offers_config"] = test_overrides["offers_config"]
        if "supported_languages" in test_overrides:
            ctx.config["supported_languages"] = test_overrides["supported_languages"]

    # 5d. Compile system_config if present
    system_config = ctx.config.get("system_config")
    if system_config and isinstance(system_config, dict):
        # Deep-merge test_config_overrides before compiling
        if ctx.is_test_mode:
            test_config = ctx.config.get("test_config_overrides")
            if test_config and isinstance(test_config, dict):
                system_config = deep_merge(system_config, test_config)

        _compile_system_config(ctx, system_config)
        logger.info("DATA_LOADING | system_config compiled | keys=%s", list(ctx.compiled.keys()))

    # 6. Update lead's last_reply_datetime and channel (if lead exists and this is a reply)
    if ctx.lead and ctx.trigger_type == "reply":
        await supabase.update_lead(
            ctx.lead["id"],
            {
                "last_reply_datetime": datetime.now(timezone.utc).isoformat(),
                "last_reply_channel": ctx.channel,
            },
        )

    logger.info(
        "Data loaded: test_mode=%s, has_lead=%s, contact_tags=%d",
        ctx.is_test_mode,
        ctx.lead is not None,
        len(ctx.contact_tags),
    )


def _build_prompts(config: dict[str, Any], is_test_mode: bool) -> dict[str, str]:
    """Extract prompts from client config, applying test mode overrides.

    Three-state override logic (consistent across all fields):
      null  → no override, use production value
      ""    → intentionally blank (override with empty)
      value → use this override
    """
    test_overrides = (config.get("system_config") or {}).get("test_prompt_overrides") or {}
    prompts: dict[str, str] = {}

    for field in _PROMPT_FIELDS:
        override = test_overrides.get(field)
        if is_test_mode and override is not None:
            prompts[field] = override
        else:
            prompts[field] = config.get(field) or ""

    # Business hours / days from business_schedule JSONB
    hours_start, hours_end, enabled_days = extract_business_hours(config)
    prompts["business_days"] = ", ".join(enabled_days)
    tz = config.get("timezone", "America/Chicago")
    prompts["business_hours"] = f"{hours_start} - {hours_end} ({tz})" if hours_start else ""

    return prompts


def _resolve_media_library(config: dict[str, Any], is_test_mode: bool) -> list:
    """Resolve which media library to use (test override or production).

    Three-state override logic (same as prompts):
      null  -> no override, use production media library
      ""    -> intentionally blank (no media)
      list  -> use this override library
    """
    test_overrides = (config.get("system_config") or {}).get("test_prompt_overrides") or {}
    override = test_overrides.get("media_library")

    if is_test_mode and override is not None:
        return override if isinstance(override, list) else []

    return config.get("media_library") or []


async def _get_or_create_lead(ctx: PipelineContext) -> None:
    """Get existing lead or create a new one.

    - Test mode: build a mock lead dict (no DB reads/writes)
    - Production: Get Lead (query Supabase), create if not exists
    """
    entity_id = ctx.config.get("id", ctx.entity_id)

    # Test mode: mock lead — no Supabase reads or writes
    if ctx.is_test_mode:
        from uuid import uuid4
        ctx.lead = {
            "id": str(uuid4()),
            "entity_id": entity_id,
            "ghl_contact_id": ctx.contact_id,
            "contact_name": ctx.contact_name or "Test Lead",
            "contact_email": ctx.contact_email or "",
            "contact_phone": ctx.contact_phone or "",
            "last_reply_channel": ctx.channel,
            "source": "simulator",
            "qualification_status": getattr(ctx, "qualification_status", "undetermined"),
            "qualification_notes": getattr(ctx, "qualification_notes", None),
            "extracted_data": {},
        }
        logger.info("LEAD_LOOKUP | test_mode=True | mock lead created")
        return

    lead = await supabase.get_lead(ctx.contact_id, entity_id)

    if lead:
        ctx.lead = lead
        logger.info("LEAD_LOOKUP | found=True | lead_id=%s", lead.get("id", "")[:12])
        return

    # Create new lead
    new_lead = await supabase.create_lead(
        {
            "entity_id": entity_id,
            "ghl_contact_id": ctx.contact_id,
            "contact_name": ctx.contact_name,
            "contact_email": ctx.contact_email,
            "contact_phone": ctx.contact_phone,
            "last_reply_channel": ctx.channel,
            "source": "Unknown",
        },
    )
    ctx.lead = new_lead
    logger.info("Created new lead: %s", new_lead.get("id", ""))


def resolve_supported_languages(ctx: PipelineContext) -> list[str]:
    """Resolve supported languages from matched setter, defaulting to English."""
    # Check matched setter first (populated during _compile_system_config)
    setter = ctx.compiled.get("_matched_setter")
    if setter and isinstance(setter, dict):
        langs = setter.get("supported_languages")
        if langs and isinstance(langs, list) and len(langs) > 0:
            return langs
    # Fallback to top-level config
    langs = ctx.config.get("supported_languages")
    if langs and isinstance(langs, list) and len(langs) > 0:
        return langs
    return ["English"]


# =========================================================================
# SYSTEM_CONFIG COMPILATION
# =========================================================================


def _compile_system_config(ctx: PipelineContext, sc: dict[str, Any]) -> None:
    """Pre-compile all system_config sections into ctx.compiled dict.

    Each pipeline file reads from ctx.compiled instead of ctx.prompts.

    Setter architecture: sc contains {"setters": {"setter_1": {...}, ...}}.
    We resolve the matched setter first, then compile everything from it.
    Legacy flat layout (no "setters" key) falls through to the setter itself.
    """
    company = ctx.config.get("name", "")
    setter_key = ctx.agent_type or ""

    # ── Resolve setter from system_config.setters ──
    setter = resolve_setter(sc, setter_key)
    if not setter:
        logger.warning("No setter found for key=%s in system_config.setters", setter_key)
        return

    ctx.compiled["_matched_setter"] = setter

    # Reply + follow-up config live under setter.conversation
    conversation = setter.get("conversation", {})
    matched_agent = conversation.get("reply")  # reply config = the "agent"
    follow_up_config = conversation.get("follow_up")

    # Bot persona (3 variants: full, followup subset, media subset)
    bp = setter.get("bot_persona")
    ctx.compiled["bot_persona_full"] = compile_bot_persona(bp, company=company)
    ctx.compiled["bot_persona_followup"] = compile_bot_persona(
        bp, company=company, section_filter="followup",
    )
    ctx.compiled["bot_persona_media"] = compile_bot_persona(
        bp, company=company, section_filter="media",
    )

    # Agent/setter prompt (for the matched reply config)
    ctx.compiled["agent_prompt"] = compile_agent_prompt(matched_agent)
    ctx.compiled["agent_goals_summary"] = compile_agent_goals_summary(matched_agent)
    ctx.compiled["agent_lead_source"] = get_agent_lead_source(matched_agent)
    ctx.compiled["agent_urgency_scarcity"] = get_agent_urgency_scarcity(matched_agent)

    # Store the matched agent dict for downstream use (keep key name for minimal downstream changes)
    ctx.compiled["_matched_agent"] = matched_agent

    # Booking
    booking = setter.get("booking")
    from app.text_engine.utils import get_timezone
    _tz = ctx.tz or get_timezone(ctx.config)
    _now_str = datetime.now(_tz).strftime("%B %d, %Y %I:%M %p %Z")
    ctx.compiled["booking_config"] = compile_booking_config(booking, now_str=_now_str, agent_key=setter_key)
    ctx.compiled["booking_rules"] = compile_booking_rules(booking)

    # Services (no agent filtering needed — setter owns its services)
    services_config = setter.get("services")
    ctx.compiled["services_names"] = compile_services_list(services_config, names_only=True, agent_key=setter_key)
    ctx.compiled["offers_text"] = compile_all_offers(services_config, agent_key=setter_key)
    ctx.compiled["offers_deployment"] = compile_offers_deployment(
        services_config,
        setter.get("offers_deployment"),
    )

    # Transfer
    transfer = setter.get("transfer")
    services = services_config.get("services", []) if services_config else []
    supported_langs = resolve_supported_languages(ctx)
    agent_caps = compile_agent_capabilities(
        matched_agent,
        services=services,
        supported_languages=supported_langs,
    )
    ctx.compiled["transfer_prompt"] = compile_transfer_prompt(transfer, agent_caps)
    ctx.compiled["opt_out_sections"] = compile_opt_out_sections(transfer)

    # Security
    security = setter.get("security")
    ctx.compiled["security_protections"] = compile_security_protections(security)
    ctx.compiled["compliance_rules"] = compile_compliance_rules(security)
    ctx.compiled["term_replacements"] = get_term_replacements(security)

    # Case studies (no agent filtering — setter owns its case studies)
    case_studies = setter.get("case_studies")
    ctx.compiled["case_studies_reply"] = compile_case_studies(case_studies, agent_key=setter_key)
    ctx.compiled["case_studies_followup"] = compile_case_studies(case_studies, agent_key="follow_up")
    ctx.compiled["case_study_media_reply"] = get_case_study_media(case_studies, agent_key=setter_key, path_type="reply")
    ctx.compiled["case_study_media_followup"] = get_case_study_media(case_studies, agent_key="follow_up", path_type="followup")

    # Follow-up
    follow_up = follow_up_config
    ctx.compiled["followup_preferences"] = compile_followup_preferences(follow_up)
    ctx.compiled["followup_banned_phrases"] = compile_followup_banned_phrases(follow_up)
    ctx.compiled["followup_positions_determination"] = compile_followup_positions_for_determination(follow_up)

    # Media library from reply config + case study media
    if matched_agent:
        agent_media = matched_agent.get("media_items", [])
        cs_media_reply = ctx.compiled.get("case_study_media_reply", [])
        if agent_media or cs_media_reply:
            ctx.media_library = list(agent_media) + list(cs_media_reply)

    # Follow-up media
    fu_config = follow_up or {}
    fu_media = fu_config.get("media_items", [])
    cs_media_followup = ctx.compiled.get("case_study_media_followup", [])
    if fu_media or cs_media_followup:
        ctx.followup_media_library = list(fu_media) + list(cs_media_followup)

    # AI model + temperature overrides (per-setter)
    ctx.compiled["ai_models"] = setter.get("ai_models") or {}
    ctx.compiled["ai_temperatures"] = setter.get("ai_temperatures") or {}

    # Data collection (custom extraction fields)
    data_collection = setter.get("data_collection")
    ctx.compiled["data_collection_fields"] = (
        data_collection.get("fields", []) if data_collection else []
    )



# =========================================================================
# DEEP MERGE UTILITY
# =========================================================================


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge override into base. Returns new dict (does not mutate base).

    For dicts: recursively merge.
    For everything else: override wins.
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result
