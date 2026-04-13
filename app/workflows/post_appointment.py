"""Post-Appointment Automation — proactive outreach after appointments.

Fires from message_scheduler_loop when a post_appointment row comes due.
Standalone workflow (not full pipeline) — generates a natural check-in
message picking 1-2 actions from the enabled menu:
  - Post-appointment follow-up ("how did it go?")
  - Schedule next visit (rebooking nudge)
  - Request review (with review links)
  - Request referral

Two LLM calls:
  1. Determination (lightweight, conditional) — only if lead already texted
     post-appointment. Checks which actions are already covered.
  2. Generation (heavier) — generates 1-3 natural SMS messages picking 1-2 actions.

Security pass runs on each generated message before delivery.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Action keys and their human-readable labels
_ACTION_LABELS = {
    "post_appointment_followup": "Post-Appointment Follow-Up",
    "schedule_next_visit": "Schedule Next Visit",
    "request_review": "Request Review",
    "suggest_related_service": "Upsell / Cross-Sell",
}


# ============================================================================
# CONFIG RESOLUTION
# ============================================================================


def _resolve_post_booking_config(config: dict[str, Any]) -> tuple[dict, dict]:
    """Resolve default setter and its post_booking config.

    Returns (setter_dict, post_booking_dict). Both empty dicts if not found.
    """
    from app.text_engine.agent_compiler import resolve_setter

    sc = config.get("system_config") or {}
    setter = resolve_setter(sc, "")  # default setter
    if not setter:
        return {}, {}

    pb = (
        setter.get("conversation", {})
        .get("reply", {})
        .get("sections", {})
        .get("post_booking", {})
    )
    return setter, pb


def _collect_enabled_actions(
    pb_config: dict[str, Any],
    matched_service: dict[str, Any] | None = None,
    *,
    all_services: list[dict[str, Any]] | None = None,
    active_offers: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Collect enabled post-appointment action prompts.

    Returns list of {key, label, prompt, extras} for enabled actions.
    Order is randomized to prevent position bias in the LLM.
    """
    import random

    actions: list[dict[str, Any]] = []

    # post_appointment_followup is NOT included — check-in is always implicit in the prompt
    for key in ("schedule_next_visit", "request_review", "suggest_related_service"):
        entry = pb_config.get(key, {})
        if not isinstance(entry, dict) or not entry.get("enabled"):
            continue

        prompt = (entry.get("prompt") or "").strip()
        if not prompt:
            continue

        action: dict[str, Any] = {
            "key": key,
            "label": _ACTION_LABELS.get(key, key),
            "prompt": prompt,
        }

        # Add extras for specific actions
        if key == "request_review":
            links = entry.get("review_links", [])
            if links:
                primary = links[0]
                action["review_link"] = primary.get("url", "")
                action["review_label"] = primary.get("label", "Review")

        if key == "schedule_next_visit":
            # Matched service interval takes priority, then show all intervals
            intervals = []
            if matched_service:
                interval = (matched_service.get("rebooking_interval") or "").strip()
                if interval:
                    action["rebooking_interval"] = interval
                    intervals.append(f"{matched_service.get('name', 'Service')}: {interval}")
            # Also list other services with intervals for context
            for svc in (all_services or []):
                svc_name = svc.get("name", "")
                svc_interval = (svc.get("rebooking_interval") or "").strip()
                if svc_interval and svc_name != (matched_service or {}).get("name"):
                    intervals.append(f"{svc_name}: {svc_interval}")
            if intervals:
                action["all_intervals"] = intervals

        if key == "suggest_related_service" and all_services:
            # Attach other services for upsell context (exclude the matched/booked service)
            matched_name = (matched_service or {}).get("name", "")
            other_services = [
                s.get("name", "") for s in all_services
                if s.get("name") and s.get("name") != matched_name
            ]
            if other_services:
                action["other_services"] = other_services

        actions.append(action)

    # Randomize order to prevent position bias
    random.shuffle(actions)
    return actions


# ============================================================================
# CONVERSATION ANALYSIS
# ============================================================================


def _filter_post_appointment_messages(
    chat_history: list[dict[str, Any]],
    appointment_reference_iso: str,
) -> list[dict[str, Any]]:
    """Filter chat history to only messages AFTER the appointment reference time.

    Returns messages (newest first, same as chat_history order) that occurred
    after the appointment ended (preferred) or after the appointment started
    when no end time is available. Empty list = no post-appointment conversation.
    """
    if not appointment_reference_iso or not chat_history:
        return []

    try:
        appt_start = datetime.fromisoformat(
            appointment_reference_iso.replace("Z", "+00:00")
        )
    except (ValueError, AttributeError):
        return []

    post_msgs = []
    for msg in chat_history:
        ts = msg.get("timestamp") or msg.get("created_at") or ""
        if not ts:
            continue
        try:
            if isinstance(ts, str):
                msg_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            elif isinstance(ts, datetime):
                msg_time = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            else:
                continue

            if msg_time > appt_start:
                post_msgs.append(msg)
        except (ValueError, AttributeError):
            continue

    return post_msgs


def _has_lead_messages(messages: list[dict[str, Any]]) -> bool:
    """Check if any messages in the list are from the lead (not AI)."""
    for msg in messages:
        role = msg.get("role", "")
        if role in ("human", "user"):
            return True
    return False


# ============================================================================
# LLM CALLS
# ============================================================================


_DETERMINATION_SCHEMA = {
    "type": "object",
    "properties": {
        "should_send": {
            "type": "boolean",
            "description": "Whether to still send a proactive post-appointment message",
        },
        "uncovered_actions": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Action keys that haven't been addressed in the post-appointment conversation",
        },
        "reasoning": {
            "type": "string",
            "description": "Brief explanation of the decision",
        },
    },
    "required": ["should_send", "uncovered_actions", "reasoning"],
}


_GENERATION_SCHEMA = {
    "type": "object",
    "properties": {
        "message": {
            "type": "string",
            "description": "The full post-appointment message to send. Write it as one natural text — splitting into multiple SMS bubbles happens separately.",
        },
        "actions_used": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Which action keys were included in the message. Must use the exact action keys provided in the prompt.",
        },
        "reasoning": {
            "type": "string",
            "description": "Brief explanation of why these actions were chosen",
        },
    },
    "required": ["message", "actions_used", "reasoning"],
}


async def _run_determination(
    post_convo: list[dict[str, Any]],
    enabled_actions: list[dict[str, Any]],
    model_ctx: Any,
    tracker: Any,
) -> list[dict[str, Any]] | None:
    """Determine if we should still send, and which actions are uncovered.

    Returns filtered action list (uncovered only), or None to skip entirely.
    """
    from app.services.ai_client import classify
    from app.text_engine.model_resolver import resolve_model, resolve_temperature

    # Format post-appointment conversation for context
    convo_lines = []
    for msg in reversed(post_convo):  # chronological order
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        speaker = "Lead" if role in ("human", "user") else "AI"
        convo_lines.append(f"{speaker}: {content}")
    convo_text = "\n".join(convo_lines[-20:])  # last 20 messages max

    action_list = "\n".join(
        f"- {a['key']}: {a['label']} — {a['prompt'][:100]}"
        for a in enabled_actions
    )

    system_prompt = (
        "You are analyzing a post-appointment conversation to decide if a proactive follow-up message is still needed.\n\n"
        "A lead just had an appointment and has already been texting with the AI since then. "
        "Check which of the available post-appointment actions have already been addressed in the conversation.\n\n"
        "If ALL actions have been covered, set should_send to false.\n"
        "If some actions are still uncovered, set should_send to true and list the uncovered action keys.\n"
        "If the conversation ended on a negative note (complaints, frustration), set should_send to false."
    )

    user_prompt = (
        f"Post-appointment conversation:\n{convo_text}\n\n"
        f"Available actions:\n{action_list}\n\n"
        "Which actions have NOT been addressed yet? Should we still reach out?"
    )

    try:
        model = resolve_model(model_ctx, "post_appointment_determination")
        temp = resolve_temperature(model_ctx, "post_appointment_determination")

        result = await classify(
            prompt=user_prompt,
            schema=_DETERMINATION_SCHEMA,
            system_prompt=system_prompt,
            model=model,
            temperature=temp,
            label="post_appointment_determination",
        )

        tracker.add_llm_call(
            model=model,
            provider="openrouter",
            label="post_appointment_determination",
        )

        if not result.get("should_send", False):
            return None

        # Filter enabled actions to only uncovered ones
        uncovered_keys = set(result.get("uncovered_actions", []))
        if not uncovered_keys:
            return None

        return [a for a in enabled_actions if a["key"] in uncovered_keys]

    except Exception as e:
        logger.warning("POST_APPT | determination failed, proceeding with all actions: %s", e)
        return enabled_actions  # On error, proceed with all actions


async def _run_generation(
    actions: list[dict[str, Any]],
    bot_persona: str,
    conversation_summary: str,
    appointment_context: str,
    extra_context: dict[str, str],
    model_ctx: Any,
    tracker: Any,
    *,
    approach: str = "goals_upfront",
) -> dict[str, Any] | None:
    """Generate the post-appointment message(s).

    Returns {messages: list[str], actions_used: list[str], reasoning: str} or None on failure.
    """
    from app.services.ai_client import classify
    from app.text_engine.model_resolver import resolve_model, resolve_temperature

    # Detect whether there's existing conversation (drives intro behavior)
    has_history = "No prior conversation history" not in conversation_summary

    # Check-in only mode: override actions with pure check-in instruction
    checkin_only = approach == "checkin_only"

    # Build action descriptions for the prompt
    actions_section = ""
    if checkin_only:
        actions_section = (
            "# POST-APPOINTMENT CHECK-IN\n"
            "Your only goal is a genuine check-in — ask how their appointment went, "
            "how they're feeling, if everything went well. That's it.\n\n"
            "Do NOT pitch anything, do NOT ask for reviews, do NOT suggest rebooking, "
            "do NOT ask for referrals. Just be a human checking in.\n\n"
            "actions_used must be an empty array since this is a pure check-in."
        )
    elif actions:
        action_block = []
        for a in actions:
            block = f"## {a['label']}\nAction key: {a['key']}\n{a['prompt']}"
            # Per-goal context: review links
            if a.get("review_link"):
                block += f"\nReview link to share: {a['review_link']}"
            if a.get("review_already_sent"):
                block += "\nNOTE: A review link was already sent in a previous conversation. Do not send it again unless the lead asks."
            # Per-goal context: rebooking intervals
            if a.get("rebooking_interval"):
                block += f"\nRecommended rebooking interval for their service: {a['rebooking_interval']}"
            if a.get("all_intervals"):
                block += "\nService rebooking intervals: " + ", ".join(a["all_intervals"])
            # Per-goal context: other services for upsell
            if a.get("other_services"):
                block += "\nOther services this business offers (pick the one that best complements their appointment):\n"
                block += "\n".join(f"  - {s}" for s in a["other_services"])
            action_block.append(block)
        actions_section = (
            "# POST-APPOINTMENT OUTREACH STRUCTURE\n"
            "Your message MUST have two parts:\n"
            "1. A genuine check-in — ask how their appointment went, how they're feeling. This always comes first.\n"
            "2. Exactly ONE goal from the options below. You MUST pick one. "
            "A check-in with no goal is not acceptable when goals are available. "
            "Pick whichever goal fits this lead's situation best, but you must pick one.\n\n"
            "GOAL SELECTION GUIDANCE:\n"
            "- All goals are equally valid and valuable. Do NOT default to rebooking or review just because they feel safe.\n"
            "- If the business has complementary services, upsell/cross-sell is a great choice — "
            "the lead just experienced the business and is primed to try something new.\n"
            "- Choose based on the lead's tone, their specific service, and what would feel most natural and helpful.\n\n"
            "# AVAILABLE GOALS\n\n"
            + "\n\n".join(action_block)
        )
    else:
        actions_section = (
            "# POST-APPOINTMENT CHECK-IN\n"
            "No specific action prompts are configured. Send a natural, warm check-in message "
            "asking how their appointment went. Keep it casual and genuine.\n\n"
            "Because no action prompts are configured, actions_used must be an empty array."
        )

    # Build enriched system prompt — section order follows reply agent convention
    prompt_parts: list[str] = []

    # --- 1. Security / compliance rules (highest priority) ---
    if extra_context.get("security_rules"):
        prompt_parts.append(f"<security_rules>\n{extra_context['security_rules']}\n</security_rules>")
    if extra_context.get("compliance_rules"):
        prompt_parts.append(f"<compliance_rules>\n{extra_context['compliance_rules']}\n</compliance_rules>")

    # --- 2. Supported languages ---
    langs = extra_context.get("supported_languages", "English")
    prompt_parts.append(
        f"<supported_languages>\nSupported languages: {langs}.\n"
        "Match the language used in the conversation history. "
        f"If no prior conversation, use {langs.split(',')[0].strip()}.\n</supported_languages>"
    )

    # --- 3. Business context (offers moved to per-goal sections) ---
    if extra_context.get("business_hours"):
        prompt_parts.append(f"<business_context>\nBusiness hours: {extra_context['business_hours']}\n</business_context>")

    # --- 4. Services ---
    if extra_context.get("services"):
        prompt_parts.append(f"<services>\n{extra_context['services']}\n</services>")

    # --- 5. Task description + available actions ---
    history_guidance = ""
    if has_history:
        history_guidance = (
            "This lead has an EXISTING conversation with you. They already know who you are. "
            "Do NOT reintroduce yourself — no 'Hey, it's [name] from [business]'. "
            "Just pick up naturally like a real person continuing a thread. "
            "Think: a friend checking back in after their appointment, not a business sending a follow-up."
        )
    else:
        history_guidance = (
            "This lead has NO prior conversation with you — they booked directly without chatting first. "
            "A brief, natural introduction is appropriate since this is first contact. "
            "Keep it light — one short line to say who you are, then get into the check-in."
        )

    prompt_parts.append(
        "<post_appointment_task>\n"
        "# POST-APPOINTMENT OUTREACH\n\n"
        "You are reaching out to a lead AFTER their appointment. This is a proactive message — they didn't text you first.\n\n"
        f"{history_guidance}\n\n"
        f"{actions_section}\n"
        "</post_appointment_task>"
    )

    # --- 6. Bot persona ---
    prompt_parts.append(f"<bot_persona>\n{bot_persona}\n</bot_persona>")

    # --- 7. Agent freeform prompt + custom sections ---
    if extra_context.get("agent_freeform"):
        prompt_parts.append(f"<additional_instructions>\n{extra_context['agent_freeform']}\n</additional_instructions>")
    if extra_context.get("custom_sections"):
        prompt_parts.append(extra_context["custom_sections"])

    # --- 8. Output rules ---
    prompt_parts.append(
        "<output_rules>\n"
        "- Write ONE natural message the way a real person would text. Do not split it yourself — that is handled separately.\n"
        "- This is a casual check-in from someone they know, not a business broadcast\n"
        "- The check-in and goal should flow naturally together, not feel like two separate blocks\n"
        "- Vary your approach every time — different openings, different structures, different wording. "
        "Never fall into a pattern of 'greeting then question then pitch'. Mix it up.\n"
        "- Reference specific details from their appointment or prior conversation when you have them\n"
        "- If including a review link, weave it into the message naturally — never a bare URL on its own line\n"
        "- If including an offer or promotion, mention it only if it is actually relevant to what they got done\n"
        "- Do not guess relative timing like 'today' or 'yesterday' — you do not know when this message will be read\n"
        "- In actions_used, return the exact action key of the ONE goal you picked — no labels, paraphrases, or variants. "
        "If no goals were available, return an empty array.\n"
        "- No markdown, no bullet points, no formatting\n"
        "- Match the language and energy level of the prior conversation. "
        "If they were enthusiastic, match that. If they were chill and short, keep it short too.\n"
        "</output_rules>"
    )

    system_prompt = "".join(prompt_parts)

    user_prompt = f"{appointment_context}\n\n{conversation_summary}\n\nGenerate your post-appointment outreach message now."

    try:
        model = resolve_model(model_ctx, "post_appointment_generation")
        temp = resolve_temperature(model_ctx, "post_appointment_generation")

        result = await classify(
            prompt=user_prompt,
            schema=_GENERATION_SCHEMA,
            system_prompt=system_prompt,
            model=model,
            temperature=temp,
            label="post_appointment_generation",
        )

        tracker.add_llm_call(
            model=model,
            provider="openrouter",
            label="post_appointment_generation",
        )

        message = (result.get("message") or "").strip()
        if not message:
            logger.warning("POST_APPT | generation returned empty message")
            return None

        return result

    except Exception as e:
        logger.error("POST_APPT | generation failed: %s", e)
        return None


# ============================================================================
# SECURITY
# ============================================================================


async def _apply_security_to_messages(
    messages: list[str],
    config: dict[str, Any],
    setter: dict[str, Any],
) -> list[str]:
    """Apply term replacements and security checks to each message.

    Security runs on each message individually BEFORE delivery.
    """
    from app.text_engine.security import (
        apply_term_replacements_standalone,
        strip_punctuation_dashes,
    )

    security_config = setter.get("security") or {}
    term_replacements = security_config.get("term_replacements") or []

    cleaned = []
    for msg in messages:
        text = msg.strip()
        if not text:
            continue

        # Layer 1: Strip stray punctuation/dashes
        text = strip_punctuation_dashes(text)

        # Layer 2: Term replacements
        if term_replacements:
            text = apply_term_replacements_standalone(text, term_replacements)

        cleaned.append(text)

    return cleaned


# ============================================================================
# MAIN HANDLER
# ============================================================================


async def fire_post_appointment(row: dict[str, Any]) -> dict[str, Any]:
    """Fire a post-appointment automation message.

    Called by message_scheduler_loop when a post_appointment row comes due.

    Flow:
    1. Resolve entity + setter config
    2. Guard checks (toggle still on, actions enabled)
    3. Fetch conversation history
    4. If lead texted post-appointment → run determination gate
    5. Generate message (1-3 SMS texts)
    6. Apply security to each message
    7. Deliver via SMS
    8. Store in chat history
    9. Reschedule reactivation timer
    """
    from app.models import TokenUsage
    from app.services.ai_client import set_ai_context, clear_ai_context
    from app.services.workflow_tracker import WorkflowTracker
    from app.text_engine.model_resolver import resolve_model, resolve_temperature

    entity_id = row.get("entity_id", "")
    contact_id = row.get("contact_id", "")
    raw_meta = row.get("metadata") or {}
    if isinstance(raw_meta, str):
        try:
            raw_meta = json.loads(raw_meta)
        except (ValueError, TypeError):
            raw_meta = {}
    metadata = raw_meta

    tracker = WorkflowTracker(
        "post_appointment",
        entity_id=entity_id,
        ghl_contact_id=contact_id,
        trigger_source="scheduled",
    )
    tracker.set_metadata("appointment_id", metadata.get("appointment_id", ""))

    try:
        # ── 1. Resolve entity config ──
        from app.main import supabase, _resolve_tenant_ai_keys

        config = await supabase.resolve_entity(entity_id)
        slug = config.get("slug") or config.get("name", entity_id[:8])
        tracker.set_system_config(config.get("system_config"))
        tracker.set_runtime_context({
            "contact_name": metadata.get("contact_name", ""),
            "appointment_id": metadata.get("appointment_id", ""),
            "calendar_name": metadata.get("calendar_name", ""),
            "appointment_start": metadata.get("appointment_start", ""),
            "appointment_end": metadata.get("appointment_end", ""),
        })

        # ── 2. Resolve setter + post-booking config ──
        setter, pb_config = _resolve_post_booking_config(config)
        if not setter or not pb_config:
            tracker.set_status("skipped")
            tracker.set_decisions({"skipped": True, "reason": "no_setter_or_config"})
            return {"status": "skipped", "result": "no_config", "reason": "No setter or post-booking config found"}

        # Guard: proactive_outreach still enabled?
        if not pb_config.get("proactive_outreach"):
            tracker.set_status("skipped")
            tracker.set_decisions({"skipped": True, "reason": "proactive_outreach_disabled"})
            return {"status": "skipped", "result": "disabled", "reason": "Proactive outreach disabled since scheduling"}

        # ── 3. Collect enabled actions (may be empty — LLM still generates a natural check-in) ──
        matched_service = None
        interest = (metadata.get("interest") or "").strip()
        if interest:
            try:
                from app.text_engine.qualification import match_form_interest

                services_cfg_for_match = setter.get("services") or {}
                matched_name = match_form_interest(interest, services_cfg_for_match)
                if matched_name:
                    for svc in services_cfg_for_match.get("services", []) or []:
                        if svc.get("name") == matched_name:
                            matched_service = svc
                            break
            except Exception as e:
                logger.warning("POST_APPT | service matching failed: %s", e)

        # Gather all services (for rebooking intervals) and active offers (for referral incentives)
        services_cfg = setter.get("services") or {}
        all_services_list = services_cfg.get("services", []) or []
        try:
            from app.text_engine.offers import get_active_offers
            active_offers_list = get_active_offers(services_cfg)
        except Exception:
            active_offers_list = []

        actions = _collect_enabled_actions(
            pb_config, matched_service,
            all_services=all_services_list,
            active_offers=active_offers_list,
        )

        # ── 4. Resolve tenant AI keys + token tracking ──
        tenant_keys = await _resolve_tenant_ai_keys(entity_id)
        token_tracker = TokenUsage()

        # Import _MinimalModelCtx pattern from reactivation
        from app.workflows.reactivation import _MinimalModelCtx
        model_ctx = _MinimalModelCtx(config)

        set_ai_context(
            api_key=tenant_keys["openrouter"],
            token_tracker=token_tracker,
            google_key=tenant_keys.get("google", ""),
            openai_key=tenant_keys.get("openai", ""),
            deepseek_key=tenant_keys.get("deepseek", ""),
            xai_key=tenant_keys.get("xai", ""),
            anthropic_key=tenant_keys.get("anthropic", ""),
        )

        # ── 5. Fetch conversation history ──
        from app.services.postgres_client import postgres

        chat_table = config.get("chat_history_table_name", "")
        chat_history = []
        if chat_table:
            try:
                chat_history = await postgres.get_chat_history(chat_table, contact_id, limit=50)
            except Exception as e:
                logger.warning("POST_APPT | chat history fetch failed: %s", e)

        # ── 5b. Review link dedup — check if a review link was already sent ──
        for action in actions:
            if action["key"] == "request_review" and action.get("review_link"):
                review_url = action["review_link"]
                for msg in chat_history:
                    content = (msg.get("content") or "").lower()
                    if review_url.lower() in content:
                        action["review_already_sent"] = True
                        break

        # ── 6. Check for post-appointment conversation ──
        appt_reference_iso = metadata.get("appointment_end") or metadata.get("appointment_start", "")
        post_convo = _filter_post_appointment_messages(chat_history, appt_reference_iso)
        has_post_convo = _has_lead_messages(post_convo)

        determination_result = None
        final_actions = actions

        if has_post_convo:
            # Lead already texted post-appointment → run determination
            determination_result = await _run_determination(post_convo, actions, model_ctx, tracker)
            if determination_result is None:
                tracker.set_status("skipped")
                tracker.set_decisions({
                    "skipped": True,
                    "reason": "determination_skip",
                    "post_appointment_conversation": True,
                    "actions_available": [a["key"] for a in actions],
                })
                return {"status": "skipped", "result": "determination_skip", "reason": "All actions already covered in conversation"}
            final_actions = determination_result

        # ── 7. Build context for generation ──
        from app.text_engine.bot_persona_compiler import compile_bot_persona

        # Bot persona
        bp = setter.get("bot_persona")
        bot_persona = compile_bot_persona(bp, company=config.get("name", ""), section_filter="followup")

        # Supported languages
        supported_langs = setter.get("supported_languages") or ["English"]
        langs_str = ", ".join(supported_langs)

        # Security/compliance rules (given to LLM as guidelines, NOT term replacements)
        security_cfg = setter.get("security") or {}
        from app.text_engine.security_compiler import compile_security_protections, compile_compliance_rules
        security_text = compile_security_protections(security_cfg)
        compliance_text = compile_compliance_rules(security_cfg)

        # Services (names + descriptions, no qualification criteria)
        services_cfg = setter.get("services") or {}
        services_list = services_cfg.get("services", [])
        services_text = ""
        if services_list:
            svc_lines = []
            for svc in services_list:
                name = svc.get("name", "")
                desc = (svc.get("description") or "").strip()
                interval = (svc.get("rebooking_interval") or "").strip()
                line = f"- {name}"
                if desc:
                    line += f": {desc}"
                if interval:
                    line += f" (rebooking interval: {interval})"
                svc_lines.append(line)
            services_text = "Services offered:\n" + "\n".join(svc_lines)

        # Offers & promotions
        from app.text_engine.services_compiler import compile_all_offers
        offers_text = compile_all_offers(services_cfg)

        # Agent freeform prompt + custom sections (tone rules, special instructions)
        reply_config = setter.get("conversation", {}).get("reply") or {}
        agent_freeform = (reply_config.get("prompt") or "").strip()
        custom_sections_text = ""
        custom_sections = reply_config.get("custom_sections", [])
        if custom_sections:
            cs_parts = []
            for cs in custom_sections:
                if isinstance(cs, dict) and cs.get("enabled"):
                    label = cs.get("label", "Custom")
                    prompt = (cs.get("prompt") or "").strip()
                    if prompt:
                        cs_parts.append(f"## {label}\n{prompt}")
            if cs_parts:
                custom_sections_text = "\n\n".join(cs_parts)

        # Business context (hours, days) — from business_schedule JSONB on entity
        from app.text_engine.utils import extract_business_hours
        _hours_start, _hours_end, _enabled_days = extract_business_hours(config)
        business_days = ", ".join(_enabled_days)
        _tz_name = config.get("timezone", "America/Chicago")
        business_hours_str = f"{_hours_start} - {_hours_end} ({_tz_name})" if _hours_start else ""

        # Conversation summary (last 30 messages, chronological)
        summary_lines = []
        for msg in reversed(chat_history[:30]):
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            speaker = "Lead" if role in ("human", "user") else "AI"
            summary_lines.append(f"{speaker}: {content}")

        conversation_summary = "Recent conversation:\n" + "\n".join(summary_lines) if summary_lines else "No prior conversation history."

        # Appointment context
        cal_name = metadata.get("calendar_name", "their appointment")
        appt_context = f"Lead: {metadata.get('contact_name', 'the lead')}\nAppointment: {cal_name}"
        if metadata.get("interest"):
            appt_context += f"\nService interest: {metadata['interest']}"

        # Build enriched context dict for generation
        extra_context = {
            "supported_languages": langs_str,
            "security_rules": security_text,
            "compliance_rules": compliance_text,
            "services": services_text,
            "offers": offers_text,
            "agent_freeform": agent_freeform,
            "custom_sections": custom_sections_text,
            "business_hours": f"{business_days}\n{business_hours_str}".strip() if business_days or business_hours_str else "",
        }

        # ── 8. Generate message ──
        approach = pb_config.get("post_appointment_approach", "goals_upfront")
        gen_result = await _run_generation(
            final_actions, bot_persona, conversation_summary, appt_context,
            extra_context, model_ctx, tracker,
            approach=approach,
        )

        if not gen_result:
            tracker.set_status("error")
            tracker.set_decisions({
                "skipped": False,
                "reason": "generation_failed",
                "actions_available": [a["key"] for a in final_actions],
            })
            return {"status": "failed", "result": "generation_failed", "reason": "LLM generation returned no message"}

        raw_message = (gen_result.get("message") or "").strip()
        actions_used = gen_result.get("actions_used", [])

        # ── 9. Apply security on full message (before splitting — same order as reply engine) ──
        from app.text_engine.security import (
            apply_term_replacements_standalone,
            strip_punctuation_dashes,
        )

        secured_message = strip_punctuation_dashes(raw_message)
        security_config = setter.get("security") or {}
        term_replacements = security_config.get("term_replacements") or []
        if term_replacements:
            secured_message = apply_term_replacements_standalone(secured_message, term_replacements)

        if not secured_message.strip():
            tracker.set_status("error")
            tracker.set_decisions({"skipped": False, "reason": "security_cleared_message"})
            return {"status": "failed", "result": "security_failed", "reason": "Message cleared by security"}

        # ── 10. Split message (same splitter as reply engine — handles typos + punctuation style) ──
        from app.text_engine.delivery import split_message_standalone

        bp = setter.get("bot_persona") or {}
        bp_sections = bp.get("sections") or {}
        skip_splitting = bp_sections.get("message_split", {}).get("skip_splitting", False)

        messages = await split_message_standalone(
            secured_message,
            bp_sections=bp_sections,
            temperature=0.5,
            skip_splitting=skip_splitting,
        )

        if not messages:
            messages = [secured_message]

        # ── 11. Deliver via DeliveryService (same as all other workflows) ──
        from app.services.ghl_client import GHLClient
        from app.services.delivery_service import DeliveryService

        ghl = GHLClient(
            api_key=config.get("ghl_api_key", ""),
            location_id=config.get("ghl_location_id", ""),
        )
        delivery_svc = DeliveryService(ghl, config)

        to_phone = row.get("to_phone")
        if not to_phone:
            try:
                contact = await ghl.get_contact(contact_id)
                to_phone = (contact or {}).get("phone")
            except Exception:
                pass

        if len(messages) == 1:
            sms_result = await delivery_svc.send_sms(
                contact_id=contact_id,
                message=messages[0],
                to_phone=to_phone,
                message_type="post_appointment",
            )
            if sms_result.status == "failed":
                tracker.set_status("error")
                tracker.set_decisions({
                    "skipped": False,
                    "reason": "delivery_failed",
                    "actions_available": [a["key"] for a in final_actions],
                })
                return {"status": "failed", "result": "delivery_failed", "reason": sms_result.error_message or "SMS delivery failed"}
        else:
            split_result = await delivery_svc.send_split_messages(
                contact_id=contact_id,
                messages=messages,
                to_phone=to_phone,
            )
            if not split_result.all_delivered:
                failed_msgs = [r for r in split_result.results if r.status == "failed"]
                err = failed_msgs[0].error_message if failed_msgs else "Split delivery failed"
                tracker.set_status("error")
                tracker.set_decisions({
                    "skipped": False,
                    "reason": "delivery_failed",
                    "actions_available": [a["key"] for a in final_actions],
                })
                return {"status": "failed", "result": "delivery_failed", "reason": err or "Split delivery failed"}

        # ── 12. Store in chat history ──
        if chat_table:
            try:
                # Fetch lead_id for linking messages to the lead record
                _lead = await supabase.get_lead(contact_id, entity_id)
                _lead_id = _lead.get("id") if _lead else None

                for msg_text in messages:
                    await postgres.insert_message(
                        table=chat_table,
                        session_id=contact_id,
                        message={
                            "type": "ai",
                            "content": msg_text,
                            "additional_kwargs": {
                                "source": "post_appointment",
                                "channel": "SMS",
                            },
                        },
                        lead_id=_lead_id,
                        workflow_run_id=tracker.run_id,
                    )
            except Exception as e:
                logger.warning("POST_APPT | chat history insert failed: %s", e)

        # ── 13. Reschedule reactivation ──
        try:
            from app.workflows.reactivation_scheduler import reschedule_reactivation
            await reschedule_reactivation(entity_id, contact_id)
        except Exception as e:
            logger.warning("POST_APPT | reactivation reschedule failed: %s", e)

        # ── 14. Track decisions ──
        tracker.set_decisions({
            "skipped": False,
            "post_appointment_conversation": has_post_convo,
            "determination_ran": has_post_convo,
            "actions_available": [a["key"] for a in actions],
            "actions_final": [a["key"] for a in final_actions],
            "actions_used": actions_used,
            "generation_reasoning": gen_result.get("reasoning", ""),
            "messages_count": len(messages),
        })

        tracker.set_runtime_context({
            "contact_name": metadata.get("contact_name", ""),
            "appointment_id": metadata.get("appointment_id", ""),
            "calendar_name": metadata.get("calendar_name", ""),
            "appointment_start": metadata.get("appointment_start", ""),
            "appointment_end": metadata.get("appointment_end", ""),
            "approach": approach,
            "has_conversation_history": len(chat_history) > 0,
            "chat_history_length": len(chat_history),
            "post_appointment_messages": len(post_convo),
        })

        tracker.set_system_config(config.get("system_config"))

        logger.info(
            "POST_APPT | sent | entity=%s | contact=%s | actions=%s | msgs=%d",
            slug, contact_id, actions_used, len(messages),
        )

        return {
            "status": "sent",
            "result": "delivered",
            "reason": f"Post-appointment outreach sent ({len(messages)} msg, actions: {', '.join(actions_used)})",
        }

    except Exception as e:
        logger.exception("POST_APPT | handler failed | entity=%s | contact=%s", entity_id, contact_id)
        tracker.set_error(str(e))
        return {"status": "failed", "result": "error", "reason": str(e)[:200]}

    finally:
        try:
            clear_ai_context()
        except Exception:
            pass
        if 'token_tracker' in dir():
            tracker.set_token_usage(token_tracker)
        await tracker.save()
