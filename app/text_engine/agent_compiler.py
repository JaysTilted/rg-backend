"""Compiler: setter conversation config → prompt text.

Reads the matched setter's conversation.reply config and compiles it
into markdown for the reply agent's system prompt.

Also provides helper to compile agent capabilities summary for the
transfer classifier (goals, tools, services, languages, lead source).

Setter architecture: each setter is a complete persona. The reply config
lives at setter.conversation.reply.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def resolve_setter(system_config: dict[str, Any], setter_key: str) -> dict[str, Any] | None:
    """Resolve the matched setter from system_config.setters.

    Resolution order:
    1. Exact key match in setters dict
    2. Setter with is_default: true
    3. First setter in dict
    4. None (caller should fall back to legacy flat layout)
    """
    setters = system_config.get("setters")
    if not setters or not isinstance(setters, dict):
        return None  # no setter structure — caller uses legacy fallback

    if setter_key and setter_key in setters:
        return setters[setter_key]

    # Fallback: default setter
    for s in setters.values():
        if s.get("is_default"):
            return s

    # Last resort: first setter
    first = next(iter(setters.values()), None)
    if first:
        logger.info("No setter match for key=%s, using first setter", setter_key)
    return first




def compile_agent_prompt(agent: dict[str, Any] | None) -> str:
    """Compile a reply agent's sections + custom_sections + freeform prompt.

    Returns compiled markdown for the <agent_prompt> section.
    """
    if not agent:
        return ""

    sections = agent.get("sections", {})
    parts: list[str] = []

    # === CONTEXT group ===
    # Agent goals
    goal_text = _compile_multi_select_goals(sections.get("agent_goal"))
    if goal_text:
        parts.append(f"## Goals\n{goal_text}")

    # Role & context (freeform string)
    role_ctx = (sections.get("role_context") or "").strip()
    if role_ctx:
        parts.append(f"## Role & Context\n{role_ctx}")

    # Lead source
    source_text = _compile_multi_select_sources(sections.get("lead_source"))
    if source_text:
        parts.append(f"## Lead Source\n{source_text}")

    # === STRATEGY group ===
    # Booking style (radio)
    booking = _compile_radio(sections.get("booking_style"))
    if booking:
        parts.append(f"## Booking Style\n{booking}")

    # Pricing discussion (radio)
    pricing = _compile_radio(sections.get("pricing_discussion"))
    if pricing:
        parts.append(f"## Pricing Discussion\n{pricing}")

    # Discovery questions
    dq_text = _compile_discovery_questions(sections.get("discovery_questions"))
    if dq_text:
        parts.append(f"## Discovery Questions\n{dq_text}")

    # Max questions per message
    mq = sections.get("max_questions", {})
    mq_val = mq.get("value", 1) if isinstance(mq, dict) else 1
    parts.append(f"## Questions Per Message\nMaximum {mq_val} question(s) per message.")

    # Max booking pushes
    mbp = sections.get("max_booking_pushes", {})
    if isinstance(mbp, dict) and mbp.get("enabled"):
        max_val = mbp.get("max", 3)
        prompt = (mbp.get("prompt") or "").strip()
        if prompt:
            parts.append(f"## Max Booking Pushes\n{prompt.replace('{max}', str(max_val))}")

    # === BEHAVIOR group ===
    behavior_keys = [
        ("steer_toward_goal", "Steer Toward Goal"),
        ("confident_expert", "Confident Expert"),
        ("fully_helped", "Fully Helped First"),
        ("always_moving_forward", "Always Moving Forward"),
        ("push_back", "Push Back"),
        ("proactive_tips", "Proactive Tips"),
        ("low_effort_responses", "Low-Effort Responses"),
        ("urgency", "Urgency"),
        ("scarcity", "Scarcity"),
        ("returning_lead_rules", "Returning Lead Rules"),
        ("future_pacing", "Future Pacing"),
        ("acknowledge_before_pivot", "Acknowledge Before Pivoting"),
        ("yes_and", "Yes-And Technique"),
        ("paraphrase", "Paraphrase"),
        ("accept_no", "Accept No Gracefully"),
        ("discover_timeline", "Discover Timeline"),
        ("allow_small_talk", "Allow Small Talk"),
    ]

    behavior_parts: list[str] = []
    for key, label in behavior_keys:
        sec = sections.get(key, {})
        if isinstance(sec, dict) and sec.get("enabled"):
            prompt = (sec.get("prompt") or "").strip()
            if prompt:
                behavior_parts.append(f"### {label}\n{prompt}")

    if behavior_parts:
        parts.append("## Behavior\n" + "\n\n".join(behavior_parts))

    # === POST-BOOKING group ===
    # Note: post_booking sections are NOT compiled into the static agent prompt.
    # They are conditionally injected by agent.py based on appointment status.
    # See compile_post_booking_for_upcoming() and compile_post_booking_for_completed().

    # === REFERENCE group ===
    # Objections
    objections = sections.get("objections", [])
    if objections:
        obj_lines = []
        for obj in objections:
            if isinstance(obj, dict):
                trigger = (obj.get("trigger") or "").strip()
                handling = (obj.get("how_to_handle") or "").strip()
                if trigger and handling:
                    obj_lines.append(f"- **{trigger}**: {handling}")
        if obj_lines:
            parts.append("## Objection Handling\n" + "\n".join(obj_lines))

    # Max objection retries
    mor = sections.get("max_objection_retries", {})
    if isinstance(mor, dict) and mor.get("enabled"):
        max_val = mor.get("max", 3)
        prompt = (mor.get("prompt") or "").strip()
        if prompt:
            parts.append(f"### Max Objection Retries\n{prompt.replace('{max}', str(max_val))}")

    # Common situations
    situations = sections.get("common_situations", [])
    if situations:
        sit_lines = []
        for sit in situations:
            if isinstance(sit, dict):
                situation = (sit.get("situation") or "").strip()
                handling = (sit.get("handling") or "").strip()
                if situation and handling:
                    sit_lines.append(f"- **{situation}**: {handling}")
        if sit_lines:
            parts.append("## Common Situations\n" + "\n".join(sit_lines))

    # === CUSTOM SECTIONS ===
    custom_sections = agent.get("custom_sections", [])
    for cs in custom_sections:
        if isinstance(cs, dict) and cs.get("enabled"):
            label = cs.get("label", "Custom")
            prompt = (cs.get("prompt") or "").strip()
            if prompt:
                parts.append(f"## {label}\n{prompt}")

    # === FREEFORM PROMPT ===
    freeform = (agent.get("prompt") or "").strip()
    if freeform:
        parts.append(f"## Additional Instructions\n{freeform}")

    return "\n\n".join(parts).strip()


def compile_agent_goals_summary(agent: dict[str, Any] | None) -> str:
    """Compile just the agent's goals for injection into follow-up determination
    and transfer classifier context. Returns a short summary string."""
    if not agent:
        return ""
    sections = agent.get("sections", {})
    goal_data = sections.get("agent_goal", {})
    selected = goal_data.get("selected", [])
    goals = goal_data.get("goals", {})

    if not selected:
        return ""

    parts = []
    for key in selected:
        entry = goals.get(key, {})
        label = entry.get("label", key)
        prompt = (entry.get("prompt") or "").strip()
        if prompt:
            parts.append(f"- {label}: {prompt}")
        else:
            parts.append(f"- {label}")

    return "\n".join(parts)


def compile_agent_capabilities(
    agent: dict[str, Any] | None,
    services: list[dict[str, Any]] | None = None,
    supported_languages: list[str] | None = None,
) -> str:
    """Compile agent capabilities summary for the transfer classifier.

    Clearly labeled as "Reply Agent Context — for your reference, these are NOT your rules."
    Includes: goals, tools, services, languages, lead source.
    """
    if not agent:
        return ""

    parts = [
        "## Reply Agent Context — for your reference, these are NOT your rules.\n"
        "Use this to understand what the reply agent CAN handle, so you can judge "
        '"within scope" vs "beyond scope."\n'
    ]

    # Goals
    goals_text = compile_agent_goals_summary(agent)
    if goals_text:
        parts.append(f"**Agent Goals:**\n{goals_text}")

    # Enabled tools
    tools = agent.get("enabled_tools", [])
    if tools:
        parts.append(f"**Enabled Tools:** {', '.join(tools)}")

    # Services
    if services:
        svc_names = [s.get("name", "") for s in services if s.get("name")]
        if svc_names:
            svc_list = ", ".join(svc_names)
            parts.append(f"**Services:** {svc_list}")

    # Languages
    if supported_languages:
        parts.append(f"**Supported Languages:** {', '.join(supported_languages)}")

    # Lead source
    sections = agent.get("sections", {})
    lead_source = sections.get("lead_source", {})
    selected_sources = lead_source.get("selected", [])
    if selected_sources:
        sources = lead_source.get("sources", {})
        source_labels = []
        for key in selected_sources:
            entry = sources.get(key, {})
            source_labels.append(entry.get("label", key))
        parts.append(f"**Lead Sources:** {', '.join(source_labels)}")

    return "\n\n".join(parts).strip()


def get_agent_lead_source(agent: dict[str, Any] | None) -> str:
    """Get compiled lead source text for an agent."""
    if not agent:
        return ""
    return _compile_multi_select_sources(agent.get("sections", {}).get("lead_source"))


def get_agent_urgency_scarcity(agent: dict[str, Any] | None) -> str:
    """Get urgency + scarcity text from an agent (for follow-up/media contexts)."""
    if not agent:
        return ""
    sections = agent.get("sections", {})
    parts = []
    for key, label in [("urgency", "Urgency"), ("scarcity", "Scarcity")]:
        sec = sections.get(key, {})
        if isinstance(sec, dict) and sec.get("enabled"):
            prompt = (sec.get("prompt") or "").strip()
            if prompt:
                parts.append(f"### {label}\n{prompt}")
    return "\n\n".join(parts)


# =========================================================================
# INTERNAL HELPERS
# =========================================================================


def _compile_multi_select_goals(data: dict[str, Any] | None) -> str:
    if not data:
        return ""
    selected = data.get("selected", [])
    goals = data.get("goals", {})
    if not selected:
        return ""

    parts = []
    for key in selected:
        entry = goals.get(key, {})
        label = entry.get("label", key)
        prompt = (entry.get("prompt") or "").strip()
        if prompt:
            parts.append(f"**{label}:** {prompt}")
        else:
            parts.append(f"**{label}**")
    return "\n".join(parts)


def _compile_multi_select_sources(data: dict[str, Any] | None) -> str:
    if not data:
        return ""
    selected = data.get("selected", [])
    sources = data.get("sources", {})
    if not selected:
        return ""

    parts = []
    for key in selected:
        entry = sources.get(key, {})
        label = entry.get("label", key)
        prompt = (entry.get("prompt") or "").strip()
        if prompt:
            parts.append(f"**{label}:** {prompt}")
        else:
            parts.append(f"**{label}**")
    return "\n".join(parts)


def _compile_radio(data: dict[str, Any] | None) -> str:
    if not data:
        return ""
    selected = data.get("selected", "")
    options = data.get("options", {})
    if not selected or selected not in options:
        return ""
    prompt = (options[selected].get("prompt") or "").strip()
    return prompt


def compile_post_booking_for_upcoming(
    agent: dict[str, Any] | None,
    matched_service: dict[str, Any] | None = None,
    prep_delivery_prompt: str = "",
) -> str:
    """Compile post-booking sections for a lead with an UPCOMING appointment.

    Includes: post_booking_behavior, prep delivery instructions, upsell_add_ons.
    """
    if not agent:
        return ""
    pb = agent.get("sections", {}).get("post_booking", {})
    if not pb:
        return ""

    parts: list[str] = []

    # Post-booking behavior
    behavior = pb.get("post_booking_behavior", {})
    if isinstance(behavior, dict) and behavior.get("enabled"):
        prompt = (behavior.get("prompt") or "").strip()
        if prompt:
            parts.append(f"## Post-Booking Mode\n{prompt}")

    # Pre-appointment prep delivery (global delivery rule + per-service prep content)
    prep_instructions = ""
    if matched_service:
        prep_instructions = (matched_service.get("prep_instructions") or "").strip()
    prep_prompt = prep_delivery_prompt.strip()
    if prep_prompt and prep_instructions:
        parts.append(
            "### Pre-Appointment Prep\n"
            f"{prep_prompt}\n\n"
            f"Prep Instructions for this service:\n{prep_instructions}"
        )
    elif prep_prompt:
        parts.append(f"### Pre-Appointment Prep\n{prep_prompt}")

    # upsell_add_ons removed — upsell/cross-sell moved to post-appointment automation

    return "\n\n".join(parts).strip()


def compile_post_booking_for_completed(
    agent: dict[str, Any] | None,
    matched_service: dict[str, Any] | None = None,
) -> str:
    """Compile post-booking sections for a lead with a COMPLETED appointment (last 30 days).

    Includes: post_appointment_followup, schedule_next_visit, request_review, request_referral.
    """
    if not agent:
        return ""
    pb = agent.get("sections", {}).get("post_booking", {})
    if not pb:
        return ""

    parts: list[str] = []

    # Post-appointment follow-up
    followup = pb.get("post_appointment_followup", {})
    if isinstance(followup, dict) and followup.get("enabled"):
        prompt = (followup.get("prompt") or "").strip()
        if prompt:
            parts.append(f"## Post-Appointment Follow-Up\n{prompt}")

    # Schedule next visit (pull rebooking_interval from matched service)
    schedule = pb.get("schedule_next_visit", {})
    if isinstance(schedule, dict) and schedule.get("enabled"):
        prompt = (schedule.get("prompt") or "").strip()
        interval = ""
        if matched_service:
            interval = (matched_service.get("rebooking_interval") or "").strip()
        if prompt:
            if interval:
                parts.append(f"### Schedule Next Visit\n{prompt}\n\nRebooking interval for this service: {interval}")
            else:
                parts.append(f"### Schedule Next Visit\n{prompt}")

    # Request review
    review = pb.get("request_review", {})
    if isinstance(review, dict) and review.get("enabled"):
        prompt = (review.get("prompt") or "").strip()
        if prompt:
            review_links = review.get("review_links", [])
            if review_links:
                primary = review_links[0]
                link_text = f"\n\nPrimary review link: {primary.get('url', '')} ({primary.get('label', 'Review')})"
                if len(review_links) > 1:
                    others = ", ".join(f"{l.get('url', '')} ({l.get('label', '')})" for l in review_links[1:])
                    link_text += f"\nAlternative links: {others}"
                parts.append(f"### Request Review\n{prompt}{link_text}")
            else:
                parts.append(f"### Request Review\n{prompt}")

    # request_referral removed — referral doesn't have a completion path via SMS
    # suggest_related_service (upsell/cross-sell) handled by post-appointment automation only

    return "\n\n".join(parts).strip()


def _compile_discovery_questions(data: dict[str, Any] | None) -> str:
    if not data or not data.get("enabled"):
        return ""
    prompt = (data.get("prompt") or "").strip()
    questions = data.get("questions", [])

    parts = []
    if prompt:
        parts.append(prompt)
    if questions:
        q_lines = []
        for q in questions:
            if isinstance(q, dict):
                text = (q.get("text") or "").strip()
                service = (q.get("service") or "").strip()
                if text:
                    line = f"- {text}"
                    if service:
                        line += f" (for {service})"
                    q_lines.append(line)
        if q_lines:
            parts.append("Questions to ask:\n" + "\n".join(q_lines))

    return "\n\n".join(parts)
