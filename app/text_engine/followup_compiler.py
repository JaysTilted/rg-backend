"""Compiler: system_config.follow_up → follow-up specific prompt sections.

Reads follow_up config and compiles:
- Follow-up context (renamed from business_context)
- Positions (if enabled)
- Re-engagement angles
- Tone overrides
- Appointment context rules
- Banned phrases (follow-up specific)
- Service examples
- Cadence timing
"""

from __future__ import annotations

from typing import Any


_BRANCH_GUIDANCE = {
    "counter_offer": (
        "This lead pushed back on commercial structure (offered alt pricing like "
        "'$X per closed deal' / 'pay on results only'). The redirect-to-call already "
        "fired in the reply path. For this followup: address the pricing-model question "
        "head-on, briefly. Acknowledge the alt model exists for our larger setups (results-tied "
        "billing for high-volume operators), then pivot back to the 10-min call. "
        "Do NOT re-debate price via SMS."
    ),
    "vetting": (
        "This lead questioned legitimacy (asked who you are / scam / suspicion). The "
        "vetting reply already named Iron Automations and dropped the URL. For this "
        "followup: briefly re-establish trust without sounding defensive. Reference one "
        "concrete proof point (an industry stat, a real shop's outcome, the watch link). "
        "Do NOT repeat the company-name introduction verbatim."
    ),
    "niche_correction": (
        "This lead corrected the trade we assumed (e.g. drain cleaning vs plumbing, "
        "electrical vs HVAC). Use their actual trade name in the followup, NEVER the "
        "wrong one. Same automation pitch — adapt the example to their real category."
    ),
    "wrong_line": (
        "This contact said the line is for customer service inquiries only. DO NOT send "
        "any further followups. Return early without generating a message — this lead "
        "should be skipped entirely going forward."
    ),
    "referral_handoff": (
        "This lead was already routed to a human (referred us to a partner/associate). "
        "DO NOT send a followup — a person from our team is handling the referred "
        "contact directly. Return early without generating a message."
    ),
    "wrong_number_pivot": (
        "This contact said wrong number. DO NOT send any further followups — we already "
        "sent one polite reply with the URL referral. Return early without generating "
        "a message."
    ),
    "competitive_positioning": (
        "This lead asked how Iron Automations differs from another channel (FB ads, "
        "Google ads, named agency). The reply already gave the one-line differentiator "
        "(we automate response/booking, not lead-gen). For this followup: lean into "
        "the differentiator with one concrete tactical example, then pivot back to the call."
    ),
    "immediate_call_request": (
        "This lead asked to be called immediately. The reply offered today's calendar "
        "slots. For this followup: acknowledge they wanted action fast — reoffer the "
        "next earliest slot today or tomorrow morning. Stay urgent in tone."
    ),
    "booking_management": (
        "This lead asked about cancelling/rescheduling an appointment. The reply asked "
        "them to confirm intent (cancel vs reschedule). For this followup: nudge for the "
        "confirmation if they haven't replied, e.g. 'just checking — did you want to cancel "
        "outright or reschedule?'"
    ),
}


def compile_branch_context(contact_tags: list[str] | None) -> str:
    """Detect branch:* tags on the contact and return per-branch guidance for the
    followup agent's system prompt. Returns empty string when no branch tag is set.

    For ``wrong_line`` / ``referral_handoff`` / ``wrong_number_pivot`` the guidance
    instructs the followup agent to skip the message entirely (the message_scheduler
    won't enforce skip on its own — the agent must decide).
    """
    if not contact_tags:
        return ""
    branches: list[str] = []
    for tag in contact_tags:
        if isinstance(tag, str) and tag.lower().startswith("branch:"):
            branch = tag.split(":", 1)[1].strip().lower()
            if branch and branch not in branches:
                branches.append(branch)
    if not branches:
        return ""
    blocks: list[str] = []
    for branch in branches:
        guidance = _BRANCH_GUIDANCE.get(branch)
        if guidance:
            blocks.append(f"### Branch: {branch}\n{guidance}")
        else:
            blocks.append(f"### Branch: {branch}\n(no specific guidance — handle generically)")
    header = "## Branch Context\nThis lead came in via a branch reply path. Tailor the followup accordingly:\n"
    return header + "\n\n".join(blocks)


def compile_followup_preferences(
    follow_up: dict[str, Any] | None,
    contact_tags: list[str] | None = None,
) -> str:
    """Compile follow-up preferences for the text generator prompt.

    This is injected as {followup_agent_prompt} in _FOLLOWUP_AGENT_SYSTEM.

    If the contact has a ``branch:*`` tag (set by the reply agent's mark_branch
    tool), branch-specific guidance is appended so the followup ladder tailors
    its copy to the reply path the lead came in through.
    """
    if not follow_up:
        return ""

    sections = follow_up.get("sections", {})
    custom_sections = follow_up.get("custom_sections", [])
    freeform = (follow_up.get("prompt") or "").strip()

    parts: list[str] = []

    # Follow-up context (renamed from business_context)
    ctx = (sections.get("followup_context") or "").strip()
    if ctx:
        parts.append(f"## Follow-Up Context\n{ctx}")

    # Positions (if enabled)
    if sections.get("positions_enabled"):
        positions = sections.get("follow_up_positions", [])
        if positions:
            pos_lines = []
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                num = pos.get("position", 0)
                timing = pos.get("timing_label", "")
                goal = (pos.get("goal") or "").strip()
                approach = (pos.get("approach") or "").strip()
                if goal or approach:
                    line = f"**FU#{num}** ({timing})"
                    if goal:
                        line += f": Goal: {goal}"
                    if approach:
                        line += f". Approach: {approach}"
                    pos_lines.append(line)
            if pos_lines:
                parts.append("## Follow-Up Positions\n" + "\n".join(pos_lines))

    # Re-engagement angles
    angles = sections.get("re_engagement_angles", [])
    if angles:
        angle_lines = []
        for angle in angles:
            if not isinstance(angle, dict):
                continue
            context = (angle.get("context") or "").strip()
            approach = (angle.get("approach") or "").strip()
            if context and approach:
                angle_lines.append(f"- **{context}:** {approach}")
        if angle_lines:
            parts.append("## Re-Engagement Angles\n" + "\n".join(angle_lines))

    # Max offer pushes
    mop = sections.get("max_offer_pushes", {})
    if isinstance(mop, dict) and mop.get("enabled"):
        max_val = mop.get("max", 2)
        prompt = (mop.get("prompt") or "").strip()
        if prompt:
            parts.append(f"## Max Offer Pushes\n{prompt.replace('{max}', str(max_val))}")

    # Tone overrides
    tone = sections.get("tone_overrides", {})
    if isinstance(tone, dict) and tone.get("enabled"):
        prompt = (tone.get("prompt") or "").strip()
        if prompt:
            parts.append(f"## Tone\n{prompt}")

    # Timing follow-up rules
    timing = sections.get("timing_followup_rules", {})
    if isinstance(timing, dict) and timing.get("enabled"):
        prompt = (timing.get("prompt") or "").strip()
        if prompt:
            parts.append(f"## Timing Follow-Up Rules\n{prompt}")

    # Service examples
    examples = sections.get("service_examples", [])
    if examples:
        ex_lines = []
        for ex in examples:
            if not isinstance(ex, dict):
                continue
            service = (ex.get("service") or "").strip()
            approach = (ex.get("approach") or "").strip()
            if service and approach:
                ex_lines.append(f"- **{service}:** {approach}")
        if ex_lines:
            parts.append("## Service-Specific Approaches\n" + "\n".join(ex_lines))

    # Custom sections
    for cs in custom_sections:
        if isinstance(cs, dict) and cs.get("enabled"):
            label = cs.get("label", "Custom")
            prompt = (cs.get("prompt") or "").strip()
            if prompt:
                parts.append(f"## {label}\n{prompt}")

    # Freeform prompt
    if freeform:
        parts.append(f"## Additional Instructions\n{freeform}")

    # Branch context — only present when the reply agent fired one of the branch
    # scripts and tagged the contact. Appended last so it sits after baseline
    # preferences in the followup agent's system prompt.
    branch_block = compile_branch_context(contact_tags)
    if branch_block:
        parts.append(branch_block)

    return "\n\n".join(parts).strip()


def compile_followup_banned_phrases(follow_up: dict[str, Any] | None) -> list[str]:
    """Get follow-up specific banned phrases (separate from bot_persona banned phrases)."""
    if not follow_up:
        return []
    sections = follow_up.get("sections", {})
    return sections.get("banned_phrases", [])


def compile_appointment_context(follow_up: dict[str, Any] | None) -> str:
    """Compile appointment context rules for the follow-up text generator.

    Replaces the hardcoded <appointment_context> section content.
    """
    if not follow_up:
        return ""

    sections = follow_up.get("sections", {})
    rules = sections.get("appointment_context_rules", {})

    if not isinstance(rules, dict) or not rules.get("enabled", True):
        return ""

    parts: list[str] = []

    cancelled = (rules.get("cancelled") or "").strip()
    if cancelled:
        parts.append(f"**Cancelled appointment:**\n{cancelled}")

    no_show = (rules.get("no_show") or "").strip()
    if no_show:
        parts.append(f"**No-show (missed appointment):**\n{no_show}")

    if not parts:
        return ""

    return "\n\n".join(parts)


def compile_no_finality_language(follow_up: dict[str, Any] | None) -> str:
    """Compile no-finality-language rule for follow-up text generator."""
    if not follow_up:
        return ""
    sections = follow_up.get("sections", {})
    nfl = sections.get("no_finality_language", {})
    if not isinstance(nfl, dict) or not nfl.get("enabled", True):
        return ""
    return (nfl.get("prompt") or "").strip()


def get_cadence_timing(follow_up: dict[str, Any] | None) -> list[str] | None:
    """Get custom cadence timing if configured, else None for defaults."""
    if not follow_up:
        return None
    sections = follow_up.get("sections", {})
    cadence = sections.get("cadence_timing", {})
    if not isinstance(cadence, dict):
        return None
    if not cadence.get("customized"):
        return None
    timings = cadence.get("timings", [])
    return timings if timings else None


def compile_followup_positions_for_determination(
    follow_up: dict[str, Any] | None,
) -> str:
    """Compile follow-up positions context for the determination agent.

    Only included if positions_enabled is True.
    """
    if not follow_up:
        return ""
    sections = follow_up.get("sections", {})
    if not sections.get("positions_enabled"):
        return ""

    positions = sections.get("follow_up_positions", [])
    if not positions:
        return ""

    lines = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        num = pos.get("position", 0)
        timing = pos.get("timing_label", "")
        goal = (pos.get("goal") or "").strip()
        if goal:
            lines.append(f"- FU#{num} ({timing}): {goal}")

    if not lines:
        return ""

    return "## Configured Follow-Up Positions\n" + "\n".join(lines)
