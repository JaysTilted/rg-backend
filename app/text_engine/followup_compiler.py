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


def compile_followup_preferences(follow_up: dict[str, Any] | None) -> str:
    """Compile follow-up preferences for the text generator prompt.

    This is injected as {followup_agent_prompt} in _FOLLOWUP_AGENT_SYSTEM.
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
