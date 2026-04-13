"""Compiler: system_config.booking → booking config + rules prompt text.

Reads the booking JSONB and compiles:
- Calendar list with descriptions and agent assignments
- Booking links
- Tool rules (enabled only)
- Link rules (enabled only)
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def _agent_match(item: dict[str, Any], agent_key: str | None) -> bool:
    """Return True if item is available to the given agent."""
    if not agent_key:
        return True
    setter_keys = item.get("setter_keys", [])
    legacy_agents = item.get("agents", [])
    scoped = setter_keys or legacy_agents
    return not scoped or agent_key in scoped


def compile_booking_config(
    config: dict[str, Any] | None,
    now_str: str = "",
    agent_key: str | None = None,
) -> str:
    """Compile booking configuration section for the agent prompt.

    Includes booking window, calendars, and links. Filters by agent_key if provided.
    """
    if not config:
        return ""

    parts: list[str] = []
    default_window = config.get("booking_window_days", 10)

    # Booking window context
    if now_str:
        parts.append(f"**Current Date/Time:** {now_str}")

    # Calendars — split by booking_mode
    calendars = config.get("calendars", [])
    enabled_cals = [c for c in calendars if isinstance(c, dict) and c.get("enabled", True) and _agent_match(c, agent_key)]

    # Conversational calendars (mode = "conversational" or "both") — available for booking tools
    tool_cals = [c for c in enabled_cals if c.get("booking_mode", "conversational") in ("conversational", "both")]
    if tool_cals:
        cal_data = []
        for cal in tool_cals:
            window = cal.get("booking_window_days", default_window)
            appt_len = cal.get("appointment_length_minutes")
            entry: dict[str, Any] = {
                "name": cal.get("name", ""),
                "id": cal.get("id", ""),
                "booking_window": f"{window} days",
            }
            if appt_len:
                entry["appointment_length"] = f"{appt_len} minutes"
            if cal.get("description"):
                entry["description"] = cal["description"]
            services = cal.get("services", [])
            if services:
                entry["services"] = services
            cal_data.append(entry)
        parts.append(f"**Calendars (use booking tools):**\n{json.dumps(cal_data, indent=2)}")

    # Calendar booking links (mode = "link_only" or "both") — AI sends these links
    link_cals = [c for c in enabled_cals if c.get("booking_mode", "conversational") in ("link_only", "both")]
    cal_link_lines = []
    for cal in link_cals:
        name = cal.get("name", "Booking")
        cal_id = cal.get("id", "")
        link = cal.get("booking_link_override") or (f"https://api.leadconnectorhq.com/widget/booking/{cal_id}" if cal_id else "")
        desc = cal.get("description", "")
        window = cal.get("booking_window_days", default_window)
        appt_len = cal.get("appointment_length_minutes")
        if link:
            line = f"- **{name}**: {link} (booking window: {window} days"
            if appt_len:
                line += f", {appt_len} min appointment"
            line += ")"
            if desc:
                line += f" — {desc}"
            services = cal.get("services", [])
            if services:
                line += f" | Services: {', '.join(services)}"
            cal_link_lines.append(line)

    # External booking links (manually configured, non-GHL)
    ext_links = config.get("booking_links", [])
    enabled_ext = [l for l in ext_links if isinstance(l, dict) and l.get("enabled", True) and _agent_match(l, agent_key)]
    ext_link_lines = []
    for link in enabled_ext:
        name = link.get("name", "Booking Link")
        url = link.get("url", "")
        desc = link.get("description", "")
        window = link.get("booking_window_days", default_window)
        appt_len = link.get("appointment_length_minutes")
        if url:
            line = f"- **{name}**: {url} (booking window: {window} days"
            if appt_len:
                line += f", {appt_len} min appointment"
            line += ")"
            if desc:
                line += f" — {desc}"
            services = link.get("services", [])
            if services:
                line += f" | Services: {', '.join(services)}"
            ext_link_lines.append(line)

    all_link_lines = cal_link_lines + ext_link_lines
    if all_link_lines:
        parts.append("**Booking Links (send to lead):**\n" + "\n".join(all_link_lines))

    return "\n\n".join(parts)


def compile_booking_rules(config: dict[str, Any] | None) -> str:
    """Compile enabled booking tool rules into prompt text."""
    if not config:
        return ""

    method = config.get("booking_method", "conversational")
    parts: list[str] = []

    # Tool rules (for conversational / both)
    if method in ("conversational", "both", ""):
        tool_rules = config.get("tool_rules", [])
        enabled_rules = [r for r in tool_rules if isinstance(r, dict) and r.get("enabled", True)]
        if enabled_rules:
            parts.append("# BOOKING RULES\n")
            for rule in enabled_rules:
                label = rule.get("label", "")
                prompt = (rule.get("prompt") or "").strip()
                if prompt:
                    parts.append(f"**{label}**\n{prompt}\n")

    # Link rules (for via_link / both)
    if method in ("via_link", "both"):
        link_rules = config.get("link_rules", [])
        enabled_link_rules = [r for r in link_rules if isinstance(r, dict) and r.get("enabled", True)]
        if enabled_link_rules:
            parts.append("# BOOKING LINK RULES\n")
            for rule in enabled_link_rules:
                label = rule.get("label", "")
                prompt = (rule.get("prompt") or "").strip()
                if prompt:
                    parts.append(f"**{label}**\n{prompt}\n")

    # Custom rules
    custom_rules = config.get("custom_rules", [])
    if custom_rules:
        for rule in custom_rules:
            if isinstance(rule, dict) and rule.get("enabled", True):
                label = rule.get("label", "Custom Rule")
                prompt = (rule.get("prompt") or "").strip()
                if prompt:
                    parts.append(f"**{label}**\n{prompt}\n")

    return "\n".join(parts).strip()
