"""Compiler: system_config.case_studies → reference text + media pool.

Reads the case_studies array and compiles:
- Reference text for agent/follow-up prompts (filtered by agent/service)
- Media pool entries (for injection into media library)
"""

from __future__ import annotations

from typing import Any


def compile_case_studies(
    case_studies: list[dict[str, Any]] | None,
    agent_key: str | None = None,
    service_filter: list[str] | None = None,
) -> str:
    """Compile case studies into reference text for an agent prompt.

    Args:
        case_studies: system_config.case_studies array
        agent_key: If set, only include case studies tagged for this agent
        service_filter: If set, only include case studies for these services

    Returns:
        Compiled markdown with case study details.
    """
    if not case_studies:
        return ""

    filtered = _filter_case_studies(case_studies, agent_key, service_filter)
    if not filtered:
        return ""

    parts: list[str] = []
    for cs in filtered:
        title = (cs.get("title") or "").strip()
        details = (cs.get("details") or "").strip()
        if not title:
            continue

        entry_parts = [f"### {title}"]
        if details:
            entry_parts.append(details)

        # Include video URL if present
        video = (cs.get("video_url") or "").strip()
        if video:
            entry_parts.append(f"Video: {video}")

        # Include service tags for context
        services = cs.get("services", [])
        if services:
            entry_parts.append(f"Services: {', '.join(services)}")

        # Deployment style
        style = cs.get("deployment_style", "natural")
        dp = (cs.get("deployment_prompt") or "").strip()
        if dp:
            entry_parts.append(f"When to use: {dp}")
        elif style == "proactive":
            entry_parts.append(
                "When to use: Actively bring up when you see an opportunity, "
                "even if the lead didn't ask."
            )

        parts.append("\n".join(entry_parts))

    if not parts:
        return ""

    return "## Case Studies\n\n" + "\n\n".join(parts)


def get_case_study_media(
    case_studies: list[dict[str, Any]] | None,
    agent_key: str | None = None,
    path_type: str = "reply",
) -> list[dict[str, Any]]:
    """Extract media items from case studies for injection into media library.

    Returns media items with case study title tagged for context.
    """
    if not case_studies:
        return []

    filtered = _filter_case_studies(case_studies, agent_key)
    media_items: list[dict[str, Any]] = []

    for cs in filtered:
        title = (cs.get("title") or "").strip()
        cs_media = cs.get("media", [])

        for item in cs_media:
            if not isinstance(item, dict):
                continue
            media_items.append({
                "name": item.get("name", f"Case Study: {title}"),
                "url": item.get("url", ""),
                "description": item.get("description", f"Case study image: {title}"),
                "type": item.get("type", "image"),
                "path": [path_type],
                "enabled": True,
                "case_study": title,
            })

    return media_items


def get_case_study_titles(
    case_studies: list[dict[str, Any]] | None,
    agent_key: str | None = None,
) -> list[str]:
    """Get just the titles of filtered case studies (for media selector context)."""
    if not case_studies:
        return []
    filtered = _filter_case_studies(case_studies, agent_key)
    return [cs.get("title", "") for cs in filtered if cs.get("title")]


# =========================================================================
# INTERNAL
# =========================================================================


def _filter_case_studies(
    case_studies: list[dict[str, Any]],
    agent_key: str | None = None,
    service_filter: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Filter case studies by enabled + agent + service."""
    result = []
    for cs in case_studies:
        if not isinstance(cs, dict):
            continue
        if not cs.get("enabled", True):
            continue

        # Agent filter
        if agent_key:
            setter_keys = cs.get("setter_keys", [])
            legacy_agents = cs.get("agents", [])
            scoped = setter_keys or legacy_agents
            if scoped and agent_key not in scoped:
                continue

        # Service filter
        if service_filter:
            services = cs.get("services", [])
            if services and not any(s in service_filter for s in services):
                continue

        result.append(cs)

    return result
