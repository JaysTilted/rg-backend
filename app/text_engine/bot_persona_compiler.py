"""Compiler: system_config.bot_persona → prompt text.

Reads the structured bot_persona JSONB and compiles enabled sections
into markdown for injection into LLM system prompts.

Different consumers get different section subsets:
- Reply agent: ALL sections
- Follow-up text generator: most sections (excludes ai_disclosure, skip_greetings)
- Media selectors: identity + tone + humor + message_length only
"""

from __future__ import annotations

from typing import Any


# Section keys included per consumer filter
_FOLLOWUP_SECTIONS = frozenset({
    "identity", "tone", "humor", "emojis", "message_length",
    "name_usage",
    "be_direct", "mirror_style", "stories_examples",
    "casual_language", "punctuation_style", "typos",
    "sarcasm", "light_swearing", "sentence_fragments",
    "validate_feelings", "hype_celebrate", "remember_details",
    "banned_phrases",
})

_MEDIA_SECTIONS = frozenset({
    "identity", "tone", "humor", "message_length",
})

# Group definitions matching the UI layout
_GROUPS = [
    ("Identity", ["identity", "ai_disclosure"]),
    ("How To Talk", [
        "tone", "punctuation_style", "humor", "emojis", "message_length",
        "typos", "skip_greetings", "be_direct", "mirror_style", "stories_examples",
        "casual_language", "sarcasm", "light_swearing", "sentence_fragments",
    ]),
    ("How To Connect", [
        "name_usage", "validate_feelings", "hype_celebrate", "remember_details",
    ]),
    ("Restrictions", ["banned_phrases"]),
]

# Human-readable labels for each section key
_SECTION_LABELS = {
    "identity": "Identity",
    "ai_disclosure": "AI Disclosure",
    "tone": "Tone & Style",
    "name_usage": "Name Usage",
    "punctuation_style": "Punctuation Style",
    "humor": "Humor",
    "emojis": "Emojis",
    "message_length": "Max Message Length",
    "typos": "Natural Typos",
    "skip_greetings": "Skip Greetings",
    "be_direct": "Be Direct",
    "mirror_style": "Mirror Style",
    "stories_examples": "Stories & Examples",
    "validate_feelings": "Validate Feelings",
    "hype_celebrate": "Hype & Celebrate",
    "remember_details": "Remember Details",
    "casual_language": "Casual Language",
    "sarcasm": "Sarcasm",
    "light_swearing": "Light Swearing",
    "sentence_fragments": "Sentence Fragments",
    "banned_phrases": "Banned Phrases",
}


def compile_bot_persona(
    config: dict[str, Any] | None,
    *,
    company: str = "",
    section_filter: str | None = None,
) -> str:
    """Compile bot_persona config into prompt text.

    Args:
        config: system_config.bot_persona dict (sections + custom_sections)
        company: Company name for identity compilation
        section_filter: None = all sections, "followup" = follow-up subset,
                        "media" = media selector subset

    Returns:
        Compiled markdown string, or "" if no config.
    """
    if not config:
        return ""

    sections = config.get("sections", {})
    if not sections:
        return ""

    allowed = None
    if section_filter == "followup":
        allowed = _FOLLOWUP_SECTIONS
    elif section_filter == "media":
        allowed = _MEDIA_SECTIONS

    parts: list[str] = []

    for group_name, group_keys in _GROUPS:
        group_parts: list[str] = []

        for key in group_keys:
            if allowed is not None and key not in allowed:
                continue

            section_data = sections.get(key)
            if not section_data:
                continue

            text = _compile_section(key, section_data, company)
            if text:
                group_parts.append(text)

        if group_parts:
            parts.append(f"## {group_name}\n")
            parts.extend(group_parts)

    # Custom sections
    custom_sections = config.get("custom_sections", [])
    if custom_sections:
        custom_parts: list[str] = []
        for cs in custom_sections:
            if not isinstance(cs, dict):
                continue
            if not cs.get("enabled", False):
                continue
            label = cs.get("label", "Custom")
            prompt = (cs.get("prompt") or "").strip()
            if prompt:
                custom_parts.append(f"### {label}\n{prompt}\n")

        if custom_parts:
            parts.append("## Custom\n")
            parts.extend(custom_parts)

    return "\n".join(parts).strip()


def _compile_section(key: str, data: dict[str, Any], company: str) -> str:
    """Compile a single bot_persona section into text."""
    label = _SECTION_LABELS.get(key, key.replace("_", " ").title())

    if key == "identity":
        return _compile_identity(data, company)
    elif key == "ai_disclosure":
        return _compile_radio(label, data)
    elif key == "casual_language":
        return _compile_radio(label, data)
    elif key == "punctuation_style":
        return _compile_radio(label, data)
    elif key == "name_usage":
        return _compile_radio(label, data)
    elif key == "tone":
        return _compile_tone(data)
    elif key == "emojis":
        return _compile_emojis(data)
    elif key == "banned_phrases":
        return _compile_banned_phrases(data)
    else:
        # Standard toggle_prompt
        return _compile_toggle(label, data)


def _compile_identity(data: dict[str, Any], company: str) -> str:
    """Compile identity section: name + role + extra."""
    name = (data.get("name") or "").strip()
    role = (data.get("role") or "").strip()
    extra = (data.get("extra_prompt") or "").strip()

    if not name:
        return ""

    parts = []
    if role and company:
        parts.append(f"You're {name}, a {role} at {company}.")
    elif role:
        parts.append(f"You're {name}, a {role}.")
    elif company:
        parts.append(f"You're {name} at {company}.")
    else:
        parts.append(f"You're {name}.")

    if extra:
        parts.append(extra)

    return "### Identity\n" + " ".join(parts) + "\n"


def _compile_radio(label: str, data: dict[str, Any]) -> str:
    """Compile radio_prompt section (ai_disclosure)."""
    selected = data.get("selected", "")
    options = data.get("options", {})
    if not selected or selected not in options:
        return ""
    prompt = (options[selected].get("prompt") or "").strip()
    if not prompt:
        return ""
    return f"### {label}\n{prompt}\n"


def _compile_tone(data: dict[str, Any]) -> str:
    """Compile tone section: traits + prompt."""
    traits = data.get("traits", [])
    prompt = (data.get("prompt") or "").strip()

    if not traits and not prompt:
        return ""

    parts = []
    if traits:
        parts.append(f"Tone: {', '.join(traits)}.")
    if prompt:
        parts.append(prompt)

    return "### Tone & Style\n" + " ".join(parts) + "\n"


def _compile_emojis(data: dict[str, Any]) -> str:
    """Compile emoji section: enabled + allowed list + prompt."""
    if not data.get("enabled", False):
        return ""

    prompt = (data.get("prompt") or "").strip()
    allowed = data.get("allowed", [])

    parts = []
    if prompt:
        parts.append(prompt)
    if allowed:
        parts.append(f"Allowed emojis: {' '.join(allowed)}")
    elif not prompt:
        parts.append("Use emojis at your best judgment.")

    return "### Emojis\n" + " ".join(parts) + "\n"


def _compile_banned_phrases(data: dict[str, Any]) -> str:
    """Compile banned phrases list."""
    items = data.get("items", [])
    if not items:
        return ""
    quoted = [f'"{p}"' for p in items if p.strip()]
    if not quoted:
        return ""
    return f"### Banned Phrases\nNever use: {', '.join(quoted)}\n"


def _compile_toggle(label: str, data: dict[str, Any]) -> str:
    """Compile a toggle_prompt section. Returns "" if disabled."""
    if not data.get("enabled", False):
        return ""
    prompt = (data.get("prompt") or "").strip()
    if not prompt:
        return ""
    return f"### {label}\n{prompt}\n"
