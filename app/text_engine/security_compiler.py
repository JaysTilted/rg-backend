"""Compiler: system_config.security → security protections + compliance rules.

Reads the security JSONB and compiles:
- Security protections (3 toggles → replaces _SECURITY_RULES)
- Compliance rules (enabled presets + custom → replaces security_prompt)
- Term replacements (enabled entries → replaces term_replacements + dash cleanup)
"""

from __future__ import annotations

from typing import Any


def compile_security_protections(config: dict[str, Any] | None) -> str:
    """Compile security protection rules (prompt/output protection, jailbreak rejection).

    Replaces the hardcoded _SECURITY_RULES constant in agent.py.
    """
    if not config:
        return ""

    protections = config.get("protections", {})
    if not protections:
        return ""

    parts: list[str] = ["## CRITICAL SECURITY RULES - NEVER VIOLATE\n"]

    # Prompt protection
    pp = protections.get("prompt_protection", {})
    if isinstance(pp, dict) and pp.get("enabled", True):
        prompt = (pp.get("prompt") or "").strip()
        if prompt:
            parts.append(prompt)

    # Output protection
    op = protections.get("output_protection", {})
    if isinstance(op, dict) and op.get("enabled", True):
        prompt = (op.get("prompt") or "").strip()
        if prompt:
            parts.append(prompt)

    # Jailbreak rejection
    jr = protections.get("jailbreak_rejection", {})
    if isinstance(jr, dict) and jr.get("enabled", True):
        prompt = (jr.get("prompt") or "").strip()
        if prompt:
            parts.append(prompt)

    if len(parts) <= 1:
        return ""

    return "\n\n".join(parts).strip()


def compile_compliance_rules(config: dict[str, Any] | None) -> str:
    """Compile enabled compliance rules into a single prompt string.

    Replaces reading from the security_prompt text field.
    Returns "" if no compliance rules are enabled.
    """
    if not config:
        return ""

    rules: list[str] = []

    # Preset compliance rules
    preset = config.get("compliance_rules", [])
    for rule in preset:
        if isinstance(rule, dict) and rule.get("enabled"):
            prompt = (rule.get("prompt") or "").strip()
            label = rule.get("label", "")
            if prompt:
                rules.append(f"- **{label}:** {prompt}" if label else f"- {prompt}")

    # Custom compliance rules
    custom = config.get("custom_compliance_rules", [])
    for rule in custom:
        if isinstance(rule, dict) and rule.get("enabled"):
            prompt = (rule.get("prompt") or "").strip()
            label = rule.get("label", "")
            if prompt:
                rules.append(f"- **{label}:** {prompt}" if label else f"- {prompt}")

    if not rules:
        return ""

    return "\n".join(rules)


def get_term_replacements(config: dict[str, Any] | None) -> list[dict[str, str]]:
    """Extract enabled term replacements from security config.

    Returns list of {"find": "...", "replace": "..."} dicts for the
    deterministic replacement engine in security.py.

    Default entries (is_default: true) are expanded into actual regex patterns.
    The consolidated "All dashes" default becomes 4 separate replacements.
    """
    if not config:
        return []

    replacements = config.get("term_replacements", [])
    if not replacements:
        return []

    enabled = []
    for entry in replacements:
        if not isinstance(entry, dict):
            continue
        if entry.get("enabled") is False:
            continue

        # Default entries get expanded to actual patterns
        if entry.get("is_default"):
            # Consolidated dash cleanup → expand to 4 actual replacements
            enabled.extend([
                {"find": "\u2014", "replace": ","},     # em dash
                {"find": "\u2013", "replace": ","},     # en dash
                {"find": "--", "replace": ","},          # double hyphen
                {"find": " - ", "replace": ", "},        # spaced hyphen
            ])
            continue

        find = (entry.get("find") or "").strip()
        replace_with = entry.get("replace", "")
        if find:
            enabled.append({"find": find, "replace": replace_with})

    return enabled
