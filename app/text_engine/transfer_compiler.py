"""Compiler: system_config.transfer → transfer classifier prompt.

Reads the transfer JSONB and compiles:
- Transfer philosophy
- Transfer scenarios (enabled only)
- Do-not-transfer rules (enabled only)
- Opt-out sections (phrases, solicitor, not-opt-out)
- Agent capabilities summary (auto-compiled)
"""

from __future__ import annotations

from typing import Any


def compile_transfer_prompt(
    transfer_config: dict[str, Any] | None,
    agent_capabilities: str = "",
) -> str:
    """Compile the full transfer classifier system prompt content.

    This replaces the {transfer_to_human_prompt} variable in _TTH_SYSTEM_PROMPT.
    The hardcoded sections (call_history_context, continue_criteria, decision_tree,
    output_format) stay in classification.py.
    """
    if not transfer_config:
        return ""

    parts: list[str] = []

    # Agent capabilities (clearly labeled as reference, not rules)
    if agent_capabilities:
        parts.append(agent_capabilities)

    # Transfer philosophy
    philosophy = (transfer_config.get("philosophy") or "").strip()
    if philosophy:
        parts.append(f"## Transfer Philosophy\n{philosophy}")

    # Transfer scenarios (enabled only)
    scenarios = transfer_config.get("scenarios", [])
    enabled_scenarios = [s for s in scenarios if isinstance(s, dict) and s.get("enabled", True)]
    if enabled_scenarios:
        scenario_parts = []
        for sc in enabled_scenarios:
            title = sc.get("title", "Scenario")
            transfer_when = (sc.get("transfer_when") or "").strip()
            dont_transfer_when = (sc.get("dont_transfer_when") or "").strip()
            lines = [f"### {title}"]
            if transfer_when:
                lines.append(f"**Transfer when:** {transfer_when}")
            if dont_transfer_when:
                lines.append(f"**Do NOT transfer when:** {dont_transfer_when}")
            scenario_parts.append("\n".join(lines))
        parts.append("## Transfer Scenarios\n\n" + "\n\n".join(scenario_parts))

    # Do-not-transfer rules (enabled only)
    dnt_rules = transfer_config.get("do_not_transfer", [])
    enabled_dnt = [r for r in dnt_rules if isinstance(r, dict) and r.get("enabled", True)]
    if enabled_dnt:
        dnt_lines = []
        for rule in enabled_dnt:
            title = rule.get("title", "Rule")
            desc = (rule.get("description") or "").strip()
            if desc:
                dnt_lines.append(f"- **{title}:** {desc}")
            else:
                dnt_lines.append(f"- **{title}**")
        parts.append("## Do NOT Transfer\nClassify as Continue for these situations:\n" + "\n".join(dnt_lines))

    return "\n\n".join(parts).strip()


def compile_opt_out_sections(transfer_config: dict[str, Any] | None) -> str:
    """Compile opt-out/solicitor sections for the transfer classifier.

    Replaces the hardcoded opt_out_criteria in _TTH_SYSTEM_PROMPT.
    """
    if not transfer_config:
        return ""

    opt_out = transfer_config.get("opt_out", {})
    if not opt_out:
        return ""

    parts: list[str] = []

    # Opt-out phrases
    phrases = opt_out.get("phrases", {})
    if isinstance(phrases, dict) and phrases.get("enabled", True):
        prompt = (phrases.get("prompt") or "").strip()
        if prompt:
            parts.append(f"### Opt-Out Phrases\n{prompt}")

    # Solicitor detection
    solicitor = opt_out.get("solicitor", {})
    if isinstance(solicitor, dict) and solicitor.get("enabled", True):
        prompt = (solicitor.get("prompt") or "").strip()
        if prompt:
            parts.append(f"### Solicitor / Salesperson Detection\n{prompt}")

    # Not-opt-out exceptions
    not_opt_out = opt_out.get("not_opt_out", {})
    if isinstance(not_opt_out, dict) and not_opt_out.get("enabled", True):
        prompt = (not_opt_out.get("prompt") or "").strip()
        if prompt:
            parts.append(f"### DO NOT Classify as Opt Out\n{prompt}")

    if not parts:
        return ""

    return "## Opt Out / Solicitor Rules\n\n" + "\n\n".join(parts)
