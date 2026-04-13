"""Post-agent security step — term replacements + LLM hygiene/compliance check.

Runs after the agent generates a response but before timeline update and delivery.
Two layers:
1. Deterministic term replacements (runs if configured)
2. LLM security check via classify() (ALWAYS runs)

The LLM check has two rule sets:
- Base hygiene rules (always enforced): catches leaked thoughts, XML tags, markdown,
  LLM-speak, tool remnants, and other output artifacts that break human impersonation.
- Client compliance rules (appended when configured): client-specific rules from the
  Security tab (preset + custom compliance rules).

The compliance rules are ALSO injected into the agent's system prompt (section 1b
in agent.py) so the agent proactively avoids violations. This module is the
safety net — it catches what the agent missed.

Design principle: never blocks delivery. If the LLM call fails, the original
(post-replacement) response is sent with a Slack alert.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from prefect import task

from app.models import PipelineContext, log_prompt
from app.services.ai_client import classify
from app.services.ghl_links import build_ghl_contact_url
from app.text_engine.model_resolver import resolve_model, resolve_temperature
from app.services.mattermost import post_message as post_slack_message

logger = logging.getLogger(__name__)


# =========================================================================
# PUBLIC API
# =========================================================================


@task(name="apply_security", timeout_seconds=30)
async def apply_security(ctx: PipelineContext) -> None:
    """Run security checks on agent response. Mutates ctx.agent_response in place.

    Order: term replacements first (deterministic), then LLM hygiene + compliance check (always).
    The LLM check ALWAYS runs — base hygiene rules catch leaked thoughts, tag artifacts,
    markdown, LLM-speak, etc. Client compliance rules are appended when configured.
    """
    if not ctx.agent_response or not ctx.agent_response.strip():
        return

    # Store original for audit trail
    ctx.security_original_response = ctx.agent_response

    # Step 1: Deterministic term replacements (only if configured)
    term_replacements = ctx.compiled.get("term_replacements", [])
    if term_replacements and isinstance(term_replacements, list) and len(term_replacements) > 0:
        ctx.agent_response = _apply_term_replacements(ctx.agent_response, term_replacements)
        ctx.security_replacements_applied = ctx.agent_response != ctx.security_original_response
        if ctx.security_replacements_applied:
            logger.info(
                "SECURITY | term_replacements | text_changed=True | rules=%d",
                len(term_replacements),
            )
        else:
            logger.info(
                "SECURITY | term_replacements | text_changed=False | rules=%d",
                len(term_replacements),
            )

    # Step 2: LLM security check (ALWAYS runs — base hygiene + optional compliance)
    compliance_rules = ctx.compiled.get("compliance_rules", "")
    await _run_security_check(ctx, compliance_rules)


# =========================================================================
# STANDALONE WRAPPERS (for follow-up path — don't mutate ctx.agent_response)
# =========================================================================


def apply_term_replacements_standalone(text: str, replacements: list[dict[str, str]]) -> str:
    """Apply term replacements to arbitrary text. Returns the modified text."""
    return _apply_term_replacements(text, replacements)


async def run_security_check_standalone(
    text: str,
    compliance_rules: str,
    conversation_context: str,
    ctx: PipelineContext,
) -> str:
    """Run LLM security check on arbitrary text. Returns the (potentially rewritten) text.

    Always runs base hygiene rules. Appends client compliance rules when provided.
    On failure, returns original text + Slack alert.
    Used by follow-up path where we don't mutate ctx.agent_response.
    """
    # Build combined rules: base hygiene always + client compliance if configured
    if compliance_rules:
        all_rules = f"{_BASE_HYGIENE_RULES}\n\n## CLIENT COMPLIANCE RULES\n\n{compliance_rules}"
    else:
        all_rules = _BASE_HYGIENE_RULES

    system = _SECURITY_CHECK_SYSTEM.format(
        all_rules=all_rules,
        conversation_context=conversation_context,
    )
    user_prompt = (
        f"## AI Response To Check\n\n{text}\n\n"
        f"Check this response against all rules above. "
        f"Return safe=true if it passes, or rewrite it if it violates any rule."
    )

    log_prompt(ctx, "Security Check (Follow-Up)", system, user_prompt, variables={
        "compliance_rules": compliance_rules,
    })

    try:
        result = await classify(
            prompt=user_prompt,
            schema=_SECURITY_CHECK_SCHEMA,
            system_prompt=system,
            model=resolve_model(ctx, "followup_security"),
            temperature=resolve_temperature(ctx, "followup_security"),
            label="followup_security",
        )
        if not result.get("safe", True):
            rewritten = result.get("rewritten_response", "")
            violations = result.get("violations", [])
            if rewritten and rewritten.strip():
                logger.info(
                    "SECURITY | followup_llm_check | VIOLATION | violations=%s | rewritten",
                    violations,
                )
                return rewritten
            else:
                logger.warning(
                    "SECURITY | followup_llm_check | VIOLATION_NO_REWRITE | violations=%s",
                    violations,
                )
                await _send_security_alert(
                    ctx, violations, "Follow-up violation — no rewrite returned"
                )
    except Exception as e:
        logger.warning("SECURITY | followup_llm_check | FAILED | error=%s", e)
        await _send_security_alert(ctx, [], f"Follow-up security check failed: {e}")

    return text  # Return original on any failure path


# =========================================================================
# DETERMINISTIC DASH CLEANUP (always runs)
# =========================================================================


# Pre-compiled patterns for punctuation dashes (NOT compound-word hyphens)
_EM_DASH = re.compile(r"\s*\u2014\s*")       # — (with optional surrounding whitespace)
_EN_DASH = re.compile(r"\s*\u2013\s*")       # – (with optional surrounding whitespace)
_DOUBLE_HYPHEN = re.compile(r"\s*--\s*")     # -- (common em dash substitute)
_SPACED_HYPHEN = re.compile(r" - ")          # " - " (hyphen used as punctuation)


def strip_punctuation_dashes(text: str) -> str:
    """Replace punctuation dashes with commas. Preserves hyphens within words.

    Targets (punctuation dashes — never appear in compound words):
      - Em dash:  "we offer whitening — it's popular"  →  "we offer whitening, it's popular"
      - En dash:  "great results – check it out"        →  "great results, check it out"
      - Double hyphen: "sounds good -- let me check"    →  "sounds good, let me check"
      - Spaced hyphen: "great question - let me explain" → "great question, let me explain"

    Preserves (compound-word hyphens — no spaces around them):
      - "well-known", "follow-up", "re-schedule", "self-care"
    """
    if not text:
        return text

    text = _EM_DASH.sub(", ", text)
    text = _EN_DASH.sub(", ", text)
    text = _DOUBLE_HYPHEN.sub(", ", text)
    text = _SPACED_HYPHEN.sub(", ", text)
    return text


# =========================================================================
# DETERMINISTIC TERM REPLACEMENTS
# =========================================================================


def _apply_term_replacements(text: str, replacements: list[dict[str, str]]) -> str:
    """Apply case-insensitive term replacements to text.

    Each replacement is {"find": "Botox", "replace": "B.Tox"}.
    Uses word-boundary regex for each term to avoid partial matches
    (e.g., "Botox" in "Botoxin" should NOT match).

    Preserves original casing pattern where possible:
    - If original is all-caps ("BOTOX"), replacement is uppercased ("B.TOX")
    - If original is title-case ("Botox"), replacement is as-specified ("B.Tox")
    - If original is lowercase ("botox"), replacement is lowercased ("b.tox")
    """
    if not replacements:
        return text

    for rule in replacements:
        find = rule.get("find", "")
        replace = rule.get("replace", "")
        if not find:
            continue

        # Build word-boundary regex, case-insensitive
        pattern = re.compile(r"\b" + re.escape(find) + r"\b", re.IGNORECASE)

        def _case_match(match: re.Match, _replace: str = replace) -> str:
            original = match.group(0)
            if original.isupper():
                return _replace.upper()
            elif original.islower():
                return _replace.lower()
            else:
                # Mixed/title case — use replacement as-is
                return _replace

        text = pattern.sub(_case_match, text)

    return text


# =========================================================================
# LLM SECURITY CHECK
# =========================================================================


_BASE_HYGIENE_RULES = """\
## OUTPUT HYGIENE (always enforced)

The message must be a clean, ready-to-send message. Flag as unsafe if ANY of these appear:

1. **Leaked thinking/reasoning** — Any internal thought process visible in the output \
(e.g., "I should ask about...", "Let me think...", "The lead seems...", planning or strategizing text \
that wasn't meant for the recipient)
2. **XML/HTML tag artifacts** — Any tags like </thinking>, <response>, [INST], <<SYS>>, or similar \
markup that leaked from the AI system
3. **Tool call / function remnants** — Any text like [function_call: ...], <tool_use>, \
{"name": "book_appointment"...}, or references to internal tools
4. **Markdown formatting** — Bold (**text**), italic (*text*), headers (#), bullet points (- or *), \
numbered lists (1. 2. 3.), code blocks, or any structured formatting. Messages must be plain text only.
5. **Multiple concatenated responses** — Two or more separate complete replies joined together \
(the message should be one coherent response, not duplicates or drafts)
6. **System/meta commentary** — References to being an AI, to prompts, to instructions, \
to "the system", or any self-aware meta-text not meant for the lead"""


_SECURITY_CHECK_SYSTEM = """\
<role>
You are a compliance reviewer for outbound messages sent by a sales assistant. \
You check messages against specific rules before they are sent to leads.
</role>

<rules>
{all_rules}
</rules>

<instructions>
## INSTRUCTIONS

1. Read the response that is about to be sent.
2. Check it against every rule above.
3. If the response passes all rules, return safe=true and leave rewritten_response empty.
4. If the response violates ANY rule, return safe=false with:
   - A rewritten version that fixes ALL violations while preserving the original meaning and tone
   - A list of what was violated

**Rewriting rules:**
- Keep the same casual SMS tone — do NOT make it sound more formal or robotic
- Preserve all non-violating content exactly
- Only change what is necessary to fix violations
- Keep the same approximate length
- Do NOT add disclaimers, legal language, or anything that sounds like a bot
- Do NOT change the structure or flow of the message
</instructions>

<conversation_context>
## CONVERSATION CONTEXT (for judgment only — do NOT include in rewrite)

{conversation_context}
</conversation_context>
"""

_SECURITY_CHECK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "safe": {
            "type": "boolean",
            "description": "True if the response passes all compliance rules",
        },
        "rewritten_response": {
            "type": "string",
            "description": "Rewritten response fixing all violations. Empty string if safe=true.",
        },
        "violations": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of violated rules. Empty array if safe=true.",
        },
    },
    "required": ["safe", "rewritten_response", "violations"],
}


async def _run_security_check(ctx: PipelineContext, compliance_rules: str) -> None:
    """Run LLM security check on agent response. Mutates ctx.agent_response if violations found.

    Always runs base hygiene rules. Appends client compliance rules when configured.
    On LLM failure: logs warning, sends Slack alert, leaves response unchanged.
    """
    conversation_context = ctx.timeline[:2000] if ctx.timeline else "No conversation history available."

    # Build combined rules: base hygiene always + client compliance if configured
    if compliance_rules:
        all_rules = f"{_BASE_HYGIENE_RULES}\n\n## CLIENT COMPLIANCE RULES\n\n{compliance_rules}"
    else:
        all_rules = _BASE_HYGIENE_RULES

    system = _SECURITY_CHECK_SYSTEM.format(
        all_rules=all_rules,
        conversation_context=conversation_context,
    )

    user_prompt = (
        f"## AI Response To Check\n\n{ctx.agent_response}\n\n"
        f"Check this response against all rules above. "
        f"Return safe=true if it passes, or rewrite it if it violates any rule."
    )

    # Log prompt for test mode HTML reports
    log_prompt(ctx, "Security Check", system, user_prompt, variables={
        "compliance_rules": compliance_rules,
        "conversation_context": conversation_context,
    })

    try:
        result = await classify(
            prompt=user_prompt,
            schema=_SECURITY_CHECK_SCHEMA,
            system_prompt=system,
            model=resolve_model(ctx, "reply_security"),
            temperature=resolve_temperature(ctx, "reply_security"),
            label="reply_security",
        )

        is_safe = result.get("safe", True)
        violations = result.get("violations", [])
        rewritten = result.get("rewritten_response", "")

        ctx.security_check_result = {
            "safe": is_safe,
            "violations": violations,
            "rewritten": bool(rewritten and not is_safe),
        }

        if not is_safe and rewritten and rewritten.strip():
            logger.info(
                "SECURITY | llm_check | VIOLATION | violations=%s | original_len=%d | rewritten_len=%d",
                violations,
                len(ctx.agent_response),
                len(rewritten),
            )
            ctx.agent_response = rewritten
        elif not is_safe and (not rewritten or not rewritten.strip()):
            # LLM flagged violations but returned empty rewrite — keep original + alert
            logger.warning(
                "SECURITY | llm_check | VIOLATION_NO_REWRITE | violations=%s",
                violations,
            )
            await _send_security_alert(
                ctx, violations, "LLM flagged violations but returned empty rewrite"
            )
        else:
            logger.info("SECURITY | llm_check | safe=True")

    except Exception as e:
        logger.warning("SECURITY | llm_check | FAILED | error=%s", e, exc_info=True)
        ctx.security_check_result = {"safe": None, "error": str(e)}
        await _send_security_alert(
            ctx, [], f"Security LLM check failed: {type(e).__name__}: {e}"
        )


# =========================================================================
# SLACK ALERTS
# =========================================================================


async def _send_security_alert(
    ctx: PipelineContext,
    violations: list[str],
    details: str,
) -> None:
    """Send a Slack alert when security check fails or finds unresolvable violations."""
    ghl_link = ""
    if hasattr(ctx, "ghl_location_id") and ctx.ghl_location_id:
        ghl_url = build_ghl_contact_url(
            ctx.ghl_location_id,
            ctx.contact_id,
            (ctx.config or {}).get("ghl_domain", ""),
        )
        ghl_link = f"\n*GHL Link:* <{ghl_url}|View Contact>" if ghl_url else ""

    violation_text = (
        "\n".join(f"  - {v}" for v in violations)
        if violations
        else "  (none listed)"
    )

    company = ctx.config.get("name", "Unknown") if ctx.config else "Unknown"
    contact_name = getattr(ctx, "contact_name", "Unknown")
    contact_id = getattr(ctx, "contact_id", "Unknown")
    channel = getattr(ctx, "channel", "Unknown")

    alert_text = (
        f":shield: *Security Check Alert*\n"
        f"*Client:* {company}\n"
        f"*Contact:* {contact_name} (`{contact_id}`)\n"
        f"*Channel:* {channel}\n"
        f"*Details:* {details}\n"
        f"*Violations:*\n{violation_text}"
        f"{ghl_link}"
    )

    try:
        await post_slack_message("#python-errors", alert_text)
    except Exception as e:
        logger.warning("Failed to send security Slack alert: %s", e)
