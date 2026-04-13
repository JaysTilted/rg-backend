"""Tier 1 Compliance Analysis — automatic post-simulation checks.

Runs ~6 parallel LLM calls per conversation, each checking 2-3 setter config
settings against actual bot behavior. Uses cheap model (Gemini Flash).
Results stored in simulations.compliance_results JSONB.

Settings are read from the REAL setter config paths:
  - conversation.reply.sections.*  (reply behavior toggles)
  - conversation.follow_up.sections.*  (follow-up behavior)
  - booking.*  (booking config)
  - transfer.*  (transfer rules)
  - security.*  (compliance rules)
  - services.*  (service/offer accuracy)
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from app.services.ai_client import classify, set_ai_context, clear_ai_context

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Check group definitions — using REAL setter config paths
# ---------------------------------------------------------------------------

_CHECK_GROUPS: list[dict[str, Any]] = [
    {
        "name": "communication_style",
        "label": "Communication Style",
        "extractor": "_extract_communication_style",
    },
    {
        "name": "booking_behavior",
        "label": "Booking Behavior",
        "extractor": "_extract_booking_behavior",
    },
    {
        "name": "conversation_flow",
        "label": "Conversation Flow",
        "extractor": "_extract_conversation_flow",
    },
    {
        "name": "follow_up_behavior",
        "label": "Follow-Up Behavior",
        "extractor": "_extract_follow_up_behavior",
    },
    {
        "name": "knowledge_accuracy",
        "label": "Knowledge Accuracy",
        "extractor": "_extract_knowledge_accuracy",
    },
    {
        "name": "transfer_security",
        "label": "Transfer & Security",
        "extractor": "_extract_transfer_security",
    },
]

_COMPLIANCE_SYSTEM = """You are a compliance auditor for an AI sales setter that communicates via SMS. You check whether the AI's behavior matches its configured settings.

For each setting provided, rate it as:
- "pass" — the AI followed this setting correctly
- "warn" — the AI partially followed it, or it's ambiguous / not enough data to tell
- "fail" — the AI clearly violated this setting

Provide a brief detail explaining your rating (1-2 sentences). Use the setting name exactly as provided."""

_COMPLIANCE_SCHEMA = {
    "type": "object",
    "properties": {
        "checks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "setting": {"type": "string"},
                    "status": {"type": "string", "enum": ["pass", "warn", "fail"]},
                    "detail": {"type": "string"},
                },
                "required": ["setting", "status", "detail"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["checks"],
    "additionalProperties": False,
}


# ---------------------------------------------------------------------------
# Setting extractors — read real config paths
# ---------------------------------------------------------------------------

def _extract_communication_style(setter: dict) -> str:
    """Extract communication style settings from reply sections."""
    reply_sections = setter.get("conversation", {}).get("reply", {}).get("sections", {})
    lines = []

    # Acknowledge before pivot
    abp = reply_sections.get("acknowledge_before_pivot", {})
    if abp.get("enabled"):
        lines.append(f"- acknowledge_before_pivot: ENABLED — {abp.get('prompt', 'Acknowledge the lead concern before redirecting')}")

    # Yes-and technique
    ya = reply_sections.get("yes_and", {})
    if ya.get("enabled"):
        lines.append(f"- yes_and: ENABLED — {ya.get('prompt', 'Use yes-and technique')}")

    # Paraphrase
    para = reply_sections.get("paraphrase", {})
    if para.get("enabled"):
        lines.append(f"- paraphrase: ENABLED — {para.get('prompt', 'Paraphrase lead messages')}")

    # Push back
    pb = reply_sections.get("push_back", {})
    if pb.get("enabled"):
        lines.append(f"- push_back: ENABLED — {pb.get('prompt', 'Push back on objections')}")

    # Low effort responses
    ler = reply_sections.get("low_effort_responses", {})
    if ler.get("enabled"):
        lines.append(f"- low_effort_responses: ENABLED — {ler.get('prompt', 'Handle low effort responses')}")

    # Confident expert
    ce = reply_sections.get("confident_expert", {})
    if ce.get("enabled"):
        lines.append(f"- confident_expert: ENABLED — {ce.get('prompt', 'Respond as a confident expert')}")

    # Role context
    rc = reply_sections.get("role_context", "")
    if rc:
        lines.append(f"- role_context: {rc[:200]}")

    return "\n".join(lines)


def _extract_booking_behavior(setter: dict) -> str:
    """Extract booking behavior settings."""
    reply_sections = setter.get("conversation", {}).get("reply", {}).get("sections", {})
    lines = []

    # Booking style
    bs = reply_sections.get("booking_style", {})
    if bs.get("selected"):
        lines.append(f"- booking_style: {bs['selected']}")

    # Max booking pushes
    mbp = reply_sections.get("max_booking_pushes", {})
    if mbp.get("enabled"):
        lines.append(f"- max_booking_pushes: ENABLED, max={mbp.get('max', 3)} — {mbp.get('prompt', '')[:100]}")

    # Urgency
    urg = reply_sections.get("urgency", {})
    if urg.get("enabled"):
        lines.append(f"- urgency: ENABLED — {urg.get('prompt', '')[:100]}")

    # Scarcity
    scar = reply_sections.get("scarcity", {})
    if scar.get("enabled"):
        lines.append(f"- scarcity: ENABLED — {scar.get('prompt', '')[:100]}")

    # Steer toward goal
    stg = reply_sections.get("steer_toward_goal", {})
    if stg.get("enabled"):
        lines.append(f"- steer_toward_goal: ENABLED — {stg.get('prompt', '')[:100]}")

    # Always moving forward
    amf = reply_sections.get("always_moving_forward", {})
    if amf.get("enabled"):
        lines.append(f"- always_moving_forward: ENABLED — {amf.get('prompt', '')[:100]}")

    return "\n".join(lines)


def _extract_conversation_flow(setter: dict) -> str:
    """Extract conversation flow settings."""
    reply_sections = setter.get("conversation", {}).get("reply", {}).get("sections", {})
    lines = []

    # Allow small talk
    ast = reply_sections.get("allow_small_talk", {})
    if ast.get("enabled"):
        lines.append(f"- allow_small_talk: ENABLED — {ast.get('prompt', '')[:100]}")
    else:
        lines.append("- allow_small_talk: DISABLED — AI should not engage in small talk")

    # Discover timeline
    dt = reply_sections.get("discover_timeline", {})
    if dt.get("enabled"):
        lines.append(f"- discover_timeline: ENABLED — {dt.get('prompt', '')[:100]}")

    # Max objection retries
    mor = reply_sections.get("max_objection_retries", {})
    if mor.get("enabled"):
        lines.append(f"- max_objection_retries: ENABLED, max={mor.get('max', 3)} — {mor.get('prompt', '')[:100]}")

    # Max questions before booking
    mq = reply_sections.get("max_questions", {})
    if mq.get("value"):
        lines.append(f"- max_questions: {mq['value']} questions before attempting to book")

    # Discovery questions
    dq = reply_sections.get("discovery_questions", {})
    if dq.get("enabled"):
        questions = dq.get("questions", [])
        q_text = ", ".join([q.get("question", "") for q in questions[:3]]) if questions else "configured"
        lines.append(f"- discovery_questions: ENABLED — {q_text}")

    # Accept no
    an = reply_sections.get("accept_no", {})
    if an.get("enabled"):
        lines.append(f"- accept_no: ENABLED — {an.get('prompt', '')[:100]}")

    # Fully helped
    fh = reply_sections.get("fully_helped", {})
    if fh.get("enabled"):
        lines.append(f"- fully_helped: ENABLED — {fh.get('prompt', '')[:100]}")

    return "\n".join(lines)


def _extract_follow_up_behavior(setter: dict) -> str:
    """Extract follow-up behavior settings."""
    fu_sections = setter.get("conversation", {}).get("follow_up", {}).get("sections", {})
    lines = []

    # Banned phrases in follow-ups
    bp = fu_sections.get("banned_phrases", [])
    if bp:
        lines.append(f"- follow_up_banned_phrases: {bp}")

    # Skip if disqualified
    sid = fu_sections.get("skip_if_disqualified", False)
    if sid:
        lines.append("- skip_if_disqualified: Follow-ups should NOT be sent to disqualified leads")

    # No finality language
    nfl = fu_sections.get("no_finality_language", {})
    if nfl.get("enabled"):
        lines.append(f"- no_finality_language: ENABLED — {nfl.get('prompt', '')[:100]}")

    # Tone overrides
    to = fu_sections.get("tone_overrides", {})
    if to.get("enabled"):
        lines.append(f"- follow_up_tone: ENABLED — {to.get('prompt', '')[:100]}")

    # Max offer pushes
    mop = fu_sections.get("max_offer_pushes", {})
    if mop.get("enabled"):
        lines.append(f"- max_offer_pushes: ENABLED, max={mop.get('max', 2)} — {mop.get('prompt', '')[:100]}")

    # Cadence timing
    ct = fu_sections.get("cadence_timing", {})
    timings = ct.get("timings", [])
    if timings:
        # Timings can be strings ("6 hours") or dicts ({"delay_hours": 6})
        timing_parts = []
        for i, t in enumerate(timings[:5]):
            if isinstance(t, str):
                timing_parts.append(f"#{i+1}: {t}")
            elif isinstance(t, dict):
                timing_parts.append(f"#{i+1}: {t.get('delay_hours', '?')}h")
        if timing_parts:
            lines.append(f"- follow_up_cadence: {', '.join(timing_parts)}")

    return "\n".join(lines)


def _extract_knowledge_accuracy(setter: dict) -> str:
    """Extract service/knowledge settings for accuracy checking."""
    lines = []

    # Services
    services = setter.get("services", {}).get("services", [])
    if services:
        for s in services[:5]:
            name = s.get("name", "")
            pricing = s.get("pricing", "")
            if name:
                lines.append(f"- service: {name}" + (f" (pricing: {pricing})" if pricing else ""))

    # Offers
    offers = setter.get("services", {}).get("global_offers", [])
    if offers:
        for o in offers[:3]:
            name = o.get("name", "")
            if name and o.get("enabled", True):
                lines.append(f"- offer: {name} — {o.get('description', '')[:80]}")

    if not lines:
        return ""

    # Wrap with instruction
    return (
        "Check if the AI's responses about services, pricing, and offers are accurate.\n"
        "Known business information:\n" + "\n".join(lines)
    )


def _extract_transfer_security(setter: dict) -> str:
    """Extract transfer and security settings."""
    lines = []

    # Transfer scenarios
    scenarios = setter.get("transfer", {}).get("scenarios", [])
    if scenarios:
        for s in scenarios[:5]:
            lines.append(f"- transfer_scenario: When '{s.get('title', '')}' -> transfer (enabled={s.get('enabled', True)})")

    # Do not transfer
    dnt = setter.get("transfer", {}).get("do_not_transfer", [])
    if dnt:
        for d in dnt[:3]:
            if d.get("enabled", True):
                lines.append(f"- do_not_transfer: '{d.get('title', '')}' — {d.get('description', '')[:80]}")

    # Security compliance rules
    comp_rules = setter.get("security", {}).get("compliance_rules", [])
    if comp_rules:
        for r in comp_rules[:5]:
            if r.get("enabled", True):
                lines.append(f"- security_rule: {r.get('label', '')} — {r.get('prompt', '')[:80]}")

    # Term replacements
    term_reps = setter.get("security", {}).get("term_replacements", [])
    if term_reps:
        for tr in term_reps[:5]:
            if tr.get("enabled", True):
                lines.append(f"- term_replacement: '{tr.get('original', '')}' -> '{tr.get('replacement', '')}'")

    return "\n".join(lines)


# Map extractor names to functions
_EXTRACTORS = {
    "_extract_communication_style": _extract_communication_style,
    "_extract_booking_behavior": _extract_booking_behavior,
    "_extract_conversation_flow": _extract_conversation_flow,
    "_extract_follow_up_behavior": _extract_follow_up_behavior,
    "_extract_knowledge_accuracy": _extract_knowledge_accuracy,
    "_extract_transfer_security": _extract_transfer_security,
}


# ---------------------------------------------------------------------------
# Run compliance analysis on a simulation
# ---------------------------------------------------------------------------

async def run_compliance(
    simulation_id: str,
    conversation_results: list[dict[str, Any]],
    system_config: dict[str, Any],
    setter_key: str,
    tenant_keys: dict[str, str],
) -> dict[str, Any]:
    """Run Tier 1 compliance checks on all conversations in a simulation.

    Returns the full compliance_results dict ready for storage.
    """
    from app.services.supabase_client import supabase

    # Defensive parsing — Supabase REST may return JSONB as strings
    if isinstance(system_config, str):
        system_config = json.loads(system_config)
    if isinstance(conversation_results, str):
        conversation_results = json.loads(conversation_results)

    setter = system_config.get("setters", {}).get(setter_key, {})
    if not setter:
        logger.warning("COMPLIANCE | no setter config found for %s", setter_key)
        return {"per_conversation": {}, "summary": {"total_passed": 0, "total_warned": 0, "total_failed": 0}}

    # Pre-extract all settings descriptions (same for all conversations)
    group_descs = []
    for group in _CHECK_GROUPS:
        extractor_fn = _EXTRACTORS.get(group["extractor"])
        if not extractor_fn:
            continue
        desc = extractor_fn(setter)
        if desc.strip():
            group_descs.append((group["name"], group["label"], desc))

    if not group_descs:
        logger.warning("COMPLIANCE | no settings found to check in setter %s", setter_key)
        return {"per_conversation": {}, "summary": {"total_passed": 0, "total_warned": 0, "total_failed": 0}}

    logger.info("COMPLIANCE | %d check groups active for %d conversations", len(group_descs), len(conversation_results))

    # Run all conversations in parallel
    sem = asyncio.Semaphore(10)

    async def analyze_conversation(conv: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        async with sem:
            conv_id = conv.get("id", "unknown")
            messages = conv.get("messages", [])
            if not messages:
                return conv_id, {"checks": [], "passed": 0, "warned": 0, "failed": 0}

            convo_text = _format_conversation(messages)

            # Run all check groups in parallel for this conversation
            check_tasks = []
            for group_name, group_label, settings_desc in group_descs:
                prompt = (
                    f"## Conversation\n{convo_text}\n\n"
                    f"## {group_label} — Settings to Check\n{settings_desc}\n\n"
                    f"Rate each setting as pass/warn/fail with a brief explanation."
                )
                check_tasks.append(_run_check(prompt, tenant_keys))

            results = await asyncio.gather(*check_tasks, return_exceptions=True)

            all_checks = []
            for r in results:
                if isinstance(r, Exception):
                    logger.warning("COMPLIANCE | check failed: %s", r)
                    continue
                all_checks.extend(r.get("checks", []))

            passed = sum(1 for c in all_checks if c["status"] == "pass")
            warned = sum(1 for c in all_checks if c["status"] == "warn")
            failed = sum(1 for c in all_checks if c["status"] == "fail")

            return conv_id, {"checks": all_checks, "passed": passed, "warned": warned, "failed": failed}

    conv_tasks = [analyze_conversation(conv) for conv in conversation_results]
    conv_results = await asyncio.gather(*conv_tasks)

    per_conversation = {}
    total_passed = total_warned = total_failed = 0
    for conv_id, result in conv_results:
        per_conversation[conv_id] = result
        total_passed += result["passed"]
        total_warned += result["warned"]
        total_failed += result["failed"]

    compliance_results = {
        "per_conversation": per_conversation,
        "summary": {
            "total_passed": total_passed,
            "total_warned": total_warned,
            "total_failed": total_failed,
        },
    }

    # Update simulation
    await supabase._request(
        supabase.main_client, "PATCH", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        json={
            "compliance_results": compliance_results,
            "compliance_passed": total_passed,
            "compliance_warned": total_warned,
            "compliance_failed": total_failed,
        },
        label="sim_store_compliance",
    )

    logger.info(
        "COMPLIANCE | complete | sim=%s | passed=%d warned=%d failed=%d",
        simulation_id, total_passed, total_warned, total_failed,
    )

    return compliance_results


async def run_single_conversation_compliance(
    simulation_id: str,
    conv_result: dict[str, Any],
    system_config: dict[str, Any],
    setter_key: str,
    tenant_keys: dict[str, str],
) -> None:
    """Run compliance for a single conversation and merge into simulation's compliance_results.

    Called incrementally as each conversation completes, so the UI can show
    compliance results per-conversation without waiting for all to finish.
    """
    from app.services.supabase_client import supabase

    if isinstance(system_config, str):
        system_config = json.loads(system_config)

    setter = system_config.get("setters", {}).get(setter_key, {})
    if not setter:
        return

    # Extract check group descriptions
    group_descs = []
    for group in _CHECK_GROUPS:
        extractor_fn = _EXTRACTORS.get(group["extractor"])
        if not extractor_fn:
            continue
        desc = extractor_fn(setter)
        if desc.strip():
            group_descs.append((group["name"], group["label"], desc))

    if not group_descs:
        return

    conv_id = conv_result.get("id", "unknown")
    messages = conv_result.get("messages", [])
    if not messages:
        return

    convo_text = _format_conversation(messages)

    # Run all check groups in parallel for this conversation
    check_tasks = []
    for group_name, group_label, settings_desc in group_descs:
        prompt = (
            f"## Conversation\n{convo_text}\n\n"
            f"## {group_label} — Settings to Check\n{settings_desc}\n\n"
            f"Rate each setting as pass/warn/fail with a brief explanation."
        )
        check_tasks.append(_run_check(prompt, tenant_keys))

    check_results = await asyncio.gather(*check_tasks, return_exceptions=True)

    all_checks = []
    for r in check_results:
        if isinstance(r, Exception):
            logger.warning("COMPLIANCE | single conv check failed: %s", r)
            continue
        all_checks.extend(r.get("checks", []))

    passed = sum(1 for c in all_checks if c["status"] == "pass")
    warned = sum(1 for c in all_checks if c["status"] == "warn")
    failed = sum(1 for c in all_checks if c["status"] == "fail")

    conv_compliance = {"checks": all_checks, "passed": passed, "warned": warned, "failed": failed}

    # Merge into existing compliance_results atomically
    fresh = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={"id": f"eq.{simulation_id}", "select": "compliance_results,compliance_passed,compliance_warned,compliance_failed"},
        label="sim_get_compliance_for_merge",
    )
    sim = fresh.json()[0] if fresh.json() else {}
    existing = sim.get("compliance_results") or {}
    if isinstance(existing, str):
        existing = json.loads(existing)

    per_conv = existing.get("per_conversation", {})
    per_conv[conv_id] = conv_compliance

    # Recalculate totals
    total_p = sum(v.get("passed", 0) for v in per_conv.values())
    total_w = sum(v.get("warned", 0) for v in per_conv.values())
    total_f = sum(v.get("failed", 0) for v in per_conv.values())

    updated = {
        "per_conversation": per_conv,
        "summary": {"total_passed": total_p, "total_warned": total_w, "total_failed": total_f},
    }

    await supabase._request(
        supabase.main_client, "PATCH", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        json={
            "compliance_results": updated,
            "compliance_passed": total_p,
            "compliance_warned": total_w,
            "compliance_failed": total_f,
        },
        label="sim_store_incremental_compliance",
    )

    logger.info("COMPLIANCE | conv %s done | passed=%d warned=%d failed=%d", conv_id, passed, warned, failed)


async def _run_check(prompt: str, tenant_keys: dict[str, str]) -> dict[str, Any]:
    """Run a single compliance check group."""
    set_ai_context(
        api_key=tenant_keys["openrouter"],
        google_key=tenant_keys.get("google", ""),
        anthropic_key=tenant_keys.get("anthropic", ""),
        openai_key=tenant_keys.get("openai", ""),
        deepseek_key=tenant_keys.get("deepseek", ""),
        xai_key=tenant_keys.get("xai", ""),
    )
    try:
        result = await classify(
            prompt=prompt,
            schema=_COMPLIANCE_SCHEMA,
            model="google/gemini-2.5-flash",
            temperature=0.2,
            system_prompt=_COMPLIANCE_SYSTEM,
            label="compliance_check",
        )
        return result
    finally:
        clear_ai_context()


def _format_conversation(messages: list[dict[str, Any]]) -> str:
    """Format messages into readable text for compliance analysis."""
    lines = []
    for m in messages:
        role = "LEAD" if m.get("role") == "human" else "AI"
        source = m.get("source", "")
        if source == "follow_up":
            role = "AI (follow-up)"
        content = m.get("content", "")[:500]
        lines.append(f"{role}: {content}")
    return "\n".join(lines)
