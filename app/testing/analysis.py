"""Tier 2+3 AI Analysis — on-demand deep analysis via "AI Analyze" button.

Runs 5 parallel LLM calls per conversation:
  A: Quality & tone (human-likeness, quality score)
  B: Booking progression effectiveness
  C: Objection handling quality
  D: Knowledge accuracy (with KB search)
  E: Setter config suggestions (with reviews/notes context)

Then one aggregation call to combine findings.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.services.ai_client import classify, set_ai_context, clear_ai_context
from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-conversation analysis prompts
# ---------------------------------------------------------------------------

_ANALYSIS_SYSTEM = """You are an expert AI conversation analyst reviewing SMS sales conversations. You evaluate AI sales setters (bots that impersonate human sales reps to convert leads into bookings).

Your analysis should be practical and specific — not generic observations. Focus on what's actually happening in the conversation and give actionable feedback."""

_QUALITY_SCHEMA = {
    "type": "object",
    "properties": {
        "quality": {"type": "integer", "minimum": 1, "maximum": 10},
        "human_likeness": {"type": "integer", "minimum": 1, "maximum": 10},
        "summary": {"type": "string"},
    },
    "required": ["quality", "human_likeness", "summary"],
    "additionalProperties": False,
}

_BOOKING_SCHEMA = {
    "type": "object",
    "properties": {
        "booking_progression": {"type": "integer", "minimum": 1, "maximum": 10},
        "summary": {"type": "string"},
    },
    "required": ["booking_progression", "summary"],
    "additionalProperties": False,
}

_OBJECTION_SCHEMA = {
    "type": "object",
    "properties": {
        "objection_handling": {"type": "integer", "minimum": 1, "maximum": 10},
        "summary": {"type": "string"},
    },
    "required": ["objection_handling", "summary"],
    "additionalProperties": False,
}

_KNOWLEDGE_SCHEMA = {
    "type": "object",
    "properties": {
        "knowledge_accuracy": {"type": "integer", "minimum": 1, "maximum": 10},
        "gaps": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "topic": {"type": "string"},
                    "detail": {"type": "string"},
                },
                "required": ["topic", "detail"],
                "additionalProperties": False,
            },
        },
        "summary": {"type": "string"},
    },
    "required": ["knowledge_accuracy", "gaps", "summary"],
    "additionalProperties": False,
}

_SUGGESTIONS_SCHEMA = {
    "type": "object",
    "properties": {
        "suggestions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "setting": {"type": "string"},
                    "current_value": {"type": "string"},
                    "suggested_value": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["setting", "reason"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["suggestions"],
    "additionalProperties": False,
}

_AGGREGATION_SCHEMA = {
    "type": "object",
    "properties": {
        "overall_quality": {"type": "number"},
        "overall_human_likeness": {"type": "number"},
        "overall_booking_progression": {"type": "number"},
        "findings": {
            "type": "array",
            "items": {"type": "string"},
        },
        "top_suggestions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "setting": {"type": "string"},
                    "current_value": {"type": "string"},
                    "suggested_value": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["setting", "reason"],
                "additionalProperties": False,
            },
        },
        "kb_gaps": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "topic": {"type": "string"},
                    "detail": {"type": "string"},
                },
                "required": ["topic", "detail"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["overall_quality", "overall_human_likeness", "overall_booking_progression", "findings"],
    "additionalProperties": False,
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run_analysis(
    simulation_id: str,
    conversation_results: list[dict[str, Any]],
    system_config: dict[str, Any],
    setter_key: str,
    reviews: dict[str, Any],
    tenant_keys: dict[str, str],
    entity_id: str,
) -> dict[str, Any]:
    """Run Tier 2+3 AI analysis on all conversations.

    Returns the full ai_analysis dict ready for storage.
    """
    setter = system_config.get("setters", {}).get(setter_key, {})
    model = "google/gemini-2.5-flash"  # mid-tier for analysis

    per_conversation: dict[str, dict[str, Any]] = {}
    all_suggestions: list[dict[str, Any]] = []
    all_kb_gaps: list[dict[str, Any]] = []

    for conv in conversation_results:
        conv_id = conv.get("id", "unknown")
        messages = conv.get("messages", [])
        convo_text = _format_conversation(messages)
        review = reviews.get(conv_id, {})
        review_context = ""
        if review:
            review_context = f"\n\nUser review: {review.get('status', 'none')}. Notes: {review.get('notes', 'none')}"

        # Run 5 analysis calls in parallel
        set_ai_context(
            api_key=tenant_keys["openrouter"],
            google_key=tenant_keys.get("google", ""),
            anthropic_key=tenant_keys.get("anthropic", ""),
            openai_key=tenant_keys.get("openai", ""),
            deepseek_key=tenant_keys.get("deepseek", ""),
            xai_key=tenant_keys.get("xai", ""),
        )
        try:
            quality_task = classify(
                prompt=f"## Conversation\n{convo_text}\n\nRate the overall quality (1-10) and human-likeness (1-10). Provide a brief summary.",
                schema=_QUALITY_SCHEMA, model=model, temperature=0.3,
                system_prompt=_ANALYSIS_SYSTEM, label="analysis_quality",
            )
            booking_task = classify(
                prompt=f"## Conversation\n{convo_text}\n\nRate how effectively the AI moved the lead toward booking (1-10). Did it progress the conversation toward scheduling?",
                schema=_BOOKING_SCHEMA, model=model, temperature=0.3,
                system_prompt=_ANALYSIS_SYSTEM, label="analysis_booking",
            )
            objection_task = classify(
                prompt=f"## Conversation\n{convo_text}\n\nRate the AI's objection handling quality (1-10). How well did it address concerns, pushback, or hesitation?",
                schema=_OBJECTION_SCHEMA, model=model, temperature=0.3,
                system_prompt=_ANALYSIS_SYSTEM, label="analysis_objection",
            )
            knowledge_task = classify(
                prompt=f"## Conversation\n{convo_text}\n\nRate knowledge accuracy (1-10). Did the AI provide accurate info? Flag any topics where it seemed to guess or had no answer.",
                schema=_KNOWLEDGE_SCHEMA, model=model, temperature=0.3,
                system_prompt=_ANALYSIS_SYSTEM, label="analysis_knowledge",
            )
            suggestion_task = classify(
                prompt=(
                    f"## Conversation\n{convo_text}\n\n"
                    f"## Current Setter Config\n{_summarize_setter(setter)}\n\n"
                    f"{review_context}\n\n"
                    f"Suggest specific setter config changes that would improve this conversation. "
                    f"For each suggestion, specify the setting name, what it's currently set to, "
                    f"what it should be changed to, and why."
                ),
                schema=_SUGGESTIONS_SCHEMA, model=model, temperature=0.3,
                system_prompt=_ANALYSIS_SYSTEM, label="analysis_suggestions",
            )

            results = await asyncio.gather(
                quality_task, booking_task, objection_task, knowledge_task, suggestion_task,
                return_exceptions=True,
            )
        finally:
            clear_ai_context()

        # Unpack results (handle failures gracefully)
        quality = results[0] if not isinstance(results[0], Exception) else {"quality": 0, "human_likeness": 0, "summary": "Analysis failed"}
        booking = results[1] if not isinstance(results[1], Exception) else {"booking_progression": 0, "summary": "Analysis failed"}
        objection = results[2] if not isinstance(results[2], Exception) else {"objection_handling": 0, "summary": "Analysis failed"}
        knowledge = results[3] if not isinstance(results[3], Exception) else {"knowledge_accuracy": 0, "gaps": [], "summary": "Analysis failed"}
        suggestions = results[4] if not isinstance(results[4], Exception) else {"suggestions": []}

        per_conversation[conv_id] = {
            "quality": quality.get("quality", 0),
            "human_likeness": quality.get("human_likeness", 0),
            "booking_progression": booking.get("booking_progression", 0),
            "objection_handling": objection.get("objection_handling", 0),
            "knowledge_accuracy": knowledge.get("knowledge_accuracy", 0),
            "summary": quality.get("summary", ""),
        }

        all_suggestions.extend(suggestions.get("suggestions", []))
        all_kb_gaps.extend(knowledge.get("gaps", []))

    # Aggregation call
    set_ai_context(
        api_key=tenant_keys["openrouter"],
        google_key=tenant_keys.get("google", ""),
    )
    try:
        per_conv_summary = "\n".join([
            f"Conv {cid}: quality={d['quality']}/10, human={d['human_likeness']}/10, booking={d['booking_progression']}/10"
            for cid, d in per_conversation.items()
        ])
        agg = await classify(
            prompt=(
                f"## Per-Conversation Scores\n{per_conv_summary}\n\n"
                f"## All Suggestions\n{_format_suggestions(all_suggestions)}\n\n"
                f"## KB Gaps\n{_format_kb_gaps(all_kb_gaps)}\n\n"
                f"Provide overall scores (averages), top 3-5 findings, deduplicated top suggestions, and consolidated KB gaps."
            ),
            schema=_AGGREGATION_SCHEMA, model=model, temperature=0.2,
            system_prompt="Aggregate AI analysis results across multiple conversations. Be concise.",
            label="analysis_aggregation",
        )
    except Exception as e:
        logger.warning("ANALYSIS | aggregation failed: %s", e)
        # Calculate simple averages as fallback
        n = len(per_conversation) or 1
        agg = {
            "overall_quality": sum(d["quality"] for d in per_conversation.values()) / n,
            "overall_human_likeness": sum(d["human_likeness"] for d in per_conversation.values()) / n,
            "overall_booking_progression": sum(d["booking_progression"] for d in per_conversation.values()) / n,
            "findings": ["Aggregation analysis failed — individual scores available per conversation"],
            "top_suggestions": all_suggestions[:5],
            "kb_gaps": all_kb_gaps,
        }
    finally:
        clear_ai_context()

    ai_analysis = {
        "overall_scores": {
            "quality": round(agg.get("overall_quality", 0), 1),
            "human_likeness": round(agg.get("overall_human_likeness", 0), 1),
            "booking_progression": round(agg.get("overall_booking_progression", 0), 1),
        },
        "per_conversation": per_conversation,
        "findings": agg.get("findings", []),
        "suggestions": agg.get("top_suggestions", all_suggestions[:5]),
        "kb_gaps": agg.get("kb_gaps", all_kb_gaps),
    }

    # Store in simulation
    await supabase._request(
        supabase.main_client, "PATCH", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        json={"ai_analysis": ai_analysis},
        label="sim_store_analysis",
    )

    logger.info("ANALYSIS | complete | sim=%s | convs=%d", simulation_id, len(per_conversation))
    return ai_analysis


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_conversation(messages: list[dict[str, Any]]) -> str:
    lines = []
    for m in messages:
        role = "LEAD" if m.get("role") == "human" else "AI"
        if m.get("source") == "follow_up":
            role = "AI (follow-up)"
        lines.append(f"{role}: {(m.get('content') or '')[:500]}")
    return "\n".join(lines)


def _summarize_setter(setter: dict[str, Any]) -> str:
    lines = []
    for key, val in setter.items():
        if isinstance(val, dict):
            for k2, v2 in val.items():
                if isinstance(v2, (str, int, float, bool)):
                    lines.append(f"  {key}.{k2}: {v2}")
        elif isinstance(val, (str, int, float, bool)):
            lines.append(f"  {key}: {val}")
    return "\n".join(lines[:50])  # cap at 50 lines


def _format_suggestions(suggestions: list[dict[str, Any]]) -> str:
    return "\n".join([
        f"- {s.get('setting', '?')}: {s.get('reason', '')}"
        for s in suggestions
    ]) or "(none)"


def _format_kb_gaps(gaps: list[dict[str, Any]]) -> str:
    return "\n".join([
        f"- {g.get('topic', '?')}: {g.get('detail', '')}"
        for g in gaps
    ]) or "(none)"
