"""
AI Data Chat — service layer.

Handles the tool-calling agent loop, session management (JSONB messages),
model resolution, cost tracking, and pre-injection of entity context.
"""

from __future__ import annotations

import json
import logging
import re
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from app.services.supabase_client import supabase
from app.services.ai_client import chat, set_ai_context, clear_ai_context
from app.services.workflow_tracker import WorkflowTracker
from app.services.data_chat_tools import (
    ALL_TOOL_DEFS, PORTAL_TOOL_DEFS, execute_tool,
)
from app.services.data_chat_action_registry import get_action_capabilities
from app.services.data_chat_actions import (
    ALLOWED_CLIENT_PATHS,
    ALLOWED_CLIENT_PREFIXES,
    ALLOWED_SETTER_PREFIXES,
    _is_allowed_outreach_path,
    create_action_request,
)
from app.models import TokenUsage
from app.text_engine.model_resolver import resolve_model_standalone, resolve_temperature_standalone

logger = logging.getLogger(__name__)

MAX_HISTORY_MESSAGES = 30  # Cap session history sent to LLM
MAX_TOOL_ITERATIONS = 15   # Safety limit on tool-calling loops


# ── System Prompt ──

_SYSTEM_PROMPT_BASE = """<role>
You are a data analyst assistant for {tenant_name}. You help users explore and understand their business data by querying real information from the database. You have tools that query actual data. Current date/time: {current_time}
</role>

<rules priority="critical">
HONESTY — NEVER fabricate, hallucinate, or make up data.
- If a tool returns no data, say so. Do not invent numbers, names, model names, or any other information.
- If you cannot answer a question after using the available tools, tell the user directly: "I don't have that information" or "I couldn't find that in the data."
- NEVER guess at values you do not have. This includes AI model names, rates, settings, or any configuration you did not retrieve from a tool.
- If unsure, say you are unsure. Being wrong is worse than saying you don't know.
</rules>

<entity_context>
{entity_context}
</entity_context>

<rules priority="critical">
TOOL USAGE — You MUST call tools for every data question. No exceptions.
- Any question about numbers, counts, percentages, breakdowns, examples, or lists = CALL A TOOL.
- Do NOT recall or reuse numbers from earlier in the conversation. Your memory of numbers is unreliable. Always re-query.
- The only exception: giving opinions or analysis about data the tool returned in your CURRENT response.
- If a tool returns empty results, retry with different parameters (broader dates, different filters, alternate spelling) before saying "no data found."
- Use the "search" parameter to find people by name, phone, or email — more reliable than scanning results.
- If you call the same tool 3+ times with identical parameters, STOP and try a different tool or approach.
</rules>

<rules priority="critical">
MATH ACCURACY — Getting billing math wrong is unacceptable.
- Structure every billing response like this: (1) State the billing period dates. (2) Show each line item with multiplication. (3) Show the addition of line items. (4) State the total LAST.
- NEVER state a dollar total before showing the math that produces it.
- Write out every step: "38 x $25 = $950" then "$950 + $270 + $0 = $1,220"
- The final total MUST equal the sum of the line items above it. Verify before responding.
- Use the EXACT rates from the billing config in the entity context above. Never guess, round, or average rates.
</rules>

<rules priority="critical">
BILLING CALCULATIONS
- Use get_dashboard_stats to get booking counts. It returns "ai_direct_count", "ai_assisted_count", and "manual_count". Do NOT loop through query_bookings.
- Tiered billing formula: (ai_direct_count x ai_direct_rate) + (ai_assisted_count x ai_assisted_rate) + (manual_count x manual_rate) = total
- Flat billing: total_bookings x flat_rate = total
- Retainer billing: fixed monthly amount
- Always list all three booking types separately for tiered billing. Never combine assisted and manual.

BILLING CYCLE
- If billing_start_date is set in the entity context (e.g. "2026-02-22"), each cycle runs from the 22nd of one month to the 22nd of the next: Feb 22 to Mar 22, Mar 22 to Apr 22, etc.
- If no billing_start_date, cycles are the 1st to the 1st: Jan 1 to Feb 1, Feb 1 to Mar 1, etc.
- "How much do I owe" or "this month" = current cycle start to today.
- "Last month" or "last bill" = the previous COMPLETED cycle.
- Always state the exact date range you are calculating.
- Pass dates to get_dashboard_stats matching the billing cycle, NOT calendar month boundaries.

MID-CYCLE RATE CHANGES
- First, check if any rate change date in the timeline falls WITHIN the billing cycle dates. If a rate change date is BEFORE the cycle start date, IGNORE it — those rates are not relevant to this cycle.
- If rates DID change during the cycle, split precisely — call get_dashboard_stats separately for each sub-period.
- Apply the correct rates from the timeline to each sub-period, then sum.
- Example for cycle Feb 22-Mar 22 with rate change on Mar 10:
  Call 1: date_start=2026-02-22, date_end=2026-03-10 -> apply rates from the "2026-02-20 to 2026-03-10" timeline period
  Call 2: date_start=2026-03-10, date_end=2026-03-22 -> apply rates from the "2026-03-10 to now" timeline period
  Total = Call 1 total + Call 2 total
- If NO rate changes fall within the cycle dates, use the single rate that was active during that period — do not split.

CROSS-ENTITY COMPARISON
- When comparing entities (e.g. "which client has more leads"), call tools SEPARATELY for each entity using the entity_id parameter.
- Do NOT call a tool once with all entities and expect per-entity breakdowns — the tool aggregates them together.
- Example: get_dashboard_stats(entity_id="abc") then get_dashboard_stats(entity_id="xyz"), compare the results.
</rules>

<rules>
DATE AND TIME FORMATTING
- Format dates as "Jan 14, 2026 at 9:20 PM" — never raw ISO like "2026-01-14 21:20:55"
- For conversation history: "Jan 14 at 9:20 PM - Lead: message text"
- When the user says "this week", use Monday-Sunday of the current week.
- When the user is vague about time (no specific period mentioned), provide 7-day, 30-day, 90-day, and all-time breakdowns.
- "Total" or "all time" = you MUST pass date_start="2020-01-01" explicitly. The tool defaults to 30 days if you omit dates, so "all time" requires you to set date_start.
- When the result says "total" in your response, label it clearly: "All Time Total: X" so the user knows it covers everything.

OUTPUT FORMATTING
- No markdown. No asterisks, pipe tables, headers with #, or backticks.
- Plain text only. Use dashes for lists. Use spacing for alignment.
- Format numbers with commas (1,234) and round percentages to 1 decimal.
</rules>

<rules>
AI BEHAVIOR FEEDBACK AND CONFIG SUGGESTIONS
- When a user complains about AI behavior (too pushy, unprofessional, not booking enough, etc.), use the get_setter_config tool to see current settings.
- Cross-reference the complaint with the config and suggest specific changes.
- NEVER use technical names with underscores. Say "Max Booking Pushes" not "max_booking_pushes". Say "Steer Toward Goal" not "steer_toward_goal".
- Format suggestions as: "I recommend turning OFF [Setting Name] in the [Section] settings. Currently it is enabled, which may be causing [the behavior they complained about]."
- Always explain WHY the change would help — connect the setting to the behavior.
- You cannot make changes — only suggest. Tell the user to go to the Setters tab to make the change.
</rules>

<rules>
ONBOARDING QUESTIONS
- Use get_onboarding_template for tenant-level questions about the onboarding process, phases, required steps, and system checks.
- Use query_onboarding_queue for tenant-wide onboarding status questions like which clients are blocked, still onboarding, completed, or out of sync with the current template.
- Use get_entity_onboarding_detail for a single client's onboarding progress, blockers, ready checks, and remaining work.
- Distinguish carefully between:
  - tenant onboarding template = the default onboarding process/checklist
  - entity onboarding = one client's current onboarding snapshot and progress
  - tenant default setter config = the seeded AI configuration, not the onboarding template
- "Ready" means a system-check item currently passes.
- "Done" means the item has been manually marked done or skipped.
- Do not say you synced, completed, edited, or changed onboarding. You can only report what the data shows.
</rules>

<rules>
PLATFORM HOW-TO AND CHANGE REQUESTS
- Use search_platform_knowledge for questions about how to use the platform, what a page does, where settings live, how onboarding works, and where to edit something.
- When platform knowledge returns actions or screenshots, use them naturally in your answer.
- For portal users who want something changed, use draft_change_request instead of implying they can edit it directly.
- For portal users asking to change settings, prompts, knowledge base, outreach, or setup, your answer should clearly say they cannot edit it directly and that you prepared a request-change path for them.
- For admin users asking where to change something, prefer platform knowledge and shortcuts unless you are explicitly answering from a live config tool.
</rules>

<rules>
ADMIN EDIT REQUESTS
- Before proposing an admin-side edit, use get_action_capabilities so you know exactly what Ask AI can and cannot change.
- Use get_setter_snapshot for AI Setter edit requests and get_client_settings_snapshot for Client Settings edit requests so you see the live values before proposing a change.
- Use get_outreach_template_snapshot for lead-sequence or appointment-reminder content edits so you see the live template values before proposing a change.
- If the requested change is outside the capability whitelist, say so clearly and do not imply you can do it.
- If the requested change is inside the capability whitelist, explain the intended change in plain English and prepare a confirmable action only after the relevant live config has been inspected.
</rules>

<rules priority="critical">
REMINDER: ALWAYS USE TOOLS FOR DATA. Even if you saw a number earlier in this conversation, call the tool again. Your memory of numbers is wrong. The tool is always right. No exceptions.
</rules>"""


def _build_system_prompt(
    tenant_name: str,
    entity_context: str,
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return _SYSTEM_PROMPT_BASE.format(
        tenant_name=tenant_name,
        current_time=now,
        entity_context=entity_context,
    )


async def _build_entity_context(
    entity_id: str | None,
    entity_ids: list[str],
    entities_info: list[dict],
) -> str:
    """Build the entity context section for the system prompt.

    Entity-scoped: pre-inject name, timezone, stage, billing, features.
    Global: just list entity names + IDs.
    """
    if entity_id and entity_id in entity_ids:
        # Entity-scoped — pre-inject detailed info
        entity = next((e for e in entities_info if e["id"] == entity_id), None)
        if entity:
            billing = entity.get("billing_config") or {}
            billing_model = billing.get("model", "unknown")
            booking_val = entity.get("average_booking_value", 0)

            # Build billing details
            billing_lines = [f"Billing model: {billing_model}"]
            if billing_model == "tiered":
                billing_lines.append(f"AI direct rate: ${billing.get('ai_direct_rate', 0)} per booking")
                billing_lines.append(f"AI assisted rate: ${billing.get('ai_assisted_rate', 0)} per booking")
                billing_lines.append(f"Manual rate: ${billing.get('manual_rate', 0)} per booking")
            elif billing_model == "flat":
                billing_lines.append(f"Flat rate: ${billing.get('rate', 0)} per booking")
            elif billing_model == "retainer":
                billing_lines.append(f"Monthly retainer: ${billing.get('amount', 0)}")

            if billing.get("trial_start") or billing.get("trial_end"):
                billing_lines.append(f"Trial period: {billing.get('trial_start', '?')} to {billing.get('trial_end', '?')}")

            # Billing cycle — use billing_start_date if set
            billing_start = billing.get("billing_start_date")
            if billing_start:
                try:
                    day = int(billing_start.split("-")[2])
                    billing_lines.append(f"Billing start date: {billing_start}")
                    billing_lines.append(f"Billing cycle: the {day}th of every month. Each cycle runs from the {day}th to the {day}th of the next month (e.g. Feb 22 to Mar 22, Mar 22 to Apr 22, etc.)")
                except (IndexError, ValueError):
                    billing_lines.append(f"Billing start date: {billing_start}")
                    billing_lines.append("Billing cycle: runs monthly from the billing start date")
            else:
                billing_lines.append("Billing cycle: the 1st of every month (1st to 1st of the next month)")

            # Build explicit rate timeline with date ranges (not ambiguous "before X" format)
            history = billing.get("history") or []
            if history:
                # Filter out no-op history entries (save without actual rate change)
                current_rates = (billing.get("ai_direct_rate", 0), billing.get("ai_assisted_rate", 0), billing.get("manual_rate", 0))
                filtered_history = []
                for entry in sorted(history, key=lambda h: h.get("changed_at", "")):
                    prev = entry.get("previous", {})
                    prev_rates = (prev.get("ai_direct_rate", 0), prev.get("ai_assisted_rate", 0), prev.get("manual_rate", 0))
                    # Skip if this entry's "previous" rates are the same as what came after (no actual change)
                    if filtered_history:
                        last_prev = filtered_history[-1].get("previous", {})
                        last_rates = (last_prev.get("ai_direct_rate", 0), last_prev.get("ai_assisted_rate", 0), last_prev.get("manual_rate", 0))
                        if prev_rates == last_rates:
                            continue  # Duplicate — no real change
                    filtered_history.append(entry)

                # Build timeline
                periods = []
                sorted_history = filtered_history
                for i, entry in enumerate(sorted_history):
                    prev = entry.get("previous", {})
                    changed = entry.get("changed_at", "")[:10]
                    if i == 0:
                        periods.append({"start": "beginning", "end": changed, "rates": prev})
                    else:
                        prev_changed = sorted_history[i - 1].get("changed_at", "")[:10]
                        periods.append({"start": prev_changed, "end": changed, "rates": prev})
                # Current rates period
                last_changed = sorted_history[-1].get("changed_at", "")[:10] if sorted_history else "beginning"
                periods.append({"start": last_changed, "end": "now", "rates": {"ai_direct_rate": billing.get("ai_direct_rate", 0), "ai_assisted_rate": billing.get("ai_assisted_rate", 0), "manual_rate": billing.get("manual_rate", 0)}})

                billing_lines.append("Rate timeline (use this to determine which rates were active during any period):")
                for p in periods:
                    r = p["rates"]
                    billing_lines.append(f"  {p['start']} to {p['end']}: ${r.get('ai_direct_rate', 0)}/direct, ${r.get('ai_assisted_rate', 0)}/assisted, ${r.get('manual_rate', 0)}/manual")
                billing_lines.append("When calculating past bills, match the billing period dates to the rate timeline above to find the correct rates.")

            # Business schedule
            schedule = entity.get("business_schedule")
            schedule_str = ""
            if schedule and isinstance(schedule, dict):
                schedule_str = f"Business schedule: {json.dumps(schedule)}\n"

            return (
                f"You are scoped to: {entity.get('name', 'Unknown')} (ID: {entity_id})\n"
                f"Type: {entity.get('entity_type', 'client')}\n"
                f"Timezone: {entity.get('timezone', 'America/Chicago')}\n"
                f"Journey stage: {entity.get('journey_stage', 'unknown')}\n"
                + "\n".join(billing_lines) + "\n"
                f"Average booking value: ${booking_val}\n"
                + schedule_str
                + "All queries are scoped to this entity."
            )
        return f"You are scoped to entity ID: {entity_id}. All queries are for this entity."
    else:
        # Global — list only client entities (not internal setters)
        client_entities = [e for e in entities_info if e.get("entity_type") == "client"]
        lines = ["Available clients you can query:"]
        for e in client_entities[:50]:
            lines.append(f"- {e.get('name', 'Unknown')} (ID: {e['id']})")
        lines.append("\nYou can query any or all of these clients. Use entity_id to scope tool calls to a specific client.")
        return "\n".join(lines)


def _merge_ui_payload(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key in ("sections", "actions", "suggestions", "media", "citations"):
        current = merged.get(key) or []
        extra = incoming.get(key) or []
        if not isinstance(current, list):
            current = []
        if not isinstance(extra, list):
            extra = []
        for item in extra:
            if item not in current:
                current.append(item)
        merged[key] = current
    if "proposed_action" in incoming:
        merged["proposed_action"] = incoming.get("proposed_action")
    return merged


def _build_shortcut_candidates(ui_payload: dict[str, Any]) -> list[dict[str, Any]]:
    actions = ui_payload.get("actions") or []
    if not isinstance(actions, list):
        actions = []
    candidates: list[dict[str, Any]] = []
    for index, action in enumerate(actions):
        if not isinstance(action, dict):
            continue
        candidates.append({
            "shortcut_id": f"forced_{index + 1}",
            "label": action.get("label") or f"Shortcut {index + 1}",
            "type": action.get("type") or "",
            "href": action.get("href") or "",
            "topic": action.get("topic") or "",
            "description": action.get("description") or "",
            "entity_id": action.get("entity_id") or "",
            "_raw": action,
        })
    return candidates


def _build_registry_shortcuts(origin: str, entity_id: str | None, entity_name: str | None = None) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    entity_label = (entity_name or "Client").strip()

    def add(label: str, raw: dict[str, Any]) -> None:
        candidates.append({
            "shortcut_id": f"registry_{len(candidates) + 1}",
            "label": label,
            "type": raw.get("type") or "",
            "href": raw.get("href") or "",
            "topic": raw.get("topic") or "",
            "description": raw.get("description") or "",
            "entity_id": raw.get("entity_id") or "",
            "_raw": raw,
        })

    if origin == "portal":
        if entity_id:
            add(f"Open {entity_label} Portal", {"type": "open_route", "label": f"Open {entity_label} Portal", "href": f"/portal/{entity_id}"})
        add("Open Request Change", {"type": "open_request_change", "label": "Open Request Change"})
        return candidates

    add("Open Dashboard", {"type": "open_route", "label": "Open Dashboard", "href": "/dashboard"})
    add("Open Clients", {"type": "open_route", "label": "Open Clients", "href": "/clients"})
    add("Open Settings", {"type": "open_route", "label": "Open Settings", "href": "/settings"})
    add("Open Onboarding Queue", {"type": "open_route", "label": "Open Onboarding Queue", "href": "/onboarding"})
    if entity_id:
        add(f"Open {entity_label} Onboarding", {"type": "open_route", "label": f"Open {entity_label} Onboarding", "href": f"/onboarding/{entity_id}"})
        add(f"Open {entity_label} AI Setter", {"type": "open_route", "label": f"Open {entity_label} AI Setter", "href": f"/clients/{entity_id}?tab=ai-setter"})
        add(f"Open {entity_label} Bot Persona", {"type": "open_route", "label": f"Open {entity_label} Bot Persona", "href": f"/clients/{entity_id}?tab=ai-setter&dialog=bot_persona"})
        add(f"Open {entity_label} Services & Offers", {"type": "open_route", "label": f"Open {entity_label} Services & Offers", "href": f"/clients/{entity_id}?tab=ai-setter&dialog=services_and_offers"})
        add(f"Open {entity_label} Automations", {"type": "open_route", "label": f"Open {entity_label} Automations", "href": f"/clients/{entity_id}?tab=ai-setter&dialog=automations"})
        add(f"Open {entity_label} Client Settings", {"type": "open_route", "label": f"Open {entity_label} Client Settings", "href": f"/clients/{entity_id}?tab=client-settings"})
        add(f"Open {entity_label} Knowledge Base", {"type": "open_route", "label": f"Open {entity_label} Knowledge Base", "href": f"/clients/{entity_id}?tab=knowledge-base"})
    return candidates


def _build_final_format_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "answer": {"type": "string"},
            "shortcut_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": ["answer", "shortcut_ids"],
        "additionalProperties": False,
    }


async def _format_final_response(
    *,
    tenant_name: str,
    origin: str,
    entity_id: str | None,
    entity_name: str | None,
    user_message: str,
    draft_answer: str,
    ui_payload: dict[str, Any],
    model: str,
) -> tuple[str, dict[str, Any]]:
    forced_shortcuts = _build_shortcut_candidates(ui_payload)
    registry_shortcuts = _build_registry_shortcuts(origin, entity_id, entity_name=entity_name)
    shortcut_candidates: list[dict[str, Any]] = []
    seen_labels: set[str] = set()
    for candidate in [*forced_shortcuts, *registry_shortcuts]:
        label = candidate.get("label") or ""
        if not label or label in seen_labels:
            continue
        seen_labels.add(label)
        shortcut_candidates.append(candidate)
    guide_titles = [section.get("title") for section in (ui_payload.get("sections") or []) if isinstance(section, dict) and section.get("title")]
    shortcut_candidate_payload = [
        {
            "shortcut_id": candidate["shortcut_id"],
            "label": candidate["label"],
            "type": candidate["type"],
            "href": candidate["href"],
        }
        for candidate in shortcut_candidates
    ]

    formatter_system = (
        "You are formatting the final Ask AI response.\n"
        "Your job is to keep the main answer clear, concise, and personalized.\n"
        "The hidden guides were already used as references. Do not repeat the guide titles unless needed.\n"
        "Choose shortcut buttons only if they directly help the user act on the answer.\n"
        "Choose between 1 and 6 shortcuts when useful.\n"
        "Prefer the most direct shortcut first, but keep 1-2 useful fallback shortcuts when they are clearly relevant.\n"
        "If a shortcut is not clearly useful, leave it out.\n"
        "Portal users cannot directly edit settings; keep that constraint clear.\n"
        "No markdown. No asterisks. No bold. Do not wrap button labels or page names in ** or backticks.\n"
        "Return valid JSON only."
    )
    formatter_user = (
        f"Tenant: {tenant_name}\n"
        f"Origin: {origin}\n"
        f"User question: {user_message}\n\n"
        f"Draft answer:\n{draft_answer}\n\n"
        f"Guide titles used internally: {guide_titles}\n\n"
        f"Available shortcut candidates:\n{json.dumps(shortcut_candidate_payload, indent=2)}"
    )

    response = await chat(
        messages=[
            {"role": "system", "content": formatter_system},
            {"role": "user", "content": formatter_user},
        ],
        model=model,
        temperature=0.2,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "ask_ai_final_response",
                "strict": True,
                "schema": _build_final_format_schema(),
            },
        },
        label="data_chat_format",
    )

    content = response.choices[0].message.content or "{}"
    payload = json.loads(content)
    selected_ids = set(payload.get("shortcut_ids") or [])
    selected_actions = [
        candidate["_raw"]
        for candidate in shortcut_candidates
        if candidate["shortcut_id"] in selected_ids
    ]
    selected_actions = _ensure_direct_shortcuts(selected_actions, shortcut_candidates, user_message)
    final_ui_payload = dict(ui_payload)
    final_ui_payload["actions"] = selected_actions
    final_ui_payload["suggestions"] = []
    return str(payload.get("answer") or draft_answer), final_ui_payload


def _looks_like_change_request(message: str) -> bool:
    text = message.lower()
    triggers = [
        "change ", "update ", "edit ", "turn off", "turn on", "fix ", "adjust ",
        "can you ", "please ", "set ", "remove ", "add ", "switch ",
        "notification", "notify ", "send ",
    ]
    return any(trigger in text for trigger in triggers)


def _infer_change_request_topic(message: str) -> str:
    text = message.lower()
    if any(term in text for term in ["knowledge base", "kb article", "faq"]):
        return "Knowledge Base"
    if any(term in text for term in ["outreach", "reactivation", "missed call", "template", "automation"]):
        return "Outreach"
    if any(term in text for term in ["booking rate", "analytics", "dashboard", "performance", "report"]):
        return "Analytics"
    if any(term in text for term in ["bot persona", "message splitting", "setter", "prompt", "ai model", "conversation setting"]):
        return "Setters"
    return "Other"


def _sanitize_plain_text_answer(text: str) -> str:
    return (text or "").replace("**", "").replace("`", "").strip()


def _align_answer_with_proposed_action(answer: str, summary: str) -> str:
    cleaned = (answer or "").strip()
    lower = cleaned.lower()
    contradiction_markers = [
        "cannot apply this change directly",
        "submit this change request",
        "submit a change request",
        "prepared a change request",
        "prepared a request",
        "would you like me to submit",
        "portal user",
        "i cannot directly",
        "i can't directly",
        "unable to directly edit",
        "not currently in my admin edit whitelist",
    ]
    if not cleaned or any(marker in lower for marker in contradiction_markers):
        return f"{summary}\n\nPlease confirm if you want me to apply this change."
    if "confirm" not in lower:
        return f"{cleaned}\n\nPlease confirm if you want me to apply this change."
    return cleaned


def _tool_result_map(tool_contexts: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in tool_contexts:
        name = item.get("tool")
        result = item.get("result")
        if not isinstance(name, str):
            continue
        if isinstance(result, dict):
            out[name] = result
            continue
        if isinstance(result, str):
            try:
                out[name] = json.loads(result)
            except Exception:
                continue
    return out


async def _ensure_admin_action_tool_contexts(
    *,
    tool_contexts: list[dict[str, Any]],
    tenant_id: str,
    entity_id: str,
    tz_name: str,
    origin: str,
) -> list[dict[str, Any]]:
    context = list(tool_contexts)
    existing = _tool_result_map(context)

    async def ensure(tool_name: str, args: dict[str, Any]) -> None:
        if tool_name in existing:
            return
        raw_result = await execute_tool(
            tool_name,
            args,
            [entity_id],
            tz_name=tz_name,
            tenant_id=tenant_id,
            origin=origin,
        )
        try:
            parsed = json.loads(raw_result)
        except Exception:
            parsed = raw_result
        context.append({
            "tool": tool_name,
            "arguments": args,
            "result": parsed,
        })
        existing[tool_name] = parsed

    await ensure("get_action_capabilities", {})
    await ensure("get_setter_snapshot", {"entity_id": entity_id})
    await ensure("get_client_settings_snapshot", {"entity_id": entity_id})
    await ensure("get_outreach_template_snapshot", {"entity_id": entity_id})
    return context


def _is_allowed_client_proposal_path(path: str) -> bool:
    return path in ALLOWED_CLIENT_PATHS or any(
        path == prefix
        or path.startswith(f"{prefix}.")
        or path.startswith(f"{prefix}[")
        for prefix in ALLOWED_CLIENT_PREFIXES
    )


def _format_editable_value(value: Any) -> str:
    if isinstance(value, str):
        raw = json.dumps(value)
    elif isinstance(value, (int, float, bool)) or value is None:
        raw = json.dumps(value)
    elif isinstance(value, list):
        if all(isinstance(item, dict) for item in value[:10]):
            labels = []
            for index, item in enumerate(value[:10]):
                label = item.get("name") or item.get("title") or item.get("label") or item.get("id") or f"item {index + 1}"
                labels.append(f"[{index}] {label}")
            extra = f" (+{len(value) - 10} more)" if len(value) > 10 else ""
            raw = f"list[{len(value)}]: " + "; ".join(labels) + extra
        else:
            raw = json.dumps(value)
    elif isinstance(value, dict):
        keys = list(value.keys())[:8]
        suffix = ", ..." if len(value) > 8 else ""
        raw = "object{" + ", ".join(str(key) for key in keys) + suffix + "}"
    else:
        raw = repr(value)
    return raw if len(raw) <= 180 else f"{raw[:177]}..."


def _collect_editable_paths(
    value: Any,
    prefix: str,
    out: list[tuple[str, Any]],
    *,
    depth: int = 0,
    max_entries: int = 180,
) -> None:
    if len(out) >= max_entries or depth > 8:
        return

    if isinstance(value, (str, int, float, bool)) or value is None:
        if prefix:
            out.append((prefix, value))
        return

    if isinstance(value, list):
        if prefix:
            out.append((prefix, value))
        for index, item in enumerate(value[:20]):
            if len(out) >= max_entries:
                return
            item_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
            if isinstance(item, dict):
                label = item.get("name") or item.get("title") or item.get("label") or item.get("id")
                if label is not None:
                    out.append((item_prefix, label))
                for key, child in item.items():
                    _collect_editable_paths(
                        child,
                        f"{item_prefix}.{key}",
                        out,
                        depth=depth + 1,
                        max_entries=max_entries,
                    )
            else:
                _collect_editable_paths(
                    item,
                    item_prefix,
                    out,
                    depth=depth + 1,
                    max_entries=max_entries,
                )
        return

    if isinstance(value, dict):
        for key, child in value.items():
            if len(out) >= max_entries:
                return
            next_prefix = f"{prefix}.{key}" if prefix else key
            _collect_editable_paths(
                child,
                next_prefix,
                out,
                depth=depth + 1,
                max_entries=max_entries,
            )


def _build_live_editable_paths(tool_contexts: list[dict[str, Any]]) -> list[str]:
    tool_map = _tool_result_map(tool_contexts)
    lines: list[str] = []
    pairs: list[tuple[str, Any]] = []

    setter_snapshot = tool_map.get("get_setter_snapshot") or {}
    setter_data: Any = setter_snapshot.get("setter")
    if not isinstance(setter_data, dict):
        surface = setter_snapshot.get("surface")
        value = setter_snapshot.get("value")
        if isinstance(surface, str) and surface:
            setter_data = {surface: value}
    if isinstance(setter_data, dict):
        _collect_editable_paths(setter_data, "", pairs)

    client_snapshot = tool_map.get("get_client_settings_snapshot") or {}
    if isinstance(client_snapshot, dict) and client_snapshot:
        safe_client_root = {
            "journey_stage": client_snapshot.get("journey_stage"),
            "timezone": client_snapshot.get("timezone"),
            "business_phone": client_snapshot.get("business_phone"),
            "notes": client_snapshot.get("notes"),
            "average_booking_value": client_snapshot.get("average_booking_value"),
            "system_config": {
                "sms_provider": client_snapshot.get("sms_provider"),
                "pause_bot_on_human_activity": client_snapshot.get("pause_bot_on_human_activity"),
                "human_takeover_minutes": client_snapshot.get("human_takeover_minutes"),
                "notifications": {
                    "recipients": client_snapshot.get("notification_recipients") or [],
                },
            },
        }
        _collect_editable_paths(safe_client_root, "", pairs)

    outreach_snapshot = tool_map.get("get_outreach_template_snapshot") or {}
    templates = outreach_snapshot.get("templates") if isinstance(outreach_snapshot, dict) else None
    if isinstance(templates, list):
        for template in templates[:20]:
            if not isinstance(template, dict):
                continue
            template_id = str(template.get("id") or "").strip()
            label = str(template.get("form_service_interest") or ("Appointment Template" if template.get("is_appointment_template") else "Lead Sequence"))
            if template_id:
                lines.append(
                    f"- outreach_template[{template_id}] "
                    f"kind={'appointment' if template.get('is_appointment_template') else 'lead'} "
                    f"label={label}"
                )
            positions = template.get("positions")
            if isinstance(positions, list):
                outreach_pairs: list[tuple[str, Any]] = []
                _collect_editable_paths({"positions": positions}, "", outreach_pairs, max_entries=80)
                for path, value in outreach_pairs:
                    if not path or path == "positions":
                        continue
                    if not _is_allowed_outreach_path(path):
                        continue
                    lines.append(f"- outreach_template[{template_id}].{path} = {_format_editable_value(value)}")
                    if len(lines) >= 180:
                        return lines

    seen_paths: set[str] = set()
    for path, value in pairs:
        if not path or path in seen_paths:
            continue
        if not (
            any(path == prefix or path.startswith(f"{prefix}.") or path.startswith(f"{prefix}[") for prefix in ALLOWED_SETTER_PREFIXES)
            or _is_allowed_client_proposal_path(path)
        ):
            continue
        seen_paths.add(path)
        lines.append(f"- {path} = {_format_editable_value(value)}")
        if len(lines) >= 140:
            break
    return lines


def _ensure_direct_shortcuts(
    selected_actions: list[dict[str, Any]],
    shortcut_candidates: list[dict[str, Any]],
    user_message: str,
) -> list[dict[str, Any]]:
    text = user_message.lower()
    required_label_fragments: list[str] = []

    def require_if_any(terms: list[str], fragments: list[str]) -> None:
        if any(term in text for term in terms):
            required_label_fragments.extend(fragments)

    require_if_any(["knowledge base", " kb "], ["Knowledge Base"])
    require_if_any(
        ["client settings", "client info", "business phone", "timezone", "journey stage", "client status", "notification", "sms provider", "human takeover"],
        ["Client Settings"],
    )
    require_if_any(["bot persona", "message splitting", "tone", "ai disclosure", "punctuation"], ["Bot Persona", "AI Setter"])
    require_if_any(["service", "services", "offers", "pricing"], ["Services & Offers", "AI Setter"])
    require_if_any(["automation", "automations", "lead sequence", "lead sequences", "missed call", "reactivation", "appointment reminder"], ["Automations", "AI Setter"])
    require_if_any(["onboarding"], ["Onboarding"])

    selected_labels = {str(action.get("label") or "") for action in selected_actions if isinstance(action, dict)}
    for fragment in required_label_fragments:
        if any(fragment in label for label in selected_labels):
            continue
        match = next(
            (
                candidate["_raw"]
                for candidate in shortcut_candidates
                if fragment.lower() in str(candidate.get("label") or "").lower()
            ),
            None,
        )
        if match:
            selected_actions.append(match)
            selected_labels.add(str(match.get("label") or ""))

    return selected_actions[:6]


def _heuristic_proposed_action(
    *,
    entity_name: str | None,
    user_message: str,
    tool_contexts: list[dict[str, Any]],
) -> dict[str, Any] | None:
    text = user_message.lower()
    tool_map = _tool_result_map(tool_contexts)
    setter_snapshot = tool_map.get("get_setter_snapshot") or {}
    client_snapshot = tool_map.get("get_client_settings_snapshot") or {}
    setter_data = setter_snapshot.get("setter") if isinstance(setter_snapshot.get("setter"), dict) else {}
    if not setter_data and isinstance(setter_snapshot.get("surface"), str):
        setter_data = {setter_snapshot["surface"]: setter_snapshot.get("value")}

    setter_key = setter_snapshot.get("setter_key") or ""

    if "message splitting" in text:
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop", "remove"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            skip_splitting = True if turn_off else False
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_setter_value",
                "label": "Update Message Splitting",
                "summary": f"Turn {state_label} message splitting for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "bot_persona.sections.message_split.skip_splitting",
                "value": skip_splitting,
            }

    if ("bot name" in text or "rename the bot" in text or "change the bot name" in text or "bot persona name" in text) and setter_data:
        identity = (((setter_data.get("bot_persona") or {}).get("sections") or {}).get("identity") or {})
        current_name = str(identity.get("name") or "").strip()
        rename_match = re.search(
            r"(?:bot name|rename the bot|change the bot name|bot persona name)\s+(?:to|from\s+.+?\s+to)\s+([A-Za-z][A-Za-z\s'\-]{0,40})",
            user_message,
            re.IGNORECASE,
        )
        target_name = rename_match.group(1).strip(" .!?\"'") if rename_match else ""
        if target_name and target_name != current_name:
            return {
                "action_type": "rename_bot_identity",
                "label": "Rename Bot Persona",
                "summary": f"Rename the bot for {entity_name or 'this client'} from {current_name or 'its current name'} to {target_name}, and update the identity text to match.",
                "setter_key": setter_key,
                "path": "bot_persona.sections.identity.name",
                "value": target_name,
                "old_name": current_name,
                "new_name": target_name,
            }

    if "punctuation style" in text:
        punctuation_map = {
            "casual": "casual",
            "proper": "proper",
            "expressive": "expressive",
        }
        target_style = next((value for phrase, value in punctuation_map.items() if phrase in text), None)
        if target_style:
            current_style = (((setter_data.get("bot_persona") or {}).get("sections") or {}).get("punctuation_style") or {}).get("selected")
            if current_style != target_style:
                return {
                    "action_type": "set_setter_value",
                    "label": "Update Punctuation Style",
                    "summary": f"Change the punctuation style from {current_style or 'Unknown'} to {target_style} for {entity_name or 'this client'}.",
                    "setter_key": setter_key,
                    "path": "bot_persona.sections.punctuation_style.selected",
                    "value": target_style,
                }

    if "max booking pushes" in text:
        match = re.search(r"(\d+)", text)
        if match:
            pushes = int(match.group(1))
            current_max = (((((setter_data.get("conversation") or {}).get("reply") or {}).get("sections") or {}).get("max_booking_pushes") or {}).get("max"))
            if current_max != pushes:
                return {
                    "action_type": "set_setter_value",
                    "label": "Update Max Booking Pushes",
                    "summary": f"Set the maximum number of booking pushes to {pushes} for {entity_name or 'this client'}.",
                    "setter_key": setter_key,
                    "path": "conversation.reply.sections.max_booking_pushes.max",
                    "value": pushes,
                }

    if "steer toward goal" in text:
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            next_value = False if turn_off else True
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_setter_value",
                "label": "Update Steer Toward Goal",
                "summary": f"Turn {state_label} steer toward goal for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "conversation.reply.sections.steer_toward_goal.enabled",
                "value": next_value,
            }

    if "ai disclosure" in text or "reveal when asked" in text or "never reveal" in text or "upfront ai" in text:
        disclosure_map = {
            "reveal when asked": "reveal_when_asked",
            "never reveal": "never_reveal",
            "upfront ai": "upfront_ai",
            "up front ai": "upfront_ai",
        }
        target_disclosure = next((value for phrase, value in disclosure_map.items() if phrase in text), None)
        if target_disclosure:
            current_disclosure = (((setter_data.get("bot_persona") or {}).get("sections") or {}).get("ai_disclosure") or {}).get("selected")
            if current_disclosure != target_disclosure:
                return {
                    "action_type": "set_setter_value",
                    "label": "Update AI Disclosure",
                    "summary": f"Change the AI disclosure setting for {entity_name or 'this client'} from {current_disclosure or 'Unknown'} to {target_disclosure}.",
                    "setter_key": setter_key,
                    "path": "bot_persona.sections.ai_disclosure.selected",
                    "value": target_disclosure,
                }

    services = (((setter_data.get("services") or {}).get("services")) or [])
    if any(term in text for term in ["pricing", "price"]) and isinstance(services, list):
        for index, service in enumerate(services):
            if not isinstance(service, dict):
                continue
            service_name = str(service.get("name") or "").strip()
            if not service_name or service_name.lower() not in text:
                continue
            price_match = re.search(r"\bto\b\s+(.+?)(?:\s+for this client)?[.!?]*$", user_message, re.IGNORECASE)
            next_price = price_match.group(1).strip() if price_match else ""
            if next_price:
                return {
                    "action_type": "set_setter_value",
                    "label": f"Update {service_name} Pricing",
                    "summary": f"Update the pricing for {service_name} from {service.get('pricing') or 'Unknown'} to {next_price} for {entity_name or 'this client'}.",
                    "setter_key": setter_key,
                    "path": f"services.services[{index}].pricing",
                    "value": next_price,
                }

    if "status" in text or "journey stage" in text:
        stage_map = {
            "onboarding": "Onboarding",
            "active": "Active",
            "paused": "Paused",
            "churned": "Churned",
        }
        target_stage = next((value for key, value in stage_map.items() if key in text), None)
        if target_stage:
            current_stage = client_snapshot.get("journey_stage")
            if current_stage != target_stage:
                return {
                    "action_type": "set_client_value",
                    "label": "Update Client Status",
                    "summary": f"Update the journey stage for {entity_name or 'this client'} from {current_stage or 'Unknown'} to {target_stage}.",
                    "setter_key": "",
                    "path": "journey_stage",
                    "value": target_stage,
                }

    if "business phone" in text or "text phone" in text:
        match = re.search(r"(\+?\d[\d\-\s\(\)]{7,}\d)", user_message)
        if match:
            phone = re.sub(r"[^\d+]", "", match.group(1))
            return {
                "action_type": "set_client_value",
                "label": "Update Business Phone",
                "summary": f"Update the business phone for {entity_name or 'this client'} to {phone}.",
                "setter_key": "",
                "path": "business_phone",
                "value": phone,
            }

    if "notification" in text:
        recipients = client_snapshot.get("notification_recipients") or []
        safe_recipients = deepcopy(recipients) if isinstance(recipients, list) else []
        if not safe_recipients:
            safe_recipients = [{
                "sms": "",
                "name": "",
                "email": "",
                "events": {
                    "booking": False,
                    "new_lead": False,
                    "missed_call": False,
                    "transfer_to_human": False,
                },
            }]
        first_recipient = safe_recipients[0] if isinstance(safe_recipients[0], dict) else {
            "sms": "",
            "name": "",
            "email": "",
            "events": {},
        }
        safe_recipients[0] = first_recipient
        first_recipient.setdefault("events", {})

        email_match = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", user_message, re.IGNORECASE)
        if email_match:
            email = email_match.group(1)
            first_recipient["email"] = email
            if "booking" in text:
                first_recipient["events"]["booking"] = True
            if "new lead" in text:
                first_recipient["events"]["new_lead"] = True
            if "missed call" in text:
                first_recipient["events"]["missed_call"] = True
            if "transfer" in text:
                first_recipient["events"]["transfer_to_human"] = True
            return {
                "action_type": "set_client_value",
                "label": "Update Notification Recipient",
                "summary": f"Update the first notification recipient for {entity_name or 'this client'} to use {email}.",
                "setter_key": "",
                "path": "system_config.notifications.recipients",
                "value": safe_recipients,
            }

        if "name" in text and "notification recipient" in text:
            name_match = re.search(r"\bto\b\s+(.+?)(?:\s+for this client)?[.!?]*$", user_message, re.IGNORECASE)
            recipient_name = name_match.group(1).strip(" .!?\"'") if name_match else ""
            if recipient_name:
                first_recipient["name"] = recipient_name
                return {
                    "action_type": "set_client_value",
                    "label": "Update Notification Recipient Name",
                    "summary": f"Set the name for the first notification recipient on {entity_name or 'this client'} to {recipient_name}.",
                    "setter_key": "",
                    "path": "system_config.notifications.recipients",
                    "value": safe_recipients,
                }

        sms_match = re.search(r"(\+?\d[\d\-\s\(\)]{7,}\d)", user_message)
        if sms_match:
            sms_number = re.sub(r"[^\d+]", "", sms_match.group(1))
            first_recipient["sms"] = sms_number
            return {
                "action_type": "set_client_value",
                "label": "Update Notification SMS",
                "summary": f"Set the SMS notification number to {sms_number} for the first recipient on {entity_name or 'this client'}.",
                "setter_key": "",
                "path": "system_config.notifications.recipients",
                "value": safe_recipients,
            }

        event_map = {
            "booking": ("booking", "booking notifications"),
            "new lead": ("new_lead", "new lead notifications"),
            "missed call": ("missed_call", "missed call notifications"),
            "transfer": ("transfer_to_human", "transfer notifications"),
        }
        event_match = next(((key, label) for phrase, (key, label) in event_map.items() if phrase in text), None)
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if event_match and (turn_off or turn_on):
            event_key, event_label = event_match
            next_value = False if turn_off else True
            first_recipient["events"][event_key] = next_value
            state_label = "on" if next_value else "off"
            return {
                "action_type": "set_client_value",
                "label": "Update Notification Events",
                "summary": f"Turn {state_label} {event_label} for the first notification recipient on {entity_name or 'this client'}.",
                "setter_key": "",
                "path": "system_config.notifications.recipients",
                "value": safe_recipients,
            }

    if "appointment length" in text or "booking length" in text:
        match = re.search(r"(\d+)", text)
        calendars = (setter_snapshot.get("value") or {}).get("calendars") if isinstance(setter_snapshot.get("value"), dict) else []
        calendar_id = ""
        calendar_name = "the default calendar"
        if isinstance(calendars, list) and calendars:
            first = calendars[0] or {}
            if isinstance(first, dict):
                calendar_id = first.get("calendar_id") or first.get("id") or ""
                calendar_name = first.get("name") or calendar_name
        if match:
            minutes = int(match.group(1))
            return {
                "action_type": "set_booking_appointment_length",
                "label": "Update Appointment Length",
                "summary": f"Update the appointment length for {calendar_name} to {minutes} minutes for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "booking.calendars.appointment_length_minutes",
                "value": minutes,
                "calendar_id": calendar_id,
            }

    if "booking window" in text and "day" in text:
        match = re.search(r"(\d+)", text)
        if match:
            days = int(match.group(1))
            return {
                "action_type": "set_setter_value",
                "label": "Update Booking Window",
                "summary": f"Set the booking window to {days} days for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "booking.booking_window_days",
                "value": days,
            }

    if "average booking value" in text or "booking value" in text:
        match = re.search(r"(\d+(?:\.\d+)?)", text)
        if match:
            amount = float(match.group(1))
            return {
                "action_type": "set_client_value",
                "label": "Update Average Booking Value",
                "summary": f"Update the average booking value for {entity_name or 'this client'} to ${amount}.",
                "setter_key": "",
                "path": "average_booking_value",
                "value": amount,
            }

    if "clear notes" in text or "remove notes" in text:
        return {
            "action_type": "set_client_value",
            "label": "Clear Client Notes",
            "summary": f"Clear the notes for {entity_name or 'this client'}.",
            "setter_key": "",
            "path": "notes",
            "value": None,
        }

    if "set notes to" in text or "update notes to" in text:
        note_value = user_message.split("to", 1)[-1].strip().strip("\"' ")
        if note_value:
            return {
                "action_type": "set_client_value",
                "label": "Update Client Notes",
                "summary": f"Update the notes for {entity_name or 'this client'}.",
                "setter_key": "",
                "path": "notes",
                "value": note_value,
            }

    if "timezone" in text:
        for zone in [
            "America/New_York", "America/Chicago", "America/Denver", "America/Phoenix",
            "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu",
        ]:
            if zone.lower() in text:
                return {
                    "action_type": "set_client_value",
                    "label": "Update Timezone",
                    "summary": f"Update the timezone for {entity_name or 'this client'} to {zone}.",
                    "setter_key": "",
                    "path": "timezone",
                    "value": zone,
                }

    if "sms provider" in text:
        provider_map = {
            "signal house": "signalhouse",
            "signalhouse": "signalhouse",
            "ghl default": "ghl_default",
            "imessage": "imessage",
            "other": "other",
        }
        for phrase, provider in provider_map.items():
            if phrase in text:
                return {
                    "action_type": "set_client_value",
                    "label": "Update SMS Provider",
                    "summary": f"Update the SMS provider for {entity_name or 'this client'} to {provider}.",
                    "setter_key": "",
                    "path": "system_config.sms_provider",
                    "value": provider,
                }

    if "pause bot on human activity" in text:
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            next_value = False if turn_off else True
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_client_value",
                "label": "Update Pause Bot on Human Activity",
                "summary": f"Turn {state_label} pause bot on human activity for {entity_name or 'this client'}.",
                "setter_key": "",
                "path": "system_config.pause_bot_on_human_activity",
                "value": next_value,
            }

    if "human takeover" in text and "minute" in text:
        match = re.search(r"(\d+)", text)
        if match:
            minutes = int(match.group(1))
            return {
                "action_type": "set_client_value",
                "label": "Update Human Takeover Timeout",
                "summary": f"Set human takeover timeout for {entity_name or 'this client'} to {minutes} minutes.",
                "setter_key": "",
                "path": "system_config.human_takeover_minutes",
                "value": minutes,
            }

    if "reply model" in text or "reply ai model" in text:
        match = re.search(r"([a-z0-9\-_]+/[a-z0-9\.\-_]+)", text)
        if match:
            model_id = match.group(1).rstrip(".,)")
            return {
                "action_type": "set_setter_value",
                "label": "Update Reply Model",
                "summary": f"Update the reply model for {entity_name or 'this client'} to {model_id}.",
                "setter_key": setter_key,
                "path": "ai_models.reply",
                "value": model_id,
            }

    if "prompt protection" in text:
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            next_value = False if turn_off else True
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_setter_value",
                "label": "Update Prompt Protection",
                "summary": f"Turn {state_label} prompt protection for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "security.protections.prompt_protection.enabled",
                "value": next_value,
            }

    if "jailbreak rejection" in text:
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            next_value = False if turn_off else True
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_setter_value",
                "label": "Update Jailbreak Rejection",
                "summary": f"Turn {state_label} jailbreak rejection for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "security.protections.jailbreak_rejection.enabled",
                "value": next_value,
            }

    if "missed call" in text and "delay" in text:
        match = re.search(r"(\d+)", text)
        if match:
            seconds = int(match.group(1))
            return {
                "action_type": "set_setter_value",
                "label": "Update Missed Call Text-back Max Delay",
                "summary": f"Update the maximum delay for missed call text-backs to {seconds} seconds for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "missed_call_textback.delay_max_seconds",
                "value": seconds,
            }

    if "review request" in text or "review requests" in text or "ask for reviews" in text:
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            next_value = False if turn_off else True
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_setter_value",
                "label": "Update Review Requests",
                "summary": f"Turn {state_label} post-appointment review requests for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "conversation.post_booking.request_review.enabled",
                "value": next_value,
            }

    if "output protection" in text:
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            next_value = False if turn_off else True
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_setter_value",
                "label": "Update Output Protection",
                "summary": f"Turn {state_label} output protection for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "security.protections.output_protection.enabled",
                "value": next_value,
            }

    if "reactivation" in text and "day" in text:
        match = re.search(r"(\d+)", text)
        if match:
            days = int(match.group(1))
            return {
                "action_type": "set_setter_value",
                "label": "Update Auto Reactivation Days",
                "summary": f"Update the auto reactivation period for {entity_name or 'this client'} to {days} days.",
                "setter_key": setter_key,
                "path": "auto_reactivation.days",
                "value": days,
            }

    transfer_scenarios = (((setter_data.get("transfer") or {}).get("scenarios")) or [])
    if isinstance(transfer_scenarios, list):
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            for index, scenario in enumerate(transfer_scenarios):
                if not isinstance(scenario, dict):
                    continue
                title = str(scenario.get("title") or "").strip()
                if not title or title.lower() not in text:
                    continue
                next_value = False if turn_off else True
                state_label = "off" if turn_off else "on"
                return {
                    "action_type": "set_setter_value",
                    "label": f"Update {title}",
                    "summary": f"Turn {state_label} the '{title}' transfer scenario for {entity_name or 'this client'}.",
                    "setter_key": setter_key,
                    "path": f"transfer.scenarios[{index}].enabled",
                    "value": next_value,
                }

    offers = ((((setter_data.get("services") or {}).get("offers")) or {}).get("offers")) or []
    if isinstance(offers, list):
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            for index, offer in enumerate(offers):
                if not isinstance(offer, dict):
                    continue
                offer_name = str(offer.get("name") or "").strip()
                if not offer_name or offer_name.lower() not in text:
                    continue
                next_value = False if turn_off else True
                state_label = "off" if turn_off else "on"
                return {
                    "action_type": "set_setter_value",
                    "label": f"Update {offer_name} Offer",
                    "summary": f"Turn {state_label} the '{offer_name}' offer for {entity_name or 'this client'}.",
                    "setter_key": setter_key,
                    "path": f"services.offers.offers[{index}].enabled",
                    "value": next_value,
                }

    outreach_snapshot = tool_map.get("get_outreach_template_snapshot") or {}
    templates = outreach_snapshot.get("templates") if isinstance(outreach_snapshot, dict) else []
    if isinstance(templates, list) and templates:
        def first_matching_template(is_appointment: bool | None = None):
            filtered = [
                template for template in templates
                if isinstance(template, dict)
                and (is_appointment is None or bool(template.get("is_appointment_template")) == is_appointment)
            ]
            if is_appointment is False:
                name_matched = [
                    template for template in filtered
                    if str(template.get("form_service_interest") or "").strip()
                    and str(template.get("form_service_interest") or "").lower() in text
                ]
                if name_matched:
                    return name_matched[0]
            return filtered[0] if filtered else None

        if "lead sequence" in text or "new lead sequence" in text:
            template = first_matching_template(False)
            if isinstance(template, dict):
                template_name = str(template.get("form_service_interest") or "the lead sequence")
                if "first" in text and "sms" in text:
                    sms_match = re.search(r"\bto\b\s+(.+?)(?:\s+for this client)?[.!?]*$", user_message, re.IGNORECASE)
                    next_sms = sms_match.group(1).strip() if sms_match else ""
                    if next_sms:
                        return {
                            "action_type": "set_outreach_template_value",
                            "label": "Update Lead Sequence SMS",
                            "summary": f"Update the first SMS in the {template_name} lead sequence for {entity_name or 'this client'}.",
                            "setter_key": setter_key,
                            "template_id": template.get("id"),
                            "path": "positions[0].sms",
                            "value": next_sms,
                        }
                if "first" in text and "email subject" in text:
                    subject_match = re.search(r"\bto\b\s+(.+?)(?:\s+for this client)?[.!?]*$", user_message, re.IGNORECASE)
                    next_subject = subject_match.group(1).strip() if subject_match else ""
                    if next_subject:
                        return {
                            "action_type": "set_outreach_template_value",
                            "label": "Update Lead Sequence Email Subject",
                            "summary": f"Update the first email subject in the {template_name} lead sequence for {entity_name or 'this client'}.",
                            "setter_key": setter_key,
                            "template_id": template.get("id"),
                            "path": "positions[0].email_subject",
                            "value": next_subject,
                        }

        if "appointment reminder" in text or "appointment reminders" in text:
            template = first_matching_template(True)
            if isinstance(template, dict):
                template_name = str(template.get("form_service_interest") or "the appointment reminder")
                if "first" in text and "email subject" in text:
                    subject_match = re.search(r"\bto\b\s+(.+?)(?:\s+for this client)?[.!?]*$", user_message, re.IGNORECASE)
                    next_subject = subject_match.group(1).strip() if subject_match else ""
                    if next_subject:
                        return {
                            "action_type": "set_outreach_template_value",
                            "label": "Update Appointment Reminder Email Subject",
                            "summary": f"Update the first appointment reminder email subject in {template_name} for {entity_name or 'this client'}.",
                            "setter_key": setter_key,
                            "template_id": template.get("id"),
                            "path": "positions[0].email_subject",
                            "value": next_subject,
                        }
                if "first" in text and "sms" in text:
                    sms_match = re.search(r"\bto\b\s+(.+?)(?:\s+for this client)?[.!?]*$", user_message, re.IGNORECASE)
                    next_sms = sms_match.group(1).strip() if sms_match else ""
                    if next_sms:
                        return {
                            "action_type": "set_outreach_template_value",
                            "label": "Update Appointment Reminder SMS",
                            "summary": f"Update the first appointment reminder SMS in {template_name} for {entity_name or 'this client'}.",
                            "setter_key": setter_key,
                            "template_id": template.get("id"),
                            "path": "positions[0].sms",
                            "value": next_sms,
                        }

    if "missed call" in text and ("text-back" in text or "text back" in text):
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            next_value = False if turn_off else True
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_setter_value",
                "label": "Update Missed Call Text-Back",
                "summary": f"Turn {state_label} missed call text-back for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "missed_call_textback.enabled",
                "value": next_value,
            }

    if "auto reactivation" in text or "reactivation" in text:
        turn_off = any(phrase in text for phrase in ["turn off", "disable", "stop"])
        turn_on = any(phrase in text for phrase in ["turn on", "enable"])
        if turn_off or turn_on:
            next_value = False if turn_off else True
            state_label = "off" if turn_off else "on"
            return {
                "action_type": "set_setter_value",
                "label": "Update Auto Reactivation",
                "summary": f"Turn {state_label} auto reactivation for {entity_name or 'this client'}.",
                "setter_key": setter_key,
                "path": "auto_reactivation.enabled",
                "value": next_value,
            }

    return None


def _build_proposed_action_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "action_type": {"type": "string"},
            "label": {"type": "string"},
            "summary": {"type": "string"},
            "setter_key": {"type": "string"},
            "path": {"type": "string"},
            "value_json": {"type": "string"},
            "calendar_id": {"type": "string"},
            "template_id": {"type": "string"},
        },
        "required": ["action_type", "label", "summary", "setter_key", "path", "value_json", "calendar_id", "template_id"],
        "additionalProperties": False,
    }


def _validate_proposed_action(action: dict[str, Any]) -> bool:
    action_type = action.get("action_type")
    path = str(action.get("path") or "")
    if action_type == "set_setter_value":
        return any(
            path == prefix
            or path.startswith(f"{prefix}.")
            or path.startswith(f"{prefix}[")
            for prefix in ALLOWED_SETTER_PREFIXES
        )
    if action_type == "set_client_value":
        return _is_allowed_client_proposal_path(path)
    if action_type == "set_booking_appointment_length":
        return path == "booking.calendars.appointment_length_minutes"
    if action_type == "set_outreach_template_value":
        return bool(action.get("template_id")) and _is_allowed_outreach_path(path)
    return False


async def _propose_admin_action(
    *,
    tenant_name: str,
    entity_id: str,
    entity_name: str | None,
    user_message: str,
    answer: str,
    tool_contexts: list[dict[str, Any]],
    model: str,
) -> dict[str, Any] | None:
    if not tool_contexts:
        return None

    capabilities = get_action_capabilities()
    live_editable_paths = _build_live_editable_paths(tool_contexts)
    system_prompt = (
        "You are deciding whether Ask AI should propose a confirmed admin edit.\n"
        "Only propose one action if the request is clearly editable and you have enough live context.\n"
        "If the request is ambiguous, unsafe, or missing enough information, return action_type='none'.\n"
        "Only use paths that are clearly inside the allowed setter surfaces or allowed client settings fields.\n"
        "Prefer the exact editable paths provided in the live editable path list.\n"
        "Array items use bracket notation like services.services[0].pricing or system_config.notifications.recipients[0].email.\n"
        "Outreach template edits use action_type='set_outreach_template_value' with a template_id plus a path like positions[0].sms or positions[0].email_subject.\n"
        "Do not invent indices, surfaces, or field names that were not present in the live editable path list unless the request is a simple top-level allowed field.\n"
        "For add/remove style requests on arrays, only propose the action if the live context gives you enough information to preserve the existing entries safely.\n"
        "value_json must be valid JSON as a string.\n"
        "Examples:\n"
        "- turn off message splitting -> action_type='set_setter_value', path='bot_persona.sections.message_split.skip_splitting', value_json='true'\n"
        "- set appointment length to 45 -> action_type='set_booking_appointment_length', path='booking.calendars.appointment_length_minutes', value_json='45'\n"
        "- turn on urgency -> action_type='set_setter_value', path='conversation.reply.sections.urgency.enabled', value_json='true'\n"
        "- change the first service price -> action_type='set_setter_value', path='services.services[0].pricing', value_json='\"$15/unit\"'\n"
        "- turn off the first transfer scenario -> action_type='set_setter_value', path='transfer.scenarios[0].enabled', value_json='false'\n"
        "- set the first notification email -> action_type='set_client_value', path='system_config.notifications.recipients[0].email', value_json='\"ops@example.com\"'\n"
        "- turn on missed call notifications for the first recipient -> action_type='set_client_value', path='system_config.notifications.recipients[0].events.missed_call', value_json='true'\n"
        "- update the first lead sequence sms -> action_type='set_outreach_template_value', template_id='template-id', path='positions[0].sms', value_json='\"Hey {{contact.first_name}}\"'\n"
        "- change timezone to America/Chicago -> action_type='set_client_value', path='timezone', value_json='\"America/Chicago\"'\n"
        "- change business phone -> action_type='set_client_value', path='business_phone', value_json='\"+15551230000\"'\n"
        "- update client status -> action_type='set_client_value', path='journey_stage', value_json='\"Paused\"'\n"
        "No markdown. Return valid JSON only."
    )
    user_prompt = (
        f"Tenant: {tenant_name}\n"
        f"Entity: {entity_name or entity_id} ({entity_id})\n"
        f"User request: {user_message}\n\n"
        f"Current answer draft:\n{answer}\n\n"
        f"Allowed capabilities:\n{json.dumps(capabilities, indent=2)}\n\n"
        f"Live tool context:\n{json.dumps(tool_contexts, indent=2)}\n\n"
        f"Live editable paths:\n{chr(10).join(live_editable_paths) if live_editable_paths else '- none'}\n\n"
        "If a safe edit can be proposed, return one of:\n"
        "- action_type='set_setter_value'\n"
        "- action_type='set_client_value'\n"
        "- action_type='set_booking_appointment_length'\n"
        "- action_type='set_outreach_template_value'\n"
        "Otherwise return action_type='none'.\n"
        "For setter actions, setter_key is required.\n"
        "For booking appointment length actions, include the target calendar_id when available.\n"
        "For outreach template actions, template_id is required.\n"
        "For client actions, setter_key should be empty.\n"
        "For none, all other fields can be empty strings."
    )

    response = await chat(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        model=model,
        temperature=0.1,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "ask_ai_proposed_action",
                "strict": True,
                "schema": _build_proposed_action_schema(),
            },
        },
        label="data_chat_propose_action",
    )

    content = response.choices[0].message.content or "{}"
    payload = json.loads(content)
    if payload.get("action_type") == "none":
        return None
    if not _validate_proposed_action(payload):
        return None

    try:
        value = json.loads(payload.get("value_json") or "null")
    except Exception:
        return None

    return {
        "action_type": payload.get("action_type"),
        "label": payload.get("label"),
        "summary": payload.get("summary"),
        "setter_key": payload.get("setter_key") or None,
        "path": payload.get("path"),
        "value": value,
        "calendar_id": payload.get("calendar_id") or None,
        "template_id": payload.get("template_id") or None,
    }


# ── Session Management ──

async def _load_session(session_id: str) -> dict[str, Any]:
    resp = await supabase._request(
        supabase.main_client, "GET", "/ai_chat_sessions",
        params={"id": f"eq.{session_id}"},
        label="data_chat_load_session",
    )
    data = resp.json()
    if not data:
        return {}
    return data[0]


async def _create_session(
    tenant_id: str,
    entity_id: str | None,
    user_id: str | None,
    origin: str,
    title: str,
) -> dict[str, Any]:
    row = {
        "tenant_id": tenant_id,
        "entity_id": entity_id,
        "user_id": user_id,
        "origin": origin,
        "title": title[:100] if title else "New Chat",
        "messages": [],
        "total_cost": 0,
        "message_count": 0,
    }
    resp = await supabase._request(
        supabase.main_client, "POST", "/ai_chat_sessions",
        json=row,
        headers={"Prefer": "return=representation"},
        label="data_chat_create_session",
    )
    data = resp.json()
    return data[0] if data else {}


async def _append_messages_and_update(
    session_id: str,
    new_messages: list[dict],
    cost_delta: float = 0,
) -> None:
    """Append messages to session JSONB + update totals. Same pattern as sandbox."""
    resp = await supabase._request(
        supabase.main_client, "GET", "/ai_chat_sessions",
        params={"id": f"eq.{session_id}", "select": "messages,total_cost,message_count"},
        label="data_chat_get_messages",
    )
    data = resp.json()
    if not data:
        return
    session = data[0]

    existing = session.get("messages") or []
    # Merge with dedup (same pattern as sandbox_state.merge_session_messages)
    seen_ids: set[str] = set()
    merged: list[dict] = []
    for msg in [*existing, *new_messages]:
        m = dict(msg)
        m["id"] = m.get("id") or str(uuid4())
        if m["id"] in seen_ids:
            continue
        seen_ids.add(m["id"])
        merged.append(m)
    merged.sort(key=lambda m: str(m.get("timestamp", "")))

    current_cost = float(session.get("total_cost") or 0)
    current_count = int(session.get("message_count") or 0)

    update = {
        "messages": merged,
        "total_cost": round(current_cost + cost_delta, 6),
        "message_count": current_count + len(new_messages),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    await supabase._request(
        supabase.main_client, "PATCH", "/ai_chat_sessions",
        params={"id": f"eq.{session_id}"},
        json=update,
        label="data_chat_append_messages",
    )


async def _get_entities_for_tenant(tenant_id: str) -> list[dict]:
    """Get all active entities for a tenant."""
    resp = await supabase._request(
        supabase.main_client, "GET", "/entities",
        params={
            "tenant_id": f"eq.{tenant_id}",
            "status": "eq.active",
            "select": "id,name,entity_type,timezone,journey_stage,billing_config,average_booking_value,business_schedule",
            "order": "name.asc",
        },
        label="data_chat_get_entities",
    )
    return resp.json() or []


async def _get_tenant_info(tenant_id: str) -> dict:
    resp = await supabase._request(
        supabase.main_client, "GET", "/tenants",
        params={"id": f"eq.{tenant_id}", "select": "id,name"},
        label="data_chat_get_tenant",
    )
    data = resp.json()
    return data[0] if data else {"id": tenant_id, "name": "Unknown"}


# ── Core Agent Loop ──

async def handle_message(
    tenant_id: str,
    user_id: str | None,
    entity_id: str | None,
    message: str,
    session_id: str | None = None,
    model_override: str | None = None,
    origin: str = "admin",
) -> dict[str, Any]:
    """Process a chat message through the tool-calling agent.

    Returns: {session_id, message: {id, role, content, tool_calls, cost, ...}, session_total_cost}
    """
    # 1. Resolve tenant + entities
    tenant = await _get_tenant_info(tenant_id)
    entities_info = await _get_entities_for_tenant(tenant_id)
    entity_ids = [e["id"] for e in entities_info]

    # If entity_id provided, verify it's accessible
    if entity_id and entity_id not in entity_ids:
        # Check if entity exists but different tenant
        raise ValueError(f"Entity {entity_id} not accessible for this tenant")

    # 2. Load or create session
    session = None
    if session_id:
        session = await _load_session(session_id)

    if not session:
        title = message[:60].strip() if message else "New Chat"
        session = await _create_session(tenant_id, entity_id, user_id, origin, title)
        session_id = session["id"]
    else:
        session_id = session["id"]

    # 3. Build context
    entity_context = await _build_entity_context(entity_id, entity_ids, entities_info)
    system_prompt = _build_system_prompt(tenant["name"], entity_context)
    current_entity_name = None
    if entity_id:
        scoped_entity = next((e for e in entities_info if e["id"] == entity_id), None)
        current_entity_name = scoped_entity.get("name") if scoped_entity else None

    # Scope entity_ids for tools + resolve timezone deterministically
    # Global scope ("All Clients") = only client entities, exclude internal setters
    if entity_id:
        tool_entity_ids = [entity_id]
    else:
        tool_entity_ids = [e["id"] for e in entities_info if e.get("entity_type") == "client"]
    # Use the scoped entity's timezone, or fall back to first entity's timezone, or UTC
    tool_tz = "UTC"
    if entity_id:
        scoped = next((e for e in entities_info if e["id"] == entity_id), None)
        if scoped:
            tool_tz = scoped.get("timezone") or "UTC"
    elif entities_info:
        tool_tz = entities_info[0].get("timezone") or "UTC"

    # 4. Build message history from session JSONB (last N messages)
    session_messages = session.get("messages") or []
    history: list[dict[str, Any]] = [{"role": "system", "content": system_prompt}]

    for msg in session_messages[-MAX_HISTORY_MESSAGES:]:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role in ("user", "assistant"):
            history.append({"role": role, "content": content})

    # Add current user message
    history.append({"role": "user", "content": message})

    # 5. Resolve model
    ai_models_override = {}
    if model_override:
        ai_models_override = {"data_chat": model_override}
    elif session.get("model_override"):
        ai_models_override = {"data_chat": session["model_override"]}

    model = resolve_model_standalone("data_chat", ai_models=ai_models_override, tenant_id=tenant_id)
    temperature = resolve_temperature_standalone("data_chat", ai_temperatures={}, tenant_id=tenant_id)

    # 6. Set AI context (API keys for tenant) + token tracker
    token_tracker = TokenUsage()
    tenant_full = await supabase.get_tenant_for_entity(entity_id or entity_ids[0]) if entity_ids else None
    if tenant_full:
        is_platform_owner = bool(tenant_full.get("is_platform_owner"))
        openrouter_key = (tenant_full.get("openrouter_api_key") or "").strip()
        has_shared_ai = bool(settings.azure_openai_api_key or settings.openrouter_api_key)
        has_direct_tenant_ai = bool(
            (tenant_full.get("openai_key") or "").strip()
            or (tenant_full.get("google_ai_key") or "").strip()
            or (tenant_full.get("anthropic_key") or "").strip()
            or (tenant_full.get("deepseek_key") or "").strip()
            or (tenant_full.get("xai_key") or "").strip()
        )
        if not is_platform_owner and not (openrouter_key or has_direct_tenant_ai or has_shared_ai):
            raise ValueError("No usable AI provider key is configured for this tenant.")
        set_ai_context(
            api_key=openrouter_key,
            token_tracker=token_tracker,
            google_key=(tenant_full.get("google_ai_key") or "").strip(),
            openai_key=(tenant_full.get("openai_key") or "").strip(),
            anthropic_key=(tenant_full.get("anthropic_key") or "").strip(),
            deepseek_key=(tenant_full.get("deepseek_key") or "").strip(),
            xai_key=(tenant_full.get("xai_key") or "").strip(),
        )
    else:
        set_ai_context(token_tracker=token_tracker)

    # 7. Select tools
    tool_defs = PORTAL_TOOL_DEFS if origin == "portal" else ALL_TOOL_DEFS
    allowed_tool_names = {
        definition.get("function", {}).get("name", "")
        for definition in tool_defs
        if isinstance(definition, dict)
    }

    # 8. Create workflow tracker
    tracker = WorkflowTracker(
        "data_chat",
        entity_id=entity_id,
        mode="chat",
        trigger_source="ui",
    )

    # 9. Run agent loop
    all_tool_calls: list[dict] = []
    tool_contexts: list[dict[str, Any]] = []
    assistant_content = ""
    actual_model = model
    ui_payload: dict[str, Any] = {}

    try:
        async with tracker.stage("agent_loop"):
            for iteration in range(MAX_TOOL_ITERATIONS):
                resp = await chat(
                    messages=history,
                    tools=tool_defs if tool_defs else None,
                    model=model,
                    temperature=temperature,
                    label="data_chat",
                )

                msg = resp.choices[0].message
                actual_model = resp.model if hasattr(resp, "model") else model

                if msg.tool_calls:
                    # Append assistant message with tool_calls to history
                    history.append({
                        "role": "assistant",
                        "content": msg.content or "",
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }
                            for tc in msg.tool_calls
                        ],
                    })

                    # Execute each tool
                    for tc in msg.tool_calls:
                        func_name = tc.function.name
                        try:
                            args = json.loads(tc.function.arguments)
                        except json.JSONDecodeError:
                            args = {}

                        if func_name not in allowed_tool_names:
                            history.append({
                                "role": "tool",
                                "tool_call_id": tc.id,
                                "content": json.dumps({
                                    "error": f"Tool {func_name} is not available in this context",
                                }),
                            })
                            continue

                        result = await execute_tool(
                            func_name,
                            args,
                            tool_entity_ids,
                            tz_name=tool_tz,
                            tenant_id=tenant_id,
                            origin=origin,
                        )

                        try:
                            parsed_result = json.loads(result)
                            if isinstance(parsed_result, dict) and isinstance(parsed_result.get("ui_payload"), dict):
                                ui_payload = _merge_ui_payload(ui_payload, parsed_result["ui_payload"])
                        except Exception:
                            parsed_result = None

                        history.append({
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": result,
                        })

                        tool_contexts.append({
                            "tool": func_name,
                            "arguments": args,
                            "result": parsed_result if isinstance(parsed_result, dict) else result[:3000],
                        })

                        all_tool_calls.append({
                            "name": func_name,
                            "arguments": args,
                            "result_preview": result[:200] + "..." if len(result) > 200 else result,
                        })
                    continue  # Loop back to LLM with tool results

                # No tool calls — we have the final response
                assistant_content = msg.content or ""
                break
            else:
                assistant_content = "I reached the maximum number of tool calls. Here's what I found so far based on the data retrieved."

    except Exception as e:
        logger.exception("Data chat agent error")
        assistant_content = f"Sorry, I encountered an error while processing your request: {str(e)}"
        tracker.set_status("error")
        tracker.set_error(str(e))
    finally:
        clear_ai_context()

    if origin == "portal" and _looks_like_change_request(message):
        existing_actions = (ui_payload.get("actions") or []) if isinstance(ui_payload, dict) else []
        if not any(action.get("type") == "submit_request_change" for action in existing_actions if isinstance(action, dict)):
            ui_payload = _merge_ui_payload(
                ui_payload,
                {
                    "sections": [
                        {
                            "type": "guide",
                            "title": "Change Request Draft",
                            "body": f"Topic: {_infer_change_request_topic(message)}\nRequest: {message}",
                            "steps": [],
                        }
                    ],
                    "actions": [
                        {
                            "type": "submit_request_change",
                            "label": "Submit Request Change",
                            "topic": _infer_change_request_topic(message),
                            "description": message,
                            "entity_id": entity_id,
                        }
                    ],
                    "suggestions": [],
                    "media": [],
                    "citations": ["Request Change"],
                },
            )

    if ui_payload and (ui_payload.get("actions") or ui_payload.get("sections")):
        try:
            assistant_content, ui_payload = await _format_final_response(
                tenant_name=tenant["name"],
                origin=origin,
                entity_id=entity_id,
                entity_name=current_entity_name,
                user_message=message,
                draft_answer=assistant_content,
                ui_payload=ui_payload,
                model=model,
            )
        except Exception:
            logger.exception("Data chat response formatting error")

    assistant_content = _sanitize_plain_text_answer(assistant_content)

    if origin == "admin" and entity_id and _looks_like_change_request(message):
        try:
            tool_contexts = await _ensure_admin_action_tool_contexts(
                tool_contexts=tool_contexts,
                tenant_id=tenant_id,
                entity_id=entity_id,
                tz_name=tool_tz,
                origin=origin,
            )
            proposed_action = _heuristic_proposed_action(
                entity_name=current_entity_name,
                user_message=message,
                tool_contexts=tool_contexts,
            )
            if not proposed_action:
                proposed_action = await _propose_admin_action(
                    tenant_name=tenant["name"],
                    entity_id=entity_id,
                    entity_name=current_entity_name,
                    user_message=message,
                    answer=assistant_content,
                    tool_contexts=tool_contexts,
                    model=model,
                )
            if proposed_action:
                request_id = await create_action_request(
                    session_id=session_id,
                    tenant_id=tenant_id,
                    entity_id=entity_id,
                    origin=origin,
                    action_payload=proposed_action,
                )
                ui_payload = _merge_ui_payload(
                    ui_payload,
                    {
                        "proposed_action": {
                            "request_id": request_id,
                            "label": proposed_action["label"],
                            "summary": proposed_action["summary"],
                            "confirm_label": "Confirm Change",
                            "cancel_label": "Cancel",
                        }
                    },
                )
                assistant_content = _align_answer_with_proposed_action(
                    assistant_content,
                    str(proposed_action.get("summary") or ""),
                )
            else:
                ui_payload["proposed_action"] = None
        except Exception:
            logger.exception("Data chat admin action proposal error")

    # 10. Calculate cost
    cost = token_tracker.total_cost()
    tokens_in = token_tracker.prompt_tokens
    tokens_out = token_tracker.completion_tokens

    # Determine provider from token tracker calls
    provider = "openrouter"
    if token_tracker.calls:
        provider = token_tracker.calls[-1].get("provider", "openrouter")

    # 11. Save workflow run
    tracker.set_token_usage(token_tracker)
    tracker.set_decisions({
        "tool_calls_made": len(all_tool_calls),
        "tools_used": list(set(tc["name"] for tc in all_tool_calls)),
        "model": actual_model,
        "origin": origin,
        "shortcuts_selected": [action.get("label") for action in (ui_payload.get("actions") or []) if isinstance(action, dict)],
    })
    tracker.set_runtime_context({
        "session_id": session_id,
        "entity_id": entity_id,
        "user_id": user_id,
        "message_preview": message[:100],
    })
    tracker.set_status("success")
    await tracker.save()

    # 12. Build message objects
    now = datetime.now(timezone.utc).isoformat()
    user_msg = {
        "id": str(uuid4()),
        "role": "user",
        "content": message,
        "timestamp": now,
    }
    assistant_msg = {
        "id": str(uuid4()),
        "role": "assistant",
        "content": assistant_content,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool_calls": all_tool_calls if all_tool_calls else None,
        "workflow_run_id": tracker.run_id,
        "cost": round(cost, 6),
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "model": actual_model,
        "provider": provider,
        "ui_payload": ui_payload or None,
    }

    # 13. Append to session JSONB
    await _append_messages_and_update(session_id, [user_msg, assistant_msg], cost_delta=cost)

    # 14. Return response
    return {
        "session_id": session_id,
        "message": assistant_msg,
        "session_total_cost": round(float(session.get("total_cost") or 0) + cost, 6),
    }


# ── Revert ──

async def revert_session(session_id: str, revert_to_timestamp: str) -> dict[str, Any]:
    """Revert a session to a previous point. Same pattern as sandbox."""
    session = await _load_session(session_id)
    if not session:
        raise ValueError("Session not found")

    messages = session.get("messages") or []
    cutoff = datetime.fromisoformat(revert_to_timestamp.replace("Z", "+00:00"))

    reverted = [
        m for m in messages
        if datetime.fromisoformat(str(m.get("timestamp", "")).replace("Z", "+00:00")) <= cutoff
    ]

    # Recalculate totals from kept messages
    kept_cost = sum(float(m.get("cost", 0)) for m in reverted if m.get("role") == "assistant")
    kept_count = len(reverted)

    await supabase._request(
        supabase.main_client, "PATCH", "/ai_chat_sessions",
        params={"id": f"eq.{session_id}"},
        json={
            "messages": reverted,
            "total_cost": round(kept_cost, 6),
            "message_count": kept_count,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        label="data_chat_revert",
    )

    return {
        "status": "reverted",
        "revert_to": revert_to_timestamp,
        "messages_kept": len(reverted),
        "new_total_cost": round(kept_cost, 6),
    }
