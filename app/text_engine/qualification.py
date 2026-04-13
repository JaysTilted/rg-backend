"""Dedicated qualification agent — evaluates leads against service-specific criteria.

Runs in parallel with extraction (name/email/phone). Returns structured
criteria evaluation; overall status is computed deterministically by code.
"""

from __future__ import annotations

import difflib
import json
import logging
from typing import Any

from app.models import PipelineContext
from app.services.ai_client import classify
from app.text_engine.model_resolver import resolve_model, resolve_temperature

logger = logging.getLogger(__name__)


# =========================================================================
# SYSTEM PROMPT (hardcoded, universal)
# =========================================================================

_QUAL_SYSTEM_PROMPT = """\
<role>
YOU ARE A QUALIFICATION ANALYST. Your job is to evaluate a lead's conversation \
against specific qualification criteria and return a structured assessment.
</role>

<instructions>
## INSTRUCTIONS

1. Read the conversation history between the lead and the AI
2. Read the qualification criteria provided (global + service-specific)
3. Determine which service(s) the lead is interested in based on the conversation
4. Evaluate each applicable criterion against what the lead has explicitly stated
5. Return structured results
</instructions>

<rules>
## RULES

- ONLY use facts the lead has explicitly stated in THEIR OWN messages (marked LEAD in the timeline) -- never infer criteria from AI messages, drip sequences, or other non-lead messages. A drip or AI message is NOT the lead's voice -- even if a drip says "serving [City]" and the lead replies positively, the lead has NOT stated their location. Generic replies like "yes", "sounds good", "yeah im interested", "tell me more", "how much" are expressions of interest, NOT confirmation of any criteria mentioned in the preceding AI/drip message. The lead must independently state the information (e.g., "I'm in [City]", "we're located in [Area]") for it to count
  Examples of what does NOT confirm location:
    DRIP: "We serve Cedar Park this week!" → LEAD: "sounds good" → Location = undetermined
    DRIP: "Free estimates in Bee Cave!" → LEAD: "yeah im interested" → Location = undetermined
    DRIP: "Austin area specials!" → LEAD: "yes" → Location = undetermined
  Examples of what DOES confirm location:
    LEAD: "im in round rock" → Location = evaluate against criteria
    LEAD: "we just moved to cedar park" → Location = evaluate against criteria
- If the lead hasn't mentioned something, that criterion is "undetermined" -- not confirmed, not denied
- Review the ENTIRE conversation, not just the latest message
- Criteria confirmed in earlier messages carry forward unless explicitly contradicted
- Implicit signals count -- the lead doesn't need to state criteria in exact words. Match what they say against each criterion's "qualified" and "disqualified" descriptions using reasonable inference (e.g., "I need this every week" may confirm recurring interest)
- If a lead pivots to a different service mid-conversation, update matched_services accordingly
- Previous evaluation is provided for context -- carry forward all statuses and update only what the new message changes. Never reset a previously confirmed or disqualified criterion to undetermined unless the lead explicitly contradicts it
- TRUST the qualified/disqualified descriptions provided below as the ONLY source of truth for evaluation. Do NOT override them with your own knowledge. If a city appears in the qualified list, it is qualified -- even if you believe it's geographically borderline. Your job is to match the lead's statements against the provided descriptions, not to make your own judgment about service areas or geographic boundaries
- For location-based criteria: match the lead's stated location against the qualified/disqualified descriptions provided. If the lead names a city, neighborhood, or area that appears in (or is clearly within) the qualified description, mark it "confirmed". Do not mark a stated location as "undetermined" just because it isn't an exact match -- use reasonable geographic knowledge (e.g., a neighborhood within a listed city counts). Only use "undetermined" when the lead hasn't mentioned a location at all, or the location genuinely cannot be matched to either the qualified or disqualified descriptions. Travel intent also counts ("I'm visiting next month", "I'll be in the area")
- For ownership/homeowner criteria: possessive references ("my yard", "my lawn", "my house", "my place", "our home") are NOT proof of ownership -- renters say all of these. Ownership is only "confirmed" when the lead explicitly states they own the property (e.g., "I own it", "we bought the house", "I'm the homeowner", "yes I own it") or directly confirms ownership when asked. If there is no explicit ownership statement, keep this "undetermined" -- the reply agent will ask
</rules>

<service_matching>
## SERVICE MATCHING

- Match the lead's stated interest to the closest service(s) from the list
- If the lead hasn't stated what they want yet, matched_services should be empty
- A lead can be interested in multiple services simultaneously
- form_interest (from ad opt-in) is a hint, not a lock -- the lead's conversation takes priority
</service_matching>

<criterion_evaluation>
## CRITERION EVALUATION

**CRITICAL: You MUST return an entry for EVERY criterion listed below -- no exceptions.**
Even if the lead has said nothing relevant to a criterion, include it with status "undetermined" and evidence explaining that it hasn't been discussed yet. Never skip or omit criteria.

Output exactly ONE entry per criterion name. When updating from a previous evaluation, produce the updated entry -- do not include both the old and new version of the same criterion. Your output should contain the same number of criteria as listed below, no more.

When updating from a previous evaluation:
- CARRY FORWARD all previously confirmed or disqualified criteria. If the previous evaluation shows a criterion as confirmed with evidence, keep it confirmed with that same evidence unless the lead explicitly contradicts it in a new message. Never silently reset a criterion to undetermined.
- A service pivot does NOT reset criteria. If the lead described scope for one service and then switches to a different service, re-evaluate scope based on what they said about the new service. If they described the new service's scope (e.g., "full landscape redesign for our front yard"), that IS scope information.
- Evaluate ALL criteria on EVERY turn regardless of overall qualification status. Even if the lead is already disqualified on one criterion, still update other criteria based on what they say. The per-criterion data is used for reporting and may change the overall status if the disqualifying criterion is later resolved.

For each criterion, assign one of:
- "confirmed" -- lead has explicitly stated or clearly implied something that meets the qualified description
- "disqualified" -- lead has explicitly stated something that matches the disqualified description
- "undetermined" -- not enough information yet, or lead's statement is ambiguous

Include evidence: a brief quote or paraphrase of what the lead said that supports the status. For undetermined criteria with no discussion, use evidence like "Lead has not mentioned [topic] yet."

## OUTPUT EXAMPLES

GOOD -- all criteria present, even when undetermined:
{"matched_services": ["Lawn Care"], "criteria": [{"name": "Location", "status": "undetermined", "evidence": "Lead has not mentioned location yet."}, {"name": "Homeowner", "status": "undetermined", "evidence": "Lead has not mentioned ownership status."}, {"name": "Scope", "status": "confirmed", "evidence": "Lead asked about weekly mowing."}]}

BAD -- missing criteria (NEVER do this):
{"matched_services": ["Lawn Care"], "criteria": [{"name": "Scope", "status": "confirmed", "evidence": "Lead asked about weekly mowing."}]}
This is wrong because Location and Homeowner are missing. Always include every criterion.
</criterion_evaluation>\
"""


# =========================================================================
# OUTPUT SCHEMA (fixed, universal)
# =========================================================================

_QUAL_SCHEMA = {
    "type": "object",
    "properties": {
        "matched_services": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Service name(s) the lead is interested in, from the provided services list",
        },
        "criteria": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "status": {"type": "string", "enum": ["confirmed", "disqualified", "undetermined"]},
                    "evidence": {"type": "string"},
                },
                "required": ["name", "status", "evidence"],
            },
        },
    },
    "required": ["matched_services", "criteria"],
}


# =========================================================================
# CORE FUNCTIONS
# =========================================================================


async def evaluate_qualification(ctx: PipelineContext) -> dict | None:
    """Run dedicated qualification agent. Returns structured criteria evaluation."""

    _setter = ctx.compiled.get("_matched_setter") or {}
    service_config = _setter.get("services")
    if not service_config:
        return None  # No services configured -> skip qualification entirely

    # Resolve agent_key for filtering global qualifications
    _agent_key = ctx.agent_type or None

    # Check if any required qualifications exist
    if not _has_required_qualifications(service_config, agent_key=_agent_key):
        return None  # No required quals -> skip

    # Build criteria list for the agent
    criteria_prompt = _format_criteria_for_qual_agent(service_config, ctx.form_interest, agent_key=_agent_key)

    # Build user prompt with conversation + previous evaluation
    prev_notes = ctx.qualification_notes  # JSONB dict or None
    prev_status = ctx.qualification_status

    user_prompt = (
        f"{ctx.timeline}\n\n"
        f"**form_interest (from ad opt-in):** {ctx.form_interest or 'none'}\n"
        f"**Previous qualification status:** {prev_status}\n"
        f"**Previous evaluation:** {json.dumps(prev_notes) if prev_notes else 'none -- first evaluation'}\n\n"
        f"Evaluate this lead against the criteria below."
    )

    system = _QUAL_SYSTEM_PROMPT + f"\n\n## CRITERIA TO EVALUATE\n\n{criteria_prompt}"

    from app.models import log_prompt
    _qa_vars: dict[str, Any] = {
        "criteria_prompt": criteria_prompt,
    }
    if ctx.timeline:
        _qa_vars["timeline"] = ctx.timeline
    if ctx.form_interest:
        _qa_vars["form_interest"] = ctx.form_interest
    if prev_status:
        _qa_vars["prev_status"] = prev_status
    if prev_notes:
        _qa_vars["prev_notes"] = json.dumps(prev_notes)
    log_prompt(ctx, "Qualification Agent", system, user_prompt, variables=_qa_vars)

    result = await classify(
        prompt=user_prompt,
        schema=_QUAL_SCHEMA,
        system_prompt=system,
        model=resolve_model(ctx, "qualification"),
        temperature=resolve_temperature(ctx, "qualification"),
        label="qualification",
    )

    # Deduplicate criteria by name (last entry wins if LLM duplicated)
    raw_criteria = result.get("criteria", [])
    seen: dict[str, dict] = {}
    for c in raw_criteria:
        seen[c["name"]] = c
    if len(seen) < len(raw_criteria):
        logger.warning("Qualification agent returned duplicate criteria, deduplicating %d -> %d", len(raw_criteria), len(seen))
    result["criteria"] = list(seen.values())

    logger.info(
        "Qualification agent: matched=%s, criteria=%d",
        result.get("matched_services", []),
        len(result.get("criteria", [])),
    )

    return result


def compute_overall_status(qual_result: dict, service_config: dict, agent_key: str | None = None) -> str:
    """Compute overall qualification status from per-criterion evaluations.

    Rules:
    - Any required criterion disqualified -> disqualified
    - All required criteria confirmed -> qualified
    - Otherwise -> undetermined
    """
    if not qual_result:
        return "undetermined"

    criteria = qual_result.get("criteria", [])
    matched_services = qual_result.get("matched_services", [])

    # Build set of required criterion names (global + matched service-specific)
    required_names: set[str] = set()
    for q in service_config.get("global_qualifications", []):
        if q.get("required") and _qual_agent_match(q, agent_key):
            required_names.add(q["name"])
    for svc in service_config.get("services", []):
        if svc["name"] in matched_services:
            for q in svc.get("qualifications", []):
                if q.get("required"):
                    required_names.add(q["name"])

    statuses = {c["name"]: c["status"] for c in criteria}

    # Any required criterion disqualified -> disqualified
    if any(statuses.get(n) == "disqualified" for n in required_names):
        return "disqualified"

    # All required criteria confirmed -> qualified
    if required_names and all(statuses.get(n) == "confirmed" for n in required_names):
        return "qualified"

    return "undetermined"


def apply_qualification_guard(
    new_status: str, prev_status: str, new_notes: dict | None, prev_notes: dict | None,
) -> tuple[str, dict | None]:
    """Prevent regression from qualified -> undetermined."""
    if prev_status == "qualified" and new_status == "undetermined":
        return prev_status, prev_notes
    return new_status, new_notes


# =========================================================================
# HELPERS
# =========================================================================


def _qual_agent_match(qual: dict, agent_key: str | None) -> bool:
    """Check if a qualification applies to the given agent."""
    if not agent_key:
        return True
    setter_keys = qual.get("setter_keys", [])
    legacy_agents = qual.get("agents", [])
    scoped = setter_keys or legacy_agents
    return not scoped or agent_key in scoped


def _has_required_qualifications(service_config: dict, agent_key: str | None = None) -> bool:
    """Return True if any required qualification exists in the config."""
    for q in service_config.get("global_qualifications", []):
        if q.get("required") and _qual_agent_match(q, agent_key):
            return True
    for svc in service_config.get("services", []):
        for q in svc.get("qualifications", []):
            if q.get("required"):
                return True
    return False


def _format_criteria_for_qual_agent(service_config: dict, form_interest: str = "", agent_key: str | None = None) -> str:
    """Format service_config criteria into a readable prompt for the qualification agent."""
    parts: list[str] = []

    # Form interest hint
    if form_interest:
        matched = match_form_interest(form_interest, service_config)
        if matched:
            parts.append(f"**Hint from ad opt-in (form_interest):** Lead opted in for \"{matched}\"")
        else:
            parts.append(f"**Hint from ad opt-in (form_interest):** \"{form_interest}\" (no exact service match)")
        parts.append("")

    # Global qualifications (filtered by agent_key)
    global_quals = [q for q in service_config.get("global_qualifications", []) if _qual_agent_match(q, agent_key)]
    if global_quals:
        parts.append("### Global Criteria (apply to ALL services)")
        for q in global_quals:
            req = "REQUIRED" if q.get("required") else "optional"
            parts.append(f"- **{q['name']}** ({req})")
            parts.append(f"  Qualified: {q.get('qualified', 'N/A')}")
            parts.append(f"  Disqualified: {q.get('disqualified', 'N/A')}")
            parts.append(f"  Undetermined: {q.get('undetermined', 'N/A')}")
        parts.append("")

    # Per-service qualifications
    services = service_config.get("services", [])
    if services:
        parts.append("### Services")
        for svc in services:
            parts.append(f"\n**{svc['name']}**")
            if svc.get("description"):
                parts.append(f"Description: {svc['description']}")
            if svc.get("pricing"):
                parts.append(f"Pricing: {svc['pricing']}")
            svc_quals = svc.get("qualifications", [])
            if svc_quals:
                parts.append("Service-specific criteria:")
                for q in svc_quals:
                    req = "REQUIRED" if q.get("required") else "optional"
                    parts.append(f"  - **{q['name']}** ({req})")
                    parts.append(f"    Qualified: {q.get('qualified', 'N/A')}")
                    parts.append(f"    Disqualified: {q.get('disqualified', 'N/A')}")
                    parts.append(f"    Undetermined: {q.get('undetermined', 'N/A')}")

    return "\n".join(parts)


def match_form_interest(form_interest: str, service_config: dict) -> str | None:
    """Fuzzy match form_interest to a service name. Returns matched name or None."""
    if not form_interest:
        return None
    service_names = [s["name"] for s in service_config.get("services", [])]
    if not service_names:
        return None
    # Try exact match first (case-insensitive)
    for name in service_names:
        if name.lower() == form_interest.lower():
            return name
    # Fuzzy match
    matches = difflib.get_close_matches(
        form_interest.lower(), [n.lower() for n in service_names], n=1, cutoff=0.6,
    )
    if matches:
        for name in service_names:
            if name.lower() == matches[0]:
                return name
    return None


def format_services_for_followup(service_config: dict) -> str:
    """Format service_config into a simple services reference for follow-up generators."""
    lines = []
    for svc in service_config.get("services", []):
        line = f"- {svc['name']}: {svc.get('description', '')}. Pricing: {svc.get('pricing', 'Ask for quote')}"
        lines.append(line)
    return "\n".join(lines)


def format_qual_for_ghl(notes: dict | str | None) -> str:
    """Format qualification notes for GHL custom field (human-readable string)."""
    if not notes:
        return ""
    if isinstance(notes, str):
        return notes  # Old format, pass through
    # New JSONB format
    matched = notes.get("matched_services", [])
    criteria = notes.get("criteria", [])
    parts = []
    if matched:
        parts.append(f"Services: {', '.join(matched)}")
    for c in criteria:
        line = f"{c['name']}: {c['status']}"
        if c.get("evidence"):
            line += f" -- {c['evidence']}"
        parts.append(line)
    return " | ".join(parts)


def format_qual_notes_readable(notes: dict | str | None) -> str:
    """Format qualification notes into a human-readable string for LLM prompts."""
    if not notes:
        return ""
    if isinstance(notes, str):
        return notes
    matched = notes.get("matched_services", [])
    criteria = notes.get("criteria", [])
    parts = []
    if matched:
        parts.append(f"Interested in: {', '.join(matched)}")
    for c in criteria:
        line = f"{c['name']}: {c['status']}"
        if c.get("evidence"):
            line += f" -- {c['evidence']}"
        parts.append(line)
    return "; ".join(parts)
