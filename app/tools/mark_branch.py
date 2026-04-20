"""Tool: tag the contact with a branch:* identifier so downstream followups know
which reply path the lead came in through.

Cheap, no LLM. Called alongside the agent's reply when one of the named reply
scripts fires (counter_offer, vetting, niche_correction, wrong_line,
referral_handoff, wrong_number_pivot). The followup_compiler reads
ctx.contact_tags for `branch:*` entries and injects branch-specific guidance
into the followup agent's system prompt — same ladder, tailored copy.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.services.ghl_client import GHLClient
from app.services.postgres_client import postgres

logger = logging.getLogger(__name__)


_VALID_BRANCHES = {
    "counter_offer",
    "vetting",
    "niche_correction",
    "wrong_line",
    "referral_handoff",
    "wrong_number_pivot",
    "competitive_positioning",
    "immediate_call_request",
    "booking_management",
}


MARK_BRANCH_DEF: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "mark_branch",
        "description": (
            "Tag the contact with a branch identifier so the downstream followup "
            "ladder can tailor its copy. Call this alongside your reply when you "
            "fire one of these reply scripts: counter_offer, vetting, "
            "niche_correction, wrong_line, referral_handoff, wrong_number_pivot, "
            "competitive_positioning, immediate_call_request, booking_management. "
            "Only call ONCE per turn. Do not call for Direct Interest, More Info, "
            "Pricing, or Existing Setup — those are the default flow and don't "
            "need branch tagging."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "branch": {
                    "type": "string",
                    "enum": sorted(_VALID_BRANCHES),
                    "description": "Which reply branch was fired this turn",
                },
            },
            "required": ["branch"],
        },
    },
}


async def mark_branch(
    branch: str,
    ghl: GHLClient,
    contact_id: str,
    is_test_mode: bool = False,
    channel: str = "",
    entity_id: str = "",
    lead_id: str | None = None,
    **_kwargs: Any,
) -> dict[str, Any]:
    """Add `branch:<name>` tag to contact and log the tool execution."""
    if branch not in _VALID_BRANCHES:
        return {"ok": False, "error": f"unknown branch '{branch}'", "valid": sorted(_VALID_BRANCHES)}

    tag = f"branch:{branch}"
    result: dict[str, Any] = {"ok": True, "branch": branch, "tag": tag}

    if is_test_mode:
        result["tag_added"] = f"{tag} (test mode — skipped)"
    else:
        try:
            await ghl.add_tag(contact_id, tag)
            result["tag_added"] = tag
        except Exception as e:
            logger.warning("mark_branch failed to add tag '%s': %s", tag, e)
            result["ok"] = False
            result["error"] = str(e)[:200]

    try:
        await postgres.log_tool_execution({
            "session_id": contact_id,
            "client_id": entity_id,
            "tool_name": "mark_branch",
            "tool_input": json.dumps({"branch": branch}),
            "tool_output": json.dumps(result),
            "channel": channel,
            "execution_id": None,
            "test_mode": is_test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log mark_branch tool execution", exc_info=True)

    logger.info("MARK_BRANCH | contact=%s | branch=%s | tag=%s | test_mode=%s",
                contact_id, branch, tag, is_test_mode)
    return result
