"""Tool: transfer conversation to a human agent.

Unified transfer function with intelligent notification routing.
Both the classifier path (pipeline.py) and the agent tool path call
execute_transfer() which handles tags, logging, routing, and notifications.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.services.ghl_client import GHLClient
from app.services.ghl_links import build_ghl_contact_url
from app.services.slack import post_slack_message
from app.services.postgres_client import postgres

logger = logging.getLogger(__name__)


# =========================================================================
# TOOL DEFINITION (exposed to agent — unchanged)
# =========================================================================

TRANSFER_TO_HUMAN_DEF = {
    "type": "function",
    "function": {
        "name": "transfer_to_human",
        "description": (
            "Call this tool when you need to transfer the lead to a human. "
            "Pass a reason parameter explaining why (e.g., \"User requested human\", "
            "\"Cannot find answer in knowledge base\", \"Booking tool failed repeatedly\")."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Why the transfer is needed (shown to staff)",
                },
            },
            "required": ["reason"],
        },
    },
}


# =========================================================================
# NOTIFICATION ROUTING — LLM classification
# =========================================================================

_ROUTING_CONFIDENCE_THRESHOLD = 0.6

_ROUTING_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "target": {
            "type": "string",
            "enum": ["tenant", "client", "both"],
        },
        "confidence": {
            "type": "number",
        },
    },
    "required": ["target", "confidence"],
}

_ROUTING_SYSTEM_PROMPT = """\
<role>
You classify transfer-to-human reasons to determine who should be notified.
</role>

<routing_categories>
tenant — issues the tenant (agency/system owner) must fix:
- Knowledge base gaps (AI searched but couldn't find the answer)
- Booking/calendar API errors or failures
- Workflow bugs or system malfunctions
- Missing business information the AI should have been given (pricing not configured, data not in system, info not set up)
- Tool execution failures

client — issues the client's own staff should handle:
- Lead explicitly asked to speak to a human/manager/owner
- Lead became aggressive, hostile, frustrated, or rude
- Complex sales scenario beyond AI scope (custom quotes, negotiations)
- Lead needs personal human touch to close the deal
- Lead has a complaint that needs human empathy
- Sensitive situation (medical concern, legal question, emotional distress)
- Language barrier (lead speaking Spanish, Portuguese, etc. — client's bilingual staff handles)

both — notify both parties:
- Genuinely involves both (e.g., system error AND angry lead)
- You are not confident in either classification alone
</routing_categories>

<output_format>
Classify based on the transfer reason and source context provided. Output JSON only.
</output_format>\
"""


# =========================================================================
# UNIFIED TRANSFER FUNCTION
# =========================================================================


async def execute_transfer(
    reason: str,
    ghl: Any,
    contact_id: str,
    contact_name: str = "",
    contact_email: str = "",
    contact_phone: str = "",
    client_name: str = "",
    ghl_location_id: str = "",
    ghl_domain: str = "",
    config: dict[str, Any] | None = None,
    is_test_mode: bool = False,
    channel: str = "",
    entity_id: str = "",
    lead_id: str | None = None,
    agent_type: str = "Unknown",
) -> dict[str, Any]:
    """Unified transfer: tags + log + route notifications.

    Called by both the classifier path (pipeline.py) and the agent tool path.
    In test mode, skips all external side effects except Postgres logging.
    """
    results: dict[str, Any] = {"ok": True, "reason": reason, "source": agent_type}

    # 1. Resolve transfer tags from setter config (with defaults)
    transfer_tags = ["stop bot", "human needed"]  # defaults if no config
    if config:
        try:
            sc = config.get("system_config", {})
            setter_key = agent_type if agent_type and agent_type != "Classifier" else "setter_1"
            setter = sc.get("setters", {}).get(setter_key, {})
            configured_tags = setter.get("tags", {}).get("transfer")
            if isinstance(configured_tags, list):
                transfer_tags = configured_tags
        except Exception as e:
            logger.debug("Error reading transfer tags from config, using defaults: %s", e)

    # 2. Apply all transfer tags (PROD-ONLY)
    if not is_test_mode:
        tags_added = []
        for tag in transfer_tags:
            try:
                await ghl.add_tag(contact_id, tag)
                tags_added.append(tag)
            except Exception as e:
                logger.warning("Failed to add transfer tag '%s': %s", tag, e)
        results["tags_added"] = tags_added
    else:
        results["tags_added"] = [f"{t} (test mode — skipped)" for t in transfer_tags]

    # 3. Log tool execution (ALWAYS — test_mode flag captured in the row)
    try:
        await postgres.log_tool_execution({
            "session_id": contact_id,
            "client_id": entity_id,
            "tool_name": "transfer_to_human",
            "tool_input": json.dumps({"reason": reason, "source": agent_type}),
            "tool_output": json.dumps(results),
            "channel": channel,
            "execution_id": None,
            "test_mode": is_test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log transfer_to_human tool execution", exc_info=True)

    # 4. Route notifications (PROD-ONLY — skip routing LLM in test mode)
    if is_test_mode:
        results["routing_target"] = "skipped"
        results["routing_confidence"] = 0.0
    else:
        routing = await _classify_routing(reason, agent_type)
        target = routing["target"]
        confidence = routing["confidence"]

        # Low confidence → notify both as safety fallback
        if confidence < _ROUTING_CONFIDENCE_THRESHOLD:
            logger.info("TRANSFER | low routing confidence %.2f → overriding to 'both'", confidence)
            target = "both"

        # Backward compat: LLM may still return "agency" from cached responses
        if target == "agency":
            target = "tenant"

        results["routing_target"] = target
        results["routing_confidence"] = confidence

        # 5. Tenant notification channels (tenant or both)
        if target in ("tenant", "both"):
            # Slack only for platform owner's entities (keeps Randy's existing alerts)
            try:
                is_platform_owner = await _check_platform_owner(entity_id)
                if is_platform_owner:
                    await send_slack_notification(
                        contact_id=contact_id,
                        contact_name=contact_name,
                        contact_email=contact_email,
                        contact_phone=contact_phone,
                        client_name=client_name,
                        ghl_location_id=ghl_location_id,
                        ghl_domain=ghl_domain or (config or {}).get("ghl_domain", ""),
                        reason=reason,
                        agent_type=agent_type,
                    )
                    results["slack_sent"] = True
            except Exception as e:
                logger.warning("Slack notification failed: %s", e, exc_info=True)
                results["slack_error"] = str(e)

            # Tenant-level email/SMS (Resend + GHL snapshot)
            try:
                from app.services.notifications import send_tenant_notification
                from app.services.supabase_client import supabase as sb

                entity = await sb.resolve_entity(entity_id)
                tid = entity.get("tenant_id")
                if tid:
                    tenant_results = await send_tenant_notification(
                        tenant_id=tid,
                        event="transfer_to_human",
                        context={
                            "contact_id": contact_id or "",
                            "contact_name": contact_name or "Unknown",
                            "contact_phone": contact_phone or "",
                            "contact_email": contact_email or "",
                            "reason": reason or "",
                            "ghl_location_id": ghl_location_id or "",
                            "ghl_contact_id": contact_id or "",
                            "ghl_domain": ghl_domain or (entity.get("ghl_domain") or ""),
                        },
                        client_name=client_name or "",
                        entity_id=entity_id,
                    )
                    results["tenant_notifications"] = tenant_results
                    results["notification_inserted"] = any(
                        item.get("channel") == "in_app" and item.get("status") == "sent"
                        for item in tenant_results
                    )
            except Exception as e:
                logger.warning("Tenant email/SMS notification failed: %s", e, exc_info=True)

        # 6. GHL internal comment (client or both — so staff sees context in conversation)
        if target in ("client", "both"):
            try:
                comment = (
                    "\U0001f6a8 Human Needed \U0001f6a8\n"
                    "\n"
                    f"Reason: {reason}\n"
                    "\n"
                    "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                    "When resolved:\n"
                    "\n"
                    "\u2022 Turn \"\U0001f916 AI Bot\" back ON in the right panel\n"
                    "  to resume automation and clear this from\n"
                    "  your Human Needed queue.\n"
                    "\n"
                    "Or\n"
                    "\n"
                    "\u2022 To keep the bot OFF but clear from queue,\n"
                    "  remove the \"human needed\" tag in Tags\n"
                    "  on the right panel.\n"
                    "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
                )
                await ghl.add_internal_comment(contact_id, comment)
                results["internal_comment"] = True
            except Exception as e:
                logger.warning("Internal comment failed: %s", e, exc_info=True)
                results["internal_comment_error"] = str(e)

        # 7. Client staff notification via notification service (client or both)
        if target in ("client", "both") and config:
            try:
                from app.services.notifications import send_notifications
                from app.services.supabase_client import supabase as sb2

                # Resolve tenant name for email from field
                _entity = await sb2.resolve_entity(entity_id)
                _tenant_id = _entity.get("tenant_id")
                _tenant_name = ""
                if _tenant_id:
                    try:
                        _t = await sb2._request(sb2.main_client, "GET", "/tenants", params={"id": f"eq.{_tenant_id}", "select": "name"})
                        _tenant_name = _t[0].get("name", "") if _t else ""
                    except Exception:
                        pass

                notif_results = await send_notifications(
                    config=config,
                    event="transfer_to_human",
                    context={
                        "contact_id": contact_id,
                        "contact_name": contact_name,
                        "contact_phone": contact_phone,
                        "contact_email": contact_email,
                        "reason": reason,
                        "ghl_location_id": ghl_location_id,
                        "ghl_contact_id": contact_id,
                        "ghl_domain": ghl_domain or (_entity.get("ghl_domain") or ""),
                    },
                    entity_id=entity_id,
                    tenant_name=_tenant_name,
                )
                results["notifications"] = notif_results
            except Exception as e:
                logger.warning("Notification failed: %s", e, exc_info=True)
                results["notification_error"] = str(e)

    logger.info(
        "TRANSFER | contact=%s | reason=%s | target=%s | confidence=%.2f | test_mode=%s",
        contact_id, reason[:100],
        results.get("routing_target", "unknown"),
        results.get("routing_confidence", 0.0),
        is_test_mode,
    )
    results["message"] = "Conversation transferred to human staff. They will follow up shortly."
    return results


# =========================================================================
# AGENT TOOL WRAPPER (called by _execute_tool in agent.py)
# =========================================================================


async def transfer_to_human(
    reason: str,
    ghl: GHLClient,
    contact_id: str,
    contact_name: str = "",
    contact_email: str = "",
    contact_phone: str = "",
    client_name: str = "",
    ghl_location_id: str = "",
    ghl_domain: str = "",
    is_test_mode: bool = False,
    channel: str = "",
    entity_id: str = "",
    lead_id: str | None = None,
    agent_type: str = "Unknown",
    **kwargs: Any,
) -> dict[str, Any]:
    """Transfer to human — thin wrapper for agent tool calls."""
    return await execute_transfer(
        reason=reason,
        ghl=ghl,
        contact_id=contact_id,
        contact_name=contact_name,
        contact_email=contact_email,
        contact_phone=contact_phone,
        client_name=client_name,
        ghl_location_id=ghl_location_id,
        ghl_domain=ghl_domain,
        config=kwargs.get("config", {}),
        is_test_mode=is_test_mode,
        channel=channel,
        entity_id=entity_id,
        lead_id=lead_id,
        agent_type=agent_type,
    )


# =========================================================================
# ROUTING CLASSIFIER
# =========================================================================


async def _classify_routing(reason: str, agent_type: str) -> dict[str, Any]:
    """Classify transfer reason -> tenant / client / both."""
    from app.services.ai_client import classify

    try:
        result = await classify(
            prompt=f"Transfer reason: {reason}\nSource: {agent_type}",
            schema=_ROUTING_SCHEMA,
            system_prompt=_ROUTING_SYSTEM_PROMPT,
            model="google/gemini-2.5-flash",
            temperature=0.1,
            label="transfer_routing",
        )
        target = result.get("target", "both")
        confidence = result.get("confidence", 0.0)

        # Backward compat: treat "agency" as "tenant"
        if target == "agency":
            target = "tenant"

        # Validate target value
        if target not in ("tenant", "client", "both"):
            logger.warning("TRANSFER | invalid routing target '%s' -> defaulting to 'both'", target)
            target = "both"
            confidence = 0.0

        return {"target": target, "confidence": confidence}

    except Exception as e:
        logger.warning("TRANSFER | routing LLM failed -> defaulting to 'both': %s", e)
        return {"target": "both", "confidence": 0.0}


# =========================================================================
# IN-APP NOTIFICATION INSERT (multi-tenant)
# =========================================================================


async def _insert_notification(
    entity_id: str,
    event_type: str,
    title: str,
    body: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Insert a notification into the notifications table for the entity's tenant."""
    from app.main import supabase

    # Resolve tenant_id from entity
    entity = await supabase.resolve_entity(entity_id)
    tenant_id = entity.get("tenant_id")
    if not tenant_id:
        logger.warning("TRANSFER | Cannot insert notification — entity %s has no tenant_id", entity_id)
        return

    await supabase._request(
        supabase.main_client, "POST", "/notifications",
        json={
            "tenant_id": tenant_id,
            "entity_id": entity_id,
            "event_type": event_type,
            "title": title,
            "body": body,
            "metadata": metadata or {},
        },
        label="insert_notification",
    )
    logger.info("TRANSFER | notification inserted | tenant=%s | entity=%s | type=%s", tenant_id, entity_id, event_type)


async def _check_platform_owner(entity_id: str) -> bool:
    """Check if an entity belongs to the platform owner tenant."""
    try:
        from app.main import supabase
        tenant = await supabase.get_tenant_for_entity(entity_id)
        return tenant.get("is_platform_owner", False)
    except Exception:
        return False


# =========================================================================
# SLACK NOTIFICATION (unchanged)
# =========================================================================


async def send_slack_notification(
    contact_id: str,
    contact_name: str,
    contact_email: str,
    contact_phone: str,
    client_name: str,
    ghl_location_id: str,
    ghl_domain: str,
    reason: str,
    agent_type: str = "Unknown",
) -> None:
    """Send a Slack message to the #human-needed channel with full contact details."""
    ghl_url = build_ghl_contact_url(ghl_location_id, contact_id, ghl_domain)
    ghl_link = f"\n*GHL Link:* <{ghl_url}|View Contact>" if ghl_url else ""

    text = (
        f":rotating_light: *Human Transfer Needed*\n"
        f"*Client:* {client_name}\n"
        f"*Source:* {agent_type}\n"
        f"*Contact:* {contact_name} (`{contact_id}`)\n"
        f"*Email:* {contact_email or 'N/A'}\n"
        f"*Phone:* {contact_phone or 'N/A'}\n"
        f"*Reason:* {reason}"
        f"{ghl_link}"
    )

    await post_slack_message("#human-needed", text)
