"""Staff + tenant notification service.

Client staff notifications: SMS via GHL snapshot, email via Resend.
Tenant owner notifications: in-app bell plus optional SMS/email.

Reads client-level config from system_config.notifications.
Reads tenant-level config from tenants.notification_config.
"""

from __future__ import annotations

import logging
from typing import Any

from app.config import settings
from app.services.ghl_client import GHLClient
from app.services.resend_email import (
    build_booking_email,
    build_change_request_email,
    build_missed_call_email,
    build_new_lead_email,
    build_transfer_email,
    send_email,
)

logger = logging.getLogger(__name__)


_SMS_TEMPLATES: dict[str, str] = {
    "transfer_to_human": "Human Needed\n\nName: {contact_name}\nPhone: {contact_phone}{contact_email_line}",
    "change_request": "Change Request\n\nTopic: {topic}\n\n{description}",
    "booking": "New Booking\n\nName: {contact_name}\nPhone: {contact_phone}\nStatus: {status}",
    "new_lead": "New Lead\n\nName: {contact_name}\nPhone: {contact_phone}\nInterest: {service}",
    "missed_call_active": "Missed Call\n\nName: {contact_name}\nPhone: {contact_phone}\n\nOur AI has reached out and will follow up.",
    "missed_call_inactive": "Missed Call\n\nName: {contact_name}\nPhone: {contact_phone}\n\nCall this lead back as soon as possible.",
}

_EMAIL_BUILDERS: dict[str, Any] = {
    "transfer_to_human": build_transfer_email,
    "change_request": build_change_request_email,
    "booking": build_booking_email,
    "new_lead": build_new_lead_email,
    "missed_call_active": build_missed_call_email,
    "missed_call_inactive": build_missed_call_email,
}

_TENANT_EVENT_DEFAULTS: dict[str, dict[str, bool]] = {
    "transfer_to_human": {"in_app": True, "sms": False, "email": False},
    "change_request": {"in_app": True, "sms": False, "email": False},
    "booking": {"in_app": False, "sms": False, "email": False},
    "new_lead": {"in_app": False, "sms": False, "email": False},
    "missed_call": {"in_app": False, "sms": False, "email": False},
}


def _format_message(template: str, context: dict[str, str]) -> str:
    """Safe format where missing keys become empty strings."""

    class SafeDict(dict):
        def __missing__(self, key: str) -> str:
            return ""

    return template.format_map(SafeDict(context))


def _build_email_for_event(
    template_key: str,
    context: dict[str, str],
    include_client: bool = False,
    client_name: str = "",
) -> tuple[str, str] | None:
    """Build an email body using the existing Resend builders."""
    builder = _EMAIL_BUILDERS.get(template_key)
    if not builder:
        return None

    if template_key == "change_request":
        kwargs: dict[str, Any] = {
            "topic": context.get("topic", ""),
            "description": context.get("description", ""),
            "include_client": include_client,
            "client_name": client_name,
        }
    else:
        kwargs = {
            "contact_name": context.get("contact_name", "Unknown"),
            "contact_phone": context.get("contact_phone", ""),
            "include_client": include_client,
            "client_name": client_name,
        }

    if template_key == "transfer_to_human":
        kwargs["reason"] = context.get("reason", "")
        kwargs["contact_email"] = context.get("contact_email", "")
        kwargs["ghl_location_id"] = context.get("ghl_location_id", "")
        kwargs["ghl_contact_id"] = context.get("ghl_contact_id", "")
        kwargs["ghl_domain"] = context.get("ghl_domain", "")
    elif template_key == "booking":
        kwargs["status"] = context.get("status", "")
    elif template_key == "new_lead":
        kwargs["service"] = context.get("service", "")
    elif template_key in ("missed_call_active", "missed_call_inactive"):
        kwargs["textback_active"] = template_key == "missed_call_active"

    return builder(**kwargs)


async def _resolve_or_create_contact(
    ghl: GHLClient,
    search_value: str,
    name: str,
    channel: str,
) -> str | None:
    """Find or create a GHL contact in the snapshot account."""
    if not search_value.strip():
        return None

    try:
        results = await ghl.search_contacts(search_value.strip(), limit=1)
        if results:
            return results[0].get("id")
    except Exception as e:
        logger.warning("NOTIFY | search failed for %s: %s", search_value, e)

    try:
        data: dict[str, Any] = {"firstName": name or "Staff"}
        if channel == "sms":
            data["phone"] = search_value.strip()
        else:
            data["email"] = search_value.strip()

        contact = await ghl.create_contact(data)
        return contact.get("id")
    except Exception as e:
        logger.warning("NOTIFY | create_contact failed for %s: %s", search_value, e)
        return None


async def _cache_recipient_field(
    entity_id: str,
    notifications_config: dict[str, Any],
    recipient_index: int,
    field: str,
    value: str,
) -> None:
    """Cache a resolved GHL contact ID back into Supabase system_config."""
    try:
        from app.services.supabase_client import supabase

        recipients = notifications_config.get("recipients", [])
        if recipient_index < len(recipients):
            recipients[recipient_index][field] = value

        config = await supabase.resolve_entity(entity_id)
        sys_config = config.get("system_config") or {}
        sys_config["notifications"] = notifications_config

        await supabase._request(
            supabase.main_client,
            "PATCH",
            "/entities",
            params={"id": f"eq.{entity_id}"},
            json={"system_config": sys_config},
        )
        logger.info("NOTIFY | cached %s for recipient %d", field, recipient_index)
    except Exception as e:
        logger.warning("NOTIFY | cache failed: %s", e)


async def send_notifications(
    config: dict[str, Any],
    event: str,
    context: dict[str, str],
    entity_id: str = "",
    tenant_name: str = "",
) -> list[dict[str, Any]]:
    """Send client staff notifications."""
    sys_config = config.get("system_config") or {}
    notif_config = sys_config.get("notifications")
    if not notif_config:
        return []

    recipients = notif_config.get("recipients") or []
    if not recipients:
        return []

    context.setdefault("ghl_domain", (config.get("ghl_domain") or "").strip())

    email = context.get("contact_email", "")
    context["contact_email_line"] = f"\nEmail: {email}" if email else ""

    template_key = event
    if event == "missed_call":
        textback_active = context.get("textback_active", "false")
        template_key = "missed_call_active" if textback_active == "true" else "missed_call_inactive"

    sms_template = _SMS_TEMPLATES.get(template_key)
    if not sms_template:
        logger.warning("NOTIFY | no SMS template for event: %s", template_key)
        return []

    ghl: GHLClient | None = None
    results: list[dict[str, Any]] = []

    for i, recipient in enumerate(recipients):
        name = recipient.get("name", "")
        sms_number = recipient.get("sms")
        email_addr = recipient.get("email")
        recipient_events = recipient.get("events") or {}

        if not recipient_events.get(event):
            continue

        if sms_number and isinstance(sms_number, str) and sms_number.strip():
            try:
                if not ghl:
                    if not settings.ghl_snapshot_api_key:
                        logger.warning("NOTIFY | no snapshot API key; skipping SMS")
                    else:
                        ghl = GHLClient(
                            api_key=settings.ghl_snapshot_api_key,
                            location_id=settings.ghl_snapshot_location_id,
                        )

                if ghl:
                    contact_id = recipient.get("ghl_sms_contact_id")
                    if not contact_id:
                        contact_id = await _resolve_or_create_contact(ghl, sms_number, name, "sms")
                        if contact_id and entity_id:
                            await _cache_recipient_field(
                                entity_id,
                                notif_config,
                                i,
                                "ghl_sms_contact_id",
                                contact_id,
                            )

                    if contact_id:
                        message = _format_message(sms_template, context)
                        await ghl.send_sms(contact_id, message)
                        results.append({"channel": "sms", "to": sms_number, "status": "sent"})
                        logger.info("NOTIFY | sms sent | event=%s | to=%s (%s)", event, name, sms_number)
                    else:
                        results.append(
                            {
                                "channel": "sms",
                                "to": sms_number,
                                "status": "failed",
                                "error": "Could not resolve GHL contact",
                            }
                        )
            except Exception as e:
                logger.warning("NOTIFY | sms failed | event=%s | to=%s | error=%s", event, sms_number, e)
                results.append({"channel": "sms", "to": sms_number, "status": "failed", "error": str(e)[:200]})

        if email_addr and isinstance(email_addr, str) and email_addr.strip():
            try:
                built = _build_email_for_event(
                    template_key,
                    context,
                    include_client=False,
                )
                if built:
                    subject, body_html = built
                    result = await send_email(email_addr, subject, body_html, from_name=tenant_name)
                    results.append({"channel": "email", "to": email_addr, **result})
                    logger.info(
                        "NOTIFY | email via resend | event=%s | to=%s | status=%s",
                        event,
                        email_addr,
                        result["status"],
                    )
                else:
                    results.append(
                        {
                            "channel": "email",
                            "to": email_addr,
                            "status": "failed",
                            "error": "No email builder for event",
                        }
                    )
            except Exception as e:
                logger.warning("NOTIFY | email failed | event=%s | to=%s | error=%s", event, email_addr, e)
                results.append({"channel": "email", "to": email_addr, "status": "failed", "error": str(e)[:200]})

    return results


def _clone_tenant_event_defaults() -> dict[str, dict[str, bool]]:
    return {key: value.copy() for key, value in _TENANT_EVENT_DEFAULTS.items()}


def _normalize_tenant_notification_config(raw_config: dict[str, Any] | None) -> dict[str, Any]:
    raw_config = raw_config or {}
    normalized = {
        "sms": raw_config.get("sms") if isinstance(raw_config.get("sms"), str) else None,
        "email": raw_config.get("email") if isinstance(raw_config.get("email"), str) else None,
        "event_channels": _clone_tenant_event_defaults(),
    }

    raw_event_channels = raw_config.get("event_channels")
    if isinstance(raw_event_channels, dict):
        for key, defaults in _TENANT_EVENT_DEFAULTS.items():
            current = raw_event_channels.get(key)
            if not isinstance(current, dict):
                continue
            normalized["event_channels"][key] = {
                "in_app": current.get("in_app") if isinstance(current.get("in_app"), bool) else defaults["in_app"],
                "sms": current.get("sms") if isinstance(current.get("sms"), bool) else defaults["sms"],
                "email": current.get("email") if isinstance(current.get("email"), bool) else defaults["email"],
            }
        return normalized

    legacy_events = raw_config.get("events") or []
    legacy_in_app = raw_config.get("in_app", True) is not False
    if isinstance(legacy_events, list):
        for key in legacy_events:
            if key not in normalized["event_channels"]:
                continue
            normalized["event_channels"][key] = {
                **normalized["event_channels"][key],
                "in_app": legacy_in_app,
                "sms": bool(normalized["sms"]),
                "email": bool(normalized["email"]),
            }

    return normalized


def _tenant_channel_enabled(config: dict[str, Any], event: str, channel: str) -> bool:
    event_channels = config.get("event_channels") or {}
    channel_settings = event_channels.get(event) or {}
    return bool(channel_settings.get(channel))


def _build_tenant_in_app_payload(
    event: str,
    context: dict[str, str],
) -> tuple[str, str, dict[str, Any]]:
    contact_name = context.get("contact_name") or context.get("contact_phone") or "Unknown"

    if event == "transfer_to_human":
        body = (context.get("reason") or "Your agency team needs to review this conversation.")[:500]
        return (
            f"Agency Attention Needed: {contact_name}",
            body,
            {
                "contact_id": context.get("contact_id") or context.get("ghl_contact_id") or "",
                "contact_name": context.get("contact_name") or "",
                "contact_phone": context.get("contact_phone") or "",
                "contact_email": context.get("contact_email") or "",
                "reason": context.get("reason") or "",
            },
        )

    if event == "booking":
        status = context.get("status") or "Booked"
        return (
            f"Booking: {contact_name}",
            f"Status: {status}",
            {
                "contact_id": context.get("contact_id") or "",
                "contact_name": context.get("contact_name") or "",
                "contact_phone": context.get("contact_phone") or "",
                "status": status,
            },
        )

    if event == "new_lead":
        service = context.get("service") or "No interest captured"
        return (
            f"New Lead: {contact_name}",
            service[:500],
            {
                "contact_id": context.get("contact_id") or "",
                "contact_name": context.get("contact_name") or "",
                "contact_phone": context.get("contact_phone") or "",
                "service": service,
            },
        )

    if event == "missed_call":
        textback_active = context.get("textback_active", "false") == "true"
        body = "AI text-back is active." if textback_active else "AI text-back is off."
        return (
            f"Missed Call: {contact_name}",
            body,
            {
                "contact_id": context.get("contact_id") or "",
                "contact_name": context.get("contact_name") or "",
                "contact_phone": context.get("contact_phone") or "",
                "textback_active": textback_active,
            },
        )

    title = event.replace("_", " ").title()
    body = (context.get("reason") or context.get("description") or "")[:500]
    return title, body, context


async def _insert_tenant_notification(
    tenant_id: str,
    entity_id: str,
    event_type: str,
    title: str,
    body: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    from app.services.supabase_client import supabase

    await supabase._request(
        supabase.main_client,
        "POST",
        "/notifications",
        json={
            "tenant_id": tenant_id,
            "entity_id": entity_id or None,
            "event_type": event_type,
            "title": title,
            "body": body,
            "metadata": metadata or {},
        },
        label="insert_tenant_notification",
    )


async def send_tenant_notification(
    tenant_id: str,
    event: str,
    context: dict[str, str],
    client_name: str = "",
    entity_id: str = "",
    allow_in_app: bool = True,
) -> list[dict[str, Any]]:
    """Send tenant-owner notifications using per-event, per-channel tenant config."""
    from app.services.supabase_client import supabase

    if entity_id and not (context.get("ghl_domain") or "").strip():
        try:
            entity = await supabase.resolve_entity(entity_id)
            context["ghl_domain"] = (entity.get("ghl_domain") or "").strip()
        except Exception as e:
            logger.warning("TENANT_NOTIFY | failed to resolve ghl_domain | entity=%s | %s", entity_id, e)

    try:
        resp = await supabase._request(
            supabase.main_client,
            "GET",
            "/tenants",
            params={"id": f"eq.{tenant_id}", "select": "notification_config,name"},
        )
        rows = resp.json()
        if not rows:
            return []
        notif_config = _normalize_tenant_notification_config(rows[0].get("notification_config") or {})
        tenant_display_name = rows[0].get("name") or ""
    except Exception as e:
        logger.warning("TENANT_NOTIFY | failed to fetch tenant config: %s", e)
        return []

    results: list[dict[str, Any]] = []

    template_key = event
    if event == "missed_call":
        textback_active = context.get("textback_active", "false")
        template_key = "missed_call_active" if textback_active == "true" else "missed_call_inactive"

    if allow_in_app and entity_id and _tenant_channel_enabled(notif_config, event, "in_app"):
        try:
            title, body, metadata = _build_tenant_in_app_payload(event, context)
            await _insert_tenant_notification(
                tenant_id=tenant_id,
                entity_id=entity_id,
                event_type=event,
                title=title,
                body=body,
                metadata=metadata,
            )
            results.append({"channel": "in_app", "status": "sent"})
        except Exception as e:
            logger.warning("TENANT_NOTIFY | in_app failed | %s", e)
            results.append({"channel": "in_app", "status": "failed", "error": str(e)[:200]})

    email_addr = notif_config.get("email")
    if (
        _tenant_channel_enabled(notif_config, event, "email")
        and email_addr
        and isinstance(email_addr, str)
        and email_addr.strip()
    ):
        try:
            built = _build_email_for_event(
                template_key,
                context,
                include_client=True,
                client_name=client_name,
            )
            if built:
                subject, body_html = built
                result = await send_email(email_addr, subject, body_html, from_name=tenant_display_name)
                results.append({"channel": "email", "to": email_addr, **result})
                logger.info(
                    "TENANT_NOTIFY | email | tenant=%s | event=%s | to=%s | status=%s",
                    tenant_id,
                    event,
                    email_addr,
                    result["status"],
                )
        except Exception as e:
            logger.warning("TENANT_NOTIFY | email failed | %s", e)
            results.append({"channel": "email", "to": email_addr, "status": "failed", "error": str(e)[:200]})

    sms_number = notif_config.get("sms")
    if (
        _tenant_channel_enabled(notif_config, event, "sms")
        and sms_number
        and isinstance(sms_number, str)
        and sms_number.strip()
    ):
        try:
            sms_template = _SMS_TEMPLATES.get(template_key)
            if sms_template and settings.ghl_snapshot_api_key:
                ghl = GHLClient(
                    api_key=settings.ghl_snapshot_api_key,
                    location_id=settings.ghl_snapshot_location_id,
                )
                contact_id = await _resolve_or_create_contact(ghl, sms_number, "Tenant", "sms")
                if contact_id:
                    message = (
                        f"[{client_name}] " + _format_message(sms_template, context)
                        if client_name
                        else _format_message(sms_template, context)
                    )
                    await ghl.send_sms(contact_id, message)
                    results.append({"channel": "sms", "to": sms_number, "status": "sent"})
                    logger.info("TENANT_NOTIFY | sms | tenant=%s | event=%s | to=%s", tenant_id, event, sms_number)
                else:
                    results.append(
                        {
                            "channel": "sms",
                            "to": sms_number,
                            "status": "failed",
                            "error": "Could not resolve GHL contact",
                        }
                    )
        except Exception as e:
            logger.warning("TENANT_NOTIFY | sms failed | %s", e)
            results.append({"channel": "sms", "to": sms_number, "status": "failed", "error": str(e)[:200]})

    return results
