"""Outreach Resolver — resolves outreach templates for leads.

Matches a lead's service interest to an outreach template, resolves
variables (contact name, location, appointment details), handles A/B
testing, creates a lead record, and returns the fully resolved outreach
positions for Python scheduling to deliver.

MIGRATED: Now reads from `positions` JSONB array instead of hardcoded
sms_1-9, email_1-9 columns. Supports dynamic position counts.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any

from prefect import flow

from app.models import OutreachResolverBody
from app.services.supabase_client import supabase
from app.services.workflow_tracker import WorkflowTracker

logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _parse_name(raw_name: str) -> tuple[str, str, str]:
    """Parse contact name into (first, last, full).

    If the "name" is actually a phone number (all digits/punctuation),
    treat it as empty — GHL sometimes puts the phone in the name field.
    """
    full_name = raw_name.strip()
    if re.fullmatch(r"[\d\s\-\+\(\)\.]+", full_name):
        full_name = ""
    parts = full_name.split() if full_name else []
    first_name = parts[0] if parts else ""
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    return first_name, last_name, full_name


def _build_variable_map(
    body: OutreachResolverBody, first_name: str, last_name: str, full_name: str
) -> dict[str, str]:
    """Build the {{placeholder}} -> value lookup for template substitution."""
    return {
        "{{contact.first_name}}": first_name,
        "{{contact.last_name}}": last_name,
        "{{contact.name}}": full_name,
        "{{contact.email}}": body.email,
        "{{contact.phone}}": body.phone,
        "{{location.name}}": body.location_name,
        "{{location.address}}": body.location_address,
        "{{location.city}}": body.location_city,
        "{{location.state}}": body.location_state,
        "{{appointment.title}}": body.appointment_title,
        "{{appointment.start_date}}": body.appointment_start_date,
        "{{appointment.start_time}}": body.appointment_start_time,
        "{{appointment.day_of_week}}": body.appointment_day_of_week,
        "{{appointment.reschedule_link}}": body.appointment_reschedule_link,
        "{{appointment.google_calendar_link}}": body.appointment_google_calendar_link,
        "{{appointment.ical_link}}": body.appointment_ical_link,
    }


def _resolve_text(text: Any, var_map: dict[str, str]) -> str | None:
    """Substitute variables in text, strip unresolved {{...}}, clean whitespace."""
    if not text or not isinstance(text, str):
        return None
    result = text
    for placeholder, value in var_map.items():
        result = result.replace(placeholder, value)
    result = re.sub(r"\{\{[^}]+\}\}", "", result)
    result = result.replace("%%", "%")
    result = re.sub(r"  +", " ", result).strip()
    return result or None


def _resolve_email_body(text: Any, var_map: dict[str, str]) -> str | None:
    """Resolve variables in email body, then convert newlines to <br> for HTML rendering."""
    resolved = _resolve_text(text, var_map)
    if not resolved:
        return None
    return resolved.replace("\n", "<br>")


def _pass_through(text: Any) -> str | None:
    """Return URL as-is, or None if empty. No variable substitution."""
    if not text or not isinstance(text, str) or not text.strip():
        return None
    return text.strip()


def _pick_variant(field_a: Any, field_b: Any, variant: str) -> Any:
    """Select A or B field value. Falls back to A if B is empty."""
    if variant == "B" and field_b and str(field_b).strip():
        return field_b
    return field_a


# ============================================================================
# A/B TESTING
# ============================================================================


async def _determine_variant(
    matched: dict[str, Any],
) -> tuple[str, bool, bool]:
    """Determine A/B variant for this lead.

    Returns (variant, is_ab_test, counter_update_failed).

    Checks if ANY position has a B variant. If so, alternates from the
    last assigned variant. Updates the template's ab_last_variant field.
    """
    positions = matched.get("positions") or []

    has_any_b = any(
        (pos.get("sms_b") and str(pos["sms_b"]).strip())
        or (pos.get("email_subject_b") and str(pos["email_subject_b"]).strip())
        or (pos.get("email_body_b") and str(pos["email_body_b"]).strip())
        or (pos.get("media_url_b") and str(pos["media_url_b"]).strip())
        for pos in positions
    )

    if not has_any_b:
        return "A", False, False

    last_variant = matched.get("ab_last_variant") or "B"
    variant = "B" if last_variant == "A" else "A"

    counter_update_failed = False
    for attempt in range(1, 4):
        try:
            await supabase.main_client.patch(
                "/outreach_templates",
                params={"id": f"eq.{matched['id']}"},
                json={"ab_last_variant": variant},
            )
            break
        except Exception:
            if attempt == 3:
                counter_update_failed = True
                logger.warning("A/B counter PATCH failed after 3 retries")
            else:
                await asyncio.sleep(0.5 * attempt)

    return variant, True, counter_update_failed


# ============================================================================
# RESPONSE BUILDING (JSONB POSITIONS)
# ============================================================================


def _build_snapshot_and_response(
    matched: dict[str, Any],
    variant: str,
    is_ab_test: bool,
    var_map: dict[str, str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build the raw snapshot (for lead attribution) and resolved positions.

    snapshot: unresolved template text — stored as outreach_variant on the lead
    resolved_positions: resolved text per position — used for scheduling/delivery

    Returns (snapshot_dict, resolved_positions_list).
    """
    positions = matched.get("positions") or []

    snapshot: dict[str, Any] = {
        "outreach_name": matched.get("form_service_interest", ""),
        "variant": variant,
        "ab_test_active": is_ab_test,
        "assigned_at": datetime.now(timezone.utc).isoformat(),
        "is_appointment_template": bool(matched.get("is_appointment_template")),
        "calendar_id": matched.get("calendar_id") or None,
        "calendar_name": matched.get("calendar_name") or None,
    }

    resolved_positions: list[dict[str, Any]] = []

    for pos in positions:
        pos_num = pos.get("position", len(resolved_positions) + 1)

        # SMS: pick variant, resolve variables
        raw_sms = _pick_variant(pos.get("sms"), pos.get("sms_b"), variant)
        resolved_sms = _resolve_text(raw_sms, var_map)

        # Email: pick variant, resolve variables
        raw_email_subject = _pick_variant(pos.get("email_subject"), pos.get("email_subject_b"), variant)
        raw_email_body = _pick_variant(pos.get("email_body"), pos.get("email_body_b"), variant)
        resolved_email_subject = _resolve_text(raw_email_subject, var_map)
        resolved_email_body = _resolve_email_body(raw_email_body, var_map)

        # Media: pick variant, pass through URLs
        raw_media = _pick_variant(pos.get("media_url"), pos.get("media_url_b"), variant)
        resolved_media = _pass_through(raw_media)
        media_type = pos.get("media_type") or None

        # Media transcript: pick variant, resolve variables
        raw_transcript = _pick_variant(pos.get("media_transcript"), pos.get("media_transcript_b"), variant)
        resolved_transcript = _resolve_text(raw_transcript, var_map)

        # Store unresolved in snapshot (for fingerprinting/attribution)
        snapshot[f"sms_{pos_num}"] = raw_sms or None
        snapshot[f"email_{pos_num}_subject"] = raw_email_subject or None
        snapshot[f"email_{pos_num}_body"] = raw_email_body or None
        snapshot[f"media_{pos_num}"] = raw_media or None

        # Build resolved position for scheduling
        resolved_pos: dict[str, Any] = {
            "position": pos_num,
            "delay": pos.get("delay", "0 minutes"),
            "send_window": pos.get("send_window"),
            "sms": resolved_sms,
            "email_subject": resolved_email_subject,
            "email_body": resolved_email_body,
            "media_url": resolved_media,
            "media_type": media_type,
            "media_transcript": resolved_transcript,
        }
        resolved_positions.append(resolved_pos)

    return snapshot, resolved_positions


# ============================================================================
# LEAD CREATION
# ============================================================================


async def _create_unmatched_lead(
    *,
    templates: list[dict[str, Any]],
    ghl_contact_id: str,
    body: OutreachResolverBody,
    full_name: str,
    service_interest: str,
) -> None:
    """Create a stub lead when no template matched (silent, non-fatal)."""
    if not templates:
        return

    first_template = templates[0]
    owner_id = first_template.get("entity_id")

    try:
        await supabase.create_lead(
            {
                "entity_id": owner_id,
                "ghl_contact_id": ghl_contact_id,
                "source": body.source,
                "contact_name": full_name or None,
                "contact_phone": body.phone or None,
                "contact_email": body.email or None,
                "form_interest": service_interest,
            },
        )
    except Exception:
        logger.warning("Unmatched lead creation failed (non-fatal)", exc_info=True)


async def _create_lead_and_patch_snapshot(
    *,
    matched: dict[str, Any],
    ghl_contact_id: str,
    body: OutreachResolverBody,
    full_name: str,
    service_interest: str,
    snapshot: dict[str, Any],
) -> str | None:
    """Create a lead record and patch the outreach_variant snapshot onto it."""
    owner_id = matched.get("entity_id")

    try:
        lead = await supabase.create_lead(
            {
                "entity_id": owner_id,
                "ghl_contact_id": ghl_contact_id,
                "source": body.source,
                "contact_name": full_name or None,
                "contact_phone": body.phone or None,
                "contact_email": body.email or None,
                "form_interest": service_interest,
            },
        )
        lead_id = lead.get("id")
    except Exception:
        logger.warning("Lead creation failed (non-fatal)", exc_info=True)
        return None

    if lead_id:
        try:
            await supabase.update_lead(lead_id, {"outreach_variant": snapshot})
        except Exception:
            logger.warning("outreach_variant PATCH failed (non-fatal)", exc_info=True)

    return lead_id


# ============================================================================
# MAIN RESOLVER
# ============================================================================


@flow(name="outreach-resolver", retries=0)
async def resolve_outreach(
    client_id: str,
    body: OutreachResolverBody,
) -> tuple[dict[str, Any], int]:
    """Core Outreach Resolver logic.

    Returns (response_dict, http_status_code).
    """
    tracker = WorkflowTracker(
        "outreach_resolver",
        entity_id=client_id,
        ghl_contact_id=body.id,
        trigger_source="webhook",
    )

    try:
        ghl_contact_id = body.id
        service_interest = body.form_service_interest.strip()
        appointment_mode = body.appointment_mode

        # ── Validate required fields ──
        if not service_interest:
            logger.warning("OUTREACH | missing form_service_interest | entity=%s", client_id)
            tracker.set_error("Missing form_service_interest")
            return {"success": False, "error": "Missing form_service_interest"}, 400

        # ── Parse contact name ──
        first_name, last_name, full_name = _parse_name(body.name)

        # ── Fetch all active templates for this entity ──
        templates = await supabase.get_outreach_templates(client_id)

        logger.info("OUTREACH | templates_loaded | entity=%s | count=%d", client_id, len(templates))

        if not templates:
            logger.warning("OUTREACH | no_templates | entity=%s", client_id)
            tracker.set_error("No active templates found")
            return {
                "success": False,
                "error": "No active templates found for this client/bot",
            }, 404

        # ── Match template by service interest (case-insensitive) ──
        matched = next(
            (
                t for t in templates
                if (t.get("form_service_interest") or "").strip().lower() == service_interest.lower()
            ),
            None,
        )

        if not matched:
            logger.warning(
                "OUTREACH | no_match | entity=%s | interest=%s | available=%s",
                client_id, service_interest,
                [t.get("form_service_interest") for t in templates],
            )
            if ghl_contact_id and not appointment_mode:
                await _create_unmatched_lead(
                    templates=templates,
                    ghl_contact_id=ghl_contact_id,
                    body=body,
                    full_name=full_name,
                    service_interest=service_interest,
                )

            tracker.set_decisions({"matched": False, "service_interest": service_interest})
            tracker.set_status("skipped")
            return {
                "success": False,
                "error": f"No template for service: {service_interest}",
                "available_services": [
                    t.get("form_service_interest") for t in templates if t.get("form_service_interest")
                ],
            }, 404

        is_appointment_template = bool(matched.get("is_appointment_template"))

        # ── A/B variant determination ──
        variant, is_ab_test, counter_update_failed = await _determine_variant(matched)
        logger.info("OUTREACH | variant_selected | entity=%s | variant=%s | ab_test=%s", client_id, variant, is_ab_test)

        # ── Build variable map + resolve templates ──
        var_map = _build_variable_map(body, first_name, last_name, full_name)
        snapshot, resolved_positions = _build_snapshot_and_response(matched, variant, is_ab_test, var_map)

        # ── Compute counts ──
        sms_count = sum(1 for p in resolved_positions if p.get("sms"))
        email_count = sum(1 for p in resolved_positions if p.get("email_body"))
        media_count = sum(1 for p in resolved_positions if p.get("media_url"))

        snapshot.update({
            "sms_count": sms_count,
            "email_count": email_count,
            "media_count": media_count,
            "position_count": len(resolved_positions),
        })

        # ── Lead creation (skip for appointment mode) ──
        lead_id: str | None = None
        if not appointment_mode and ghl_contact_id:
            lead_id = await _create_lead_and_patch_snapshot(
                matched=matched,
                ghl_contact_id=ghl_contact_id,
                body=body,
                full_name=full_name,
                service_interest=service_interest,
                snapshot=snapshot,
            )

        # ── Assemble final response ──
        result: dict[str, Any] = {
            "success": True,
            "template_id": matched["id"],
            "form_service_interest": matched["form_service_interest"],
            "is_appointment_template": is_appointment_template,
            "appointment_mode": appointment_mode,
            "variant": variant,
            "ab_test_active": is_ab_test,
            "resolved_positions": resolved_positions,
            "sms_count": sms_count,
            "email_count": email_count,
            "media_count": media_count,
            "position_count": len(resolved_positions),
            "outreach_variant_json": None if is_appointment_template else snapshot,
            "timing_config": matched.get("timing_config") or {},
            # Lead metadata
            "lead_owner_id": matched.get("entity_id"),
            "lead_ghl_contact_id": ghl_contact_id,
            "lead_source": body.source,
            "lead_contact_name": full_name or None,
            "lead_contact_phone": body.phone or None,
            "lead_contact_email": body.email or None,
            "lead_form_interest": matched["form_service_interest"],
        }

        if lead_id:
            result["lead_id"] = lead_id
            tracker.set_lead_id(lead_id)

        if counter_update_failed:
            result["ab_counter_warning"] = (
                "Counter PATCH failed after 3 retries — variant still assigned "
                "but next lead may duplicate"
            )

        tracker.set_decisions({
            "matched": True,
            "service_interest": service_interest,
            "variant": variant,
            "ab_test": is_ab_test,
            "appointment_mode": appointment_mode,
            "positions": len(resolved_positions),
            "sms_count": sms_count,
            "email_count": email_count,
        })

        logger.info(
            "Outreach resolved: service=%s, variant=%s, positions=%d (sms=%d, email=%d, media=%d), lead=%s",
            service_interest, variant, len(resolved_positions), sms_count, email_count, media_count,
            lead_id or "skipped",
        )

        return result, 200

    except Exception as e:
        tracker.set_error(str(e))
        raise
    finally:
        await tracker.save()
