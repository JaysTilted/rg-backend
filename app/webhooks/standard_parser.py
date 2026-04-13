"""Standard GHL webhook payload parser.

GHL's free standard webhook sends a flat JSON object containing:
- Contact fields: contact_id, first_name, last_name, full_name, email, phone, tags, etc.
- ALL custom fields as top-level keys (display names, e.g., "Agent Type": "setter_1")
- Location data: location.{name, address, city, state, id, fullAddress}
- Calendar data (appointment triggers): calendar.{appointmentId, startTime, endTime, status, ...}
- Workflow metadata: workflow.{id, name}

Confirmed via webhook.site testing (Mar 24, 2026):
- Contact trigger: all custom fields + contact data + location
- Appointment trigger: above + full calendar object
- Call trigger: above + sparse message object (sync fills the gap)
- Auth headers work (Bearer token in Headers section)
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)


class StandardWebhookPayload(BaseModel):
    """Pydantic model for GHL standard webhook payload.

    Uses extra="allow" to capture all GHL custom fields as top-level keys.
    Custom fields come with their display names (e.g., "Agent Type", "Lead Source").
    """

    # Core contact fields
    contact_id: str = ""
    first_name: str = ""
    last_name: str = ""
    full_name: str = ""
    email: str = ""
    phone: str = ""
    tags: str = ""  # comma-separated string from GHL
    company_name: str = ""
    full_address: str = ""
    city: str = ""
    country: str = ""
    date_created: str = ""
    contact_type: str = ""

    # Nested objects (may or may not be present depending on trigger type)
    location: dict[str, Any] = {}
    calendar: dict[str, Any] = {}
    workflow: dict[str, Any] = {}
    message: dict[str, Any] = {}  # Call trigger — sparse, usually just {type: 1}

    # Trigger-specific data (usually empty for standard triggers)
    triggerData: dict[str, Any] = {}
    customData: dict[str, Any] = {}

    # Allow all extra fields — GHL custom fields come as top-level keys
    model_config = ConfigDict(extra="allow")


def extract_custom_field(payload: dict[str, Any], display_name: str) -> str:
    """Extract a GHL custom field value from the flat webhook payload.

    GHL custom fields are top-level keys using their display names.
    E.g., "Agent Type": "setter_1", "Lead Source": "Meta Ads"

    Args:
        payload: Raw webhook JSON dict (not the Pydantic model).
        display_name: The GHL display name of the field (e.g., "Agent Type").

    Returns:
        The field value as a string, or empty string if not found.
    """
    value = payload.get(display_name, "")
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def extract_tags_list(payload: dict[str, Any]) -> list[str]:
    """Extract tags as a list from the webhook payload.

    GHL sends tags as a comma-separated string in standard webhooks.
    """
    tags_raw = payload.get("tags", "")
    if isinstance(tags_raw, list):
        return tags_raw
    if isinstance(tags_raw, str) and tags_raw.strip():
        return [t.strip() for t in tags_raw.split(",") if t.strip()]
    return []


def extract_calendar(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Extract calendar/appointment data from the webhook payload.

    Returns None if no calendar data present (non-appointment trigger).

    Calendar object fields (confirmed via testing):
    - appointmentId: GHL appointment UUID
    - startTime: ISO datetime (e.g., "2026-03-24T11:30:00")
    - endTime: ISO datetime
    - status: "booked" | "confirmed" | "cancelled" | "showed" | "noshow"
    - appoinmentStatus: "confirmed" | "cancelled" (note: GHL typo — "appoinment")
    - calendarName: Calendar/service name
    - selectedTimezone: IANA timezone (e.g., "America/Chicago")
    - title: Contact name
    - created_by: User who created the appointment
    - date_created: ISO datetime
    """
    cal = payload.get("calendar")
    if not cal or not isinstance(cal, dict):
        return None
    # Require at least an appointment ID to consider it valid
    if not cal.get("appointmentId"):
        return None
    return cal


def extract_location(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract location data from the webhook payload.

    Location object fields:
    - name: Location/business name
    - address: Street address
    - city, state, country, postalCode
    - fullAddress: Formatted full address
    - id: GHL location UUID
    """
    loc = payload.get("location")
    if not loc or not isinstance(loc, dict):
        return {}
    return loc


def build_contact_data_from_webhook(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a contact data dict from the webhook payload.

    This replaces the get_contact() GHL API call for webhook-triggered flows.
    The standard webhook includes all contact fields + custom fields.

    Returns a dict compatible with what load_data() expects from get_contact().
    """
    # Parse custom fields into the format GHL API returns
    # GHL API returns: {"customFields": [{"key": "field_key", "value": "..."}]}
    # Standard webhook has them as top-level keys with display names
    # We store the raw payload so data_loading.py can extract what it needs

    tags = extract_tags_list(payload)

    return {
        "id": payload.get("contact_id", ""),
        "firstName": payload.get("first_name", ""),
        "lastName": payload.get("last_name", ""),
        "name": payload.get("full_name", ""),
        "email": payload.get("email", ""),
        "phone": payload.get("phone", ""),
        "tags": tags,
        "source": extract_custom_field(payload, "Lead Source"),
        "city": payload.get("city", ""),
        "country": payload.get("country", ""),
        "companyName": payload.get("company_name", ""),
        "address1": payload.get("full_address", ""),
        "dateCreated": payload.get("date_created", ""),
        "type": payload.get("contact_type", ""),
        # Store raw webhook data so custom fields can be extracted by display name
        "_webhook_custom_fields": {
            k: v for k, v in payload.items()
            if k not in {
                "contact_id", "first_name", "last_name", "full_name",
                "email", "phone", "tags", "company_name", "full_address",
                "city", "country", "date_created", "contact_type",
                "location", "calendar", "workflow", "message",
                "triggerData", "customData",
            }
        },
    }
