"""Helpers for building GHL app links consistently."""

from __future__ import annotations

DEFAULT_GHL_APP_DOMAIN = "https://app.gohighlevel.com"


def normalize_ghl_domain(domain: str | None) -> str:
    """Normalize a stored GHL app domain, falling back to the default host."""
    value = (domain or "").strip()
    if not value:
        return DEFAULT_GHL_APP_DOMAIN
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    return value.rstrip("/")


def build_ghl_contact_url(
    ghl_location_id: str,
    ghl_contact_id: str,
    ghl_domain: str | None = None,
) -> str:
    """Build a GHL contact-detail URL for the given app domain/location/contact."""
    location_id = (ghl_location_id or "").strip()
    contact_id = (ghl_contact_id or "").strip()
    if not location_id or not contact_id:
        return ""
    base = normalize_ghl_domain(ghl_domain)
    return f"{base}/v2/location/{location_id}/contacts/detail/{contact_id}"
