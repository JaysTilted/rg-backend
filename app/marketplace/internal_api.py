"""Internal cross-product API.

Endpoints under ``/api/internal`` are NOT meant for end-users or GHL —
they're called by other Iron services (iron-bridge, iron-instantly-bridge)
to coordinate the Trilogy product family.

Auth: shared-secret header ``X-Internal-Auth: <secret>`` matched against
``settings.internal_api_secret``. Empty/missing secret on the server side
disables the endpoints entirely (fail closed). Constant-time comparison
to prevent timing leaks.

Phase 1.F1 of the PRD — these endpoints back the
``installed_products(location_id)`` cross-product helper that lets each
bridge detect siblings and configure itself accordingly (e.g. iron-setter
auto-sets ``sms_provider="telnyx"`` when iron-bridge is detected for the
same location).
"""

from __future__ import annotations

import hmac
import logging
from typing import Any

from fastapi import APIRouter, Header, HTTPException

from app.config import settings
from app.marketplace.oauth_store import get_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/internal", tags=["internal-cross-product"])


def _verify_internal_auth(header_value: str | None) -> None:
    """Constant-time check of the X-Internal-Auth header.

    Fails closed: if the secret is unset on the server, no caller can pass.
    """
    expected = (getattr(settings, "internal_api_secret", "") or "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="Internal API not configured")
    if not header_value or not hmac.compare_digest(expected, header_value):
        raise HTTPException(status_code=401, detail="Invalid internal auth")


@router.get("/install-status/{location_id}")
async def install_status(
    location_id: str,
    x_internal_auth: str | None = Header(default=None, alias="X-Internal-Auth"),
) -> dict[str, Any]:
    """Returns whether iron-setter is installed for the given GHL location.

    Response shape (NEVER includes access_token / refresh_token):

        {
            "product": "setter",
            "location_id": "ctGiSfuydlm90NJWq6RV",
            "installed": true,
            "company_id": "6HfiVUDGvkOsweO7XA2a",
            "entity_id": "bd1df814-...",
            "installed_at": "2026-04-26T16:24:49Z",
            "updated_at": "2026-05-04T20:56:22Z",
            "expires_at": "2026-05-05T20:50:17Z",
            "scopes": ["contacts.readonly", ...],
            "scope_count": 17,
        }

    Returns ``{"installed": false, "product": "setter", "location_id": ...}``
    when no install row exists for the location. 404 NOT used — null/false
    is the easier shape for the cross-product caller.
    """
    _verify_internal_auth(x_internal_auth)

    install = await get_token(location_id)
    if not install:
        return {
            "product": "setter",
            "location_id": location_id,
            "installed": False,
        }

    scopes_str = (install.get("scopes") or "").strip()
    scopes_list = [s for s in scopes_str.split(" ") if s]

    installed_at = install.get("installed_at")
    updated_at = install.get("updated_at")
    expires_at = install.get("expires_at")

    return {
        "product": "setter",
        "location_id": location_id,
        "installed": True,
        "company_id": install.get("company_id"),
        "entity_id": str(install["entity_id"]) if install.get("entity_id") else None,
        "installed_at": installed_at.isoformat() if installed_at else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "expires_at": expires_at.isoformat() if expires_at else None,
        "scopes": scopes_list,
        "scope_count": len(scopes_list),
    }
