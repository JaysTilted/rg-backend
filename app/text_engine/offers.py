"""Offers resolution — converts offers_config JSONB to prompt text.

Reads ctx.config["offers_config"], filters to active offers (enabled + within
date window), and renders to human-readable prompt text with type labels.
Returns None when no offers are configured or none are active.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from app.models import PipelineContext

_TYPE_LABELS = {
    "promotion": "PROMO",
    "bundle": "BUNDLE",
    "referral": "REFERRAL",
    "upsell": "UPSELL",
}


def _is_active(offer: dict[str, Any]) -> bool:
    """Return True if the offer is enabled and within its date window."""
    if not offer.get("enabled", True):
        return False
    today = date.today()
    starts = offer.get("starts_at")
    expires = offer.get("expires_at")
    if starts:
        try:
            if today < date.fromisoformat(starts):
                return False
        except ValueError:
            pass  # malformed date — treat as no constraint
    if expires:
        try:
            if today > date.fromisoformat(expires):
                return False
        except ValueError:
            pass
    return True


def get_active_offers(offers_config: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Extract active offers from offers_config JSONB. Returns [] if no config."""
    if not offers_config or not isinstance(offers_config, dict):
        return []
    return [o for o in offers_config.get("offers", []) if _is_active(o)]


def render_offers_text(active_offers: list[dict[str, Any]]) -> str:
    """Format active offers into prompt text with type labels.

    Output format:
        [PROMO] $50 Off First Procedure — $50 off
          $50 off any first procedure for new clients.
          Eligibility: new clients only
    """
    lines: list[str] = []
    for offer in active_offers:
        type_label = _TYPE_LABELS.get(offer.get("type", ""), "OFFER")
        name = offer.get("name", "Unnamed Offer")
        description = offer.get("description", "")
        value = offer.get("value", "")
        eligibility = offer.get("eligibility", "")

        header = f"[{type_label}] {name}"
        if value:
            header += f" — {value}"
        lines.append(header)
        if description:
            lines.append(f"  {description}")
        if eligibility:
            lines.append(f"  Eligibility: {eligibility}")
        lines.append("")  # blank line between offers
    return "\n".join(lines).rstrip()


def format_offers_for_prompt(ctx: PipelineContext) -> str | None:
    """Resolve and render offers for prompt injection.

    Returns None when nothing is configured or no offers are active,
    signaling callers to omit the offers section entirely.
    """
    # Read offers from setter's services config
    _setter = ctx.compiled.get("_matched_setter") or {}
    _services = _setter.get("services") or {}
    # Build offers from services config global_offers + per-service offers
    from app.text_engine.services_compiler import compile_all_offers
    compiled = compile_all_offers(_services)
    if compiled:
        return compiled
    # Fallback: legacy offers_config column
    offers_config = ctx.config.get("offers_config")
    if offers_config is None:
        return None

    active = get_active_offers(offers_config)
    if not active:
        return None

    return render_offers_text(active)
