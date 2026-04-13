"""Compiler: system_config.services → services text, offers text, deployment rules.

Wraps the existing offers.py logic and provides helpers for compiling
services from the new unified system_config.services location.

In system_config, services/offers/qualifications are all under:
  system_config.services = {
    "services": [...],
    "global_qualifications": [...],
    "global_offers": [...],
    "qualification_rules": "...",
    "offers_deployment": { "style": "proactive" }
  }

Per-service offers are inside each service object:
  service.offers = [...]
"""

from __future__ import annotations

from datetime import date
from typing import Any


def _agent_match(item: dict[str, Any], agent_key: str | None) -> bool:
    """Return True if item is available to the given agent.

    Empty agents list = available to all. If agent_key is None, include everything.
    """
    if not agent_key:
        return True
    setter_keys = item.get("setter_keys", [])
    legacy_agents = item.get("agents", [])
    scoped = setter_keys or legacy_agents
    return not scoped or agent_key in scoped


def compile_services_list(
    services_config: dict[str, Any] | None,
    *,
    names_only: bool = False,
    include_pricing: bool = True,
    agent_key: str | None = None,
) -> str:
    """Compile services list for injection into prompts.

    Args:
        services_config: system_config.services dict
        names_only: If True, only return service names (for media selector context)
        include_pricing: If True, include pricing info
        agent_key: If set, only include services scoped to this agent (empty agents = all)
    """
    if not services_config:
        return ""

    services = [s for s in services_config.get("services", []) if _agent_match(s, agent_key)]
    if not services:
        return ""

    if names_only:
        names = [s.get("name", "") for s in services if s.get("name")]
        return ", ".join(names) if names else ""

    lines: list[str] = []
    for i, svc in enumerate(services, 1):
        name = svc.get("name", f"Service {i}")
        desc = svc.get("description", "")
        pricing = svc.get("pricing", "")

        line = f"{i}. {name}"
        if desc:
            line += f" -- {desc}"
        if include_pricing and pricing:
            line += f". Pricing: {pricing}"
        lines.append(line)

    return "\n".join(lines)


def compile_all_offers(services_config: dict[str, Any] | None, *, agent_key: str | None = None) -> str:
    """Compile all active offers (per-service + global) into prompt text.

    Args:
        services_config: system_config.services dict
        agent_key: If set, only include offers from agent-scoped services + agent-scoped offers
    """
    if not services_config:
        return ""

    active: list[dict[str, Any]] = []

    # Per-service offers (only from services available to this agent)
    for svc in services_config.get("services", []):
        if not isinstance(svc, dict):
            continue
        if not _agent_match(svc, agent_key):
            continue
        svc_name = svc.get("name", "")
        for offer in svc.get("offers", []):
            if isinstance(offer, dict) and _is_active(offer) and _agent_match(offer, agent_key):
                offer_copy = dict(offer)
                offer_copy["_service"] = svc_name
                active.append(offer_copy)

    # Global offers (filter by agent)
    for offer in services_config.get("global_offers", []):
        if isinstance(offer, dict) and _is_active(offer) and _agent_match(offer, agent_key):
            active.append(offer)

    if not active:
        return ""

    return _render_offers(active)


def compile_offers_deployment(
    services_config: dict[str, Any] | None,
    deployment_override: dict[str, Any] | None = None,
) -> str:
    """Compile the offer deployment rules based on style setting.

    Reads from system_config.services.offers_deployment or
    system_config.offers_deployment.
    """
    if not services_config:
        return ""

    deployment = deployment_override or services_config.get("offers_deployment", {})
    if not deployment:
        return ""

    style = deployment.get("style", "proactive")
    prompts = deployment.get("prompts", {})

    prompt = (prompts.get(style) or "").strip()
    if not prompt:
        return ""

    return f"## How to Use These Offers\n\n{prompt}"


# =========================================================================
# INTERNAL
# =========================================================================

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
            pass
    if expires:
        try:
            if today > date.fromisoformat(expires):
                return False
        except ValueError:
            pass
    return True


def _render_offers(offers: list[dict[str, Any]]) -> str:
    """Format active offers into prompt text with type labels."""
    lines: list[str] = []
    for offer in offers:
        name = offer.get("name", "Offer")
        offer_type = offer.get("type", "promotion")
        label = _TYPE_LABELS.get(offer_type, offer_type.upper())
        value = offer.get("value", "")
        desc = offer.get("description", "")
        eligibility = offer.get("eligibility", "")
        service = offer.get("_service", "")

        header = f"[{label}] {name}"
        if value:
            header += f" — {value}"
        lines.append(header)

        if desc:
            lines.append(f"  {desc}")
        if eligibility:
            lines.append(f"  Eligibility: {eligibility}")
        if service:
            lines.append(f"  Service: {service}")
        lines.append("")

    return "\n".join(lines).strip()
