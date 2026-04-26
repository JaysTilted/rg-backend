"""Niche system_config templates for new client onboarding."""

from __future__ import annotations

import json
from pathlib import Path

_TEMPLATE_DIR = Path(__file__).parent

AVAILABLE_NICHES = [
    "hvac",
    "plumbing",
    "roofing",
    "electrical",
    "solar",
    "landscaping",
    "general_contractor",
]


def load_template(niche: str) -> dict:
    path = _TEMPLATE_DIR / f"{niche}.json"
    if not path.exists():
        path = _TEMPLATE_DIR / "base.json"
    return json.loads(path.read_text())


def build_config(
    *,
    niche: str,
    business_name: str,
    setter_name: str = "Alex",
    tone: str = "friendly",
    services: list[dict] | None = None,
    calendar_id: str = "",
    calendar_name: str = "Discovery Call",
    booking_link: str = "",
    booking_mode: str = "conversational",
    offer_enabled: bool = False,
    offer_description: str = "",
    offer_value: str = "",
) -> dict:
    tpl = load_template(niche)

    setter = tpl["setters"]["setter_1"]
    setter["bot_persona"]["identity"]["name"] = setter_name
    setter["bot_persona"]["identity"]["company_name"] = business_name

    tone_map = {
        "friendly": ["Friendly", "Warm", "Conversational"],
        "professional": ["Professional", "Confident", "Direct"],
        "casual": ["Casual", "Conversational", "Warm"],
        "direct": ["Direct", "Concise", "Professional"],
    }
    setter["bot_persona"]["tone"]["selected"] = tone_map.get(tone, tone_map["friendly"])

    if services:
        setter["services"]["services"] = services

    if calendar_id:
        setter["booking"]["calendars"] = [
            {
                "name": calendar_name,
                "id": calendar_id,
                "enabled": True,
                "services": [s["name"] for s in (services or setter["services"]["services"])],
                "service_ids": [s["id"] for s in (services or setter["services"]["services"])],
                "booking_window_days": 14,
                "appointment_length_minutes": 30,
                "booking_mode": booking_mode,
                "booking_link_override": booking_link,
            }
        ]

    if offer_enabled and offer_description:
        setter["services"]["global_offers"] = [
            {
                "name": "New Client Offer",
                "description": offer_description,
                "value": offer_value,
                "eligibility": "new_customers",
                "enabled": True,
                "type": "discount",
            }
        ]

    return tpl
