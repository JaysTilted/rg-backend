"""Ask AI admin action registry.

This is the source of truth for what Ask AI is allowed to edit.
The knowledge base may explain these rules, but this module enforces them.
"""

from __future__ import annotations

from typing import Any


AI_SETTER_CAPABILITIES: list[dict[str, Any]] = [
    {
        "group": "ai_setter",
        "surface": "bot_persona",
        "label": "Bot Persona",
        "description": "Edit the AI identity, tone, message splitting, punctuation, and related persona settings.",
        "allowed": True,
        "save_path": "edit-entity/system_config.setters[setterKey].bot_persona",
    },
    {
        "group": "ai_setter",
        "surface": "services",
        "label": "Services & Offers",
        "description": "Edit services, offers, pricing context, and qualification context inside AI Setter.",
        "allowed": True,
        "save_path": "edit-entity/system_config.setters[setterKey].services",
    },
    {
        "group": "ai_setter",
        "surface": "conversation",
        "label": "Conversation Settings",
        "description": "Edit reply behavior, follow-up settings, objection handling, and conversation strategy.",
        "allowed": True,
        "save_path": "edit-entity/system_config.setters[setterKey].conversation",
    },
    {
        "group": "ai_setter",
        "surface": "booking",
        "label": "Booking & Calendars",
        "description": "Edit booking behavior, calendars, links, appointment length, and booking constraints.",
        "allowed": True,
        "save_path": "edit-entity/system_config.setters[setterKey].booking",
    },
    {
        "group": "ai_setter",
        "surface": "transfer_security",
        "label": "Transfer & Security",
        "description": "Edit transfer rules, compliance rules, and security-related conversation controls.",
        "allowed": True,
        "save_path": "edit-entity/system_config.setters[setterKey].transfer + security",
    },
    {
        "group": "ai_setter",
        "surface": "ai_models",
        "label": "AI Models",
        "description": "Edit model overrides and temperatures for supported AI calls.",
        "allowed": True,
        "save_path": "edit-entity/system_config.setters[setterKey].ai_models + ai_temperatures",
    },
    {
        "group": "ai_setter",
        "surface": "automations",
        "label": "Automations",
        "description": "Edit missed call text-back, auto reactivation, post-appointment automation, and lead-sequence / appointment-reminder template settings.",
        "allowed": True,
        "save_path": "edit-entity/system_config.setters[setterKey].missed_call_textback + auto_reactivation + conversation.post_booking",
    },
]


CLIENT_SETTINGS_CAPABILITIES: list[dict[str, Any]] = [
    {
        "group": "client_settings",
        "surface": "journey_stage",
        "label": "Client Status / Journey Stage",
        "description": "Update the client lifecycle stage such as Onboarding, Active, Paused, or Churned.",
        "allowed": True,
        "save_path": "edit-entity/entities.journey_stage",
    },
    {
        "group": "client_settings",
        "surface": "timezone",
        "label": "Timezone",
        "description": "Update the client's timezone.",
        "allowed": True,
        "save_path": "edit-entity/entities.timezone",
    },
    {
        "group": "client_settings",
        "surface": "business_phone",
        "label": "Business Phone",
        "description": "Update the client's main text phone number.",
        "allowed": True,
        "save_path": "edit-entity/entities.business_phone",
    },
    {
        "group": "client_settings",
        "surface": "notes",
        "label": "Client Notes",
        "description": "Update internal notes stored on the entity.",
        "allowed": True,
        "save_path": "edit-entity/entities.notes",
    },
    {
        "group": "client_settings",
        "surface": "average_booking_value",
        "label": "Average Booking Value",
        "description": "Update the client's average booking value.",
        "allowed": True,
        "save_path": "edit-entity/entities.average_booking_value",
    },
    {
        "group": "client_settings",
        "surface": "sms_provider",
        "label": "SMS Provider",
        "description": "Update the configured SMS provider in client settings.",
        "allowed": True,
        "save_path": "edit-entity/entities.system_config.sms_provider",
    },
    {
        "group": "client_settings",
        "surface": "human_takeover",
        "label": "Human Takeover Settings",
        "description": "Update pause bot on human activity and human takeover timeout settings.",
        "allowed": True,
        "save_path": "edit-entity/entities.system_config.pause_bot_on_human_activity + human_takeover_minutes",
    },
    {
        "group": "client_settings",
        "surface": "notifications",
        "label": "Notification Recipients",
        "description": "Update client notification recipients and event toggles.",
        "allowed": True,
        "save_path": "edit-entity/entities.system_config.notifications",
    },
]


FORBIDDEN_CAPABILITIES: list[dict[str, Any]] = [
    {
        "group": "blocked",
        "surface": "ghl_credentials",
        "label": "GHL Credentials",
        "description": "Ask AI must not edit GHL API key or GHL location ID.",
        "allowed": False,
    },
    {
        "group": "blocked",
        "surface": "tenant_api_keys",
        "label": "Tenant API Keys",
        "description": "Ask AI must not edit tenant API keys.",
        "allowed": False,
    },
    {
        "group": "blocked",
        "surface": "billing_subscription",
        "label": "Billing / Subscription",
        "description": "Ask AI must not edit billing or subscription settings in the first action phase.",
        "allowed": False,
    },
    {
        "group": "blocked",
        "surface": "team_auth",
        "label": "Team / Auth",
        "description": "Ask AI must not edit team roles, auth flows, or account credentials.",
        "allowed": False,
    },
    {
        "group": "blocked",
        "surface": "destructive_actions",
        "label": "Delete / Archive / Destructive Actions",
        "description": "Ask AI must not perform deletes, archives, or destructive actions in the first action phase.",
        "allowed": False,
    },
]


def get_action_capabilities() -> dict[str, Any]:
    return {
        "editable": AI_SETTER_CAPABILITIES + CLIENT_SETTINGS_CAPABILITIES,
        "blocked": FORBIDDEN_CAPABILITIES,
    }
