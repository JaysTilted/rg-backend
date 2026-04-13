"""
AI Data Chat — tool definitions and executors.

Each tool wraps an existing Supabase RPC or direct query.
Tools are read-only — they never modify data.
"""

from __future__ import annotations

import inspect
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

from app.services.supabase_client import supabase
from app.services.postgres_client import postgres
from app.services.data_chat_action_registry import get_action_capabilities

logger = logging.getLogger(__name__)

MAX_RESULT_CHARS = 6000  # Cap tool results to avoid context overflow

DEFAULT_ONBOARDING_PHASES = [
    {"key": "intake", "label": "Intake"},
    {"key": "business_setup", "label": "Business Setup"},
    {"key": "external_setup", "label": "External Setup"},
    {"key": "setter_setup", "label": "Setter Setup"},
    {"key": "knowledge_and_outreach", "label": "Knowledge & Outreach"},
    {"key": "testing_and_validation", "label": "Testing & Validation"},
    {"key": "launch", "label": "Launch"},
]


# ── Tool Definitions (OpenAI function-calling format) ──

QUERY_LEADS_DEF = {
    "type": "function",
    "function": {
        "name": "query_leads",
        "description": "Query leads data — counts, lists, sources, qualification status, activity. Use for questions about lead volume, conversion, sources, or specific leads. When comparing entities, call this once per entity with the entity_id parameter.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Query a SINGLE entity by ID. Use when comparing entities one at a time. Omit to query all scoped entities."},
                "date_start": {"type": "string", "description": "Start date ISO 8601 (default: 30 days ago)"},
                "date_end": {"type": "string", "description": "End date ISO 8601 (default: now)"},
                "search": {"type": "string", "description": "Text search across name, phone, email (e.g. 'John', '+1702', 'gmail.com')"},
                "sources": {"type": "array", "items": {"type": "string"}, "description": "Filter by lead source. Common values: 'Meta Ads', 'Instagram', 'Facebook', 'Website', 'Referral', 'Inbound Call', 'Staff Call', 'Missed Call'. If unsure of exact values, call get_dashboard_stats first to see the actual source breakdown."},
                "reply_channels": {"type": "array", "items": {"type": "string"}, "description": "Filter by reply channel: 'SMS', 'Email', 'Instagram', 'Facebook', 'WhatsApp'"},
                "qualification_statuses": {"type": "array", "items": {"type": "string"}, "description": "Filter: 'Qualified', 'Not Qualified', 'Pending'"},
                "page_size": {"type": "integer", "description": "Results per page (default 20, max 50)"},
                "page": {"type": "integer", "description": "Page number (default 1)"},
                "sort_field": {"type": "string", "description": "Sort by: 'created_at', 'last_reply_datetime'"},
                "sort_dir": {"type": "string", "description": "'desc' or 'asc' (default desc)"},
                "include_stats": {"type": "boolean", "description": "Include aggregate stats (default true)"},
            },
        },
    },
}

QUERY_BOOKINGS_DEF = {
    "type": "function",
    "function": {
        "name": "query_bookings",
        "description": "Query bookings — counts, channels, AI-booked vs human, after-hours, revenue estimates. When comparing entities, call once per entity with entity_id.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Query a SINGLE entity. Omit to query all scoped entities."},
                "date_start": {"type": "string", "description": "Start date ISO 8601"},
                "date_end": {"type": "string", "description": "End date ISO 8601"},
                "search": {"type": "string", "description": "Text search across booking data"},
                "statuses": {"type": "array", "items": {"type": "string"}, "description": "Filter: 'confirmed', 'cancelled', 'no_show', 'completed'"},
                "channels": {"type": "array", "items": {"type": "string"}, "description": "Filter by booking channel"},
                "sources": {"type": "array", "items": {"type": "string"}, "description": "Filter by lead source"},
                "after_hours": {"type": "boolean", "description": "Filter to only after-hours bookings"},
                "page_size": {"type": "integer", "description": "Results per page (default 20, max 50)"},
                "page": {"type": "integer", "description": "Page number (default 1)"},
                "include_stats": {"type": "boolean", "description": "Include aggregate stats (default true)"},
            },
        },
    },
}

QUERY_CALL_LOGS_DEF = {
    "type": "function",
    "function": {
        "name": "query_call_logs",
        "description": "Query call logs — direction, outcomes, duration, sentiment, transcripts. Transcripts excluded by default — set include_transcripts=true only when asked. When comparing entities, call once per entity with entity_id.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Query a SINGLE entity. Omit to query all scoped entities."},
                "date_start": {"type": "string", "description": "Start date ISO 8601"},
                "date_end": {"type": "string", "description": "End date ISO 8601"},
                "search": {"type": "string", "description": "Text search across call data"},
                "direction": {"type": "string", "description": "Filter: 'inbound' or 'outbound'"},
                "statuses": {"type": "array", "items": {"type": "string"}, "description": "Filter by call status"},
                "outcomes": {"type": "array", "items": {"type": "string"}, "description": "Filter by outcome"},
                "sentiment": {"type": "array", "items": {"type": "string"}, "description": "Filter by sentiment: 'positive', 'negative', 'neutral'"},
                "include_transcripts": {"type": "boolean", "description": "Include full transcripts in results (default false — transcripts are large). Only set true when user specifically asks for transcript content."},
                "page_size": {"type": "integer", "description": "Results per page (default 20, max 50)"},
                "page": {"type": "integer", "description": "Page number (default 1)"},
            },
        },
    },
}

GET_DASHBOARD_STATS_DEF = {
    "type": "function",
    "function": {
        "name": "get_dashboard_stats",
        "description": "Get dashboard statistics — lead volume, booking rates, conversion rates, channel/source breakdown, plus billing attribution (ai_direct_count, ai_assisted_count, manual_count). Best for overview and billing. When comparing entities, call once per entity with entity_id.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Query a SINGLE entity. Omit to query all scoped entities."},
                "date_start": {"type": "string", "description": "Start date ISO 8601 (default: 30 days ago)"},
                "date_end": {"type": "string", "description": "End date ISO 8601 (default: now)"},
            },
        },
    },
}

GET_TIME_SERIES_DEF = {
    "type": "function",
    "function": {
        "name": "get_time_series",
        "description": "Get daily time series data for leads, bookings, and other metrics. For trend analysis and comparisons. When comparing entities, call once per entity with entity_id.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Query a SINGLE entity. Omit to query all scoped entities."},
                "date_start": {"type": "string", "description": "Start date ISO 8601"},
                "date_end": {"type": "string", "description": "End date ISO 8601"},
                "timezone": {"type": "string", "description": "Timezone (default: America/Chicago)"},
            },
        },
    },
}

GET_AI_COSTS_DEF = {
    "type": "function",
    "function": {
        "name": "get_ai_costs",
        "description": "Get AI cost breakdown — OpenRouter costs vs direct provider costs. Use for cost analysis and budgeting questions.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Query a SINGLE entity. Omit to query all scoped entities."},
                "date_start": {"type": "string", "description": "Start date ISO 8601"},
                "date_end": {"type": "string", "description": "End date ISO 8601"},
            },
        },
    },
}

QUERY_WORKFLOW_RUNS_DEF = {
    "type": "function",
    "function": {
        "name": "query_workflow_runs",
        "description": "Query AI workflow execution logs — errors, costs, types, durations. Use for debugging, error investigation, performance analysis.",
        "parameters": {
            "type": "object",
            "properties": {
                "date_start": {"type": "string", "description": "Start date ISO 8601"},
                "date_end": {"type": "string", "description": "End date ISO 8601"},
                "search": {"type": "string", "description": "Text search across workflow run data"},
                "workflow_types": {"type": "array", "items": {"type": "string"}, "description": "Filter: 'reply', 'followup', 'reactivation', 'missed_call', 'post_appointment', etc."},
                "statuses": {"type": "array", "items": {"type": "string"}, "description": "Filter: 'success', 'error', 'skipped'"},
                "page_size": {"type": "integer", "description": "Results per page (default 20, max 50)"},
                "page": {"type": "integer", "description": "Page number (default 1)"},
            },
        },
    },
}

GET_REACTIVATION_STATS_DEF = {
    "type": "function",
    "function": {
        "name": "get_reactivation_stats",
        "description": "Get auto-reactivation campaign performance — reactivation attempts, responses, bookings from reactivation.",
        "parameters": {
            "type": "object",
            "properties": {
                "date_start": {"type": "string", "description": "Start date ISO 8601"},
                "date_end": {"type": "string", "description": "End date ISO 8601"},
            },
        },
    },
}

SEARCH_KNOWLEDGE_BASE_DEF = {
    "type": "function",
    "function": {
        "name": "search_knowledge_base",
        "description": "Search knowledge base articles by keyword or title. Use for questions about what info the AI has, client FAQ, services, pricing.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search keyword or phrase"},
                "entity_id": {"type": "string", "description": "Specific entity ID (required for global scope)"},
            },
            "required": ["query"],
        },
    },
}

GET_CHAT_HISTORY_DEF = {
    "type": "function",
    "function": {
        "name": "get_chat_history",
        "description": "Get actual conversation messages between the AI and a lead. You can pass a contact name and it will resolve to the correct conversation. Each message has a 'source' field: 'AI' = AI reply, 'follow_up' = follow-up, 'drip' = drip/outreach sequence, 'missed_call_textback' or 'missed_call' = missed call text-back, 'post_appointment' = post-appointment, 'manual' = human staff typed it (NOT AI), 'workflow' = GHL campaign, 'lead_reply' = lead message.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Entity ID (required — determines which chat table to query)"},
                "contact_name": {"type": "string", "description": "Contact/lead name to look up (e.g. 'Rosa Flores'). Will resolve to the correct GHL contact ID."},
                "contact_id": {"type": "string", "description": "GHL contact ID if already known. If not provided, use contact_name instead."},
                "limit": {"type": "integer", "description": "Max messages to return (default 30)"},
            },
            "required": ["entity_id"],
        },
    },
}

SEARCH_CHAT_MESSAGES_DEF = {
    "type": "function",
    "function": {
        "name": "search_chat_messages",
        "description": """Search across ALL chat messages for a keyword or phrase. Searches message content across all conversations. Can also count messages by source type without a keyword.

Message roles and sources (EXACT values from the system — use these for source_filter):
- role='human' = LEAD messages. source is always 'lead_reply'.
- role='ai' = AI or STAFF messages. Use 'source' to distinguish:
  - source='AI' = AI reply agent (main automated response to a lead)
  - source='follow_up' = AI follow-up message (scheduled after no reply)
  - source='drip' = outreach drip sequence (automated initial outreach)
  - source='missed_call_textback' = missed call text-back
  - source='missed_call' = missed call text-back (legacy variant)
  - source='post_appointment' = post-appointment outreach
  - source='manual' = HUMAN STAFF typed this (not AI)
  - source='workflow' = GHL workflow message (campaign, reminder)

When user asks about 'AI messages', filter role='ai' but EXCLUDE source='manual' (staff) and source='workflow' (campaigns).
When user asks about 'staff messages', filter source='manual'.
When user asks about 'lead messages', filter role='human'.
When user asks about 'drip' or 'outreach', filter source='drip'.
When user asks about 'follow-ups', filter source='follow_up'.""",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Entity ID (required)"},
                "query": {"type": "string", "description": "Search keyword or phrase to find in messages. Optional — omit to count/filter by role or source only."},
                "role_filter": {"type": "string", "description": "Filter by role: 'human' (lead messages only), 'ai' (AI + staff messages), or omit for all"},
                "source_filter": {"type": "string", "description": "Filter by exact source value: 'AI', 'follow_up', 'drip', 'missed_call_textback', 'missed_call', 'post_appointment', 'manual', 'workflow', 'lead_reply'."},
                "limit": {"type": "integer", "description": "Max matching messages to return (default 20, max 50)"},
            },
            "required": ["entity_id"],
        },
    },
}

QUERY_SCHEDULED_MESSAGES_DEF = {
    "type": "function",
    "function": {
        "name": "query_scheduled_messages",
        "description": "Query scheduled messages — follow-ups, drip sequences, reactivations waiting to be sent or already fired. Shows status (pending, fired, cancelled), timing, and results.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Entity ID (required for scoped queries, optional for global)"},
                "status": {"type": "string", "description": "Filter: 'pending', 'fired', 'cancelled'"},
                "message_type": {"type": "string", "description": "Filter: 'followup', 'drip', 'reactivation', 'human_takeover'"},
                "limit": {"type": "integer", "description": "Max results (default 20, max 50)"},
            },
        },
    },
}

QUERY_OUTREACH_TEMPLATES_DEF = {
    "type": "function",
    "function": {
        "name": "query_outreach_templates",
        "description": "Query outreach templates — drip sequence configurations with positions, timing, and calendar assignments. Shows what templates exist and their settings.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Entity ID (required for scoped queries)"},
            },
        },
    },
}

GET_OUTREACH_PERFORMANCE_DEF = {
    "type": "function",
    "function": {
        "name": "get_outreach_performance",
        "description": "Get outreach template performance — reply rates, booking rates, A/B test results per template. Shows how well each drip sequence is converting. Use for questions about outreach effectiveness, which template works best, or A/B test winners.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Entity ID (required)"},
            },
            "required": ["entity_id"],
        },
    },
}

GET_SETTER_CONFIG_DEF = {
    "type": "function",
    "function": {
        "name": "get_setter_config",
        "description": """Get the AI setter configuration for an entity. Shows all the settings that control how the AI behaves — tone, booking style, follow-up rules, transfer triggers, etc. Use this when:
- The user complains about AI behavior (too pushy, too passive, unprofessional, etc.)
- The user asks what settings are available to change
- The user wants to understand why the AI acts a certain way
Returns a readable summary of all config sections with current values. Admin only.""",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Entity ID (required)"},
                "section": {"type": "string", "description": "Optional: get only a specific section. Values: 'persona', 'conversation', 'services', 'booking', 'transfer', 'security', 'follow_up', 'missed_call', 'reactivation', 'ai_models'. Omit for full config overview."},
            },
            "required": ["entity_id"],
        },
    },
}

GET_ENTITY_INFO_DEF = {
    "type": "function",
    "function": {
        "name": "get_entity_info",
        "description": "Get entity details — name, type, timezone, journey stage, billing config, booking value, features enabled. Use for configuration questions.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Entity ID to look up"},
            },
            "required": ["entity_id"],
        },
    },
}

GET_ONBOARDING_TEMPLATE_DEF = {
    "type": "function",
    "function": {
        "name": "get_onboarding_template",
        "description": "Get the active tenant onboarding template. Use for questions about onboarding phases, required steps, system checks, and the default tenant onboarding process. Admin only.",
        "parameters": {
            "type": "object",
            "properties": {
                "phase_key": {"type": "string", "description": "Optional phase to focus on, like 'setter_setup' or 'launch'."},
                "include_instructions": {"type": "boolean", "description": "Include full item instructions. Default false."},
                "include_actions": {"type": "boolean", "description": "Include action payload details. Default false."},
                "include_resources": {"type": "boolean", "description": "Include resource payload details. Default false."},
                "include_inactive": {"type": "boolean", "description": "Include inactive template items. Default false."},
            },
        },
    },
}

QUERY_ONBOARDING_QUEUE_DEF = {
    "type": "function",
    "function": {
        "name": "query_onboarding_queue",
        "description": "Query tenant onboarding progress across clients. Use for questions like which clients are still onboarding, blocked, complete, or out of sync with the latest template. Admin only.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Optional single client entity ID."},
                "status": {"type": "string", "description": "Optional onboarding status filter: 'in_progress' or 'completed'."},
                "only_blocked": {"type": "boolean", "description": "Only return clients with blocked items."},
                "only_out_of_sync": {"type": "boolean", "description": "Only return clients whose onboarding snapshot differs from the active tenant template."},
                "limit": {"type": "integer", "description": "Max rows to return (default 25, max 100)."},
            },
        },
    },
}

GET_ENTITY_ONBOARDING_DETAIL_DEF = {
    "type": "function",
    "function": {
        "name": "get_entity_onboarding_detail",
        "description": "Get detailed onboarding progress for one client, including phase progress, blocked items, ready system checks, and what is left to finish. Admin only.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Client entity ID."},
                "phase_key": {"type": "string", "description": "Optional phase to focus on."},
                "include_notes": {"type": "boolean", "description": "Include internal onboarding notes. Default false."},
                "include_completed_items": {"type": "boolean", "description": "Include completed and skipped items. Default false."},
                "include_instructions": {"type": "boolean", "description": "Include full item instructions. Default false."},
                "include_actions": {"type": "boolean", "description": "Include action payload details. Default false."},
                "include_resources": {"type": "boolean", "description": "Include resource payload details. Default false."},
            },
            "required": ["entity_id"],
        },
    },
}

SEARCH_PLATFORM_KNOWLEDGE_DEF = {
    "type": "function",
    "function": {
        "name": "search_platform_knowledge",
        "description": "Search the internal Ask AI platform knowledge base. Use this for how-to questions, where-to-edit questions, page explanations, onboarding guidance, platform workflows, and portal usage questions.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What the user wants help with, like 'bot persona', 'tenant defaults', 'onboarding', or 'request change'."},
                "page_key": {"type": "string", "description": "Optional exact page or concept key if known."},
                "include_content": {"type": "boolean", "description": "Include full article content. Default true for guide questions."},
                "include_media": {"type": "boolean", "description": "Include linked screenshots or guide images when available. Default true."},
                "limit": {"type": "integer", "description": "Max articles to return (default 3, max 6)."},
            },
            "required": ["query"],
        },
    },
}

DRAFT_CHANGE_REQUEST_DEF = {
    "type": "function",
    "function": {
        "name": "draft_change_request",
        "description": "Draft a portal-safe change request when the user wants something changed but cannot edit it directly. Use this especially for portal users asking to update settings, prompts, knowledge base content, or outreach.",
        "parameters": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Best topic bucket: 'Knowledge Base', 'Setters', 'Outreach', 'Analytics', or 'Other'."},
                "requested_change": {"type": "string", "description": "Clear plain-language description of the exact change the user wants."},
                "entity_id": {"type": "string", "description": "Optional entity ID for client-scoped requests."},
            },
            "required": ["topic", "requested_change"],
        },
    },
}

GET_ACTION_CAPABILITIES_DEF = {
    "type": "function",
    "function": {
        "name": "get_action_capabilities",
        "description": "Get the exact Ask AI admin edit capability whitelist. Use this before proposing edits so you know what Ask AI can and cannot change.",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
}

GET_SETTER_SNAPSHOT_DEF = {
    "type": "function",
    "function": {
        "name": "get_setter_snapshot",
        "description": "Get the current structured AI Setter config for a client. Use this before proposing an admin edit so you know the live values. Admin only.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Client entity ID."},
                "setter_key": {"type": "string", "description": "Optional setter key. Defaults to the client's default setter."},
                "surface": {"type": "string", "description": "Optional surface filter: 'bot_persona', 'services', 'conversation', 'booking', 'transfer', 'security', 'ai_models', 'missed_call_textback', 'auto_reactivation', 'case_studies'."},
            },
            "required": ["entity_id"],
        },
    },
}

GET_CLIENT_SETTINGS_SNAPSHOT_DEF = {
    "type": "function",
    "function": {
        "name": "get_client_settings_snapshot",
        "description": "Get the current safe Client Settings values for a client. Use this before proposing an admin edit so you know the live values. Admin only.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Client entity ID."},
            },
            "required": ["entity_id"],
        },
    },
}

GET_OUTREACH_TEMPLATE_SNAPSHOT_DEF = {
    "type": "function",
    "function": {
        "name": "get_outreach_template_snapshot",
        "description": "Get the current outreach template and reminder template config for a client, including editable position content. Use this before proposing admin edits for lead sequences or appointment reminders. Admin only.",
        "parameters": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Client entity ID."},
            },
            "required": ["entity_id"],
        },
    },
}


# ── All tool definitions ──

ALL_TOOL_DEFS = [
    QUERY_LEADS_DEF,
    QUERY_BOOKINGS_DEF,
    QUERY_CALL_LOGS_DEF,
    GET_DASHBOARD_STATS_DEF,
    GET_TIME_SERIES_DEF,
    GET_AI_COSTS_DEF,
    QUERY_WORKFLOW_RUNS_DEF,
    GET_REACTIVATION_STATS_DEF,
    SEARCH_KNOWLEDGE_BASE_DEF,
    GET_CHAT_HISTORY_DEF,
    SEARCH_CHAT_MESSAGES_DEF,
    QUERY_SCHEDULED_MESSAGES_DEF,
    QUERY_OUTREACH_TEMPLATES_DEF,
    GET_OUTREACH_PERFORMANCE_DEF,
    SEARCH_PLATFORM_KNOWLEDGE_DEF,
    DRAFT_CHANGE_REQUEST_DEF,
    GET_ACTION_CAPABILITIES_DEF,
    GET_SETTER_SNAPSHOT_DEF,
    GET_CLIENT_SETTINGS_SNAPSHOT_DEF,
    GET_OUTREACH_TEMPLATE_SNAPSHOT_DEF,
    GET_ONBOARDING_TEMPLATE_DEF,
    QUERY_ONBOARDING_QUEUE_DEF,
    GET_ENTITY_ONBOARDING_DETAIL_DEF,
    GET_SETTER_CONFIG_DEF,
    GET_ENTITY_INFO_DEF,
]

# Portal: no AI costs, no entity config, no workflow runs, no reactivation stats.
PORTAL_TOOL_DEFS = [
    QUERY_LEADS_DEF,
    QUERY_BOOKINGS_DEF,
    QUERY_CALL_LOGS_DEF,
    GET_DASHBOARD_STATS_DEF,
    GET_TIME_SERIES_DEF,
    SEARCH_KNOWLEDGE_BASE_DEF,
    GET_CHAT_HISTORY_DEF,
    SEARCH_CHAT_MESSAGES_DEF,
    QUERY_SCHEDULED_MESSAGES_DEF,
    QUERY_OUTREACH_TEMPLATES_DEF,
    GET_OUTREACH_PERFORMANCE_DEF,
    SEARCH_PLATFORM_KNOWLEDGE_DEF,
    DRAFT_CHANGE_REQUEST_DEF,
    GET_ACTION_CAPABILITIES_DEF,
    GET_SETTER_SNAPSHOT_DEF,
    GET_CLIENT_SETTINGS_SNAPSHOT_DEF,
]


# ── Helpers ──

def _default_dates(args: dict, tz_name: str = "UTC") -> tuple[str | None, str | None]:
    """Return (start, end) ISO dates.

    When a text search is active and no dates are specified, returns (None, None)
    so the search spans all time. Otherwise defaults to last 30 days.
    """
    from zoneinfo import ZoneInfo
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    now = datetime.now(tz)

    has_explicit_dates = args.get("date_start") or args.get("date_end")
    has_search = bool(args.get("search"))

    end = args.get("date_end") or now.isoformat()
    # If searching by name/text and no explicit dates, don't restrict the date range
    if not has_explicit_dates and has_search:
        return None, None
    start = args.get("date_start") or (now - timedelta(days=30)).isoformat()
    return start, end


def _truncate(data: Any) -> str:
    """Serialize and truncate tool result to stay within context budget."""
    text = json.dumps(data, default=str)
    if len(text) > MAX_RESULT_CHARS:
        text = text[:MAX_RESULT_CHARS] + "... (truncated)"
    return text


async def _get_billing_attribution(
    entity_ids: list[str],
    start: str | None,
    end: str | None,
) -> dict[str, int]:
    """Return exact billing attribution counts for the scoped entities.

    booking_type is the source of truth when present. For older rows with NULL
    booking_type, fall back to ai_booked_directly + lead.last_reply_datetime.
    Reactivation bookings are excluded to mirror main dashboard stats.
    """
    if not entity_ids or not start or not end:
        return {
            "ai_direct_count": 0,
            "ai_assisted_count": 0,
            "manual_count": 0,
            "total_bookings": 0,
        }

    # Normalize dates to ensure consistent boundary handling
    # Use gte start (inclusive) and lt end (exclusive) to avoid double-counting at boundaries
    norm_start = start[:10] if start else start  # "2026-03-22"
    norm_end = end[:10] if end else end            # "2026-04-22"

    path = (
        "/bookings"
        "?select=entity_id,lead_id,ai_booked_directly,booking_type,leads(source,last_reply_datetime)"
        f"&entity_id=in.({','.join(entity_ids)})"
        f"&created_at=gte.{norm_start}"
        f"&created_at=lt.{norm_end}"
        "&lead_id=not.is.null"
        "&limit=10000"
    )

    try:
        rows = await supabase.rest_get("main", path)
    except Exception:
        return {
            "ai_direct_count": 0,
            "ai_assisted_count": 0,
            "manual_count": 0,
            "total_bookings": 0,
        }

    ai_direct = 0
    ai_assisted = 0
    manual = 0

    for row in rows:
        lead = row.get("leads") or {}
        if isinstance(lead, list):
            lead = lead[0] if lead else {}
        source = (lead or {}).get("source")
        if source in ("Auto Reactivation", "Reactivation Campaign"):
            continue

        booking_type = row.get("booking_type")
        if booking_type == "ai_direct":
            ai_direct += 1
            continue
        if booking_type == "ai_assisted":
            ai_assisted += 1
            continue
        if booking_type == "manual":
            manual += 1
            continue

        if row.get("ai_booked_directly") is True:
            ai_direct += 1
        elif lead.get("last_reply_datetime"):
            ai_assisted += 1
        else:
            manual += 1

    return {
        "ai_direct_count": ai_direct,
        "ai_assisted_count": ai_assisted,
        "manual_count": manual,
        "total_bookings": ai_direct + ai_assisted + manual,
    }


def _safe_list(val: Any) -> list | None:
    """Convert to list or None for RPC params."""
    if isinstance(val, list) and len(val) > 0:
        return val
    return None


def _resolve_entity_ids(args: dict, entity_ids: list[str]) -> list[str]:
    """If args has entity_id (singular), use just that one. Otherwise use the full scoped list."""
    single = args.get("entity_id")
    if single and single in entity_ids:
        return [single]
    return entity_ids


def _parse_phase_config(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        return [dict(entry) for entry in DEFAULT_ONBOARDING_PHASES]

    parsed: list[dict[str, str]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        key = entry.get("key")
        label = entry.get("label")
        if isinstance(key, str) and key and isinstance(label, str) and label:
            parsed.append({"key": key, "label": label})
    return parsed or [dict(entry) for entry in DEFAULT_ONBOARDING_PHASES]


def _parse_actions(value: Any) -> list[dict[str, Any]]:
    return [entry for entry in value if isinstance(entry, dict)] if isinstance(value, list) else []


def _parse_resources(value: Any) -> list[dict[str, Any]]:
    return [entry for entry in value if isinstance(entry, dict)] if isinstance(value, list) else []


def _get_default_setter(system_config: Any) -> dict[str, Any]:
    if not isinstance(system_config, dict):
        return {}
    setters = system_config.get("setters")
    if not isinstance(setters, dict) or not setters:
        return {}
    for setter in setters.values():
        if isinstance(setter, dict) and setter.get("is_default"):
            return setter
    first_key = next(iter(setters.keys()), None)
    if not first_key:
        return {}
    first = setters.get(first_key)
    return first if isinstance(first, dict) else {}


def _count_enabled_bot_persona_sections(bot_persona: Any) -> int:
    if not isinstance(bot_persona, dict):
        return 0
    sections = bot_persona.get("sections")
    if not isinstance(sections, dict):
        return 0

    count = 0
    if isinstance(sections.get("identity"), dict):
        count += 1
    if isinstance(sections.get("ai_disclosure"), dict):
        count += 1

    tone = sections.get("tone")
    if isinstance(tone, dict) and isinstance(tone.get("traits"), list) and len(tone.get("traits")) > 0:
        count += 1

    if isinstance(sections.get("name_usage"), dict):
        count += 1
    if isinstance(sections.get("punctuation_style"), dict):
        count += 1

    casual_language = sections.get("casual_language")
    if isinstance(casual_language, dict) and casual_language.get("selected") not in (None, "", "off"):
        count += 1

    toggle_keys = [
        "humor", "emojis", "message_length", "typos", "skip_greetings",
        "be_direct", "mirror_style", "stories_examples", "sarcasm",
        "light_swearing", "sentence_fragments", "validate_feelings",
        "hype_celebrate", "remember_details",
    ]
    for key in toggle_keys:
        section = sections.get(key)
        if isinstance(section, dict) and section.get("enabled") is True:
            count += 1

    banned_phrases = sections.get("banned_phrases")
    if isinstance(banned_phrases, dict) and isinstance(banned_phrases.get("items"), list) and len(banned_phrases.get("items")) > 0:
        count += 1

    custom_sections = bot_persona.get("custom_sections")
    if isinstance(custom_sections, list):
        count += sum(1 for entry in custom_sections if isinstance(entry, dict) and entry.get("enabled") is True)

    return count


def _evaluate_onboarding_check(
    check_key: str | None,
    entity: dict[str, Any],
    knowledge_base_count: int = 0,
    outreach_count: int = 0,
) -> bool:
    if not check_key:
        return False

    setter = _get_default_setter(entity.get("system_config"))
    bot_persona = setter.get("bot_persona") if isinstance(setter, dict) else {}
    booking = setter.get("booking") if isinstance(setter, dict) else {}
    services = setter.get("services") if isinstance(setter, dict) else {}
    notifications = ((entity.get("system_config") or {}).get("notifications") or {}) if isinstance(entity.get("system_config"), dict) else {}

    calendars = booking.get("calendars") if isinstance(booking, dict) else []
    configured_services = services.get("services") if isinstance(services, dict) else []
    recipients = notifications.get("recipients") if isinstance(notifications, dict) else []

    if check_key == "has_entity_basics":
        return bool(
            entity.get("name")
            and entity.get("timezone")
            and (entity.get("contact_name") or entity.get("contact_email") or entity.get("contact_phone"))
        )
    if check_key == "has_ghl_credentials":
        return bool(entity.get("ghl_location_id") and entity.get("ghl_api_key"))
    if check_key == "has_business_phone":
        return bool(entity.get("business_phone"))
    if check_key == "has_calendar":
        return any(isinstance(calendar, dict) and calendar.get("enabled", True) is not False and calendar.get("id") for calendar in calendars or [])
    if check_key == "has_bot_persona":
        return _count_enabled_bot_persona_sections(bot_persona) > 0
    if check_key == "has_services":
        return isinstance(configured_services, list) and len(configured_services) > 0
    if check_key == "has_outreach_templates":
        return outreach_count > 0
    if check_key == "has_kb_articles":
        return knowledge_base_count > 0
    if check_key == "has_notification_recipients":
        return isinstance(recipients, list) and len(recipients) > 0
    return False


def _is_onboarding_item_complete(item: dict[str, Any]) -> bool:
    return item.get("status") in ("done", "skipped")


def _is_onboarding_item_ready(
    item: dict[str, Any],
    entity: dict[str, Any],
    knowledge_base_count: int = 0,
    outreach_count: int = 0,
) -> bool:
    if item.get("completion_mode") != "system_check":
        return False
    if _is_onboarding_item_complete(item):
        return False
    return _evaluate_onboarding_check(
        item.get("completion_check_key"),
        entity,
        knowledge_base_count=knowledge_base_count,
        outreach_count=outreach_count,
    )


def _get_onboarding_completion_label(
    item: dict[str, Any],
    entity: dict[str, Any],
    knowledge_base_count: int = 0,
    outreach_count: int = 0,
) -> str:
    if item.get("status") == "blocked":
        return "Blocked"
    if item.get("status") == "skipped":
        return "Skipped"
    if item.get("completion_mode") == "info_only":
        return "Reference"
    if _is_onboarding_item_complete(item):
        return "Done"
    if _is_onboarding_item_ready(item, entity, knowledge_base_count=knowledge_base_count, outreach_count=outreach_count):
        return "Ready"
    if item.get("status") == "in_progress":
        return "In Progress"
    return "To Do"


def _summarize_actions(actions: Any, include_details: bool = False) -> list[dict[str, Any]]:
    summarized: list[dict[str, Any]] = []
    for action in _parse_actions(actions):
        entry: dict[str, Any] = {
            "type": action.get("type"),
            "label": action.get("label"),
        }
        if include_details:
            for key in ("dialogKey", "tabKey", "panelKey", "href"):
                if action.get(key) not in (None, ""):
                    entry[key] = action.get(key)
        summarized.append(entry)
    return summarized


def _summarize_resources(resources: Any, include_details: bool = False) -> list[dict[str, Any]]:
    summarized: list[dict[str, Any]] = []
    for resource in _parse_resources(resources):
        entry: dict[str, Any] = {
            "type": resource.get("type"),
            "label": resource.get("label"),
        }
        if include_details:
            for key in ("href", "path"):
                if resource.get(key) not in (None, ""):
                    entry[key] = resource.get(key)
        summarized.append(entry)
    return summarized


def _summarize_template_item(
    item: dict[str, Any],
    include_instructions: bool = False,
    include_actions: bool = False,
    include_resources: bool = False,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "title": item.get("title"),
        "description": item.get("description"),
        "section_key": item.get("section_key"),
        "required": bool(item.get("is_required")),
        "completion_mode": item.get("completion_mode"),
        "completion_check_key": item.get("completion_check_key"),
        "action_labels": [entry.get("label") for entry in _summarize_actions(item.get("actions")) if entry.get("label")],
        "resource_labels": [entry.get("label") for entry in _summarize_resources(item.get("resources")) if entry.get("label")],
    }
    if include_instructions and item.get("instructions"):
        summary["instructions"] = item.get("instructions")
    if include_actions:
        summary["actions"] = _summarize_actions(item.get("actions"), include_details=True)
    if include_resources:
        summary["resources"] = _summarize_resources(item.get("resources"), include_details=True)
    if item.get("is_active") is not None:
        summary["is_active"] = bool(item.get("is_active"))
    return summary


def _summarize_entity_onboarding_item(
    item: dict[str, Any],
    entity: dict[str, Any],
    knowledge_base_count: int = 0,
    outreach_count: int = 0,
    include_notes: bool = False,
    include_instructions: bool = False,
    include_actions: bool = False,
    include_resources: bool = False,
) -> dict[str, Any]:
    ready = _is_onboarding_item_ready(
        item,
        entity,
        knowledge_base_count=knowledge_base_count,
        outreach_count=outreach_count,
    )
    summary: dict[str, Any] = {
        "title": item.get("title"),
        "description": item.get("description"),
        "section_key": item.get("section_key"),
        "status": item.get("status"),
        "completion_label": _get_onboarding_completion_label(
            item,
            entity,
            knowledge_base_count=knowledge_base_count,
            outreach_count=outreach_count,
        ),
        "required": bool(item.get("is_required")),
        "completion_mode": item.get("completion_mode"),
        "completion_check_key": item.get("completion_check_key"),
        "ready": ready,
        "is_custom": bool(item.get("is_custom")),
        "action_labels": [entry.get("label") for entry in _summarize_actions(item.get("actions")) if entry.get("label")],
        "resource_labels": [entry.get("label") for entry in _summarize_resources(item.get("resources")) if entry.get("label")],
    }
    if include_notes and item.get("notes"):
        summary["notes"] = item.get("notes")
    if include_instructions and item.get("instructions"):
        summary["instructions"] = item.get("instructions")
    if include_actions:
        summary["actions"] = _summarize_actions(item.get("actions"), include_details=True)
    if include_resources:
        summary["resources"] = _summarize_resources(item.get("resources"), include_details=True)
    return summary


async def _get_active_onboarding_template(tenant_id: str) -> dict[str, Any] | None:
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/onboarding_templates",
        params={
            "tenant_id": f"eq.{tenant_id}",
            "is_active": "eq.true",
            "select": "id,tenant_id,name,version,phase_config,created_at,updated_at",
            "limit": "1",
        },
        label="data_chat_onboarding_template",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []
    return rows[0] if rows else None


async def _get_onboarding_template_items(template_id: str, include_inactive: bool = False) -> list[dict[str, Any]]:
    params = {
        "template_id": f"eq.{template_id}",
        "select": "id,template_id,tenant_id,phase_key,section_key,title,description,instructions,actions,resources,completion_mode,completion_check_key,is_required,sort_order,is_active,created_at,updated_at",
        "order": "sort_order.asc",
        "limit": "1000",
    }
    if not include_inactive:
        params["is_active"] = "eq.true"
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/onboarding_template_items",
        params=params,
        label="data_chat_onboarding_template_items",
    )
    return resp.json() if isinstance(resp.json(), list) else []


async def _get_onboarding_entities(
    tenant_id: str,
    entity_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "tenant_id": f"eq.{tenant_id}",
        "entity_type": "eq.client",
        "status": "eq.active",
        "select": "id,name,entity_type,journey_stage,status,contact_name,contact_email,contact_phone,timezone,business_phone,ghl_location_id,ghl_api_key,system_config,created_at",
        "order": "name.asc",
        "limit": "1000",
    }
    if entity_ids:
        params["id"] = f"in.({','.join(entity_ids)})"
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/entities",
        params=params,
        label="data_chat_onboarding_entities",
    )
    return resp.json() if isinstance(resp.json(), list) else []


async def _get_entity_count_map(table: str, entity_ids: list[str]) -> dict[str, int]:
    if not entity_ids:
        return {}
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        f"/{table}",
        params={
            "entity_id": f"in.({','.join(entity_ids)})",
            "select": "entity_id",
            "limit": "10000",
        },
        label=f"data_chat_{table}_entity_counts",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []
    counts: dict[str, int] = {}
    for row in rows:
        entity_id = row.get("entity_id")
        if entity_id:
            counts[entity_id] = counts.get(entity_id, 0) + 1
    return counts


async def _get_entity_onboardings(
    tenant_id: str,
    entity_id: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "tenant_id": f"eq.{tenant_id}",
        "select": "id,tenant_id,entity_id,template_id,template_version,phase_config,status,started_at,completed_at,completed_by,completion_notes,completion_stage_choice,created_at,updated_at",
        "order": "updated_at.desc",
        "limit": "1000",
    }
    if entity_id:
        params["entity_id"] = f"eq.{entity_id}"
    if status:
        params["status"] = f"eq.{status}"
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/entity_onboardings",
        params=params,
        label="data_chat_entity_onboardings",
    )
    return resp.json() if isinstance(resp.json(), list) else []


async def _get_entity_onboarding_items(
    onboarding_ids: list[str],
    full: bool = False,
) -> list[dict[str, Any]]:
    if not onboarding_ids:
        return []
    select = (
        "id,onboarding_id,tenant_id,entity_id,template_item_id,phase_key,section_key,title,description,"
        "instructions,actions,resources,completion_mode,completion_check_key,is_required,sort_order,status,"
        "notes,completed_at,completed_by,is_custom,source_hash,created_at,updated_at"
    ) if full else (
        "id,onboarding_id,entity_id,phase_key,section_key,title,description,actions,resources,completion_mode,"
        "completion_check_key,is_required,sort_order,status,is_custom,updated_at"
    )
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/entity_onboarding_items",
        params={
            "onboarding_id": f"in.({','.join(onboarding_ids)})",
            "select": select,
            "order": "sort_order.asc",
            "limit": "5000",
        },
        label="data_chat_entity_onboarding_items",
    )
    return resp.json() if isinstance(resp.json(), list) else []


def _build_phase_groups(
    items: list[dict[str, Any]],
    phases: list[dict[str, str]],
) -> list[tuple[dict[str, str], list[dict[str, Any]]]]:
    ordered: list[tuple[dict[str, str], list[dict[str, Any]]]] = []
    known = {phase.get("key") for phase in phases}
    for phase in phases:
        phase_items = [item for item in items if item.get("phase_key") == phase.get("key")]
        ordered.append((phase, phase_items))
    extra_phase_keys = sorted({item.get("phase_key") for item in items if item.get("phase_key") not in known})
    for phase_key in extra_phase_keys:
        if not phase_key:
            continue
        ordered.append(({"key": phase_key, "label": phase_key}, [item for item in items if item.get("phase_key") == phase_key]))
    return ordered


def _score_platform_article(article: dict[str, Any], query: str) -> int:
    haystacks = [
        str(article.get("title") or ""),
        str(article.get("summary") or ""),
        str(article.get("content") or ""),
        str(article.get("page_key") or ""),
        str(article.get("route_path") or ""),
        str(article.get("nav_group") or ""),
        " ".join(str(tag) for tag in (article.get("tags") or []) if isinstance(tag, str)),
    ]
    joined = "\n".join(haystacks).lower()
    score = 0
    for term in [part.strip().lower() for part in query.split() if part.strip()]:
        if term in joined:
            score += 1
        if term and term in str(article.get("title") or "").lower():
            score += 3
        if term and term in str(article.get("page_key") or "").lower():
            score += 2
    return score


def _substitute_action_templates(
    action: dict[str, Any],
    entity_id: str | None,
    user_request: str | None = None,
    origin: str = "admin",
) -> dict[str, Any]:
    out = dict(action)
    template_keys = ["href", "href_template", "description", "description_template"]
    for key in template_keys:
        val = out.get(key)
        if isinstance(val, str):
            if entity_id:
                val = val.replace("{entity_id}", entity_id)
            if user_request:
                val = val.replace("{user_request}", user_request)
            out[key] = val
    if out.get("href_template") and not out.get("href"):
        out["href"] = out["href_template"]
    if origin == "portal" and isinstance(out.get("href"), str) and out["href"].startswith("/clients/") and "{entity_id}" not in out["href"]:
        out["href"] = out["href"].replace("/clients/", "/portal/", 1)
    out.pop("href_template", None)
    out.pop("description_template", None)
    return out


async def _get_platform_articles(
    tenant_id: str | None,
    origin: str,
    query: str,
    page_key: str | None = None,
    limit: int = 2,
) -> list[tuple[dict[str, Any], int]]:
    params = {
        "select": "id,tenant_id,slug,title,summary,content,article_type,audience,page_key,route_path,nav_group,tags,related_routes,sort_order",
        "is_active": "eq.true",
        "order": "sort_order.asc,created_at.asc",
        "limit": "200",
    }
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/assistant_knowledge_articles",
        params=params,
        label="data_chat_platform_articles",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []

    allowed_audiences = {"portal", "both"} if origin == "portal" else {"admin", "both"}
    filtered: list[tuple[dict[str, Any], int]] = []
    for row in rows:
        audience = str(row.get("audience") or "admin")
        if audience not in allowed_audiences:
            continue
        row_tenant_id = row.get("tenant_id")
        if row_tenant_id is not None and tenant_id and row_tenant_id != tenant_id:
            continue
        score = _score_platform_article(row, query)
        if page_key and row.get("page_key") == page_key:
            filtered.append((row, max(score, 100)))
            continue
        if score > 0:
            filtered.append((row, score))

    filtered.sort(
        key=lambda item: (
            -item[1],
            0 if item[0].get("page_key") == page_key and page_key else 1,
            int(item[0].get("sort_order") or 0),
        )
    )
    return filtered[:limit]


async def _get_platform_assets(article_ids: list[str], origin: str, tenant_id: str | None) -> dict[str, list[dict[str, Any]]]:
    if not article_ids:
        return {}
    resp = await supabase._request(
        supabase.main_client,
        "GET",
        "/assistant_knowledge_assets",
        params={
            "article_id": f"in.({','.join(article_ids)})",
            "select": "id,article_id,type,url,caption,alt_text,audience,sort_order",
            "is_active": "eq.true",
            "order": "sort_order.asc",
            "limit": "200",
        },
        label="data_chat_platform_assets",
    )
    rows = resp.json() if isinstance(resp.json(), list) else []
    allowed_audiences = {"portal", "both"} if origin == "portal" else {"admin", "portal", "both"}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row.get("audience") or "admin") not in allowed_audiences:
            continue
        article_id = row.get("article_id")
        if article_id:
            grouped.setdefault(article_id, []).append(row)
    return grouped


def _build_platform_ui_payload(
    articles_with_scores: list[tuple[dict[str, Any], int]],
    assets_by_article: dict[str, list[dict[str, Any]]],
    entity_id: str | None,
    user_request: str,
    include_content: bool,
    include_media: bool,
    origin: str,
) -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    media: list[dict[str, Any]] = []
    citations: list[str] = []

    articles = [article for article, _score in articles_with_scores]
    if not articles:
        return {
            "sections": [],
            "actions": [],
            "suggestions": [],
            "media": [],
            "citations": [],
        }

    primary = articles[0]
    related_articles = articles[:2]

    for article in related_articles:
        citations.append(article.get("title") or article.get("slug") or "Guide")
        body = article.get("content") if include_content else article.get("summary")
        steps = [line[2:].strip() for line in str(article.get("content") or "").splitlines() if line.strip().startswith("- ")]
        sections.append({
            "type": "guide",
            "title": article.get("title"),
            "body": body,
            "steps": steps[:8],
        })
    if include_media:
        for asset in assets_by_article.get(primary.get("id"), []):
            media.append({
                "type": asset.get("type"),
                "url": asset.get("url"),
                "caption": asset.get("caption"),
                "alt_text": asset.get("alt_text"),
            })

    return {
        "sections": sections[:2],
        "actions": [],
        "suggestions": [],
        "media": media[:3],
        "citations": citations[:2],
    }


# ── Tool Executors ──

async def execute_query_leads(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_ids = _resolve_entity_ids(args, entity_ids)
    start, end = _default_dates(args, tz_name)
    page = args.get("page", 1)
    page_size = min(args.get("page_size", 20), 50)
    include_stats = args.get("include_stats", True)

    result: dict[str, Any] = {}

    # Main list — exact params from: entity_leads_page_list(p_entity_ids, p_created_start, p_created_end,
    # p_sources, p_reply_channels, p_qualification, p_has_booking, p_reply_status, p_after_hours_booking,
    # p_repeat_unique, p_contact_ids, p_exclude_contact_ids, p_search, p_sort_field, p_sort_dir, p_page, p_page_size)
    # Stats FIRST (small, never truncated) — so the AI always sees the counts
    if include_stats:
        try:
            stats = await supabase.rpc("main", "entity_leads_page_stats", {
                "p_entity_ids": entity_ids,
                "p_start": start,
                "p_end": end,
            })
            result["stats"] = stats
        except Exception as e:
            result["stats_error"] = str(e)

    # Paginated list SECOND (large, may be truncated — but counts are already above)
    try:
        data = await supabase.rpc("main", "entity_leads_page_list", {
            "p_entity_ids": entity_ids,
            "p_created_start": start,
            "p_created_end": end,
            "p_sources": _safe_list(args.get("sources")),
            "p_reply_channels": _safe_list(args.get("reply_channels")),
            "p_qualification": _safe_list(args.get("qualification_statuses")),
            "p_search": args.get("search") or None,
            "p_page": page,
            "p_page_size": page_size,
            "p_sort_field": args.get("sort_field", "created_at"),
            "p_sort_dir": args.get("sort_dir", "desc"),
        })
        # Extract total_count to top level so it's always visible even if rows are truncated
        if isinstance(data, dict):
            result["total_count"] = data.get("total_count", 0)
            result["page"] = data.get("page", 1)
            result["page_size"] = data.get("page_size", page_size)
            result["rows"] = data.get("rows", [])
        else:
            result["leads"] = data
    except Exception as e:
        result["leads_error"] = str(e)

    return _truncate(result)


async def execute_query_bookings(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_ids = _resolve_entity_ids(args, entity_ids)
    start, end = _default_dates(args, tz_name)
    page = args.get("page", 1)
    page_size = min(args.get("page_size", 20), 50)
    include_stats = args.get("include_stats", True)

    result: dict[str, Any] = {}

    # Stats FIRST
    if include_stats:
        try:
            stats = await supabase.rpc("main", "entity_bookings_page_stats", {
                "p_entity_ids": entity_ids,
                "p_start": start,
                "p_end": end,
            })
            result["stats"] = stats
        except Exception as e:
            result["stats_error"] = str(e)

    # Bookings list AFTER stats
    try:
        data = await supabase.rpc("main", "entity_bookings_page_list", {
            "p_entity_ids": entity_ids,
            "p_start": start,
            "p_end": end,
            "p_status": _safe_list(args.get("statuses")),
            "p_channels": _safe_list(args.get("channels")),
            "p_sources": _safe_list(args.get("sources")),
            "p_search": args.get("search") or None,
            "p_page": page,
            "p_page_size": page_size,
            "p_sort_field": "created_at",
            "p_sort_dir": "desc",
        })
        if isinstance(data, dict):
            result["total_count"] = data.get("total_count", 0)
            result["rows"] = data.get("rows", [])
        else:
            result["bookings"] = data
    except Exception as e:
        result["bookings_error"] = str(e)

    return _truncate(result)


async def execute_query_call_logs(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_ids = _resolve_entity_ids(args, entity_ids)
    start, end = _default_dates(args, tz_name)
    page = args.get("page", 1)
    page_size = min(args.get("page_size", 20), 50)

    # RPC requires p_direction (non-optional). If no direction specified, query both and merge.
    direction = args.get("direction")
    include_transcripts = args.get("include_transcripts", False)
    common_params = {
        "p_entity_ids": entity_ids,
        "p_start": start,
        "p_end": end,
        "p_status": _safe_list(args.get("statuses")),
        "p_call_type": _safe_list(args.get("outcomes")),
        "p_sentiment": _safe_list(args.get("sentiment")),
        "p_search": args.get("search") or None,
        "p_page": page,
        "p_page_size": page_size,
        "p_sort_field": "created_at",
        "p_sort_dir": "desc",
    }

    def _strip_transcripts(data: dict) -> None:
        if not include_transcripts and "rows" in data:
            for row in data["rows"]:
                if isinstance(row, dict) and "transcript" in row:
                    t_len = len(row["transcript"] or "")
                    row["transcript"] = f"[{t_len} chars — set include_transcripts=true to see]" if t_len > 0 else None

    try:
        if direction:
            data = await supabase.rpc("main", "entity_call_logs_page_list", {**common_params, "p_direction": direction})
            if isinstance(data, dict):
                _strip_transcripts(data)
                # Restructure: counts first, rows last
                result = {"total_count": data.get("total_count", 0), "direction": direction}
                result["rows"] = data.get("rows", [])
                return _truncate(result)
            return _truncate(data)
        else:
            # Query both directions and merge counts
            inbound = await supabase.rpc("main", "entity_call_logs_page_list", {**common_params, "p_direction": "inbound"})
            outbound = await supabase.rpc("main", "entity_call_logs_page_list", {**common_params, "p_direction": "outbound"})

            in_count = inbound.get("total_count", 0) if isinstance(inbound, dict) else 0
            out_count = outbound.get("total_count", 0) if isinstance(outbound, dict) else 0
            in_rows = inbound.get("rows", []) if isinstance(inbound, dict) else []
            out_rows = outbound.get("rows", []) if isinstance(outbound, dict) else []

            # Merge rows sorted by created_at desc, limited to page_size
            all_rows = sorted(in_rows + out_rows, key=lambda r: r.get("created_at", ""), reverse=True)[:page_size]
            merged = {
                "total_count": in_count + out_count,
                "inbound_count": in_count,
                "outbound_count": out_count,
                "page": page,
                "page_size": page_size,
                "rows": all_rows,
            }
            _strip_transcripts(merged)
            return _truncate(merged)
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_dashboard_stats(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_ids = _resolve_entity_ids(args, entity_ids)
    start, end = _default_dates(args, tz_name)

    # Exact params: entity_dashboard_main_stats(p_entity_ids, p_start, p_end, p_sources,
    # p_reply_channels, p_qualification, p_reply_status, p_form_interest, p_has_booking,
    # p_repeat_unique, p_contact_ids, p_booking_channels, p_booking_status, p_after_hours)
    try:
        data = await supabase.rpc("main", "entity_dashboard_main_stats", {
            "p_entity_ids": entity_ids,
            "p_start": start,
            "p_end": end,
        })
        if isinstance(data, dict):
            billing_attribution = await _get_billing_attribution(entity_ids, start, end)
            data["billing_attribution"] = billing_attribution
            data["ai_direct_count"] = billing_attribution["ai_direct_count"]
            data["ai_assisted_count"] = billing_attribution["ai_assisted_count"]
            data["manual_count"] = billing_attribution["manual_count"]
        return _truncate(data)
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_time_series(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_ids = _resolve_entity_ids(args, entity_ids)
    start, end = _default_dates(args, tz_name)
    tz = args.get("timezone", "America/Chicago")

    # Exact params: entity_dashboard_time_series(p_entity_ids, p_start, p_end, ...filters..., p_timezone)
    try:
        data = await supabase.rpc("main", "entity_dashboard_time_series", {
            "p_entity_ids": entity_ids,
            "p_start": start,
            "p_end": end,
            "p_timezone": tz,
        })
        return _truncate(data)
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_ai_costs(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_ids = _resolve_entity_ids(args, entity_ids)
    start, end = _default_dates(args, tz_name)

    # Exact params: entity_dashboard_ai_cost(p_start, p_end, p_entity_ids)
    try:
        data = await supabase.rpc("main", "entity_dashboard_ai_cost", {
            "p_start": start,
            "p_end": end,
            "p_entity_ids": entity_ids,
        })
        return _truncate(data)
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_query_workflow_runs(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_ids = _resolve_entity_ids(args, entity_ids)
    start, end = _default_dates(args, tz_name)
    page = args.get("page", 1)
    page_size = min(args.get("page_size", 20), 50)

    # Exact params: workflow_runs_page_list(p_entity_ids, p_start, p_end, p_workflow_types,
    # p_statuses, p_trigger_sources, p_modes, p_search, p_sort_field, p_sort_dir, p_page, p_page_size)
    try:
        data = await supabase.rpc("main", "workflow_runs_page_list", {
            "p_entity_ids": entity_ids,
            "p_start": start,
            "p_end": end,
            "p_workflow_types": _safe_list(args.get("workflow_types")),
            "p_statuses": _safe_list(args.get("statuses")),
            "p_modes": ["production"],
            "p_search": args.get("search") or None,
            "p_page": page,
            "p_page_size": page_size,
            "p_sort_field": "started_at",
            "p_sort_dir": "desc",
        })
        # Restructure: counts first, rows last
        if isinstance(data, dict):
            result = {"total_count": data.get("total_count", 0), "page": data.get("page", 1)}
            result["rows"] = data.get("rows", [])
            return _truncate(result)
        return _truncate(data)
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_reactivation_stats(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_ids = _resolve_entity_ids(args, entity_ids)
    start, end = _default_dates(args, tz_name)

    # Exact params: entity_dashboard_reactivation_stats(p_entity_ids, p_start, p_end, p_source)
    try:
        data = await supabase.rpc("main", "entity_dashboard_reactivation_stats", {
            "p_entity_ids": entity_ids,
            "p_start": start,
            "p_end": end,
        })
        return _truncate(data)
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_search_knowledge_base(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    query = args.get("query", "")
    # If specific entity_id provided, use it; otherwise search across all
    target_entity = args.get("entity_id")
    target_ids = [target_entity] if target_entity and target_entity in entity_ids else entity_ids

    try:
        # Direct table query with ilike search
        results = []
        for eid in target_ids[:5]:  # Cap to 5 entities to avoid huge queries
            resp = await supabase._request(
                supabase.main_client, "GET", "/knowledge_base_articles",
                params={
                    "entity_id": f"eq.{eid}",
                    "select": "id,title,content,tag,created_at",
                    "or": f"(title.ilike.%{query}%,content.ilike.%{query}%,tag.ilike.%{query}%)",
                    "limit": "10",
                },
                label="data_chat_search_kb",
            )
            data = resp.json()
            if data:
                results.extend(data)
        return _truncate({"articles": results, "total": len(results)})
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_chat_history(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    contact_id = args.get("contact_id", "")
    contact_name = args.get("contact_name", "")
    limit = min(args.get("limit", 30), 50)

    try:
        # Get chat table name for entity
        resp = await supabase._request(
            supabase.main_client, "GET", "/entities",
            params={"id": f"eq.{entity_id}", "select": "chat_history_table_name"},
            label="data_chat_get_table",
        )
        erows = resp.json()
        if not erows or not erows[0].get("chat_history_table_name"):
            return _truncate({"error": "No chat history table for this entity"})

        table = erows[0]["chat_history_table_name"]

        # Resolve contact_name to GHL contact_id via leads table
        if contact_name and not contact_id:
            resp = await supabase._request(
                supabase.main_client, "GET", "/leads",
                params={
                    "entity_id": f"eq.{entity_id}",
                    "contact_name": f"ilike.%{contact_name}%",
                    "select": "ghl_contact_id,contact_name",
                    "limit": "1",
                },
                label="data_chat_resolve_contact",
            )
            leads = resp.json()
            if leads and isinstance(leads, list) and leads[0].get("ghl_contact_id"):
                contact_id = leads[0]["ghl_contact_id"]
            else:
                return _truncate({"error": f"No lead found matching '{contact_name}' for this entity"})

        if contact_id:
            # Get specific contact's messages
            history = await postgres.get_chat_history(table, contact_id, limit=limit)
            messages = [
                {"role": r["role"], "content": r["content"], "source": r.get("source", ""),
                 "channel": r.get("channel", ""), "timestamp": str(r.get("timestamp", ""))}
                for r in history
            ]
            return _truncate({"messages": messages, "contact_id": contact_id, "message_count": len(messages)})
        else:
            # Get total counts first
            totals = await postgres.chat_pool.fetchrow(
                f'SELECT COUNT(DISTINCT session_id) as total_conversations, COUNT(*) as total_messages FROM "{table}"'
            )

            # List recent conversations (distinct session_ids) with lead name join
            chat_rows = await postgres.chat_pool.fetch(
                f'SELECT session_id, MAX("timestamp") as last_msg, COUNT(*) as msg_count '
                f'FROM "{table}" GROUP BY session_id ORDER BY last_msg DESC LIMIT $1',
                limit,
            )
            # Enrich with lead names
            contacts = []
            for r in chat_rows:
                entry: dict[str, Any] = {
                    "contact_id": r["session_id"],
                    "last_message": str(r["last_msg"]),
                    "message_count": r["msg_count"],
                }
                # Look up lead name by ghl_contact_id
                lead_resp = await supabase._request(
                    supabase.main_client, "GET", "/leads",
                    params={
                        "entity_id": f"eq.{entity_id}",
                        "ghl_contact_id": f"eq.{r['session_id']}",
                        "select": "contact_name",
                        "limit": "1",
                    },
                    label="data_chat_lead_name",
                )
                lead_data = lead_resp.json()
                if lead_data and isinstance(lead_data, list):
                    entry["contact_name"] = lead_data[0].get("contact_name", "Unknown")
                contacts.append(entry)
            return _truncate({
                "total_conversations": totals["total_conversations"],
                "total_messages": totals["total_messages"],
                "showing": len(contacts),
                "recent_conversations": contacts,
            })
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_search_chat_messages(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    query = args.get("query", "")
    role_filter = args.get("role_filter", "")
    source_filter = args.get("source_filter", "")
    limit = min(args.get("limit", 20), 50)

    try:
        resp = await supabase._request(
            supabase.main_client, "GET", "/entities",
            params={"id": f"eq.{entity_id}", "select": "chat_history_table_name"},
            label="data_chat_search_table",
        )
        erows = resp.json()
        if not erows or not erows[0].get("chat_history_table_name"):
            return _truncate({"error": "No chat history table for this entity"})

        table = erows[0]["chat_history_table_name"]

        # Build WHERE clause dynamically
        conditions: list[str] = []
        params: list = []
        idx = 1

        if query:
            conditions.append(f"content ILIKE ${idx}")
            params.append(f"%{query}%")
            idx += 1

        if role_filter in ("human", "ai"):
            conditions.append(f"role = ${idx}")
            params.append(role_filter)
            idx += 1

        if source_filter:
            conditions.append(f"source = ${idx}")
            params.append(source_filter)
            idx += 1

        where = " AND ".join(conditions) if conditions else "TRUE"

        # Count total matches
        total_row = await postgres.chat_pool.fetchrow(
            f'SELECT COUNT(*) as cnt FROM "{table}" WHERE {where}', *params
        )
        total_matches = total_row["cnt"]

        # Count distinct conversations
        conv_row = await postgres.chat_pool.fetchrow(
            f'SELECT COUNT(DISTINCT session_id) as cnt FROM "{table}" WHERE {where}', *params
        )
        conversations_with_matches = conv_row["cnt"]

        # Fetch sample matches
        matches = await postgres.chat_pool.fetch(
            f'SELECT session_id, role, source, content, "timestamp" FROM "{table}" '
            f'WHERE {where} ORDER BY "timestamp" DESC LIMIT ${idx}',
            *params, limit,
        )

        results = [
            {
                "contact_id": r["session_id"],
                "role": r["role"],
                "source": r["source"],
                "content": r["content"][:200],
                "timestamp": str(r["timestamp"]),
            }
            for r in matches
        ]

        # Auto-include role + source breakdown (saves the AI from needing multiple calls)
        breakdown: dict[str, Any] = {}
        bd_sql = f'SELECT role, source, COUNT(*) as cnt FROM "{table}" GROUP BY role, source ORDER BY role, cnt DESC'
        bd_rows = await postgres.chat_pool.fetch(bd_sql)
        role_totals: dict[str, int] = {}
        source_counts: list[dict] = []
        for r in bd_rows:
            role_totals[r["role"]] = role_totals.get(r["role"], 0) + r["cnt"]
            source_counts.append({"role": r["role"], "source": r["source"], "count": r["cnt"]})
        breakdown = {"role_totals": role_totals, "by_source": source_counts}

        return _truncate({
            "total_matches": total_matches,
            "conversations_with_matches": conversations_with_matches,
            "showing": len(results),
            "filters": {"role": role_filter or "all", "source": source_filter or "all"},
            **({"breakdown": breakdown} if breakdown else {}),
            "matches": results,
        })
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_query_scheduled_messages(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    target_entity = args.get("entity_id")
    target_ids = [target_entity] if target_entity and target_entity in entity_ids else entity_ids
    status = args.get("status")
    message_type = args.get("message_type")
    limit = min(args.get("limit", 20), 50)

    try:
        params: dict[str, str] = {
            "select": "id,entity_id,contact_id,message_type,position,channel,status,due_at,"
                      "source,triggered_by,smart_reason,fired_at,result,result_reason,"
                      "cancelled_at,cancel_reason,created_at",
            "order": "due_at.desc",
            "limit": str(limit),
        }
        # Filter by entity_ids
        if len(target_ids) == 1:
            params["entity_id"] = f"eq.{target_ids[0]}"
        else:
            params["entity_id"] = f"in.({','.join(target_ids)})"

        if status:
            params["status"] = f"eq.{status}"
        if message_type:
            params["message_type"] = f"eq.{message_type}"

        resp = await supabase._request(
            supabase.main_client, "GET", "/scheduled_messages",
            params=params,
            headers={"Prefer": "count=exact"},
            label="data_chat_scheduled_messages",
        )
        rows = resp.json() if isinstance(resp.json(), list) else []
        count_header = resp.headers.get("content-range", "")
        total = count_header.split("/")[-1] if "/" in count_header else str(len(rows))

        return _truncate({"total": total, "showing": len(rows), "messages": rows})
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_query_outreach_templates(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    target_entity = args.get("entity_id")
    target_ids = [target_entity] if target_entity and target_entity in entity_ids else entity_ids

    try:
        params: dict[str, str] = {
            "select": "id,entity_id,form_service_interest,is_active,positions,timing_config,"
                      "calendar_id,calendar_name,setter_key,is_appointment_template,created_at",
            "order": "created_at.desc",
        }
        if len(target_ids) == 1:
            params["entity_id"] = f"eq.{target_ids[0]}"
        else:
            params["entity_id"] = f"in.({','.join(target_ids)})"

        resp = await supabase._request(
            supabase.main_client, "GET", "/outreach_templates",
            params=params,
            label="data_chat_outreach_templates",
        )
        templates = resp.json() if isinstance(resp.json(), list) else []

        # Summarize positions to avoid huge JSONB dumps
        for t in templates:
            positions = t.get("positions") or {}
            if isinstance(positions, dict):
                t["position_count"] = len(positions)
                t["position_summary"] = {k: {"type": v.get("type", ""), "channel": v.get("channel", "")} for k, v in list(positions.items())[:5]}
                del t["positions"]  # Remove raw JSONB

        return _truncate({"total": len(templates), "templates": templates})
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_outreach_performance(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    try:
        # 1. Get outreach templates
        t_resp = await supabase._request(
            supabase.main_client, "GET", "/outreach_templates",
            params={"entity_id": f"eq.{entity_id}", "select": "id,form_service_interest,is_active,positions"},
            label="data_chat_outreach_templates_perf",
        )
        templates = t_resp.json() if isinstance(t_resp.json(), list) else []

        if not templates:
            return _truncate({"message": "No outreach templates configured for this entity"})

        # 2. Get leads with outreach_variant
        l_resp = await supabase._request(
            supabase.main_client, "GET", "/leads",
            params={
                "entity_id": f"eq.{entity_id}",
                "outreach_variant": "not.is.null",
                "select": "id,outreach_variant,last_reply_datetime,contact_name",
                "limit": "10000",
            },
            label="data_chat_outreach_leads",
        )
        leads = l_resp.json() if isinstance(l_resp.json(), list) else []

        # 3. Get bookings to check which leads booked
        b_resp = await supabase._request(
            supabase.main_client, "GET", "/bookings",
            params={
                "entity_id": f"eq.{entity_id}",
                "lead_id": "not.is.null",
                "select": "lead_id",
                "limit": "10000",
            },
            label="data_chat_outreach_bookings",
        )
        booked_ids = set()
        bookings_raw = b_resp.json() if isinstance(b_resp.json(), list) else []
        for b in bookings_raw:
            if b.get("lead_id"):
                booked_ids.add(b["lead_id"])

        # 4. Group leads by outreach_name + variant
        groups: dict[str, dict[str, list]] = {}  # {template_name: {"A": [leads], "B": [leads]}}
        for lead in leads:
            ov = lead.get("outreach_variant")
            if not ov or not isinstance(ov, dict):
                continue
            name = (ov.get("outreach_name") or "").strip()
            variant = (ov.get("variant") or "A").upper()
            if not name:
                continue
            if name not in groups:
                groups[name] = {"A": [], "B": []}
            if variant in groups[name]:
                groups[name][variant].append(lead)

        # 5. Build performance summary per template
        results = []
        for tmpl in templates:
            tname = tmpl.get("form_service_interest", "")
            group = groups.get(tname, {"A": [], "B": []})

            def calc_stats(lead_list: list) -> dict:
                total = len(lead_list)
                replied = sum(1 for l in lead_list if l.get("last_reply_datetime"))
                booked = sum(1 for l in lead_list if l.get("id") in booked_ids)
                return {
                    "leads": total,
                    "replied": replied,
                    "reply_rate": round(replied / total * 100, 1) if total > 0 else 0,
                    "bookings": booked,
                    "booking_rate": round(booked / total * 100, 1) if total > 0 else 0,
                }

            a_stats = calc_stats(group["A"])
            b_stats = calc_stats(group["B"])
            has_ab = b_stats["leads"] > 0

            entry: dict[str, Any] = {
                "template": tname,
                "active": tmpl.get("is_active", False),
                "variant_a": a_stats,
            }
            if has_ab:
                entry["variant_b"] = b_stats
                # Determine winner
                if a_stats["reply_rate"] > b_stats["reply_rate"]:
                    entry["ab_winner"] = "A (higher reply rate)"
                elif b_stats["reply_rate"] > a_stats["reply_rate"]:
                    entry["ab_winner"] = "B (higher reply rate)"
                else:
                    entry["ab_winner"] = "Tied"
            entry["total_leads"] = a_stats["leads"] + b_stats["leads"]
            entry["total_replied"] = a_stats["replied"] + b_stats["replied"]
            entry["total_bookings"] = a_stats["bookings"] + b_stats["bookings"]
            entry["overall_reply_rate"] = round(
                (a_stats["replied"] + b_stats["replied"]) / max(a_stats["leads"] + b_stats["leads"], 1) * 100, 1
            )
            entry["overall_booking_rate"] = round(
                (a_stats["bookings"] + b_stats["bookings"]) / max(a_stats["leads"] + b_stats["leads"], 1) * 100, 1
            )
            results.append(entry)

        return _truncate({
            "templates": results,
            "total_outreach_leads": len(leads),
            "templates_with_data": sum(1 for r in results if r["total_leads"] > 0),
        })

    except Exception as e:
        return _truncate({"error": str(e)})


def _humanize_key(key: str) -> str:
    """Convert snake_case keys to readable labels."""
    return key.replace("_", " ").replace("  ", " ").title()


def _format_setter_section(data: dict, section: str) -> str:
    """Format a setter config section into readable text with FULL prompt content."""
    lines = []

    if section == "persona":
        bp = data.get("bot_persona") or {}
        lines.append("BOT PERSONA")
        lines.append(f"  Name: {bp.get('name', 'Not set')}")
        lines.append(f"  Tone: {bp.get('tone', 'Not set')}")
        lines.append(f"  Identity: {bp.get('identity', 'Not set')}")
        lines.append(f"  Punctuation Style: {bp.get('punctuation_style', 'Not set')}")
        lines.append(f"  Natural Typos: {'Enabled' if bp.get('natural_typos') else 'Disabled'}")
        dc = bp.get("data_collection") or {}
        if dc.get("enabled"):
            fields = dc.get("fields") or []
            lines.append(f"  Data Collection: Enabled ({len(fields)} fields)")
            for f in fields:
                lines.append(f"    - {f.get('name', '?')}: {f.get('description', '')}")
        else:
            lines.append("  Data Collection: Disabled")

    elif section == "conversation":
        conv = data.get("conversation") or {}
        reply = conv.get("reply") or {}
        sections = reply.get("sections") or {}

        lines.append("CONVERSATION SETTINGS")
        # Full prompt text — no truncation
        lines.append(f"  Agent Goal: {sections.get('agent_goal', {}).get('value', 'Not set')}")
        lines.append(f"  Role Context: {sections.get('role_context', {}).get('value', 'Not set')}")

        # Strategy — show ALL options with selected marked
        # Helper for radio/select fields — shows all options with selected marked
        def _format_radio(field_name: str, field_data: dict) -> None:
            selected = field_data.get("selected", "Not set")
            options = field_data.get("options", {})
            lines.append(f"  {_humanize_key(field_name)} (selected: {_humanize_key(selected)}):")
            if options:
                for opt_key, opt_val in options.items():
                    marker = " <-- CURRENT" if opt_key == selected else ""
                    if isinstance(opt_val, dict):
                        label = opt_val.get("label", _humanize_key(opt_key))
                        prompt = opt_val.get("prompt", "")
                        lines.append(f"    - {label}{marker}")
                        if prompt:
                            lines.append(f"      Prompt: {prompt}")
                    else:
                        lines.append(f"    - {_humanize_key(opt_key)}: {opt_val}{marker}")
            else:
                lines.append(f"    (no options stored — only selected value: {_humanize_key(selected)})")

        _format_radio("booking_style", sections.get("booking_style", {}))
        _format_radio("pricing_discussion", sections.get("pricing_discussion", {}))
        dq = sections.get("discovery_questions", {})
        lines.append(f"  Discovery Questions: {'Enabled' if dq.get('enabled') else 'Disabled'}")
        if dq.get("enabled") and dq.get("value"):
            lines.append(f"    Prompt: {dq['value']}")
        mq = sections.get("max_questions", {})
        lines.append(f"  Max Questions Before Booking: {mq.get('value', 'Not set')}")
        mbp = sections.get("max_booking_pushes", {})
        mbp_status = "Enabled" if mbp.get("enabled") else "Disabled"
        mbp_val = f" ({mbp.get('value', 0)})" if mbp.get("enabled") else ""
        lines.append(f"  Max Booking Pushes: {mbp_status}{mbp_val}")

        # Behavior toggles with FULL prompt text
        lines.append("  Behavior Toggles:")
        behavior_keys = [
            "steer_toward_goal", "confident_expert", "fully_helped",
            "always_moving_forward", "push_back", "proactive_tips",
            "low_effort_responses", "urgency", "scarcity",
            "returning_lead_rules", "future_pacing", "acknowledge_before_pivot",
            "yes_and", "paraphrase", "accept_no", "discover_timeline", "allow_small_talk",
        ]
        for bk in behavior_keys:
            toggle = sections.get(bk, {})
            if isinstance(toggle, dict) and "enabled" in toggle:
                status = "ON" if toggle.get("enabled") else "OFF"
                prompt_text = toggle.get("value", "")
                lines.append(f"    {_humanize_key(bk)}: {status}")
                if prompt_text:
                    lines.append(f"      Prompt: {prompt_text}")

        # Objections with FULL response text
        objs = sections.get("objections", [])
        if objs:
            lines.append(f"  Objection Handlers ({len(objs)}):")
            for i, obj in enumerate(objs, 1):
                lines.append(f"    {i}. Trigger: {obj.get('trigger', '?')}")
                lines.append(f"       Response: {obj.get('response', 'Not set')}")

        # Common situations
        situations = sections.get("common_situations", [])
        if situations:
            lines.append(f"  Common Situations ({len(situations)}):")
            for s in situations:
                lines.append(f"    - {s.get('trigger', '?')}: {s.get('response', '')}")

    elif section == "follow_up":
        conv = data.get("conversation") or {}
        fu = conv.get("follow_up") or {}
        lines.append("FOLLOW-UP SETTINGS")
        lines.append(f"  Mode: {fu.get('mode', 'Not set')}")
        lines.append(f"  Cadence Count: {fu.get('cadence_count', 'Not set')}")
        lines.append(f"  Media in Follow-ups: {'Enabled' if fu.get('media_enabled') else 'Disabled'}")

    elif section == "services":
        svc = data.get("services") or {}
        svc_list = svc.get("services") or []
        lines.append(f"SERVICES ({len(svc_list)} configured)")
        for s in svc_list[:15]:
            lines.append(f"  - {s.get('name', 'Unnamed')}: {s.get('description', '')}")
            if s.get("price"):
                lines.append(f"    Price: {s['price']}")

    elif section == "booking":
        bk = data.get("booking") or {}
        lines.append("BOOKING SETTINGS")
        cals = bk.get("calendars") or []
        lines.append(f"  Calendars: {len(cals)}")
        for c in cals[:5]:
            lines.append(f"    - {c.get('name', 'Unnamed')} (ID: {c.get('id', '?')})")
        lines.append(f"  Appointment Length: {bk.get('appointment_length', 'Not set')} minutes")

    elif section == "transfer":
        tr = data.get("transfer") or {}
        scenarios = tr.get("scenarios") or []
        enabled = [s for s in scenarios if s.get("enabled")]
        disabled = [s for s in scenarios if not s.get("enabled")]
        lines.append(f"TRANSFER SETTINGS ({len(enabled)} active, {len(disabled)} disabled)")
        for s in enabled:
            lines.append(f"  - [ON] {s.get('name', 'Unnamed')}: {s.get('description', '')}")
        for s in disabled:
            lines.append(f"  - [OFF] {s.get('name', 'Unnamed')}: {s.get('description', '')}")

    elif section == "security":
        sec = data.get("security") or {}
        rules = sec.get("compliance_rules") or []
        custom = sec.get("custom_compliance_rules") or []
        replacements = sec.get("term_replacements") or []
        lines.append("SECURITY SETTINGS")
        lines.append(f"  Compliance Rules:")
        for r in rules + custom:
            status = "ON" if r.get("enabled") else "OFF"
            lines.append(f"    [{status}] {r.get('name', r.get('rule', '?'))}")
        if replacements:
            lines.append(f"  Term Replacements:")
            for r in replacements:
                lines.append(f"    \"{r.get('find', '?')}\" -> \"{r.get('replace', '?')}\"")

    elif section == "missed_call":
        mc = data.get("missed_call_textback") or {}
        lines.append("MISSED CALL TEXT-BACK")
        lines.append(f"  Enabled: {'Yes' if mc.get('enabled') else 'No'}")
        if mc.get("prompt"):
            lines.append(f"  Prompt: {mc['prompt']}")

    elif section == "reactivation":
        ra = data.get("auto_reactivation") or {}
        lines.append("AUTO REACTIVATION")
        lines.append(f"  Enabled: {'Yes' if ra.get('enabled') else 'No'}")
        lines.append(f"  Mode: {ra.get('mode', 'Not set')}")
        if ra.get("prompt"):
            lines.append(f"  Prompt: {ra['prompt']}")

    elif section == "ai_models":
        models = data.get("ai_models") or {}
        temps = data.get("ai_temperatures") or {}
        lines.append("AI MODELS")
        if not models:
            lines.append("  No model overrides set (using tenant defaults)")
        else:
            for call_key, model_id in sorted(models.items()):
                temp = temps.get(call_key, "default")
                lines.append(f"  {_humanize_key(call_key)}: {model_id} (temp: {temp})")

    else:
        lines.append(f"Unknown section: {section}")

    return "\n".join(lines)


async def execute_get_setter_config(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    section = args.get("section")

    try:
        resp = await supabase._request(
            supabase.main_client, "GET", "/entities",
            params={"id": f"eq.{entity_id}", "select": "system_config"},
            label="data_chat_setter_config",
        )
        rows = resp.json()
        if not rows:
            return _truncate({"error": "Entity not found"})

        system_config = rows[0].get("system_config") or {}
        setters = system_config.get("setters") or {}

        if not setters:
            return _truncate({"error": "No setter configuration found for this entity"})

        # Get the default setter (or first one)
        setter_key = None
        setter_data = None
        for k, v in setters.items():
            if v.get("is_default"):
                setter_key = k
                setter_data = v
                break
        if not setter_data:
            setter_key = list(setters.keys())[0]
            setter_data = setters[setter_key]

        if section:
            result = _format_setter_section(setter_data, section)
            return _truncate({"setter": setter_data.get("name", setter_key), "section": section, "config": result})
        else:
            # Full overview — all sections
            all_sections = ["persona", "conversation", "services", "booking", "transfer", "security", "follow_up", "missed_call", "reactivation", "ai_models"]
            overview = []
            for sec in all_sections:
                overview.append(_format_setter_section(setter_data, sec))
            result = "\n\n".join(overview)
            return _truncate({"setter": setter_data.get("name", setter_key), "total_setters": len(setters), "config": result})

    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_entity_info(args: dict, entity_ids: list[str], tz_name: str = "UTC") -> str:
    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    try:
        resp = await supabase._request(
            supabase.main_client, "GET", "/entities",
            params={
                "id": f"eq.{entity_id}",
                "select": "id,name,entity_type,timezone,journey_stage,billing_config,"
                          "average_booking_value,status,created_at,"
                          "contact_name,contact_email,contact_phone,business_phone,"
                          "business_schedule,supported_languages,notes",
            },
            label="data_chat_entity_info",
        )
        rows = resp.json()
        if not rows:
            return _truncate({"error": "Entity not found"})

        entity = rows[0]
        # Strip sensitive config details, keep useful metadata
        return _truncate(entity)
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_onboarding_template(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
) -> str:
    if not tenant_id:
        return _truncate({"error": "Tenant context unavailable"})

    phase_key = args.get("phase_key")
    include_instructions = bool(args.get("include_instructions"))
    include_actions = bool(args.get("include_actions"))
    include_resources = bool(args.get("include_resources"))
    include_inactive = bool(args.get("include_inactive"))

    try:
        template = await _get_active_onboarding_template(tenant_id)
        if not template:
            return _truncate({"message": "No active onboarding template found for this tenant"})

        phases = _parse_phase_config(template.get("phase_config"))
        items = await _get_onboarding_template_items(template["id"], include_inactive=include_inactive)
        if phase_key:
            items = [item for item in items if item.get("phase_key") == phase_key]

        grouped = []
        for phase, phase_items in _build_phase_groups(items, phases):
            if phase_key and phase.get("key") != phase_key:
                continue
            grouped.append({
                "phase_key": phase.get("key"),
                "phase_label": phase.get("label"),
                "item_count": len(phase_items),
                "required_count": sum(1 for item in phase_items if item.get("is_required")),
                "system_check_count": sum(1 for item in phase_items if item.get("completion_mode") == "system_check"),
                "items": [
                    _summarize_template_item(
                        item,
                        include_instructions=include_instructions,
                        include_actions=include_actions,
                        include_resources=include_resources,
                    )
                    for item in phase_items
                ],
            })

        return _truncate({
            "template": {
                "id": template.get("id"),
                "name": template.get("name"),
                "version": template.get("version"),
                "phase_order": phases,
                "phase_count": len(phases),
                "item_count": len(items),
                "filtered_phase": phase_key,
            },
            "phases": grouped,
        })
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_query_onboarding_queue(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
) -> str:
    if not tenant_id:
        return _truncate({"error": "Tenant context unavailable"})

    target_entity_id = args.get("entity_id")
    if target_entity_id and entity_ids and target_entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    status = args.get("status")
    only_blocked = bool(args.get("only_blocked"))
    only_out_of_sync = bool(args.get("only_out_of_sync"))
    limit = min(int(args.get("limit", 25) or 25), 100)

    try:
        active_template = await _get_active_onboarding_template(tenant_id)
        onboardings = await _get_entity_onboardings(tenant_id, entity_id=target_entity_id, status=status)
        if not onboardings:
            return _truncate({"total_count": 0, "rows": []})

        onboarding_ids = [row["id"] for row in onboardings if row.get("id")]
        matched_entity_ids = sorted({row["entity_id"] for row in onboardings if row.get("entity_id")})
        entities = await _get_onboarding_entities(tenant_id, matched_entity_ids)
        entities_by_id = {entity["id"]: entity for entity in entities if entity.get("id")}
        items = await _get_entity_onboarding_items(onboarding_ids, full=False)
        items_by_onboarding: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            onboarding_id = item.get("onboarding_id")
            if onboarding_id:
                items_by_onboarding.setdefault(onboarding_id, []).append(item)

        kb_counts = await _get_entity_count_map("knowledge_base_articles", list(entities_by_id.keys()))
        outreach_counts = await _get_entity_count_map("outreach_templates", list(entities_by_id.keys()))

        rows: list[dict[str, Any]] = []
        for onboarding in onboardings:
            entity = entities_by_id.get(onboarding.get("entity_id"))
            if not entity:
                continue

            onboarding_items = items_by_onboarding.get(onboarding["id"], [])
            knowledge_base_count = kb_counts.get(entity["id"], 0)
            outreach_count = outreach_counts.get(entity["id"], 0)
            completed_items = sum(1 for item in onboarding_items if _is_onboarding_item_complete(item))
            blocked_items = sum(1 for item in onboarding_items if item.get("status") == "blocked")
            ready_items = sum(
                1 for item in onboarding_items
                if _is_onboarding_item_ready(
                    item,
                    entity,
                    knowledge_base_count=knowledge_base_count,
                    outreach_count=outreach_count,
                )
            )
            total_items = len(onboarding_items)
            needs_sync = bool(
                active_template
                and (
                    onboarding.get("template_id") != active_template.get("id")
                    or onboarding.get("template_version") != active_template.get("version")
                )
            )
            updated_values = [onboarding.get("updated_at")] + [item.get("updated_at") for item in onboarding_items if item.get("updated_at")]
            last_updated = max(str(value) for value in updated_values if value) if any(updated_values) else None

            row = {
                "entity_id": entity.get("id"),
                "entity_name": entity.get("name"),
                "journey_stage": entity.get("journey_stage"),
                "onboarding_status": onboarding.get("status"),
                "template_version": onboarding.get("template_version"),
                "active_template_version": active_template.get("version") if active_template else None,
                "needs_sync": needs_sync,
                "total_items": total_items,
                "completed_items": completed_items,
                "blocked_items": blocked_items,
                "ready_system_check_items": ready_items,
                "progress_percent": round((completed_items / total_items) * 100) if total_items else 0,
                "last_updated": last_updated,
            }

            if only_blocked and blocked_items == 0:
                continue
            if only_out_of_sync and not needs_sync:
                continue
            rows.append(row)

        rows.sort(
            key=lambda row: (
                0 if row["onboarding_status"] == "in_progress" else 1,
                -row["blocked_items"],
                0 if row["needs_sync"] else 1,
                row["progress_percent"],
                row["last_updated"] or "",
            )
        )

        return _truncate({
            "total_count": len(rows),
            "active_template": {
                "id": active_template.get("id") if active_template else None,
                "name": active_template.get("name") if active_template else None,
                "version": active_template.get("version") if active_template else None,
            },
            "rows": rows[:limit],
        })
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_entity_onboarding_detail(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
) -> str:
    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    include_notes = bool(args.get("include_notes"))
    include_completed_items = bool(args.get("include_completed_items"))
    include_instructions = bool(args.get("include_instructions"))
    include_actions = bool(args.get("include_actions"))
    include_resources = bool(args.get("include_resources"))
    phase_key = args.get("phase_key")

    try:
        if not tenant_id:
            return _truncate({"error": "Tenant context unavailable"})

        entities = await _get_onboarding_entities(tenant_id, [entity_id])
        entity = entities[0] if entities else None
        if not entity:
            return _truncate({"error": "Entity not found or not active"})

        onboardings = await _get_entity_onboardings(tenant_id, entity_id=entity_id)
        if not onboardings:
            return _truncate({"message": "No onboarding workspace found for this client", "entity_id": entity_id})

        onboarding = onboardings[0]
        active_template = await _get_active_onboarding_template(tenant_id)
        items = await _get_entity_onboarding_items([onboarding["id"]], full=True)
        items.sort(key=lambda item: (str(item.get("phase_key") or ""), int(item.get("sort_order") or 0)))

        knowledge_base_count = (await _get_entity_count_map("knowledge_base_articles", [entity_id])).get(entity_id, 0)
        outreach_count = (await _get_entity_count_map("outreach_templates", [entity_id])).get(entity_id, 0)

        phase_config = _parse_phase_config(onboarding.get("phase_config"))
        filtered_items = [item for item in items if not phase_key or item.get("phase_key") == phase_key]
        display_items = filtered_items if include_completed_items else [item for item in filtered_items if not _is_onboarding_item_complete(item)]

        next_required_items = []
        for item in filtered_items:
            if _is_onboarding_item_complete(item) or not item.get("is_required"):
                continue
            next_required_items.append(
                _summarize_entity_onboarding_item(
                    item,
                    entity,
                    knowledge_base_count=knowledge_base_count,
                    outreach_count=outreach_count,
                )
            )

        blocked_items = [
            _summarize_entity_onboarding_item(
                item,
                entity,
                knowledge_base_count=knowledge_base_count,
                outreach_count=outreach_count,
                include_notes=include_notes,
                include_instructions=include_instructions,
                include_actions=include_actions,
                include_resources=include_resources,
            )
            for item in filtered_items
            if item.get("status") == "blocked"
        ]

        ready_items = [
            _summarize_entity_onboarding_item(
                item,
                entity,
                knowledge_base_count=knowledge_base_count,
                outreach_count=outreach_count,
            )
            for item in filtered_items
            if _is_onboarding_item_ready(
                item,
                entity,
                knowledge_base_count=knowledge_base_count,
                outreach_count=outreach_count,
            )
        ]

        phases_output = []
        for phase, phase_items in _build_phase_groups(display_items, phase_config):
            if phase_key and phase.get("key") != phase_key:
                continue
            phase_all_items = [item for item in filtered_items if item.get("phase_key") == phase.get("key")]
            completed_count = sum(1 for item in phase_all_items if _is_onboarding_item_complete(item))
            blocked_count = sum(1 for item in phase_all_items if item.get("status") == "blocked")
            ready_count = sum(
                1 for item in phase_all_items
                if _is_onboarding_item_ready(
                    item,
                    entity,
                    knowledge_base_count=knowledge_base_count,
                    outreach_count=outreach_count,
                )
            )
            phases_output.append({
                "phase_key": phase.get("key"),
                "phase_label": phase.get("label"),
                "total_items": len(phase_all_items),
                "completed_items": completed_count,
                "blocked_items": blocked_count,
                "ready_system_check_items": ready_count,
                "items": [
                    _summarize_entity_onboarding_item(
                        item,
                        entity,
                        knowledge_base_count=knowledge_base_count,
                        outreach_count=outreach_count,
                        include_notes=include_notes,
                        include_instructions=include_instructions,
                        include_actions=include_actions,
                        include_resources=include_resources,
                    )
                    for item in phase_items
                ],
            })

        total_items = len(items)
        completed_items = sum(1 for item in items if _is_onboarding_item_complete(item))
        needs_sync = bool(
            active_template
            and (
                onboarding.get("template_id") != active_template.get("id")
                or onboarding.get("template_version") != active_template.get("version")
            )
        )

        return _truncate({
            "entity": {
                "id": entity.get("id"),
                "name": entity.get("name"),
                "journey_stage": entity.get("journey_stage"),
            },
            "onboarding": {
                "id": onboarding.get("id"),
                "status": onboarding.get("status"),
                "started_at": onboarding.get("started_at"),
                "completed_at": onboarding.get("completed_at"),
                "template_id": onboarding.get("template_id"),
                "template_version": onboarding.get("template_version"),
                "progress_percent": round((completed_items / total_items) * 100) if total_items else 0,
                "completed_items": completed_items,
                "total_items": total_items,
                "knowledge_base_count": knowledge_base_count,
                "outreach_template_count": outreach_count,
                "needs_sync": needs_sync,
                "active_template_id": active_template.get("id") if active_template else None,
                "active_template_version": active_template.get("version") if active_template else None,
                "filtered_phase": phase_key,
            },
            "phase_progress": phases_output,
            "next_required_items": next_required_items[:10],
            "blocked_items": blocked_items[:10],
            "ready_system_check_items": ready_items[:10],
        })
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_search_platform_knowledge(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
    origin: str = "admin",
) -> str:
    query = str(args.get("query") or "").strip()
    if not query:
        return _truncate({"error": "Query is required"})

    page_key = args.get("page_key")
    include_content = bool(args.get("include_content", True))
    include_media = bool(args.get("include_media", True))
    limit = min(int(args.get("limit", 3) or 3), 6)
    target_entity_id = args.get("entity_id")
    if target_entity_id and target_entity_id not in entity_ids:
        target_entity_id = None
    if not target_entity_id and len(entity_ids) == 1:
        target_entity_id = entity_ids[0]

    try:
        articles_with_scores = await _get_platform_articles(tenant_id, origin, query, page_key=page_key, limit=limit)
        articles = [article for article, _score in articles_with_scores]
        if not articles:
            return _truncate({"message": "No platform guide matched this question"})
        assets_by_article = await _get_platform_assets([article["id"] for article in articles if article.get("id")], origin, tenant_id)
        ui_payload = _build_platform_ui_payload(
            articles_with_scores,
            assets_by_article,
            target_entity_id,
            query,
            include_content=include_content,
            include_media=include_media,
            origin=origin,
        )
        return _truncate({
            "articles": [
                {
                    "title": article.get("title"),
                    "summary": article.get("summary"),
                    "page_key": article.get("page_key"),
                    "route_path": article.get("route_path"),
                    "audience": article.get("audience"),
                    "content": article.get("content") if include_content else None,
                }
                for article in articles
            ],
            "ui_payload": ui_payload,
        })
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_draft_change_request(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
    origin: str = "admin",
) -> str:
    topic = str(args.get("topic") or "").strip()
    requested_change = str(args.get("requested_change") or "").strip()
    entity_id = args.get("entity_id")
    if entity_id and entity_id not in entity_ids:
        entity_id = None
    if not topic or not requested_change:
        return _truncate({"error": "Topic and requested_change are required"})

    action_type = "submit_request_change" if origin == "portal" else "open_request_change"
    action_label = "Submit Request Change" if origin == "portal" else "Open Request Change"

    return _truncate({
        "draft": {
            "topic": topic,
            "description": requested_change,
        },
        "ui_payload": {
            "sections": [
                {
                    "type": "guide",
                    "title": "Change Request Draft",
                    "body": f"Topic: {topic}\nRequest: {requested_change}",
                    "steps": [],
                }
            ],
            "actions": [
                {
                    "type": action_type,
                    "label": action_label,
                    "topic": topic,
                    "description": requested_change,
                    "entity_id": entity_id,
                }
            ],
            "suggestions": [],
            "media": [],
            "citations": ["Request Change"],
        },
    })


async def execute_get_action_capabilities(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
    origin: str = "admin",
) -> str:
    if origin == "portal":
        return _truncate({"message": "Portal users cannot execute admin edit actions"})
    return _truncate(get_action_capabilities())


async def execute_get_setter_snapshot(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
    origin: str = "admin",
) -> str:
    if origin == "portal":
        return _truncate({"error": "Not available in portal"})

    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    setter_key = args.get("setter_key")
    surface = args.get("surface")

    try:
        resp = await supabase._request(
            supabase.main_client,
            "GET",
            "/entities",
            params={"id": f"eq.{entity_id}", "select": "id,name,system_config"},
            label="data_chat_setter_snapshot",
        )
        rows = resp.json()
        if not rows:
            return _truncate({"error": "Entity not found"})

        entity = rows[0]
        system_config = entity.get("system_config") or {}
        setters = system_config.get("setters") or {}
        if not setters:
            return _truncate({"error": "No setters configured for this entity"})

        selected_key = setter_key if setter_key in setters else None
        if not selected_key:
            for key, value in setters.items():
                if isinstance(value, dict) and value.get("is_default"):
                    selected_key = key
                    break
        if not selected_key:
            selected_key = next(iter(setters.keys()))

        setter = setters.get(selected_key) or {}
        data: dict[str, Any] = {
            "entity_id": entity_id,
            "entity_name": entity.get("name"),
            "setter_key": selected_key,
        }
        if surface:
            data["surface"] = surface
            data["value"] = setter.get(surface)
        else:
            data["setter"] = setter
        return _truncate(data)
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_client_settings_snapshot(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
    origin: str = "admin",
) -> str:
    if origin == "portal":
        return _truncate({"error": "Not available in portal"})

    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    try:
        resp = await supabase._request(
            supabase.main_client,
            "GET",
            "/entities",
            params={
                "id": f"eq.{entity_id}",
                "select": "id,name,journey_stage,timezone,business_phone,notes,average_booking_value,system_config",
            },
            label="data_chat_client_settings_snapshot",
        )
        rows = resp.json()
        if not rows:
            return _truncate({"error": "Entity not found"})

        entity = rows[0]
        system_config = entity.get("system_config") or {}
        notifications = (system_config.get("notifications") or {}) if isinstance(system_config, dict) else {}
        return _truncate({
            "entity_id": entity_id,
            "entity_name": entity.get("name"),
            "journey_stage": entity.get("journey_stage"),
            "timezone": entity.get("timezone"),
            "business_phone": entity.get("business_phone"),
            "notes": entity.get("notes"),
            "average_booking_value": entity.get("average_booking_value"),
            "sms_provider": system_config.get("sms_provider"),
            "pause_bot_on_human_activity": system_config.get("pause_bot_on_human_activity"),
            "human_takeover_minutes": system_config.get("human_takeover_minutes"),
            "notification_recipients": notifications.get("recipients") or [],
        })
    except Exception as e:
        return _truncate({"error": str(e)})


async def execute_get_outreach_template_snapshot(
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
    origin: str = "admin",
) -> str:
    if origin == "portal":
        return _truncate({"error": "Not available in portal"})

    entity_id = args.get("entity_id", "")
    if entity_id not in entity_ids:
        return _truncate({"error": "Entity not accessible"})

    try:
        resp = await supabase._request(
            supabase.main_client,
            "GET",
            "/outreach_templates",
            params={
                "entity_id": f"eq.{entity_id}",
                "select": "id,entity_id,form_service_interest,is_appointment_template,setter_key,calendar_id,calendar_name,positions,is_active,created_at",
                "order": "created_at.asc",
                "limit": "200",
            },
            label="data_chat_outreach_template_snapshot",
        )
        rows = resp.json() if isinstance(resp.json(), list) else []
        return _truncate({
            "entity_id": entity_id,
            "template_count": len(rows),
            "templates": rows,
        })
    except Exception as e:
        return _truncate({"error": str(e)})


# ── Tool Registry ──

TOOL_REGISTRY: dict[str, Any] = {
    "query_leads": execute_query_leads,
    "query_bookings": execute_query_bookings,
    "query_call_logs": execute_query_call_logs,
    "get_dashboard_stats": execute_get_dashboard_stats,
    "get_time_series": execute_get_time_series,
    "get_ai_costs": execute_get_ai_costs,
    "query_workflow_runs": execute_query_workflow_runs,
    "get_reactivation_stats": execute_get_reactivation_stats,
    "search_knowledge_base": execute_search_knowledge_base,
    "get_chat_history": execute_get_chat_history,
    "search_chat_messages": execute_search_chat_messages,
    "query_scheduled_messages": execute_query_scheduled_messages,
    "query_outreach_templates": execute_query_outreach_templates,
    "get_outreach_performance": execute_get_outreach_performance,
    "search_platform_knowledge": execute_search_platform_knowledge,
    "draft_change_request": execute_draft_change_request,
    "get_action_capabilities": execute_get_action_capabilities,
    "get_setter_snapshot": execute_get_setter_snapshot,
    "get_client_settings_snapshot": execute_get_client_settings_snapshot,
    "get_outreach_template_snapshot": execute_get_outreach_template_snapshot,
    "get_onboarding_template": execute_get_onboarding_template,
    "query_onboarding_queue": execute_query_onboarding_queue,
    "get_entity_onboarding_detail": execute_get_entity_onboarding_detail,
    "get_setter_config": execute_get_setter_config,
    "get_entity_info": execute_get_entity_info,
}


async def execute_tool(
    name: str,
    args: dict,
    entity_ids: list[str],
    tz_name: str = "UTC",
    tenant_id: str | None = None,
    origin: str = "admin",
) -> str:
    """Execute a tool by name. Returns serialized result string."""
    executor = TOOL_REGISTRY.get(name)
    if not executor:
        return json.dumps({"error": f"Unknown tool: {name}"})
    try:
        parameters = inspect.signature(executor).parameters
        kwargs: dict[str, Any] = {}
        if "tenant_id" in parameters:
            kwargs["tenant_id"] = tenant_id
        if "origin" in parameters:
            kwargs["origin"] = origin
        if kwargs:
            return await executor(args, entity_ids, tz_name, **kwargs)
        return await executor(args, entity_ids, tz_name)
    except Exception as e:
        logger.exception("Tool execution error: %s", name)
        return json.dumps({"error": f"Tool {name} failed: {str(e)}"})
