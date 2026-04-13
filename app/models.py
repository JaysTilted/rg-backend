"""Pydantic models — request/response schemas and pipeline data structures."""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel


# ============================================================================
# REQUEST BODIES
# ============================================================================


class ReplyWebhookBody(BaseModel):
    """POST body from the reply webhook (sent by GHL).

    All fields that were previously query params are now also in the body.
    The webhook handler reads body first, falls back to query params.
    """

    payload: str = ""  # Message text (may contain ||| separators)
    userID: str = ""  # GHL Contact ID
    userFullName: str = ""
    userEmail: str = ""
    userPhone: str = ""
    # Channel & routing (moved from query params)
    ReplyChannel: str = ""
    AgentType: str = ""
    # Outreach fields (moved from query params)
    personalizedMessage: str = ""
    personalizedfollowup1: str = ""
    personalizedfollowup2: str = ""
    personalizedfollowup3: str = ""
    outreach_position: str = ""
    # Note: initialMessage + newLeadFollowUp1-4 are legacy fields,
    # only sent via query params by older GHL workflows.
    # Attachments (moved from query params)
    attachments: str = ""
    # Entity ID (also in URL path, body is authoritative)
    entityId: str = ""
    # Form interest (new field — for later use)
    formInterest: str = ""


class FollowUpWebhookBody(BaseModel):
    """POST body from the follow-up webhook (sent by GHL).

    All fields are now in the body. Query params kept as fallback
    for backwards compatibility with older GHL workflow versions.
    """

    User_Id: str = ""
    entityId: str = ""
    ReplyChannel: str = ""
    name: str = ""
    email: str = ""
    phone: str = ""


class OutreachResolverBody(BaseModel):
    """POST body for the Outreach Resolver webhook (sent by GHL).

    GHL calls this when a lead fills out a form (Meta Ads, website) or
    when an appointment reminder fires. The resolver matches the lead's
    service interest to an outreach template and returns resolved messages.
    """

    # Lead identity
    id: str = ""  # GHL contact ID
    entityId: str = ""  # Entity ID (fallback if not in URL path)
    name: str = ""
    phone: str = ""
    email: str = ""
    source: str = "Other"

    # Template matching
    form_service_interest: str = ""

    # Location variables (for {{location.*}} substitution)
    location_name: str = ""
    location_address: str = ""
    location_city: str = ""
    location_state: str = ""

    # Appointment variables (for {{appointment.*}} substitution)
    appointment_mode: bool = False  # True = skip lead creation, just resolve templates
    appointment_title: str = ""
    appointment_start_date: str = ""
    appointment_start_time: str = ""
    appointment_day_of_week: str = ""
    appointment_reschedule_link: str = ""
    appointment_google_calendar_link: str = ""
    appointment_ical_link: str = ""


class ReactivateBody(BaseModel):
    """POST body from GHL Auto-Reactivate workflow.

    GHL calls this when a lead has been dormant for 45 days and the
    reactivation timer fires. Python generates a personalized 6-position
    drip (SMS + Email) and returns it in the HTTP response for GHL to deliver.

    Channel filtering happens on the GHL side before this call — IG/FB/WhatsApp
    contacts are rerouted to SMS or Email (or skipped if neither available).
    """

    id: str = ""              # GHL Contact ID
    entityId: str = ""        # Entity ID (also in URL path)
    name: str = ""
    phone: str = ""
    email: str = ""
    ReplyChannel: str = "SMS"  # Adjusted channel (IG/FB/WA already filtered by GHL)


class MissedCallWebhookBody(BaseModel):
    """POST body from GHL missed call webhook.

    GHL triggers this when an inbound call goes unanswered (missed, no-answer,
    voicemail). The workflow generates a personalized text-back in the background.

    GHL payload sends: {id, name, email, phone, entityId}
    - id = GHL contact ID (mapped from contactId for backwards compat)
    - entityId = entity UUID (also accepted via URL path as fallback)
    """

    id: str = ""  # GHL contact ID (new GHL format)
    contactId: str = ""  # GHL contact ID (legacy/fallback)
    name: str = ""  # Contact name
    email: str = ""  # Contact email
    phone: str = ""  # Caller phone number
    entityId: str = ""  # Entity ID (body preferred, URL path is fallback)
    callStatus: str = ""  # "missed", "no-answer", "voicemail", "error"
    voicemailTranscript: str = ""  # Transcription if voicemail left
    direction: str = "inbound"
    timestamp: str = ""  # ISO datetime of the call

    @property
    def resolved_contact_id(self) -> str:
        """Return contact ID from either new (id) or legacy (contactId) field."""
        return self.id or self.contactId


class CleanChatHistoryBody(BaseModel):
    """POST body for clean chat history webhook.

    Deletes the most recent AI message from a contact's chat history.
    Called from the dashboard to undo the last AI response.
    """

    id: str = ""  # GHL contact ID
    name: str = ""
    email: str = ""
    phone: str = ""
    entityId: str = ""  # Entity ID (body preferred, URL path is fallback)


class BookingWebhookBody(BaseModel):
    """POST body from GHL booking logger webhook.

    GHL calls this when an appointment is created, confirmed, or cancelled.
    Logs the booking into client_bookings, determines if AI booked it,
    checks business hours, and returns metadata for GHL opportunity
    custom field updates.
    """

    contactId: str = ""  # GHL contact ID
    appointmentID: str = ""  # GHL appointment ID
    calendarName: str = ""  # Calendar/service name
    leadSource: str = ""  # How the lead found the business
    appointmentDateTime: str = ""  # ISO datetime of appointment
    appointmentEndTime: str = ""  # ISO datetime of appointment end
    appointmentTimezone: str = ""  # Timezone of appointment
    status: str = ""  # "confirmed", "cancelled", etc.
    entityId: str = ""  # Entity ID (body preferred, URL path fallback)
    interest: str = ""  # form_service_interest from GHL
    id: str = ""  # GHL contact ID (alternate field)
    name: str = ""
    email: str = ""
    phone: str = ""

    @property
    def resolved_contact_id(self) -> str:
        """Return contact ID from contactId or id field."""
        return self.contactId or self.id


class UpdateKBBody(BaseModel):
    """POST body for knowledge base update webhook.

    Triggers the embedding pipeline: chunks content, generates OpenAI embeddings,
    and inserts into the documents vector store table.
    """

    title: str = ""
    content: str = ""
    tag: str  # Category tag — used as metadata key for filtering
    action: str = "upsert"  # "delete" removes embeddings only, "upsert" replaces
    kb_id: str = ""  # knowledge_base_articles.id — stored as article_id FK on documents


class ManualLearningTestBody(BaseModel):
    """POST body for manual learning test endpoint.

    Accepts inline synthetic messages and calls so tests don't touch the database.
    """

    client_id: str  # Real client ID (needed to load prompts + search KB)
    messages: list[dict] = []  # Synthetic manual messages [{content, channel}]
    calls: list[dict] = []  # Synthetic call logs [{summary, transcript, direction, status, duration_seconds}]
    model: str = "google/gemini-2.5-flash"
    call_mode: str = "transcript"


# ============================================================================
# TOKEN USAGE TRACKING
# ============================================================================

# Model pricing per 1M tokens (input, output) — Google direct rates (no markup).
# Used as fallback when actual_cost is not available (e.g. Google Gemini direct calls).
_MODEL_PRICING: dict[str, tuple[float, float]] = {
    "google/gemini-2.5-flash": (0.15, 0.60),
    "google/gemini-2.5-pro": (1.25, 10.00),
    "google/gemini-3.1-pro-preview": (1.25, 10.00),
    "google/gemini-3-flash-preview": (0.15, 0.60),
    "google/gemini-3.1-flash-lite-preview": (0.02, 0.10),
    "anthropic/claude-opus-4.6": (15.00, 75.00),
}


@dataclass
class TokenUsage:
    """Accumulator for AI token usage across a pipeline run."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    call_count: int = 0
    calls: list[dict[str, Any]] = field(default_factory=list)

    def add(
        self,
        prompt: int,
        completion: int,
        total: int,
        model: str = "",
        label: str = "",
        actual_cost: float | None = None,
        provider: str = "openrouter",
        response: str = "",
    ) -> None:
        """Record one AI call's usage.

        actual_cost: exact cost from OpenRouter's usage.cost field.
        When provided, used directly. Otherwise, calculated from _MODEL_PRICING
        (no markup — used for Google Gemini direct fallback calls).

        provider: which provider handled this call. Values:
        "openrouter", "google_direct", "anthropic_direct", "openai_direct",
        "deepseek_direct", "xai_direct".

        response: full LLM response text (no truncation).
        """
        self.prompt_tokens += prompt
        self.completion_tokens += completion
        self.total_tokens += total
        self.call_count += 1
        if actual_cost is not None:
            cost = actual_cost
        else:
            inp_rate, out_rate = _MODEL_PRICING.get(model, (1.25, 10.00))
            cost = (prompt * inp_rate + completion * out_rate) / 1_000_000
        entry: dict[str, Any] = {
            "model": model,
            "provider": provider,
            "label": label,
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": total,
            "cost_usd": round(cost, 6),
        }
        if response:
            entry["response"] = response
        self.calls.append(entry)

    def total_cost(self) -> float:
        """Total cost from all recorded calls."""
        return sum(c.get("cost_usd", 0) for c in self.calls)

    # Keep backward compat alias for test runner and artifacts
    estimated_cost = total_cost

    def summary(self) -> dict[str, Any]:
        """Return a summary dict for API responses / test results."""
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "call_count": self.call_count,
            "cost_usd": round(self.total_cost(), 6),
            "estimated_cost_usd": round(self.total_cost(), 4),  # backward compat
        }


# ============================================================================
# PIPELINE CONTEXT — mutable state passed through the pipeline
# ============================================================================


@dataclass
class PipelineContext:
    """Accumulated state passed through the pipeline steps.

    Created at the webhook route level with request data, then populated
    progressively by each pipeline step (@task). Replaces n8n's accumulated
    flow data (Set/Code node outputs).
    """

    # --- Request data (set at webhook route) ---
    entity_id: str = ""
    contact_id: str = ""  # userID / User_Id from webhook
    trigger_type: str = ""  # "reply" or "followup"
    channel: str = "SMS"  # ReplyChannel (SMS, Email, Instagram, etc.)
    agent_type: str = ""  # Setter key (setter_1, setter_2, etc.) — from GHL contact's agent_type_new field
    raw_payload: str = ""  # Raw payload string (may contain |||)
    message: str = ""  # Cleaned message text
    contact_phone: str = ""
    contact_email: str = ""
    contact_name: str = ""
    contact_source: str = ""

    # --- Entity config (set during data loading) ---
    config: dict[str, Any] = field(default_factory=dict)
    slug: str = ""  # Client slug for Prefect naming / logging

    # --- GHL contact (set during data loading) ---
    contact: dict[str, Any] = field(default_factory=dict)
    contact_tags: list[str] = field(default_factory=list)

    # --- Lead (set during data loading) ---
    lead: dict[str, Any] | None = None

    # --- Prompts (set during data loading, with test mode override applied) ---
    prompts: dict[str, str] = field(default_factory=dict)
    media_library: list[dict[str, Any]] = field(default_factory=list)  # Resolved (test or production)
    followup_media_library: list[dict[str, Any]] = field(default_factory=list)  # Follow-up agent media + case study media

    # --- Conversation context (set during context gathering) ---
    chat_history: list[dict[str, Any]] = field(default_factory=list)
    attachments: list[dict[str, Any]] = field(default_factory=list)
    attachment_urls: list[str] = field(default_factory=list)  # Raw URLs from webhook (kept for backward compat, unused)
    pending_attachments: list[dict[str, Any]] = field(default_factory=list)  # From conversation sync: [{ghl_message_id, urls, body, date_added}]
    bookings: list[dict[str, Any]] = field(default_factory=list)
    all_bookings: list[dict[str, Any]] = field(default_factory=list)  # ALL appts (incl cancelled/no-show)
    call_logs: list[dict[str, Any]] = field(default_factory=list)
    timeline: str = ""  # Formatted conversation timeline for agent prompts

    # --- Booking context ---
    has_upcoming_booking: bool = False
    upcoming_booking_text: str = ""  # Formatted upcoming confirmed appointments (with IDs)
    past_booking_text: str = ""  # Formatted past 30-day appointment history

    # --- Outreach data (for outreach sync in Step 3.3) ---
    outreach_fields: dict[str, str] = field(default_factory=dict)

    # --- Flags ---
    is_test_mode: bool = False
    transfer_triggered: bool = False  # Set True if agent calls transfer_to_human tool
    no_reply_sent: bool = False  # True when dont_respond skips agent — signals to post-processing
    determination_only: bool = False  # Short-circuit follow-up after determination agent (skip media + text)
    media_only: bool = False  # Short-circuit follow-up after media selector (skip text generator)
    text_generator_only: bool = False  # Skip determination + media selector, jump to text generator
    mock_media: dict | None = None  # Mock media data for text_generator_only {name, type, description}

    # --- GHL credentials (per-request, from client config) ---
    ghl_api_key: str = ""
    ghl_location_id: str = ""

    # --- AI API keys (per-request, resolved from tenant at pipeline start) ---
    openrouter_api_key: str = ""
    tenant_ai_keys: dict[str, str] = field(default_factory=dict)

    # --- Token usage tracking ---
    token_usage: TokenUsage = field(default_factory=TokenUsage)

    # --- Shared GHL client (created once at pipeline start) ---
    ghl: Any = None

    # --- Pipeline/Opportunity (set by ensure_opportunity) ---
    opportunity_id: str = ""
    pipeline_id: str = ""
    current_pipeline_stage: str = ""
    pipeline_name_to_id: dict[str, str] = field(default_factory=dict)
    pipeline_id_to_name: dict[str, str] = field(default_factory=dict)

    # --- Result (set by classifier/agent) ---
    response_path: str = ""  # "respond", "human", "opt_out", "dont_respond", "stop_bot"
    response_reason: str = ""
    agent_response: str = ""  # Raw agent response text
    messages: list[str] = field(default_factory=list)  # Split messages (1-3)
    chat_history_text: str = ""  # Serialized chat history for webhook response

    # --- Artifact diagnostics (populated by each step for Prefect artifact) ---
    sync_stats: dict[str, Any] = field(default_factory=dict)  # messages_synced, calls_synced
    tool_calls_log: list[dict[str, Any]] = field(default_factory=list)  # [{name, args, result_preview}]
    agent_iterations: int = 0
    classification_gates: list[dict[str, str]] = field(default_factory=list)  # [{gate, result, reason}]
    timeline_stats: dict[str, Any] = field(default_factory=dict)  # {messages, calls, attachments, channels}
    followup_stats: dict[str, Any] = field(default_factory=dict)  # {count, media_*, filtering}

    # --- Pre-agent extraction results (set before agent call) ---
    extraction_result: dict[str, Any] = field(default_factory=dict)
    qualification_status: str = "undetermined"  # "qualified", "disqualified", "undetermined"
    qualification_notes: dict[str, Any] | None = None  # JSONB: {matched_services, criteria}
    form_interest: str = ""  # From lead record or webhook body

    # --- Reply media (send_response structured output) ---
    reply_media_list: list[dict[str, Any]] = field(default_factory=list)  # Shuffled reply media for index resolution
    selected_media_idx: int = 0           # Agent's media selection (0 = none)
    selected_media_url: str = ""          # Resolved media URL
    selected_media_type: str = ""         # Resolved media type
    selected_media_name: str = ""         # Resolved media name
    selected_media_description: str = ""  # Resolved media description

    # --- Security (post-agent compliance) ---
    security_original_response: str = ""          # Pre-security text (audit trail)
    security_replacements_applied: bool = False   # Whether term replacements changed text
    security_check_result: dict[str, Any] = field(default_factory=dict)  # {safe, violations, rewritten} or {error}

    # --- Prompt capture (all runs — lightweight label + variables only) ---
    prompt_log: list[dict[str, Any]] = field(default_factory=list)  # [{label, variables?}]

    # --- Compiled config (from system_config JSONB, computed once after load_data) ---
    compiled: dict[str, Any] = field(default_factory=dict)  # Pre-compiled prompt sections from system_config

    # --- Cached derived values (computed once after load_data, reused everywhere) ---
    tz: Any = None                           # ZoneInfo — from get_timezone(config)
    offers_text: str | None = None           # from format_offers_for_prompt(ctx)
    supported_languages: list[str] = field(default_factory=lambda: ["English"])  # from resolve_supported_languages(ctx)
    services_text: str = ""                  # from format_services_for_followup(service_config)

    # --- Pipeline run timing (set at pipeline start for duration tracking) ---
    pipeline_start_time: float = 0.0

    # --- Workflow tracking (set from WorkflowTracker.run_id at pipeline start) ---
    workflow_run_id: str = ""

    # --- Standard webhook data (skip get_contact() when available) ---
    webhook_contact_data: dict[str, Any] = field(default_factory=dict)  # Raw webhook payload — used by data_loading to skip GHL API call

    # --- Scheduled message context (set by poller/scheduler when firing) ---
    scheduled_message: dict[str, Any] | None = None  # Full DB row from scheduled_messages — so followup.py can read source, metadata, position


# ============================================================================
# HELPERS
# ============================================================================


def log_prompt(
    ctx: PipelineContext,
    label: str,
    system: str = "",
    user: str = "",
    variables: dict[str, str] | None = None,
) -> None:
    """Capture a prompt sent to an LLM — lightweight label + variables only.

    Always logs (not test-mode-gated). Prompt text (system/user) is NOT stored
    — templates live in code, only the variable parts matter for diagnostics.
    ~100 bytes per entry.

    variables: optional dict of {name: value} for the client-specific parts
               that were substituted into the system prompt template.
    """
    # Skip variables already shown elsewhere or redundant with pipeline_run columns.
    # Keep all client-specific prompts + dynamic per-contact context (bookings, timers).
    _SKIP_VARS = frozenset({
        "timeline",              # full conversation history — already in chat history
        "channel",               # already a column on pipeline_runs
        "reply_channel",         # same as channel
        "current_datetime",      # already on created_at
        "conversation_context",  # security check snippet — already in chat history
    })
    entry: dict[str, Any] = {"label": label}
    if variables:
        filtered = {
            k: v for k, v in variables.items()
            if v and v != "None" and k not in _SKIP_VARS
        }
        if filtered:
            entry["variables"] = filtered
    ctx.prompt_log.append(entry)


def parse_payload(payload: str) -> str:
    """Parse webhook payload — split on ||| separators, filter nulls.

    GHL sends multi-part messages separated by ||| (e.g., text + attachment
    descriptions). We join the non-null parts into a single message.
    """
    if not payload:
        return ""
    parts = payload.split("|||")
    cleaned = [
        p.strip()
        for p in parts
        if p and p.strip() and p.strip().lower() != "null"
    ]
    return " ".join(cleaned)


def parse_attachment_urls(attachments_str: str) -> list[str]:
    """Parse attachment URLs from ||| separated query param string.

    GHL may send trailing ||| separators, which leave stray pipe chars
    after splitting. We filter to only keep actual URLs (start with http).

    Also handles:
    - HTML entity decoding (GHL sometimes sends &#x3D; instead of =)
    - Comma-separated URLs within a single ||| segment (e.g., "url1, url2|||url3")
    """
    if not attachments_str:
        return []
    segments = attachments_str.split("|||")
    urls: list[str] = []
    for segment in segments:
        # Split on ", " to handle multiple URLs packed in one segment
        for part in segment.split(", "):
            decoded = html.unescape(part.strip())
            if decoded and decoded.startswith("http"):
                urls.append(decoded)
    return urls
