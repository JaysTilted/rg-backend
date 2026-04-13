"""
LLM Call Registry — Maps every LLM call to its prompt components for token accounting.

Each entry references the actual hardcoded prompt constants (not copies), so changes
to prompts are automatically reflected in token counts. Variable keys map to
ctx.compiled[key] or ctx.prompts[key] — the same keys the frontend has from system_config.

Used by the /api/prompt-token-map endpoint to serve accurate token estimates.

history_avg_tokens values are based on actual Dr. K Beauty production data (Mar 2026):
  - Avg 7 messages per lead, ~355 tokens conversation
  - Avg 1.4 call summaries per lead, ~99 tokens
  - Avg 0.4 bookings per lead, ~11 tokens
  - Total context per lead: ~465 tokens
"""

from typing import TypedDict

# ── Import all hardcoded prompt constants ──

from app.text_engine.agent import (
    _REPLY_MEDIA_SELECTOR_PROMPT,
    _SECURITY_RULES,
    _LEAD_CONTEXT_RULES,
    _BOOKING_RULES,
    _KB_RULES,
    _TRANSFER_RULES,
    _OPERATIONAL_CONTEXT,
    _ATTACHMENT_HANDLING,
)
from app.text_engine.classification import (
    _TTH_SYSTEM_PROMPT,
    _RD_SYSTEM_PROMPT,
)
from app.text_engine.post_processing import (
    _EXTRACT_SYSTEM_PROMPT,
    _STOP_FOLLOWUP_PROMPT,
    _SLIM_NEGATIVE_PROMPT,
)
from app.text_engine.security import _SECURITY_CHECK_SYSTEM
from app.text_engine.qualification import _QUAL_SYSTEM_PROMPT
from app.text_engine.followup import (
    _DETERMINATION_PROMPT_BASE,
    _DETERMINATION_RESCHEDULE_SECTION,
    _OUTPUT_FORMAT_WITH_RESCHEDULE,
    _SMART_SCHEDULER_PROMPT,
    _FOLLOWUP_AGENT_SYSTEM,
    _MEDIA_SELECTOR_PROMPT as _FOLLOWUP_MEDIA_SELECTOR_PROMPT,
)
from app.text_engine.attachments import _SYNTHESIS_SYSTEM_PROMPT
from app.text_engine.utils import _EMAIL_FORMAT_PROMPT
from app.text_engine.conversation_sync import _CALL_SUMMARIZER_PROMPT
from app.workflows.missed_call_textback import (
    _AI_GATE_SYSTEM_PROMPT,
    _TEXTBACK_SYSTEM_PROMPT,
)
from app.workflows.reactivation import (
    _QUAL_GATE_SYSTEM,
    _ROLE_SMS,
    _VOICE,
    _GLOBAL_RULES,
    _BANNED_PATTERNS,
    _ANTI_HALLUCINATION,
    _MEDICAL_CONDITIONS,
    _SMS_FORMATTING,
    _P1_STANDALONE_FRAMEWORK,
    _SERVICE_MATCHER_SYSTEM,
    _BATCH_SECURITY_SYSTEM,
)


class LlmCallEntry(TypedDict, total=False):
    """Registry entry for a single LLM call."""
    hardcoded_texts: list[str]       # References to actual prompt constants
    variable_keys: list[str]         # Config keys injected from ctx.compiled/ctx.prompts
    service_config: bool             # True if service_config JSON is formatted and injected
    history: bool                    # True if ctx.timeline (conversation history) is injected
    history_avg_tokens: int          # Average conversation history tokens
    output_avg_tokens: int           # Average output tokens
    default_model: str               # Full OpenRouter model ID
    default_temperature: float       # Default temperature (0.0–2.0)


REGISTRY: dict[str, LlmCallEntry] = {

    # ══════════════════════════════════════════════════
    # TEXT ENGINE — REPLY PATH
    # ══════════════════════════════════════════════════

    "reply_agent": {
        "hardcoded_texts": [
            _SECURITY_RULES,
            _LEAD_CONTEXT_RULES,
            _BOOKING_RULES,
            _KB_RULES,
            _TRANSFER_RULES,
            _OPERATIONAL_CONTEXT,
            _ATTACHMENT_HANDLING,
        ],
        "variable_keys": [
            "bot_persona_full", "agent_prompt", "security_protections",
            "compliance_rules", "offers_text", "offers_deployment",
            "booking_config", "booking_rules", "case_studies_reply",
        ],
        "service_config": True,
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 400,
        "default_model": "google/gemini-2.5-pro",
        "default_temperature": 0.7,
    },

    "reply_media": {
        "hardcoded_texts": [_REPLY_MEDIA_SELECTOR_PROMPT],
        "variable_keys": ["agent_prompt", "bot_persona_media"],
        # media_library and services_text are also injected but vary per client
        "service_config": False,
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 50,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.2,
    },

    "transfer_detection": {
        "hardcoded_texts": [_TTH_SYSTEM_PROMPT],
        "variable_keys": ["transfer_to_human_prompt", "opt_out_prompt"],
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "response_determination": {
        "hardcoded_texts": [_RD_SYSTEM_PROMPT],
        "variable_keys": [],  # supported_languages appended but tiny
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 150,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "contact_extraction": {
        "hardcoded_texts": [_EXTRACT_SYSTEM_PROMPT],
        "variable_keys": ["data_collection_fields"],
        "history": False,
        "history_avg_tokens": 0,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "stop_detection": {
        "hardcoded_texts": [_STOP_FOLLOWUP_PROMPT],
        "variable_keys": [],
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 100,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "pipeline_management": {
        "hardcoded_texts": [_SLIM_NEGATIVE_PROMPT],
        "variable_keys": [],
        "history": False,
        "history_avg_tokens": 150,
        "output_avg_tokens": 50,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "reply_security": {
        "hardcoded_texts": [_SECURITY_CHECK_SYSTEM],
        "variable_keys": ["security_prompt"],
        "history": True,  # partial — 2000 chars of timeline
        "history_avg_tokens": 155,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.1,
    },

    "qualification": {
        "hardcoded_texts": [_QUAL_SYSTEM_PROMPT],
        "variable_keys": [],
        "service_config": True,  # criteria injected from service_config
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.2,
    },

    "email_formatting": {
        "hardcoded_texts": [_EMAIL_FORMAT_PROMPT],
        "variable_keys": [],
        "history": False,
        "history_avg_tokens": 0,
        "output_avg_tokens": 300,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "media_analysis": {
        "hardcoded_texts": [],  # Gemini vision — prompt is minimal
        "variable_keys": [],
        "history": False,
        "history_avg_tokens": 0,
        "output_avg_tokens": 300,
        "default_model": "google/gemini-3.1-pro-preview",
        "default_temperature": 0.3,
    },

    "link_synthesis": {
        "hardcoded_texts": [_SYNTHESIS_SYSTEM_PROMPT],
        "variable_keys": [],
        "history": True,  # partial — recent messages for context
        "history_avg_tokens": 465,
        "output_avg_tokens": 400,
        "default_model": "anthropic/claude-sonnet-4.6",
        "default_temperature": 0.4,
    },

    "call_summarization": {
        "hardcoded_texts": [_CALL_SUMMARIZER_PROMPT],
        "variable_keys": [],
        "history": False,  # call transcript is input, not SMS history
        "history_avg_tokens": 0,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    # ══════════════════════════════════════════════════
    # TEXT ENGINE — FOLLOW-UP PATH
    # ══════════════════════════════════════════════════

    "followup_text": {
        "hardcoded_texts": [_FOLLOWUP_AGENT_SYSTEM],
        "variable_keys": ["security_prompt", "bot_persona_media", "followup_agent_prompt", "case_studies_followup"],
        "service_config": False,
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 300,
        "default_model": "google/gemini-2.5-pro",
        "default_temperature": 0.7,
    },

    "followup_media": {
        "hardcoded_texts": [_FOLLOWUP_MEDIA_SELECTOR_PROMPT],
        "variable_keys": ["bot_persona_media", "followup_agent_prompt"],
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 100,
        "default_model": "google/gemini-2.5-pro",
        "default_temperature": 0.3,
    },

    "followup_determination": {
        "hardcoded_texts": [
            _DETERMINATION_PROMPT_BASE,
            _DETERMINATION_RESCHEDULE_SECTION,
            _OUTPUT_FORMAT_WITH_RESCHEDULE,
        ],
        "variable_keys": [],
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "smart_scheduler": {
        "hardcoded_texts": [_SMART_SCHEDULER_PROMPT],
        "variable_keys": [],
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 150,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "followup_security": {
        "hardcoded_texts": [_SECURITY_CHECK_SYSTEM],
        "variable_keys": ["security_prompt"],
        "history": True,
        "history_avg_tokens": 155,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.1,
    },

    "followup_email_formatting": {
        "hardcoded_texts": [_EMAIL_FORMAT_PROMPT],
        "variable_keys": [],
        "history": False,
        "history_avg_tokens": 0,
        "output_avg_tokens": 300,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    # ══════════════════════════════════════════════════
    # MISSED CALL TEXT-BACK
    # ══════════════════════════════════════════════════

    "missed_call_text": {
        "hardcoded_texts": [_TEXTBACK_SYSTEM_PROMPT],
        "variable_keys": ["bot_persona_full"],
        "service_config": False,
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 1.2,
    },

    "missed_call_gate": {
        "hardcoded_texts": [_AI_GATE_SYSTEM_PROMPT],
        "variable_keys": [],
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 100,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    # ══════════════════════════════════════════════════
    # REACTIVATION
    # ══════════════════════════════════════════════════

    "reactivation_sms": {
        "hardcoded_texts": [
            _ROLE_SMS, _VOICE, _GLOBAL_RULES,
            _BANNED_PATTERNS, _ANTI_HALLUCINATION,
            _MEDICAL_CONDITIONS, _SMS_FORMATTING,
        ],
        "variable_keys": ["offers_text"],
        "service_config": True,
        "history": False,
        "history_avg_tokens": 0,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-3.1-flash-lite-preview",
        "default_temperature": 0.5,
    },

    "reactivation_p1_only": {
        "hardcoded_texts": [
            _P1_STANDALONE_FRAMEWORK,
            _ROLE_SMS, _VOICE, _GLOBAL_RULES,
            _BANNED_PATTERNS, _ANTI_HALLUCINATION,
            _MEDICAL_CONDITIONS, _SMS_FORMATTING,
        ],
        "variable_keys": ["offers_text"],
        "service_config": True,
        "history": False,
        "history_avg_tokens": 0,
        "output_avg_tokens": 250,
        "default_model": "anthropic/claude-sonnet-4.6",
        "default_temperature": 0.5,
    },

    "reactivation_qual": {
        "hardcoded_texts": [_QUAL_GATE_SYSTEM],
        "variable_keys": [],
        "service_config": True,
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 150,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.1,
    },

    "service_matcher": {
        "hardcoded_texts": [_SERVICE_MATCHER_SYSTEM],
        "variable_keys": [],
        "service_config": True,
        "history": True,
        "history_avg_tokens": 465,
        "output_avg_tokens": 150,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.1,
    },

    "reactivation_security": {
        "hardcoded_texts": [_BATCH_SECURITY_SYSTEM],
        "variable_keys": ["security_prompt"],
        "history": False,
        "history_avg_tokens": 0,
        "output_avg_tokens": 200,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.1,
    },

    # ══════════════════════════════════════════════════
    # POST-APPOINTMENT AUTOMATION
    # ══════════════════════════════════════════════════

    "post_appointment_determination": {
        "hardcoded_texts": [],  # Dynamic prompts built at runtime from conversation + actions
        "variable_keys": [],
        "history": True,  # Reads post-appointment conversation segment
        "history_avg_tokens": 300,
        "output_avg_tokens": 150,
        "default_model": "google/gemini-2.5-flash",
        "default_temperature": 0.3,
    },

    "post_appointment_generation": {
        "hardcoded_texts": [],  # Dynamic prompts built at runtime from bot persona + actions + context
        "variable_keys": ["bot_persona_full", "security_protections", "compliance_rules", "offers_text"],
        "service_config": True,
        "history": True,  # Reads conversation history for context/tone
        "history_avg_tokens": 465,
        "output_avg_tokens": 300,
        "default_model": "google/gemini-3-flash-preview",
        "default_temperature": 0.7,
    },
}
