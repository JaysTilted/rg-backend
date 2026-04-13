"""Pydantic models for the test runner and pipeline runs.

Hierarchy:
  TestScenario -> LeadConfig, TestContext, Turn
  TestContext  -> AppointmentConfig, CallLogConfig, DripConfig, SeedMessage
  TestRunRequest  -> list[TestScenario]
  TestRunResponse -> summary of a completed batch
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, field_validator, model_validator


# ---------------------------------------------------------------------------
# MockGHLClient configuration (dataclass — not a request model)
# ---------------------------------------------------------------------------

@dataclass
class MockGHLConfig:
    """Configurable return data for MockGHLClient read methods."""

    contact: dict[str, Any] = field(default_factory=dict)
    timezone: str = "America/Chicago"
    appointments: list[dict[str, Any]] = field(default_factory=list)
    free_slots: dict[str, Any] = field(default_factory=dict)
    pipelines: list[dict[str, Any]] = field(default_factory=list)
    custom_field_defs: dict[str, str] = field(default_factory=dict)
    conversation_messages: list[dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Lead configuration
# ---------------------------------------------------------------------------

class LeadConfig(BaseModel):
    """Contact identity for the test — ONE source of truth."""

    first_name: str = "Sarah"
    last_name: str = "Johnson"
    email: str = "sarah.johnson@gmail.com"
    phone: str = "+14155559283"
    source: str = "Direct Test Runner"


# ---------------------------------------------------------------------------
# Context building blocks
# ---------------------------------------------------------------------------

class AppointmentConfig(BaseModel):
    """Declarative appointment to inject before test runs."""

    calendar_id: str = ""
    calendar_name: str = "Test Calendar"
    status: str = "confirmed"
    start: str = ""  # ISO datetime string, e.g. "2026-03-27T16:00:00". Empty = 2 days from now.
    timezone: str = ""  # IANA timezone, e.g. "America/Chicago". Stored on the booking record.


class CallLogConfig(BaseModel):
    """Declarative call log to inject before test runs."""

    direction: str = "inbound"
    summary: str = "Previous call with customer"
    duration_seconds: int = 120
    status: str = "answered"
    sentiment: str = "Neutral"
    transcript: str = ""
    after_hours: bool = False
    created_at: str = ""  # ISO datetime string. Empty = 24 hours ago.


class CustomDripMessage(BaseModel):
    """Single custom drip message (Mode B)."""

    content: str
    hours_ago: float = 168.0  # Default 1 week ago
    timestamp: str = ""  # Optional exact ISO datetime for sandbox seeding


class DripConfig(BaseModel):
    """Outreach drip seeding — template-based OR custom raw messages.

    Mode A (template): set `template` + `messages_sent`.
    Mode B (custom): set `messages` list.
    """

    template: str = ""
    template_ids: list[str] = []
    messages_sent: int = 0
    messages: list[CustomDripMessage] = []

    @model_validator(mode="after")
    def template_or_messages(self) -> DripConfig:
        has_template = bool(self.template or self.template_ids)
        has_messages = bool(self.messages)
        if has_template and has_messages:
            raise ValueError("DripConfig: set 'template' OR 'messages', not both")
        if not has_template and not has_messages:
            raise ValueError("DripConfig: must set 'template', 'template_ids', or 'messages'")
        return self


class SeedMessage(BaseModel):
    """Single message in seed conversation history."""

    role: Literal["lead", "ai"]
    content: str

    hours_ago: float = 0
    timestamp: str = ""  # Optional exact ISO datetime for sandbox seeding
    channel: str = "SMS"
    source: str = ""

    @field_validator("content")
    @classmethod
    def content_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("seed message content cannot be empty")
        return v


class TestContext(BaseModel):
    """Declarative world-state to set up before the test runs."""

    appointments: list[AppointmentConfig] = []
    call_logs: list[CallLogConfig] = []
    drip_messages: DripConfig | None = None
    seed_messages: list[SeedMessage] = []
    tags: list[str] = []
    custom_fields: dict[str, Any] = {}
    qualification_status: str = ""
    qualification_notes: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Conversation turns
# ---------------------------------------------------------------------------

class Turn(BaseModel):
    """Single conversation turn in a test scenario."""

    message: str = ""            # Hardcoded lead message
    intent: str = ""             # Dynamic generation from intent
    expect_path: str = ""        # Expected classification path
    followups: int = 0           # How many follow-ups to run after this turn
    followup_cadence: list[float] | None = None  # Per-followup intervals (hours)
    attachments: list[str] = []

    @model_validator(mode="after")
    def require_message_or_intent(self) -> Turn:
        if not self.message and not self.intent:
            raise ValueError("Turn must have 'message' or 'intent' (both empty)")
        return self


# ---------------------------------------------------------------------------
# Test scenario (top-level unit)
# ---------------------------------------------------------------------------

class TestScenario(BaseModel):
    """Complete test definition — one scenario, possibly multi-turn."""

    id: str
    category: str = ""
    description: str = ""
    lead: LeadConfig = LeadConfig()
    context: TestContext = TestContext()
    conversation: list[Turn]
    channel: str = "SMS"
    agent_type: str = "setter_1"

    # Runner flags
    determination_only: bool = False
    media_only: bool = False
    text_generator_only: bool = False
    mock_media: dict[str, Any] | None = None

    @field_validator("conversation")
    @classmethod
    def conversation_not_empty(cls, v: list) -> list:
        if not v:
            raise ValueError("test scenario must have at least one conversation turn")
        return v


# ---------------------------------------------------------------------------
# Request / response
# ---------------------------------------------------------------------------

class TestRunRequest(BaseModel):
    """POST /testing/run request body."""

    entity_id: str
    test_scenarios: list[TestScenario] = []

    # Preset execution (stub for Phase 6)
    preset_ids: list[str] | None = None
    overrides: dict[str, Any] | None = None


class TestRunResponse(BaseModel):
    """POST /testing/run response body."""

    timestamp: str
    entity_id: str
    entity_name: str
    entity_type: str
    agent_type: str
    channel: str
    test_count: int
    passed: int
    failed: int
    duration_seconds: float
    run_id: str | None = None
    token_usage: dict[str, Any] = {}
