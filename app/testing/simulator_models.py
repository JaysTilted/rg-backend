"""Pydantic models for the simulator — plan generation, execution, and analysis."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Plan generation request / response
# ---------------------------------------------------------------------------

class PersonaConfig(BaseModel):
    behaviors: list[str]           # ["friendly", "skeptical", ...]
    age_range: tuple[int, int] = (18, 65)
    gender: str = "any"            # "any" | "male" | "female"
    location: str = ""
    prompt: str = ""               # optional AI description

class ScenarioConfig(BaseModel):
    counts: dict[str, int]         # {"new_lead": 2, "price_objection": 3, ...}
    lead_source: str = "Facebook Ad"
    channel: str = "SMS"
    custom_description: str = ""
    preconditions: dict[str, Any] = {}  # {has_existing_appointment: true, appointment_date: "2026-04-05", ...}
    outreach_template_id: str | None = None  # UUID of selected outreach template (backward compat)
    outreach_template_ids: list[str] | None = None  # Multi-select: list of template UUIDs

class TurnPlan(BaseModel):
    intent: str
    followups: int = 0

class ConversationPlan(BaseModel):
    id: str
    persona: dict[str, Any]        # {name, age, gender, behavior}
    scenario_type: str
    scenario_label: str
    lead_source: str
    turns: list[TurnPlan]
    estimated_cost: float
    estimated_seconds: float
    drip_config: dict[str, Any] | None = None  # {template_id, template_name, messages, count, total_available}

class GeneratePlanRequest(BaseModel):
    entity_id: str
    setter_key: str = "setter_1"
    name: str
    personas: PersonaConfig
    scenarios: ScenarioConfig

class GeneratePlanResponse(BaseModel):
    simulation_id: str
    conversation_plans: list[ConversationPlan]
    total_estimated_cost: float
    total_estimated_seconds: float


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

class ExecuteRequest(BaseModel):
    """Body is optional — simulation_id is in the URL path."""
    pass

class SimulationStatus(BaseModel):
    status: str
    total_conversations: int
    completed_conversations: int
    elapsed_seconds: float | None = None


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

class AnalyzeEstimate(BaseModel):
    estimated_cost: float
    conversation_count: int

class RerunRequest(BaseModel):
    edited: bool = False
    config: dict[str, Any] | None = None  # only if edited=True

class RerunConversationRequest(BaseModel):
    conversation_plan_id: str
