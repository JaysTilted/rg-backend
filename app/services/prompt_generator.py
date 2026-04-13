"""AI-assisted prompt generation for the agency interface.

Provides a meta-prompting service: an AI writes/refines the prompt instructions
that other AI agents will follow. Used by the sparkle button in all dialog Textareas.
"""

import logging
import time

from pydantic import BaseModel

from app.services.ai_client import chat
from app.text_engine.model_resolver import (
    _db_defaults,
    _db_defaults_temps,
    refresh_defaults_if_stale,
)

logger = logging.getLogger(__name__)

CALL_KEY = "prompt_generator"
DEFAULT_MODEL = "anthropic/claude-sonnet-4-5"
DEFAULT_TEMP = 0.7

_META_SYSTEM = """You are a prompt engineering specialist. Your job is to write clear, effective prompt instructions that will be injected into an AI agent's system prompt.

CONTEXT: You are writing instructions for an AI lead conversion system. The AI agents handle SMS conversations with real leads — they must sound human, never robotic. The prompts you write control HOW these agents behave in specific scenarios.

RULES FOR THE PROMPTS YOU WRITE:
- Write in second person ("You should...", "When the lead...", "Never...")
- Be thorough and detailed — cover edge cases, exceptions, and nuances. A comprehensive prompt produces reliable behavior. Don't leave gaps that force the agent to guess.
- Include concrete examples when helpful (e.g., "Example: 'hey, been a while — how's the...'"). Multiple examples for complex scenarios.
- When describing behavior, explain WHEN it applies, HOW to do it, and WHY — this gives the agent enough context to handle variations
- No markdown formatting (no bullets, headers, bold) — these are injected into system prompts as plain text. Use paragraph breaks and natural language structure instead.
- Match the tone of SMS sales: casual, direct, human
- If editing an existing prompt, preserve what works and only change what the user asked for. Don't strip out details or simplify unless asked to.
- Never include meta-commentary about the prompt itself ("This prompt instructs the agent to...")
- Write the actual instructions, not a description of what the instructions should be
- Don't make up specifics about the business (prices, services, hours) — use placeholders like [service], [price range], [business name] when referencing business-specific details you don't have

FIELD TYPE MODES — adapt your output style based on what you're writing:

PROMPT mode (field_type: "prompt"):
- You're writing AI agent instructions — behavioral rules that control how an AI responds
- Write in second person: "You should...", "When the lead...", "Never..."
- Focus on behavior, edge cases, and decision-making frameworks

DESCRIPTION mode (field_type: "description"):
- You're writing business content — service descriptions, offer details, case study narratives, qualification criteria
- Write in third person or neutral voice, polished and professional
- Focus on clarity, completeness, and making the content informative
- For case studies: tell the story with results, before/after, and specifics
- For services: describe what's included, who it's for, and what makes it valuable
- For offers: make it compelling with clear value proposition and eligibility

TEMPLATE mode (field_type: "template"):
- You're writing message templates that get sent directly to leads via SMS
- MUST preserve variable placeholders exactly: {name}, {service}, {last_topic}, etc.
- Write in first person casual SMS style — short, human, no formatting
- Each template should feel like a real person texting, not a marketing blast"""


class GeneratePromptRequest(BaseModel):
    instruction: str
    existing_prompt: str = ""
    client_id: str = ""  # entity_id for cost attribution via workflow_tracker
    field_type: str = "prompt"  # "prompt" | "description" | "template"
    context: dict | None = None  # section_name, section_description, business_type, bot_name


class GeneratePromptResponse(BaseModel):
    generated_prompt: str


async def generate_prompt(req: GeneratePromptRequest) -> GeneratePromptResponse:
    """Generate or refine a prompt section based on user instruction."""
    await refresh_defaults_if_stale()

    model = _db_defaults.get(CALL_KEY, DEFAULT_MODEL)
    temperature = _db_defaults_temps.get(CALL_KEY, DEFAULT_TEMP)

    # Build the user message
    parts: list[str] = []

    parts.append(f"Field type: {req.field_type}")

    ctx = req.context or {}
    if ctx.get("section_name"):
        parts.append(f"Section: {ctx['section_name']}")
    if ctx.get("section_description"):
        parts.append(f"Purpose: {ctx['section_description']}")
    if ctx.get("business_type"):
        parts.append(f"Business type: {ctx['business_type']}")
    if ctx.get("bot_name"):
        parts.append(f"Agent name: {ctx['bot_name']}")

    if req.existing_prompt.strip():
        parts.append(f"\nCurrent prompt:\n{req.existing_prompt.strip()}")
        parts.append(f"\nUser request: {req.instruction}")
        parts.append("\nRewrite the prompt incorporating the user's feedback. Keep what works, change what they asked for.")
    else:
        parts.append(f"\nUser request: {req.instruction}")
        parts.append("\nWrite a new prompt section based on the user's description.")

    user_msg = "\n".join(parts)

    # Note: cost tracking for prompt generator will be handled by the unified
    # automation_runs table (future). For now, OpenRouter dashboard tracks spend.

    t0 = time.perf_counter()
    resp = await chat(
        messages=[
            {"role": "system", "content": _META_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        model=model,
        temperature=temperature,
        label="prompt_generator",
    )
    elapsed = time.perf_counter() - t0

    generated = resp.choices[0].message.content or ""
    # Strip any markdown code fences the model might add
    generated = generated.strip()
    if generated.startswith("```"):
        generated = "\n".join(generated.split("\n")[1:])
    if generated.endswith("```"):
        generated = "\n".join(generated.split("\n")[:-1])
    generated = generated.strip()

    logger.info(
        "PROMPT_GEN | model=%s | temp=%s | mode=%s | elapsed=%.1fs | output_len=%d",
        model, temperature,
        "refine" if req.existing_prompt.strip() else "generate",
        elapsed, len(generated),
    )

    return GeneratePromptResponse(generated_prompt=generated)
