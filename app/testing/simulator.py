"""Simulator — AI-powered conversation plan generation and execution.

Generates realistic conversation plans from persona + scenario config,
executes them through the real reply/followup pipeline, and stores
everything in the simulations table.
"""

from __future__ import annotations

import json
import logging
import random
from typing import Any
from uuid import uuid4

from app.services.ai_client import classify, set_ai_context, clear_ai_context
from app.services.supabase_client import supabase
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Startup recovery
# ---------------------------------------------------------------------------

async def recover_stale_simulations() -> None:
    """Mark stale simulations as failed on startup. Runs once."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        resp = await supabase.main_client.get(
            "/simulations",
            params={
                "status": "in.(generating,running,analyzing)",
                "updated_at": f"lt.{cutoff}",
                "select": "id,test_contact_ids,entity_id",
            },
        )
        if resp.status_code == 404:
            logger.info("SIM_RECOVERY | simulations table not installed in this backend-only setup")
            return
        stale = resp.json() if resp.status_code < 400 else []
        if not stale:
            logger.info("SIM_RECOVERY | no stale simulations found")
            return

        from app.testing.cleanup import cleanup_test_contacts

        for sim in stale:
            logger.warning("SIM_RECOVERY | marking stale simulation %s as failed", sim["id"])
            await supabase._request(
                supabase.main_client, "PATCH", "/simulations",
                params={"id": f"eq.{sim['id']}"},
                json={"status": "failed"},
                label="sim_mark_stale_failed",
            )
            if sim.get("test_contact_ids"):
                try:
                    await cleanup_test_contacts(
                        sim["test_contact_ids"],
                        entity_id=sim["entity_id"],
                        chat_table="",
                        skip_tool_executions=True,
                    )
                except Exception as e:
                    logger.warning("SIM_RECOVERY | cleanup error for %s: %s", sim["id"], e)

        logger.info("SIM_RECOVERY | recovered %d stale simulations", len(stale))
    except Exception as e:
        logger.warning("SIM_RECOVERY | failed (non-fatal): %s", e)


# ---------------------------------------------------------------------------
# Name pools for persona generation
# ---------------------------------------------------------------------------

_MALE_NAMES = [
    "James", "John", "Robert", "Michael", "David", "William", "Richard",
    "Joseph", "Thomas", "Daniel", "Matthew", "Anthony", "Mark", "Steven",
    "Andrew", "Joshua", "Kenneth", "Kevin", "Brian", "George", "Timothy",
    "Ronald", "Jason", "Edward", "Ryan", "Jacob", "Gary", "Nicholas",
    "Eric", "Jonathan", "Carlos", "Jose", "Luis", "Miguel", "Juan", "Pedro",
    "Ahmed", "Raj", "Wei", "Amir", "Omar",
]

_FEMALE_NAMES = [
    "Sarah", "Emma", "Olivia", "Ava", "Isabella", "Mia", "Sophia",
    "Charlotte", "Amelia", "Harper", "Evelyn", "Abigail", "Emily", "Ella",
    "Elizabeth", "Luna", "Sofia", "Avery", "Scarlett", "Penelope", "Layla",
    "Chloe", "Victoria", "Madison", "Eleanor", "Grace", "Nora", "Riley",
    "Zoey", "Hannah", "Maria", "Ana", "Carmen", "Rosa", "Fatima", "Priya",
    "Leila", "Yuki",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Kim", "Patel", "Chen", "Singh", "Yamamoto",
]

# ---------------------------------------------------------------------------
# Scenario type labels for the LLM
# ---------------------------------------------------------------------------

_SCENARIO_LABELS: dict[str, str] = {
    "new_lead": "New lead asking about services for the first time",
    "price_objection": "Lead interested but pushes back on pricing",
    "booking_flow": "Lead wants to book an appointment",
    "reschedule_cancel": "Lead wants to reschedule or cancel existing appointment",
    "ai_detection": "Lead asks if they're talking to a bot or AI",
    "not_interested": "Lead is not interested or wants to opt out",
    "knowledge_question": "Lead asks detailed questions about services or the business",
    "edge_case": "Unusual lead behavior — gibberish, wrong number, off-topic",
    "ghost": "Lead goes cold and stops responding mid-conversation",
    "competitor_comparison": "Lead mentions competitor pricing or services",
    "custom": "Custom scenario",
}

# ---------------------------------------------------------------------------
# Plan generation prompt
# ---------------------------------------------------------------------------

_PLAN_GEN_SYSTEM = """You generate realistic SMS conversation turn plans for testing AI sales setters.

You will receive:
- The business name and what services they offer
- Current offers/promotions
- A persona (name, age, gender, personality type)
- A scenario type describing what kind of conversation this should be
- A target number of turns

Generate a list of conversation turns. Each turn is one lead message intent — a short description (5-15 words) of what the lead says or does at that point.

CRITICAL RULES:
- The lead is a REAL POTENTIAL CUSTOMER of this specific business. They must ONLY ask about services this business actually offers. NEVER invent services the business doesn't provide.
- The first turn should reference how they found the business (e.g., "saw your ad on Facebook") and ask about one of the ACTUAL services listed.
- Make the progression natural. Don't rush to the conclusion.
- The lead should react to what a sales AI would typically say.
- Match the persona's personality (skeptical leads push back more, brief leads use short responses, friendly leads are cooperative, aggressive leads are hostile and impatient).
- The conversation should feel like a real SMS exchange between a potential customer and a business.

SCENARIO-SPECIFIC RULES:
- "new_lead": Lead genuinely asks about services. Should reference a SPECIFIC service the business offers by name.
- "price_objection": Lead asks about a specific service, then objects to pricing. Must mention actual numbers or say "too expensive" / "more than I expected".
- "booking_flow": Lead wants to book. Should progress toward picking a date/time for a specific service.
- "reschedule_cancel": Lead has an existing appointment and wants to change or cancel it. First turn should mention their existing appointment.
- "ai_detection": Lead suspects they're talking to AI. Should ask "are you a real person?", "is this a bot?", or test with trick questions. This is about AI suspicion, NOT about the business services.
- "not_interested": Lead initially engages but then declines. Should explicitly say "not interested", "no thanks", "please stop texting me", or "remove me". Keep to 2-4 turns MAX — once a lead says they're not interested, the conversation should end quickly.
- "knowledge_question": Lead asks detailed questions about specific services, procedures, recovery time, safety, ingredients, etc.
- "edge_case": Unusual behavior — wrong number, gibberish, completely off-topic (but NOT asking about services the business doesn't offer — that's just a bad lead, not an edge case). Examples: "wrong number", random characters, asking about something totally unrelated to any business.
- "ghost": Lead stops responding. Last turn intent MUST be "does not reply". The turns before should show engagement dropping off.
- "competitor_comparison": Lead mentions a specific competing business or compares pricing/quality. Should name a plausible competitor in the same industry.
- "custom": Follow the custom description provided."""


_PLAN_GEN_SCHEMA = {
    "type": "object",
    "properties": {
        "turns": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "intent": {"type": "string", "description": "5-15 word description of what the lead does/says"},
                },
                "required": ["intent"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["turns"],
    "additionalProperties": False,
}

# ---------------------------------------------------------------------------
# Cost estimation constants
# ---------------------------------------------------------------------------

_COST_PER_TURN = 0.003
_COST_PER_FOLLOWUP = 0.001


# ---------------------------------------------------------------------------
# Core: generate conversation plans
# ---------------------------------------------------------------------------

async def generate_plans(
    entity_id: str,
    setter_key: str,
    name: str,
    personas: dict[str, Any],
    scenarios: dict[str, Any],
    tenant_keys: dict[str, str],
) -> tuple[str, list[dict[str, Any]], float]:
    """Generate conversation plans and store in simulations table.

    Args:
        entity_id: Entity to generate plans for.
        setter_key: Which setter config to test.
        name: Simulation name.
        personas: PersonaConfig dict (behaviors, age_range, gender, location).
        scenarios: ScenarioConfig dict (counts, lead_source, channel, preconditions).
        tenant_keys: Resolved tenant API keys (all 6 providers).

    Returns:
        (simulation_id, conversation_plans, total_estimated_cost)
    """
    # Fetch entity config for context
    entity_resp = await supabase._request(
        supabase.main_client, "GET", "/entities",
        params={"id": f"eq.{entity_id}", "select": "id,name,tenant_id,system_config"},
        label="sim_get_entity",
    )
    entity_data = entity_resp.json()
    if not entity_data:
        raise ValueError(f"Entity {entity_id} not found")
    entity = entity_data[0]
    entity_name = entity.get("name", "Unknown")
    tenant_id = entity["tenant_id"]
    system_config = entity.get("system_config") or {}

    # Extract services/offers from setter config for LLM context
    setter = system_config.get("setters", {}).get(setter_key, {})
    services_text = _extract_services_text(system_config)
    offers_text = _extract_offers_text(system_config)

    # Create simulation row with status='generating'
    sim_row = {
        "tenant_id": tenant_id,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "setter_key": setter_key,
        "name": name,
        "persona_config": personas,
        "scenario_config": scenarios,
        "status": "generating",
    }
    sim_resp = await supabase._request(
        supabase.main_client, "POST", "/simulations",
        json=sim_row,
        headers={"Prefer": "return=representation"},
        label="sim_create",
    )
    if sim_resp.status_code >= 400:
        raise ValueError(f"Failed to create simulation: {sim_resp.text}")
    simulation_id = sim_resp.json()[0]["id"]

    try:
        # Fetch outreach template if drip precondition is set
        drip_templates_pool: list[dict[str, Any]] = []  # Multiple templates for distribution
        preconditions = scenarios.get("preconditions", {})
        if preconditions.get("has_outreach_drips"):
            # Multi-select: fetch all selected templates
            template_ids = scenarios.get("outreach_template_ids") or []
            # Backward compat: single ID
            if not template_ids and scenarios.get("outreach_template_id"):
                template_ids = [scenarios["outreach_template_id"]]
            logger.info("SIM_GEN | fetching %d outreach templates: %s", len(template_ids), template_ids)
            for tid in template_ids:
                t_resp = await supabase._request(
                    supabase.main_client, "GET", "/outreach_templates",
                    params={"id": f"eq.{tid}", "select": "id,form_service_interest,positions"},
                    label="sim_get_outreach_template",
                )
                t_data = t_resp.json()
                if t_data:
                    t = t_data[0]
                    positions = t.get("positions") or []
                    sms_msgs = [p["sms"] for p in positions if (p.get("sms") or "").strip()]
                    drip_templates_pool.append({
                        "id": t["id"],
                        "name": t.get("form_service_interest", "Outreach"),
                        "messages": sms_msgs,
                        "total_count": len(sms_msgs),
                    })
            logger.info("SIM_GEN | loaded %d drip templates", len(drip_templates_pool))

        # Generate plans for each scenario type × count
        plans: list[dict[str, Any]] = []
        counts = scenarios.get("counts", {})
        lead_source = scenarios.get("lead_source", "Facebook Ad")

        total_plan_gen_cost = 0.0

        for scenario_type, count in counts.items():
            if count <= 0:
                continue
            scenario_label = _SCENARIO_LABELS.get(scenario_type, scenarios.get("custom_description", scenario_type))

            for i in range(count):
                # Generate persona
                persona = _generate_persona(personas)

                # Random turn count: 3-8 (realistic range, keeps execution time manageable)
                target_turns = random.randint(3, 8)

                # Generate intent sequence via LLM
                set_ai_context(
                    api_key=tenant_keys["openrouter"],
                    google_key=tenant_keys.get("google", ""),
                    anthropic_key=tenant_keys.get("anthropic", ""),
                    openai_key=tenant_keys.get("openai", ""),
                    deepseek_key=tenant_keys.get("deepseek", ""),
                    xai_key=tenant_keys.get("xai", ""),
                )
                try:
                    prompt = (
                        f"## Business\n{entity_name}\n\n"
                        f"## Services This Business ACTUALLY Offers (ONLY reference these)\n{services_text}\n\n"
                        f"## Current Offers/Promotions\n{offers_text}\n\n"
                        f"## Lead Source\n{lead_source}\n"
                        f"The lead came through {lead_source}. Reference this naturally in the first turn "
                        f"(e.g., 'saw your ad', 'filled out the form', 'a friend recommended you').\n\n"
                        f"## Persona\n"
                        f"Name: {persona['name']}, Age: {persona['age']}, "
                        f"Personality: {persona['behavior']}\n\n"
                        f"## Scenario Type\n{scenario_type}\n\n"
                        f"## Scenario Description\n{scenario_label}\n\n"
                        f"## Target Turns\n{target_turns} turns\n\n"
                        f"Generate the conversation turn plan. The lead MUST only ask about "
                        f"services that {entity_name} actually offers (listed above). "
                        f"NEVER have the lead ask about services not listed."
                    )

                    result = await classify(
                        prompt=prompt,
                        schema=_PLAN_GEN_SCHEMA,
                        model="google/gemini-2.5-flash",
                        temperature=0.7,
                        system_prompt=_PLAN_GEN_SYSTEM,
                        label="sim_plan_gen",
                    )

                    intents = result.get("turns", [])
                    if not intents:
                        logger.warning("SIM_PLAN | empty intents for %s #%d, using fallback", scenario_type, i)
                        intents = [{"intent": "Asks about services"}]

                finally:
                    clear_ai_context()

                # Randomize follow-ups per turn
                turns = []
                for t in intents:
                    followups = _random_followup_count()
                    turns.append({
                        "intent": t["intent"],
                        "followups": followups,
                    })

                # Calculate cost estimate
                total_turns = len(turns)
                total_followups = sum(t["followups"] for t in turns)
                est_cost = total_turns * _COST_PER_TURN + total_followups * _COST_PER_FOLLOWUP
                est_seconds = total_turns * 5 + total_followups * 3  # rough: 5s per turn, 3s per followup

                plan: dict[str, Any] = {
                    "id": str(uuid4()),
                    "persona": persona,
                    "scenario_type": scenario_type,
                    "scenario_label": scenario_label,
                    "lead_source": lead_source,
                    "turns": turns,
                    "estimated_cost": round(est_cost, 4),
                    "estimated_seconds": round(est_seconds, 1),
                }

                # Add drip config — pick from template pool (round-robin + shuffle)
                if drip_templates_pool:
                    # Distribute templates evenly across conversations
                    drip_template_data = drip_templates_pool[len(plans) % len(drip_templates_pool)]
                    total_drips = drip_template_data["total_count"]
                    drip_count = random.randint(1, total_drips)
                    plan["drip_config"] = {
                        "template_id": drip_template_data["id"],
                        "template_name": drip_template_data["name"],
                        "messages": drip_template_data["messages"][:drip_count],
                        "count": drip_count,
                        "total_available": total_drips,
                    }

                plans.append(plan)

        total_conversations = len(plans)
        total_estimated_cost = sum(p["estimated_cost"] for p in plans)

        # Debug: log drip configs before storing
        for p in plans:
            dc = p.get("drip_config")
            logger.info("SIM_GEN | PLAN_STORE | %s | drip_config=%s", p["persona"]["name"], "YES" if dc else "NONE")

        # Update simulation with plans + status='planned'
        await supabase._request(
            supabase.main_client, "PATCH", "/simulations",
            params={"id": f"eq.{simulation_id}"},
            json={
                "status": "planned",
                "conversation_plans": plans,
                "total_conversations": total_conversations,
            },
            label="sim_update_plans",
        )

        logger.info(
            "SIM_PLAN | generated | sim=%s | conversations=%d | est_cost=%.4f",
            simulation_id, total_conversations, total_estimated_cost,
        )

        return simulation_id, plans, total_estimated_cost

    except Exception:
        # Mark simulation as failed on any error
        await supabase._request(
            supabase.main_client, "PATCH", "/simulations",
            params={"id": f"eq.{simulation_id}"},
            json={"status": "failed"},
            label="sim_mark_failed",
        )
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_persona(config: dict[str, Any]) -> dict[str, Any]:
    """Generate a single persona from the persona config."""
    age_min, age_max = config.get("age_range", (18, 65))
    if isinstance(age_min, list):  # handle [18, 65] format
        age_min, age_max = age_min[0], age_min[1]
    age = random.randint(age_min, age_max)

    gender = config.get("gender", "any")
    if gender == "any":
        gender_pick = random.choice(["male", "female"])
    else:
        gender_pick = gender

    if gender_pick == "male":
        first = random.choice(_MALE_NAMES)
    else:
        first = random.choice(_FEMALE_NAMES)
    last = random.choice(_LAST_NAMES)

    behaviors = config.get("behaviors", ["friendly"])
    behavior = random.choice(behaviors) if behaviors else "friendly"

    return {
        "name": f"{first} {last}",
        "age": age,
        "gender": gender_pick,
        "behavior": behavior,
    }


def _random_followup_count() -> int:
    """Weighted random follow-up count: 60% get 0, 25% get 1-2, 15% get 3-5."""
    roll = random.random()
    if roll < 0.60:
        return 0
    elif roll < 0.85:
        return random.randint(1, 2)
    else:
        return random.randint(3, 5)


def _extract_services_text(system_config: dict[str, Any]) -> str:
    """Extract services list from system_config for LLM context."""
    service_config = system_config.get("service_config", {})
    services = service_config.get("services", [])
    if not services:
        return "(no services configured)"
    lines = []
    for svc in services:
        name = svc.get("name", "Unknown")
        desc = svc.get("description", "")
        price = svc.get("price", "")
        line = f"- {name}"
        if price:
            line += f" ({price})"
        if desc:
            line += f": {desc}"
        lines.append(line)
    return "\n".join(lines) if lines else "(no services configured)"


def _extract_offers_text(system_config: dict[str, Any]) -> str:
    """Extract active offers from system_config for LLM context."""
    offers_config = system_config.get("offers_config", {})
    offers = offers_config.get("offers", [])
    if not offers:
        return "(no active offers)"
    lines = []
    for offer in offers:
        if not offer.get("enabled", True):
            continue
        name = offer.get("name", "Unknown")
        desc = offer.get("description", "")
        value = offer.get("value", "")
        line = f"- {name}"
        if value:
            line += f" ({value})"
        if desc:
            line += f": {desc}"
        lines.append(line)
    return "\n".join(lines) if lines else "(no active offers)"


# ===========================================================================
# EXECUTION ENGINE
# ===========================================================================

async def execute_simulation(simulation_id: str, tenant_keys: dict[str, str]) -> None:
    """Execute all conversations in a simulation through the real pipeline.

    Runs conversations concurrently (max 5 at a time). Stores results in
    simulations.conversation_results JSONB. Tracks progress atomically.
    Skips chat history writes (is_test_mode). Keeps tool_executions (is_test=true).
    """
    import asyncio
    import time as _time
    from datetime import datetime, timezone

    from app.testing.direct_runner import run_single_test
    from app.testing.cleanup import cleanup_test_contacts
    from app.testing.models import TestScenario, Turn, LeadConfig, TestContext

    logger.info("SIM_EXEC | ENTER execute_simulation | sim=%s", simulation_id)

    # Load simulation
    resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={"id": f"eq.{simulation_id}", "select": "*"},
        label="sim_load_for_exec",
    )
    sim_data = resp.json()
    if not sim_data:
        raise ValueError(f"Simulation {simulation_id} not found")
    sim = sim_data[0]

    if sim["status"] not in ("planned", "running"):
        raise ValueError(f"Simulation status is '{sim['status']}', expected 'planned' or 'running'")

    entity_id = sim["entity_id"]
    setter_key = sim["setter_key"]
    plans = sim.get("conversation_plans") or []
    scenario_config = sim.get("scenario_config") or {}

    # Fetch entity config
    entity_resp = await supabase._request(
        supabase.main_client, "GET", "/entities",
        params={"id": f"eq.{entity_id}", "select": "*"},
        label="sim_get_entity_for_exec",
    )
    entity_data = entity_resp.json()
    if not entity_data:
        raise ValueError(f"Entity {entity_id} not found")
    entity_config = entity_data[0]

    # Capture system config snapshot (may be string from Supabase REST)
    system_config_snapshot = entity_config.get("system_config")
    if isinstance(system_config_snapshot, str):
        import json as _json
        system_config_snapshot = _json.loads(system_config_snapshot)

    # Set status to running
    start_time = _time.perf_counter()
    await supabase._request(
        supabase.main_client, "PATCH", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        json={
            "status": "running",
            "system_config_snapshot": system_config_snapshot,
        },
        label="sim_set_running",
    )

    # Convert plans to TestScenarios
    # Inject entity config so _plan_to_scenario can resolve calendars + timezone
    scenario_config["_entity_system_config"] = system_config_snapshot or {}
    scenario_config["_entity_timezone"] = entity_config.get("timezone", "America/New_York")
    scenarios_and_plans: list[tuple[TestScenario, dict]] = []
    for plan in plans:
        scenario = _plan_to_scenario(plan, scenario_config, setter_key)
        scenarios_and_plans.append((scenario, plan))

    # Execute concurrently with semaphore
    semaphore = asyncio.Semaphore(5)
    results: list[dict[str, Any]] = []
    contact_ids: list[str] = []

    # In-progress placeholders keyed by plan ID — lightweight progress entries
    in_progress: dict[str, dict[str, Any]] = {}

    # Pre-populate all conversations as "ongoing" so they appear immediately
    for plan in plans:
        total_turns = len(plan.get("turns", []))
        in_progress[plan["id"]] = {
            "id": plan["id"],
            "persona": plan["persona"],
            "scenario_type": plan["scenario_type"],
            "scenario_label": plan.get("scenario_label", ""),
            "outcome": "ongoing",
            "turns_completed": 0,
            "total_turns": total_turns,
            "turns": 0, "duration_ms": 0, "cost": 0,
            "messages": [], "decisions": {}, "tools": [], "variables": [],
        }
    # Store initial placeholders
    await supabase._request(
        supabase.main_client, "PATCH", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        json={"conversation_results": list(in_progress.values())},
        label="sim_store_initial_placeholders",
    )

    async def run_one(scenario: TestScenario, plan: dict) -> dict[str, Any]:
        async with semaphore:
            logger.info("SIM_EXEC | starting conversation %s (%s)", plan["id"], plan["scenario_type"])
            total_turns = len(plan.get("turns", []))

            async def _on_progress(conversation: list[dict[str, Any]]) -> None:
                """Called after each turn — store lightweight progress placeholder."""
                turns_done = sum(1 for e in conversation if e.get("role") == "turn")
                in_progress[plan["id"]] = {
                    "id": plan["id"],
                    "persona": plan["persona"],
                    "scenario_type": plan["scenario_type"],
                    "scenario_label": plan.get("scenario_label", ""),
                    "outcome": "ongoing",
                    "turns_completed": turns_done,
                    "total_turns": total_turns,
                    "turns": 0, "duration_ms": 0, "cost": 0,
                    "messages": [], "decisions": {}, "tools": [], "variables": [],
                }
                # Merge completed + in-progress for storage
                all_results = list(results) + [
                    p for pid, p in in_progress.items()
                    if not any(r.get("id") == pid for r in results)
                ]
                try:
                    await supabase._request(
                        supabase.main_client, "PATCH", "/simulations",
                        params={"id": f"eq.{simulation_id}"},
                        json={"conversation_results": all_results},
                        label="sim_store_progress",
                    )
                except Exception:
                    pass

            try:
                raw_result = await run_single_test(
                    scenario=scenario,
                    entity_config=entity_config,
                    entity_id=entity_id,
                    tenant_keys_override=tenant_keys,
                    on_message=_on_progress,
                )

                # Track contact for cleanup
                cid = raw_result.get("contact_id", "")
                if cid:
                    contact_ids.append(cid)

                # Build conversation_results entry with messages for timeline
                # Pass entity_config + scenario_config so timestamps use real cadence timing
                raw_result["entity_config"] = entity_config
                raw_result["setter_key"] = setter_key
                raw_result["scenario_config"] = scenario_config
                conv_result = _build_conversation_result(raw_result, plan)

                # Atomic progress increment
                await supabase._request(
                    supabase.main_client, "POST", "/rpc/increment_simulation_progress",
                    json={"sim_id": simulation_id},
                    label="sim_increment_progress",
                )

                logger.info(
                    "SIM_EXEC | completed conversation %s | turns=%d | cost=%.4f",
                    plan["id"], conv_result["turns"], conv_result["cost"],
                )

                # Run compliance for this conversation immediately (non-blocking for other convs)
                try:
                    from app.testing.compliance import run_single_conversation_compliance
                    await run_single_conversation_compliance(
                        simulation_id=simulation_id,
                        conv_result=conv_result,
                        system_config=system_config_snapshot or {},
                        setter_key=setter_key,
                        tenant_keys=tenant_keys,
                    )
                except Exception as e:
                    logger.warning("SIM_EXEC | per-conv compliance failed (non-fatal): %s", e)

                # Store result incrementally — replace in-progress placeholder with final result
                in_progress.pop(plan["id"], None)
                results.append(conv_result)
                # Merge completed + remaining in-progress for storage
                all_for_store = list(results) + [
                    p for pid, p in in_progress.items()
                    if not any(r.get("id") == pid for r in results)
                ]
                running_cost = sum(r.get("cost", 0) for r in results)
                running_duration = _time.perf_counter() - start_time
                await supabase._request(
                    supabase.main_client, "PATCH", "/simulations",
                    params={"id": f"eq.{simulation_id}"},
                    json={
                        "conversation_results": all_for_store,
                        "total_cost": round(running_cost, 6),
                        "duration_seconds": round(running_duration, 1),
                        "test_contact_ids": contact_ids,
                    },
                    label="sim_store_incremental",
                )

                return conv_result

            except Exception as e:
                logger.error("SIM_EXEC | conversation %s failed: %s", plan["id"], e, exc_info=True)
                # Still increment progress so the count matches
                await supabase._request(
                    supabase.main_client, "POST", "/rpc/increment_simulation_progress",
                    json={"sim_id": simulation_id},
                    label="sim_increment_progress_failed",
                )
                return {
                    "id": plan["id"],
                    "test_contact_id": "",
                    "persona": plan["persona"],
                    "scenario_type": plan["scenario_type"],
                    "outcome": "error",
                    "duration_ms": 0,
                    "cost": 0,
                    "messages": [],
                    "decisions": {},
                    "tools": [],
                    "variables": [],
                    "error": str(e),
                }

    # Launch all conversations (results appended incrementally inside run_one)
    tasks = [run_one(scenario, plan) for scenario, plan in scenarios_and_plans]
    await asyncio.gather(*tasks)

    # Calculate totals from incrementally-stored results
    duration = _time.perf_counter() - start_time
    total_cost = sum(r.get("cost", 0) for r in results)

    # Store final results and set complete (compliance already ran per-conversation)
    await supabase._request(
        supabase.main_client, "PATCH", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        json={"status": "complete"},
        label="sim_set_complete",
    )

    # Cleanup test contacts (keep tool_executions + workflow_runs, no chat history was written)
    if contact_ids:
        try:
            await cleanup_test_contacts(
                contact_ids,
                entity_id=entity_id,
                chat_table="",  # no chat history written in test mode
                skip_tool_executions=True,  # keep tool_executions (is_test=true) for inspector
            )
            logger.info("SIM_EXEC | cleaned up %d test contacts", len(contact_ids))
        except Exception as e:
            logger.warning("SIM_EXEC | cleanup warning: %s", e)

    logger.info(
        "SIM_EXEC | complete | sim=%s | conversations=%d | duration=%.1fs | cost=%.4f",
        simulation_id, len(results), duration, total_cost,
    )


def _plan_to_scenario(
    plan: dict[str, Any],
    scenario_config: dict[str, Any],
    setter_key: str,
) -> "TestScenario":
    """Convert a conversation plan to a TestScenario for run_single_test."""
    from app.testing.models import TestScenario, Turn, LeadConfig, TestContext

    turns = [Turn(intent=t["intent"], followups=t.get("followups", 0)) for t in plan["turns"]]
    name_parts = plan["persona"]["name"].split()

    # Unique phone/email per conversation
    random_digits = ''.join([str(random.randint(0, 9)) for _ in range(7)])
    unique_suffix = str(uuid4())[:8]

    lead = LeadConfig(
        first_name=name_parts[0],
        last_name=name_parts[-1] if len(name_parts) > 1 else "",
        source=plan.get("lead_source", "Direct Test Runner"),
        phone=f"+1555{random_digits}",
        email=f"sim_{unique_suffix}@test.local",
    )

    # Build preconditions from scenario_config
    preconditions = scenario_config.get("preconditions", {})
    context = TestContext()  # Default empty — preconditions added below

    if preconditions.get("has_existing_appointment"):
        from app.testing.models import AppointmentConfig
        from datetime import datetime, timezone, timedelta
        from zoneinfo import ZoneInfo

        appt_status = preconditions.get("appointment_status", "confirmed")
        appt_date = preconditions.get("appointment_date", "")
        appt_time = preconditions.get("appointment_time", "14:00")

        # Resolve entity timezone
        _entity_sc = scenario_config.get("_entity_system_config") or {}
        _entity_tz_name = scenario_config.get("_entity_timezone", "America/New_York")
        try:
            entity_tz = ZoneInfo(_entity_tz_name)
        except (KeyError, Exception):
            entity_tz = ZoneInfo("America/New_York")

        # Parse date/time in entity timezone
        if appt_date:
            try:
                hour, minute = (int(x) for x in appt_time.split(":"))
                local_dt = datetime.strptime(appt_date, "%Y-%m-%d").replace(
                    hour=hour, minute=minute, second=0, microsecond=0, tzinfo=entity_tz,
                )
                start = local_dt.astimezone(timezone.utc)
            except (ValueError, TypeError):
                start = (datetime.now(timezone.utc) + timedelta(days=1)).replace(
                    hour=14, minute=0, second=0, microsecond=0,
                )
        else:
            # Default: tomorrow at 2 PM in entity timezone
            now_local = datetime.now(entity_tz)
            start = (now_local + timedelta(days=1)).replace(
                hour=14, minute=0, second=0, microsecond=0,
            ).astimezone(timezone.utc)

        # Resolve calendar from entity config
        _setters = _entity_sc.get("setters", {})
        _setter = _setters.get(setter_key, {})
        if not _setter:
            _setter = next(iter(_setters.values()), {}) if _setters else {}
        _booking = _setter.get("booking", {})
        _cals = [c for c in _booking.get("calendars", []) if isinstance(c, dict) and c.get("enabled", True)]
        cal_name = _cals[0].get("name", "Appointment") if _cals else "Appointment"
        cal_id = _cals[0].get("id", "") if _cals else ""

        context.appointments = [AppointmentConfig(
            calendar_id=cal_id,
            calendar_name=cal_name,
            status=appt_status,
            start=start.isoformat(),
            timezone=_entity_tz_name,
        )]

    # Drip precondition — seed outreach messages into the conversation context
    drip_config = plan.get("drip_config")
    if drip_config and drip_config.get("messages"):
        drip_msgs = drip_config["messages"]
        drip_count = drip_config.get("count", len(drip_msgs))
        # Create seed messages from the drip messages (most recent first)
        from app.testing.models import SeedMessage
        drip_seeds = []
        for i, msg in enumerate(drip_msgs[:drip_count]):
            # Space drips out: first drip was N*24 hours ago, most recent was 6 hours ago
            hours_ago = max(6, (drip_count - i) * 24)
            drip_seeds.append(SeedMessage(role="ai", content=msg, hours_ago=hours_ago))
        if drip_seeds:
            existing_seeds = context.seed_messages or []
            context.seed_messages = drip_seeds + existing_seeds

    return TestScenario(
        id=plan["id"],
        category=plan["scenario_type"],
        description=plan.get("scenario_label", ""),
        lead=lead,
        context=context,
        conversation=turns,
        channel=scenario_config.get("channel", "SMS"),
        agent_type=setter_key,
    )


def _build_conversation_result(raw_result: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    """Transform run_single_test output into the conversation_results JSONB format.

    Extracts messages for the timeline, decisions, tools, and metadata.
    """
    from datetime import datetime, timezone, timedelta
    from zoneinfo import ZoneInfo

    messages: list[dict[str, Any]] = []
    import random as _random

    # --- Resolve entity timezone ---
    entity_config = raw_result.get("entity_config", {})
    tz_name = entity_config.get("timezone", "America/New_York")
    try:
        local_tz = ZoneInfo(tz_name)
    except (KeyError, Exception):
        local_tz = ZoneInfo("America/New_York")

    # --- Parse setter config for send windows ---
    sc = entity_config.get("system_config", {})
    if isinstance(sc, str):
        import json as _j
        sc = _j.loads(sc)
    setter_key = raw_result.get("setter_key", "setter_1")
    setter = sc.get("setters", {}).get(setter_key, {})
    if not setter:
        for _k, _v in sc.get("setters", {}).items():
            if _v.get("is_default"):
                setter = _v
                break
    conversation_cfg = setter.get("conversation", {})

    # Reply window config
    reply_window_cfg = conversation_cfg.get("reply_window", {})
    rw_mode = reply_window_cfg.get("mode", "24/7")

    # Follow-up send window config
    fu_cfg = conversation_cfg.get("follow_up", {})
    fu_sections = fu_cfg.get("sections", {})
    fu_send_window_cfg = fu_cfg.get("send_window", {})
    fu_sw_mode = fu_send_window_cfg.get("mode", "24/7")

    # Business schedule (used when mode = "business_hours")
    biz_schedule = entity_config.get("business_schedule", {})

    def _get_window_for_day(mode: str, config: dict, day_name: str) -> tuple[int, int]:
        """Get (start_hour, end_hour) for a given day based on window mode."""
        if mode == "24/7" or not mode:
            return (0, 24)
        if mode == "custom":
            days = config.get("days", {})
            day_cfg = days.get(day_name, {})
            if day_cfg.get("enabled", True):
                try:
                    sh = int(day_cfg.get("start", "08:00").split(":")[0])
                    eh = int(day_cfg.get("end", "22:00").split(":")[0])
                    return (sh, eh)
                except (ValueError, AttributeError):
                    pass
            return (8, 22)
        if mode == "business_hours":
            day_cfg = biz_schedule.get(day_name, {})
            if day_cfg.get("enabled", True):
                try:
                    sh = int(day_cfg.get("start", "08:00").split(":")[0])
                    eh = int(day_cfg.get("end", "22:00").split(":")[0])
                    return (sh, eh)
                except (ValueError, AttributeError):
                    pass
            return (8, 22)
        return (0, 24)

    day_names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    def _clamp_to_window(ts: datetime, mode: str, config: dict) -> datetime:
        """If ts is outside the send window, push it into the window. 24/7 = no clamping."""
        if mode == "24/7" or not mode:
            return ts
        local = ts.astimezone(local_tz)
        day_name = day_names[local.weekday()]
        start_h, end_h = _get_window_for_day(mode, config, day_name)
        if local.hour < start_h:
            local = local.replace(hour=start_h, minute=_random.randint(0, 30), second=0, microsecond=0)
        elif local.hour >= end_h:
            local += timedelta(days=1)
            day_name = day_names[local.weekday()]
            start_h, end_h = _get_window_for_day(mode, config, day_name)
            local = local.replace(hour=start_h, minute=_random.randint(0, 30), second=0, microsecond=0)
        return local.astimezone(timezone.utc)

    # --- Start timeline: "now" in local time, 2 days ago ---
    now_local = datetime.now(local_tz)
    base_ts = (now_local - timedelta(days=2)).astimezone(timezone.utc)
    current_ts = base_ts

    # --- Drip messages: 8 AM - 10 PM local, one per day ---
    drip_config = plan.get("drip_config")
    if drip_config and drip_config.get("messages"):
        drip_msgs = drip_config["messages"]
        drip_count = drip_config.get("count", len(drip_msgs))
        drip_start = base_ts - timedelta(days=drip_count + 1)
        for i, msg in enumerate(drip_msgs[:drip_count]):
            # Place each drip on the next day at a random time 8 AM - 10 PM local
            drip_day = drip_start + timedelta(days=i + 1)
            drip_local = drip_day.astimezone(local_tz)
            drip_hour = _random.uniform(8, 21.5)  # 8:00 AM to 9:30 PM local
            drip_local = drip_local.replace(
                hour=int(drip_hour),
                minute=_random.randint(0, 59),
                second=_random.randint(0, 59),
                microsecond=0,
            )
            drip_ts = drip_local.astimezone(timezone.utc)
            messages.append({
                "role": "ai",
                "content": msg,
                "source": "drip",
                "channel": raw_result.get("channel", "SMS"),
                "timestamp": drip_ts.isoformat(),
            })
        # Update base_ts to be after the last drip
        if messages:
            last_drip_ts = datetime.fromisoformat(messages[-1]["timestamp"])
            # Lead replies 2-12 hours after the last drip (clamped to reply window)
            base_ts = last_drip_ts + timedelta(hours=_random.uniform(2, 12))
            base_ts = _clamp_to_window(base_ts, rw_mode, reply_window_cfg)
            current_ts = base_ts

    # --- Appointment system card (if precondition set) ---
    appt_context = (raw_result.get("test_context", {}) or {}).get("appointments", [])
    if appt_context:
        appt = appt_context[0]
        appt_status = appt.get("status", "confirmed")
        appt_start = appt.get("start", "")
        appointment_ts = current_ts
        timing_text = "Scheduled"
        if appt_start:
            try:
                appointment_ts = datetime.fromisoformat(str(appt_start).replace("Z", "+00:00"))
                timing_text = appointment_ts.astimezone(local_tz).strftime("%b %d at %I:%M %p")
            except (ValueError, TypeError):
                timing_text = str(appt_start)
        appt_label = f"Existing appointment: {timing_text} — {appt_status.title()}"
        messages.append({
            "role": "system",
            "source": "appointment",
            "content": appt_label,
            "timestamp": appointment_ts.isoformat(),
        })

    # --- Parse cadence from the plan's follow-up timing ---
    cadence_hours = []
    try:
        ct = fu_sections.get("cadence_timing", {})
        for t in ct.get("timings", []):
            if isinstance(t, str):
                parts = t.lower().split()
                if len(parts) >= 2:
                    num = float(parts[0])
                    if "day" in parts[1]:
                        cadence_hours.append(num * 24)
                    else:
                        cadence_hours.append(num)
    except Exception:
        pass
    if not cadence_hours:
        cadence_hours = [6, 21, 24, 48, 72]

    conversation = raw_result.get("conversation", [])
    followup_idx = 0

    for entry in conversation:
        role = entry.get("role", "")

        if role == "turn":
            lead_msg = entry.get("lead_message", "")
            if lead_msg:
                if messages:
                    current_ts += timedelta(hours=_random.uniform(1, 4))
                # Clamp lead reply to reply window
                current_ts = _clamp_to_window(current_ts, rw_mode, reply_window_cfg)
                messages.append({
                    "role": "human",
                    "content": lead_msg,
                    "source": "lead_reply",
                    "channel": raw_result.get("channel", "SMS"),
                    "timestamp": current_ts.isoformat(),
                })

            ai_text = entry.get("ai_response", "")
            if ai_text:
                # AI replies 15-45 seconds after lead (always immediate, no window needed)
                current_ts += timedelta(seconds=_random.uniform(15, 45))
                ai_parts = ai_text.split(" | ") if " | " in ai_text else [ai_text]
                for j, part in enumerate(ai_parts):
                    msg_data: dict[str, Any] = {
                        "role": "ai",
                        "content": part,
                        "source": "AI",
                        "channel": raw_result.get("channel", "SMS"),
                        "timestamp": current_ts.isoformat(),
                    }
                    if entry.get("workflow_run_id"):
                        msg_data["workflow_run_id"] = entry["workflow_run_id"]
                    if j == 0 and entry.get("media_url"):
                        msg_data["media"] = {
                            "url": entry["media_url"],
                            "type": entry.get("media_type", "image"),
                            "name": entry.get("media_name", ""),
                            "description": entry.get("media_description", ""),
                        }
                    messages.append(msg_data)
                    if j < len(ai_parts) - 1:
                        current_ts += timedelta(seconds=_random.uniform(1, 3))

            followup_idx = 0

        elif role == "followup":
            fu_msg = entry.get("followup_message", "")
            if fu_msg and entry.get("followup_needed", False):
                hours = cadence_hours[min(followup_idx, len(cadence_hours) - 1)]
                hours *= _random.uniform(0.8, 1.2)
                current_ts += timedelta(hours=hours)
                # Clamp follow-up to the follow-up send window
                current_ts = _clamp_to_window(current_ts, fu_sw_mode, fu_send_window_cfg)
                followup_idx += 1

                msg_data = {
                    "role": "ai",
                    "content": fu_msg,
                    "source": "follow_up",
                    "channel": raw_result.get("channel", "SMS"),
                    "timestamp": current_ts.isoformat(),
                }
                if entry.get("workflow_run_id"):
                    msg_data["workflow_run_id"] = entry["workflow_run_id"]
                if entry.get("media_url"):
                    msg_data["media"] = {
                        "url": entry["media_url"],
                        "type": entry.get("media_type", "image"),
                        "name": entry.get("media_name", ""),
                        "description": entry.get("media_description", ""),
                    }
                messages.append(msg_data)

    # Determine outcome from conversation data
    all_turns = [e for e in conversation if e.get("role") == "turn"]
    last_turn = all_turns[-1] if all_turns else None
    outcome = "completed"  # default — conversation ran to completion

    # Check all turns for booking/transfer/opt-out
    all_tool_calls = []
    for turn in all_turns:
        all_tool_calls.extend(turn.get("tool_calls_log") or [])

    if any(t.get("name") == "book_appointment" for t in all_tool_calls):
        outcome = "booked"
    elif last_turn:
        path = (last_turn.get("path") or "").lower()
        if path in ("opt_out", "stop"):
            outcome = "opted_out"
        elif path in ("human", "transfer"):
            outcome = "transferred"

    # Also check the last turn specifically
    if outcome == "completed" and last_turn:
        path = (last_turn.get("path") or "").lower()
        if path in ("opt_out", "stop"):
            outcome = "opted_out"
        elif path in ("human", "transfer"):
            outcome = "transferred"
        elif any(t.get("name") == "book_appointment" for t in (last_turn.get("tool_calls_log") or [])):
            outcome = "booked"

    # Collect decisions and tools from all turns
    decisions = {}
    tools = []
    variables = []
    for entry in conversation:
        if entry.get("role") == "turn":
            if entry.get("classification"):
                decisions[f"turn_{entry['turn_number']}"] = entry["classification"]
            tools.extend(entry.get("tool_calls_log") or [])
            variables.extend(entry.get("prompt_log") or [])

    token_usage = raw_result.get("token_usage", {})

    return {
        "id": plan["id"],
        "test_contact_id": raw_result.get("contact_id", ""),
        "persona": plan["persona"],
        "scenario_type": plan["scenario_type"],
        "outcome": outcome,
        "turns": raw_result.get("turns", 0),
        "duration_ms": int(raw_result.get("duration_seconds", 0) * 1000),
        "cost": token_usage.get("estimated_cost_usd", 0) if isinstance(token_usage, dict) else 0,
        "messages": messages,
        "decisions": decisions,
        "tools": tools,
        "variables": variables,
        "media": {
            "url": conversation[-1].get("media_url") if conversation else None,
        } if any(e.get("media_url") for e in conversation) else None,
    }


def _build_partial_result(
    conversation: list[dict[str, Any]],
    plan: dict[str, Any],
    scenario_config: dict[str, Any],
) -> dict[str, Any]:
    """Build a lightweight partial result from an in-progress conversation.

    Used by the on_message callback to store incremental results so the
    frontend can render messages as they arrive.
    """
    from datetime import datetime, timezone, timedelta
    import random as _random

    messages: list[dict[str, Any]] = []
    channel = scenario_config.get("channel", "SMS")
    base_ts = datetime.now(timezone.utc) - timedelta(days=2)
    current_ts = base_ts

    # Check if conversation has seed entries (drips are already there as seeds)
    has_seeds = any(e.get("role") == "seed" for e in conversation)

    # Add drip messages from plan ONLY if no seed entries yet (before first turn callback)
    if not has_seeds:
        drip_config = plan.get("drip_config")
        if drip_config and drip_config.get("messages"):
            drip_msgs = drip_config["messages"]
            drip_count = drip_config.get("count", len(drip_msgs))
            drip_start = base_ts - timedelta(days=drip_count + 1)
            for i, msg in enumerate(drip_msgs[:drip_count]):
                drip_ts = drip_start + timedelta(days=i + 1, hours=_random.uniform(8, 12))
                messages.append({
                    "role": "ai",
                    "content": msg,
                    "source": "drip",
                    "channel": channel,
                    "timestamp": drip_ts.isoformat(),
                })
            if messages:
                last_drip_ts = datetime.fromisoformat(messages[-1]["timestamp"])
                base_ts = last_drip_ts + timedelta(hours=_random.uniform(2, 12))
                current_ts = base_ts

    # Build messages from conversation entries
    for entry in conversation:
        role = entry.get("role", "")

        if role == "seed":
            # Seed entries include drip messages — render with "drip" source
            hours_ago = entry.get("seed_hours_ago", 24)
            ts = base_ts - timedelta(hours=hours_ago)
            source = entry.get("seed_source", "outreach_drip")
            # Normalize outreach_drip source to "drip" for UI styling
            if source == "outreach_drip":
                source = "drip"
            messages.append({
                "role": entry.get("seed_role", "ai"),
                "content": entry.get("seed_content", ""),
                "source": source,
                "channel": entry.get("seed_channel", channel),
                "timestamp": ts.isoformat(),
            })

        elif role == "turn":
            lead_msg = entry.get("lead_message", "")
            if lead_msg:
                if messages:
                    current_ts += timedelta(hours=_random.uniform(1, 4))
                messages.append({
                    "role": "human",
                    "content": lead_msg,
                    "source": "lead_reply",
                    "channel": channel,
                    "timestamp": current_ts.isoformat(),
                })

            ai_text = entry.get("ai_response", "")
            if ai_text:
                current_ts += timedelta(seconds=_random.uniform(15, 45))
                ai_parts = ai_text.split(" | ") if " | " in ai_text else [ai_text]
                for part in ai_parts:
                    messages.append({
                        "role": "ai",
                        "content": part,
                        "source": "AI",
                        "channel": channel,
                        "timestamp": current_ts.isoformat(),
                    })
                    current_ts += timedelta(seconds=_random.uniform(1, 3))

        elif role == "followup":
            fu_msg = entry.get("followup_message", "")
            if fu_msg and entry.get("followup_needed", False):
                current_ts += timedelta(hours=_random.uniform(4, 12))
                messages.append({
                    "role": "ai",
                    "content": fu_msg,
                    "source": "follow_up",
                    "channel": channel,
                    "timestamp": current_ts.isoformat(),
                })

    turn_count = sum(1 for e in conversation if e.get("role") == "turn")
    fu_count = sum(1 for e in conversation if e.get("role") == "followup" and e.get("followup_needed"))

    return {
        "id": plan["id"],
        "persona": plan["persona"],
        "scenario_type": plan["scenario_type"],
        "scenario_label": plan.get("scenario_label", ""),
        "outcome": "ongoing",
        "turns": turn_count,
        "duration_ms": 0,
        "cost": 0,
        "messages": messages,
        "decisions": {},
        "tools": [],
        "variables": [],
    }
