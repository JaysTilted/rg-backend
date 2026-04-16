"""FastAPI router for the test runner and pipeline runs.

POST  /testing/run — run test scenarios against the pipeline
GET   /testing/runs — list test runs
GET   /testing/runs/:id — get a test run with pipeline_runs data
PATCH /testing/runs/:id/cases/:test_id/review — update human review
GET   /testing/pipeline-runs — query pipeline runs (test + production)
GET   /testing/pipeline-runs/:id — single pipeline run by ID
"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.supabase_client import supabase
from app.testing.cleanup import cleanup_test_contacts
from app.testing.direct_runner import run_single_test
from app.testing.models import (
    TestRunRequest,
    TestRunResponse,
)
from app.text_engine.data_loading import deep_merge

logger = logging.getLogger(__name__)

testing_router = APIRouter(prefix="/testing", tags=["testing"])


# ---------------------------------------------------------------------------
# Media stats computation
# ---------------------------------------------------------------------------

def _compute_media_stats(tests: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Extract follow-up, media, and reschedule stats from test results."""
    total_fus = 0
    fus_needed = 0
    fus_not_needed = 0
    fus_with_media = 0
    fus_no_media = 0
    reschedule_count = 0
    media_names: Counter[str] = Counter()
    media_types: Counter[str] = Counter()
    library_items_used: set[str] = set()

    for t in tests:
        for c in t.get("conversation", []):
            if c.get("role") != "followup":
                continue
            total_fus += 1
            needed = c.get("followup_needed", False)
            if not needed:
                fus_not_needed += 1
                if c.get("reschedule_timeframe"):
                    reschedule_count += 1
                continue
            fus_needed += 1
            media_name = c.get("media_name") or ""
            media_type = c.get("media_type") or ""
            if media_name:
                fus_with_media += 1
                media_names[media_name] += 1
                media_types[media_type] += 1
                library_items_used.add(media_name)
            else:
                fus_no_media += 1

    if total_fus == 0:
        return None

    max_filtered = 0
    for t in tests:
        for c in t.get("conversation", []):
            if c.get("role") == "followup":
                fc = c.get("filtered_media_count")
                if isinstance(fc, int) and fc > max_filtered:
                    max_filtered = fc

    media_eligible = fus_with_media + fus_no_media
    media_rate = round(fus_with_media / media_eligible * 100) if media_eligible > 0 else 0

    return {
        "total_fus": total_fus,
        "fus_needed": fus_needed,
        "fus_not_needed": fus_not_needed,
        "fus_with_media": fus_with_media,
        "fus_no_media": fus_no_media,
        "media_rate": media_rate,
        "media_eligible": media_eligible,
        "reschedule_count": reschedule_count,
        "media_names": dict(media_names),
        "media_types": dict(media_types),
        "library_items_used": len(library_items_used),
        "library_size": max_filtered,
    }


# ---------------------------------------------------------------------------
# Split test data into pipeline_runs columns
# ---------------------------------------------------------------------------

def _split_test_data(test: dict[str, Any]) -> dict[str, Any]:
    """Split a test result into pipeline_runs column data.

    Returns dict with keys: conversation, decisions, tools, variables,
    ghl_calls_summary — ready for pipeline_runs row insertion.

    Drops: prompts (full text), context (timeline), ghl_calls (raw log).
    """
    conversation_clean: list[dict[str, Any]] = []
    tools_all: list[dict[str, Any]] = []
    variables_all: list[dict[str, Any]] = []
    decisions: dict[str, list] = {"turns": [], "followups": []}

    for entry in test.get("conversation", []):
        role = entry.get("role", "")

        if role == "turn":
            turn_num = entry.get("turn_number", 0)

            # Clean conversation entry (dialogue + lightweight metadata)
            clean_turn: dict[str, Any] = {
                "role": "turn",
                "turn_number": turn_num,
                "lead_message": entry.get("lead_message"),
                "lead_intent": entry.get("lead_intent"),
                "lead_generated": entry.get("lead_generated", False),
                "ai_response": entry.get("ai_response"),
                "path": entry.get("path"),
                "duration_seconds": entry.get("duration_seconds"),
                "issues": entry.get("issues", []),
            }
            # Reply media fields
            if entry.get("media_url"):
                clean_turn["media_url"] = entry["media_url"]
                clean_turn["media_type"] = entry.get("media_type", "")
                clean_turn["media_name"] = entry.get("media_name", "")
                clean_turn["media_description"] = entry.get("media_description", "")
            # Security
            if entry.get("security"):
                clean_turn["security"] = entry["security"]
            conversation_clean.append(clean_turn)

            # Tools (full detail with args + results)
            for t in entry.get("tool_calls_log", []):
                tools_all.append({**t, "turn": turn_num})

            # Variables (lightweight label + variables from prompt_log)
            for p in entry.get("prompt_log", []):
                variables_all.append({**p, "turn": turn_num})

            # Decisions (unified structure)
            fs = entry.get("followup_scheduling", {})
            decisions["turns"].append({
                "turn_number": turn_num,
                "classification": entry.get("classification", {}),
                "extraction": entry.get("extraction", {}),
                "qualification": entry.get("qualification", {}),
                "post_processing": entry.get("post_processing", {}),
                "tools_used": entry.get("tools_used", []),
                "followup_scheduling": fs if fs else None,
            })

        elif role == "followup":
            fu_num = entry.get("followup_number", 0)

            # Clean followup entry (strip heavy fields)
            clean_fu = {k: v for k, v in entry.items()
                        if k not in ("prompt_log", "timeline", "upcoming_booking_text", "past_booking_text")}
            conversation_clean.append(clean_fu)

            # Variables from followup prompt_log
            for p in entry.get("prompt_log", []):
                variables_all.append({**p, "followup": fu_num})

            # Decisions (determination_decision)
            if entry.get("determination_decision"):
                decisions["followups"].append({
                    "followup_number": fu_num,
                    "determination_decision": entry["determination_decision"],
                })

        else:
            # outreach_drip, error, etc — keep as-is
            conversation_clean.append(entry)

    return {
        "conversation": conversation_clean,
        "decisions": decisions,
        "tools": tools_all,
        "variables": variables_all,
        "ghl_calls_summary": test.get("ghl_calls_summary"),
    }


# ---------------------------------------------------------------------------
# Persist test run to Supabase
# ---------------------------------------------------------------------------

async def _persist_test_run(
    response: TestRunResponse,
    tests: list[dict[str, Any]],
    entity_id: str,
) -> str | None:
    """Save test run to Supabase: run metadata + individual pipeline_runs rows."""
    try:
        categories = sorted({
            t.get("category", "") for t in tests if t.get("category")
        })
        media_stats = _compute_media_stats(tests)

        # Step 1: Insert run metadata
        run_row = {
            "entity_id": entity_id,
            "entity_name": response.entity_name,
            "agent_type": response.agent_type,
            "channel": response.channel,
            "total_tests": response.test_count,
            "categories": categories,
            "duration_seconds": response.duration_seconds,
            "token_usage": response.token_usage,
            "media_stats": media_stats,
            "review_passed": 0,
            "review_failed": 0,
            "review_flagged": 0,
            "source": "batch",
        }

        resp = await supabase._request(
            supabase.main_client, "POST", "/test_runs",
            json=run_row,
            headers={"Prefer": "return=representation"},
            label="persist_test_run",
        )

        if resp.status_code >= 400:
            logger.error("Failed to persist test run: %d %s", resp.status_code, resp.text)
            return None

        data = resp.json()
        run_id = data[0]["id"] if data else None
        if not run_id:
            return None

        # Step 2: Insert each test as a pipeline_runs row
        for test in tests:
            split = _split_test_data(test)

            # Determine trigger_type and path from first turn
            first_turn = next(
                (d for d in split["decisions"].get("turns", []) if d.get("turn_number") == 1),
                {},
            )
            path = first_turn.get("classification", {}).get("path", "")

            pipeline_run_row = {
                "entity_id": entity_id,
                "contact_id": test.get("contact_id"),
                "trigger_type": "reply",
                "channel": test.get("channel", response.channel),
                "path": path,
                "decisions": split["decisions"],
                "tools": split["tools"],
                "variables": split["variables"],
                "token_usage": test.get("token_usage"),
                "duration_ms": int(test.get("duration_seconds", 0) * 1000),
                "source": "test",
                # Test-specific fields
                "test_run_id": run_id,
                "test_id": test.get("test_id", "unknown"),
                "category": test.get("category", ""),
                "description": test.get("description", ""),
                "conversation": split["conversation"],
                "lead": test.get("lead"),
                "test_context": test.get("test_context"),
                "issues": test.get("issues", []),
                "ghl_calls_summary": split["ghl_calls_summary"],
            }

            case_resp = await supabase._request(
                supabase.main_client, "POST", "/pipeline_runs",
                json=pipeline_run_row,
                label="persist_pipeline_run",
            )
            if case_resp.status_code >= 400:
                logger.error(
                    "Failed to persist pipeline_run %s: %d %s",
                    test.get("test_id"), case_resp.status_code, case_resp.text,
                )

        logger.info("SB | persist_test_run | id=%s | tests=%d", run_id, response.test_count)
        return run_id

    except Exception:
        logger.exception("Failed to persist test run")
        return None


# ---------------------------------------------------------------------------
# Run tests endpoint
# ---------------------------------------------------------------------------

@testing_router.post("/run", response_model=TestRunResponse)
async def run_tests(body: TestRunRequest) -> TestRunResponse:
    """Run a batch of test scenarios concurrently against the pipeline."""

    start = time.perf_counter()

    # Preset execution stub
    if body.preset_ids:
        raise HTTPException(status_code=501, detail="Preset execution not yet implemented")

    if not body.test_scenarios:
        raise HTTPException(status_code=400, detail="No test_scenarios provided")

    # 1. Resolve entity
    try:
        config = await supabase.resolve_entity(body.entity_id)
    except ValueError:
        raise HTTPException(
            status_code=404, detail=f"Entity {body.entity_id} not found"
        )

    if body.overrides:
        config = copy.deepcopy(config)
        existing = config.get("test_config_overrides")
        config["test_config_overrides"] = deep_merge(
            existing if isinstance(existing, dict) else {},
            body.overrides,
        )

    entity_name = config.get("name") or config.get("bot_name") or "Unknown"

    # 2. Launch all tests concurrently
    tasks = [
        run_single_test(
            scenario=scenario,
            entity_config=config,
            entity_id=body.entity_id,
        )
        for scenario in body.test_scenarios
    ]

    results: list[dict[str, Any] | BaseException] = await asyncio.gather(
        *tasks, return_exceptions=True,
    )

    # 3. Process results
    test_results: list[dict[str, Any]] = []
    contact_ids: list[str] = []
    passed = 0
    failed = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    total_calls = 0
    total_cost = 0.0

    # Collect agent_type and channel from first scenario for response
    first_agent_type = body.test_scenarios[0].agent_type if body.test_scenarios else "setter_1"
    first_channel = body.test_scenarios[0].channel if body.test_scenarios else "SMS"

    for i, r in enumerate(results):
        if isinstance(r, BaseException):
            sc = body.test_scenarios[i]
            test_results.append({
                "test_id": sc.id,
                "category": sc.category,
                "description": sc.description,
                "status": "error",
                "issues": [f"Unhandled exception: {r}"],
                "conversation": [],
                "lead": sc.lead.model_dump(),
                "test_context": sc.context.model_dump(),
            })
            failed += 1
        else:
            test_results.append(r)
            contact_ids.append(r.get("contact_id", ""))
            if r.get("status") == "passed":
                passed += 1
            else:
                failed += 1
            tu = r.get("token_usage", {})
            total_prompt_tokens += tu.get("prompt_tokens", 0)
            total_completion_tokens += tu.get("completion_tokens", 0)
            total_tokens += tu.get("total_tokens", 0)
            total_calls += tu.get("call_count", 0)
            total_cost += tu.get("estimated_cost_usd", 0.0)

    # 4. Auto-cleanup all fake UUID data
    chat_table = config.get("chat_history_table_name", "")
    cleanup_stats = await cleanup_test_contacts(
        contact_ids=[c for c in contact_ids if c],
        entity_id=body.entity_id,
        chat_table=chat_table,
    )

    # 5. Build response
    duration = time.perf_counter() - start
    entity_type = config.get("entity_type", "client")
    response = TestRunResponse(
        timestamp=datetime.now(timezone.utc).isoformat(),
        entity_id=body.entity_id,
        entity_name=entity_name,
        entity_type=entity_type,
        agent_type=first_agent_type,
        channel=first_channel,
        test_count=len(body.test_scenarios),
        passed=passed,
        failed=failed,
        duration_seconds=round(duration, 1),
        token_usage={
            "prompt_tokens": total_prompt_tokens,
            "completion_tokens": total_completion_tokens,
            "total_tokens": total_tokens,
            "call_count": total_calls,
            "estimated_cost_usd": round(total_cost, 4),
        },
    )

    # 6. Persist to Supabase (best-effort — test_runs/pipeline_runs tables
    # may not exist in backend-only deployments; ignore errors).
    run_id = await _persist_test_run(
        response, test_results, body.entity_id,
    )
    response.run_id = run_id

    # 7. Inline results so E2E callers can read Scott's actual replies
    # without needing access to the pipeline_runs table.
    response.tests = test_results

    return response


# ---------------------------------------------------------------------------
# Test runs API — read endpoints for the Testing page
# ---------------------------------------------------------------------------

@testing_router.get("/runs")
async def list_test_runs(
    entity_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """List test runs (summary only)."""
    params: dict[str, str] = {
        "select": "id,entity_id,entity_name,agent_type,channel,created_at,"
                  "total_tests,categories,duration_seconds,"
                  "token_usage,media_stats,review_passed,review_failed,review_flagged,"
                  "source,previous_run_id,run_number",
        "order": "created_at.desc",
        "limit": str(limit),
        "offset": str(offset),
    }
    if entity_id:
        params["entity_id"] = f"eq.{entity_id}"

    resp = await supabase._request(
        supabase.main_client, "GET", "/test_runs",
        params=params,
        label="list_test_runs",
    )
    return resp.json()


@testing_router.get("/runs/{run_id}")
async def get_test_run(run_id: str) -> dict[str, Any]:
    """Get a single test run with full pipeline_runs data."""
    # Fetch run metadata
    run_resp = await supabase._request(
        supabase.main_client, "GET", "/test_runs",
        params={
            "id": f"eq.{run_id}",
            "select": "id,entity_id,entity_name,agent_type,channel,created_at,"
                      "total_tests,categories,duration_seconds,"
                      "token_usage,media_stats,review_passed,review_failed,review_flagged,"
                      "source,previous_run_id,run_number",
        },
        label="get_test_run",
    )
    run_data = run_resp.json()
    if not run_data:
        raise HTTPException(status_code=404, detail="Test run not found")

    run = run_data[0]

    # Fetch pipeline_runs for this test run
    cases_resp = await supabase._request(
        supabase.main_client, "GET", "/pipeline_runs",
        params={
            "test_run_id": f"eq.{run_id}",
            "order": "created_at.asc",
        },
        label="get_pipeline_runs_for_run",
    )
    run["pipeline_runs"] = cases_resp.json()

    return run


class ReviewUpdate(BaseModel):
    status: str  # "pass", "fail", "flag"
    notes: str = ""


@testing_router.patch("/runs/{run_id}/cases/{test_id}/review")
async def update_review(run_id: str, test_id: str, body: ReviewUpdate) -> dict[str, Any]:
    """Update the human review for a specific pipeline run (test case)."""
    now = datetime.now(timezone.utc).isoformat()

    resp = await supabase._request(
        supabase.main_client, "PATCH", "/pipeline_runs",
        params={"test_run_id": f"eq.{run_id}", "test_id": f"eq.{test_id}"},
        json={
            "review_status": body.status,
            "review_notes": body.notes,
            "reviewed_at": now,
        },
        headers={"Prefer": "return=representation"},
        label="update_review",
    )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    updated = resp.json()
    if not updated:
        raise HTTPException(status_code=404, detail="Test case not found")

    # Re-count review statuses and update denormalized counters on test_runs
    counts_resp = await supabase._request(
        supabase.main_client, "GET", "/pipeline_runs",
        params={
            "test_run_id": f"eq.{run_id}",
            "select": "review_status",
        },
        label="count_reviews",
    )
    cases = counts_resp.json()
    review_passed = sum(1 for c in cases if c.get("review_status") == "pass")
    review_failed = sum(1 for c in cases if c.get("review_status") == "fail")
    review_flagged = sum(1 for c in cases if c.get("review_status") == "flag")

    await supabase._request(
        supabase.main_client, "PATCH", "/test_runs",
        params={"id": f"eq.{run_id}"},
        json={
            "review_passed": review_passed,
            "review_failed": review_failed,
            "review_flagged": review_flagged,
        },
        label="update_review_counters",
    )

    return updated[0]


@testing_router.delete("/runs/{run_id}")
async def delete_test_run(run_id: str) -> dict[str, str]:
    """Delete a test run (pipeline_runs cascade-deleted via FK)."""
    resp = await supabase._request(
        supabase.main_client, "DELETE", "/test_runs",
        params={"id": f"eq.{run_id}"},
        label="delete_test_run",
    )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"status": "deleted", "id": run_id}


# ---------------------------------------------------------------------------
# Review pull endpoint
# ---------------------------------------------------------------------------

@testing_router.get("/runs/{run_id}/reviews")
async def get_run_reviews(run_id: str) -> list[dict[str, Any]]:
    """Get all pipeline runs for a test run with review status."""
    resp = await supabase._request(
        supabase.main_client, "GET", "/pipeline_runs",
        params={
            "test_run_id": f"eq.{run_id}",
            "select": "test_id,category,description,review_status,review_notes,reviewed_at",
            "order": "created_at.asc",
        },
        label="get_run_reviews",
    )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


# ---------------------------------------------------------------------------
# Run comparison endpoint
# ---------------------------------------------------------------------------

@testing_router.get("/runs/{run_id}/comparison")
async def get_run_comparison(run_id: str) -> dict[str, Any]:
    """Compare a run against its previous_run_id."""
    run_resp = await supabase._request(
        supabase.main_client, "GET", "/test_runs",
        params={"id": f"eq.{run_id}", "select": "previous_run_id"},
        label="get_run_for_comparison",
    )
    run_data = run_resp.json()
    if not run_data:
        raise HTTPException(status_code=404, detail="Run not found")

    previous_run_id = run_data[0].get("previous_run_id")
    if not previous_run_id:
        return {"previous_run_id": None, "comparison": {}}

    current_resp, prev_resp = await asyncio.gather(
        supabase._request(
            supabase.main_client, "GET", "/pipeline_runs",
            params={
                "test_run_id": f"eq.{run_id}",
                "select": "test_id,review_status",
            },
            label="comparison_current",
        ),
        supabase._request(
            supabase.main_client, "GET", "/pipeline_runs",
            params={
                "test_run_id": f"eq.{previous_run_id}",
                "select": "test_id,review_status",
            },
            label="comparison_previous",
        ),
    )

    current_cases = {c["test_id"]: c.get("review_status") for c in current_resp.json()}
    prev_cases = {c["test_id"]: c.get("review_status") for c in prev_resp.json()}

    comparison: dict[str, str] = {}

    for test_id, cur_status in current_cases.items():
        if test_id not in prev_cases:
            comparison[test_id] = "new"
        else:
            prev_status = prev_cases[test_id]
            if cur_status == prev_status:
                comparison[test_id] = "unchanged"
            elif cur_status == "pass" and prev_status in ("fail", "flag"):
                comparison[test_id] = "fixed"
            elif cur_status in ("fail", "flag") and prev_status == "pass":
                comparison[test_id] = "regressed"
            else:
                comparison[test_id] = "unchanged"

    for test_id in prev_cases:
        if test_id not in current_cases:
            comparison[test_id] = "removed"

    return {"previous_run_id": previous_run_id, "comparison": comparison}


# ---------------------------------------------------------------------------
# Pipeline runs endpoints (test + production)
# ---------------------------------------------------------------------------

@testing_router.get("/pipeline-runs")
async def list_pipeline_runs(
    contact_id: str | None = None,
    source: str | None = None,
    entity_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Query pipeline runs with optional filters.

    Supports both test and production Inspector use cases.
    """
    params: dict[str, str] = {
        "order": "created_at.desc",
        "limit": str(limit),
        "offset": str(offset),
    }
    if contact_id:
        params["contact_id"] = f"eq.{contact_id}"
    if source:
        params["source"] = f"eq.{source}"
    if entity_id:
        params["entity_id"] = f"eq.{entity_id}"

    resp = await supabase._request(
        supabase.main_client, "GET", "/pipeline_runs",
        params=params,
        label="list_pipeline_runs",
    )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@testing_router.get("/pipeline-runs/{pipeline_run_id}")
async def get_pipeline_run(pipeline_run_id: str) -> dict[str, Any]:
    """Get a single pipeline run by ID (works for both test and production)."""
    resp = await supabase._request(
        supabase.main_client, "GET", "/pipeline_runs",
        params={
            "id": f"eq.{pipeline_run_id}",
        },
        label="get_pipeline_run",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Pipeline run not found")
    return data[0]


# ===========================================================================
# SIMULATOR endpoints
# ===========================================================================

from app.testing.simulator_models import (
    GeneratePlanRequest,
    GeneratePlanResponse,
    SimulationStatus,
    AnalyzeEstimate,
    RerunRequest,
    RerunConversationRequest,
)
from app.testing.simulator import generate_plans, execute_simulation
from app.testing.analysis import run_analysis


@testing_router.post("/simulator/generate")
async def simulator_generate(body: GeneratePlanRequest) -> GeneratePlanResponse:
    """Generate conversation plans from persona + scenario config."""
    from app.main import _resolve_tenant_ai_keys

    tenant_keys = await _resolve_tenant_ai_keys(body.entity_id)

    try:
        simulation_id, plans, total_cost = await generate_plans(
        entity_id=body.entity_id,
        setter_key=body.setter_key,
        name=body.name,
        personas=body.personas.model_dump(),
        scenarios=body.scenarios.model_dump(),
            tenant_keys=tenant_keys,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    total_seconds = sum(p["estimated_seconds"] for p in plans)

    return GeneratePlanResponse(
        simulation_id=simulation_id,
        conversation_plans=plans,
        total_estimated_cost=round(total_cost, 4),
        total_estimated_seconds=round(total_seconds, 1),
    )


@testing_router.post("/simulator/execute/{simulation_id}")
async def simulator_execute(simulation_id: str) -> dict[str, str]:
    """Start executing a planned simulation. Runs in background."""
    import asyncio
    from app.main import _resolve_tenant_ai_keys

    # Load simulation to get entity_id
    resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={"id": f"eq.{simulation_id}", "select": "status,entity_id"},
        label="sim_check_for_exec",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Simulation not found")
    if data[0]["status"] not in ("planned", "running"):
        raise HTTPException(status_code=409, detail=f"Simulation status is '{data[0]['status']}', expected 'planned' or 'running'")

    tenant_keys = await _resolve_tenant_ai_keys(data[0]["entity_id"])

    # Set status to "running" immediately so the frontend sees it on the next poll
    if data[0]["status"] == "planned":
        await supabase._request(
            supabase.main_client, "PATCH", "/simulations",
            params={"id": f"eq.{simulation_id}"},
            json={"status": "running"},
            label="sim_set_running_early",
        )

    # Run execution in background task (non-blocking)
    # Add error handler so exceptions aren't silently swallowed
    async def _run_with_error_handling():
        try:
            await execute_simulation(simulation_id, tenant_keys)
        except Exception as e:
            logger.error("SIM_EXEC | background task failed: %s", e, exc_info=True)
            # Mark simulation as failed
            try:
                await supabase._request(
                    supabase.main_client, "PATCH", "/simulations",
                    params={"id": f"eq.{simulation_id}"},
                    json={"status": "failed"},
                    label="sim_mark_failed_bg",
                )
            except Exception:
                pass

    asyncio.create_task(_run_with_error_handling())

    return {"status": "started", "simulation_id": simulation_id}


@testing_router.get("/simulator/{simulation_id}/status")
async def simulator_status(simulation_id: str) -> SimulationStatus:
    """Get simulation execution status (for progress polling)."""
    resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={
            "id": f"eq.{simulation_id}",
            "select": "status,total_conversations,completed_conversations,created_at",
        },
        label="sim_get_status",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Simulation not found")
    sim = data[0]
    return SimulationStatus(
        status=sim["status"],
        total_conversations=sim["total_conversations"],
        completed_conversations=sim["completed_conversations"],
    )


@testing_router.get("/simulator/{simulation_id}")
async def simulator_get(simulation_id: str) -> dict[str, Any]:
    """Get full simulation data (config, plans, results, compliance, analysis)."""
    resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={"id": f"eq.{simulation_id}", "select": "*"},
        label="sim_get_full",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Simulation not found")
    return data[0]


@testing_router.post("/simulator/{simulation_id}/analyze")
async def simulator_analyze(simulation_id: str) -> dict[str, Any]:
    """Run Tier 2+3 AI analysis on a completed simulation."""
    from app.main import _resolve_tenant_ai_keys

    # Load simulation
    resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={"id": f"eq.{simulation_id}", "select": "*"},
        label="sim_load_for_analyze",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Simulation not found")

    sim = data[0]
    if sim["status"] != "complete":
        raise HTTPException(status_code=409, detail=f"Can only analyze completed simulations (current: {sim['status']})")

    # Set analyzing status
    await supabase._request(
        supabase.main_client, "PATCH", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        json={"status": "analyzing"},
        label="sim_set_analyzing",
    )

    try:
        tenant_keys = await _resolve_tenant_ai_keys(sim["entity_id"])
        conversation_results = sim.get("conversation_results") or []
        if isinstance(conversation_results, str):
            import json
            conversation_results = json.loads(conversation_results)

        reviews = sim.get("reviews") or {}
        if isinstance(reviews, str):
            import json
            reviews = json.loads(reviews)

        result = await run_analysis(
            simulation_id=simulation_id,
            conversation_results=conversation_results,
            system_config=sim.get("system_config_snapshot") or {},
            setter_key=sim["setter_key"],
            reviews=reviews,
            tenant_keys=tenant_keys,
            entity_id=sim["entity_id"],
        )

        # Set back to complete
        await supabase._request(
            supabase.main_client, "PATCH", "/simulations",
            params={"id": f"eq.{simulation_id}"},
            json={"status": "complete"},
            label="sim_analyze_complete",
        )

        return {"status": "complete", "ai_analysis": result}

    except Exception as e:
        # Restore to complete on failure
        await supabase._request(
            supabase.main_client, "PATCH", "/simulations",
            params={"id": f"eq.{simulation_id}"},
            json={"status": "complete"},
            label="sim_analyze_failed_restore",
        )
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@testing_router.post("/simulator/{simulation_id}/rerun")
async def simulator_rerun(simulation_id: str, body: RerunRequest = RerunRequest()) -> dict[str, Any]:
    """Rerun a simulation (same config or edited). Creates a new simulation."""
    from app.main import _resolve_tenant_ai_keys

    # Load original
    resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={"id": f"eq.{simulation_id}", "select": "*"},
        label="sim_load_for_rerun",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Simulation not found")

    orig = data[0]
    if orig["status"] not in ("complete", "failed"):
        raise HTTPException(status_code=409, detail=f"Can only rerun completed/failed simulations (current: {orig['status']})")

    # Count existing reruns to auto-increment name
    count_resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={
            "previous_simulation_id": f"eq.{simulation_id}",
            "select": "id",
        },
        label="sim_count_reruns",
    )
    rerun_count = len(count_resp.json()) + 1
    suffix = f" — Rerun {rerun_count}" if rerun_count > 1 else " — Rerun"

    if body.edited and body.config:
        # Edited rerun — generate new plans from modified config
        tenant_keys = await _resolve_tenant_ai_keys(orig["entity_id"])
        config = body.config
        new_sim_id, plans, cost = await generate_plans(
            entity_id=orig["entity_id"],
            setter_key=config.get("setter_key", orig["setter_key"]),
            name=orig["name"] + suffix,
            personas=config.get("personas", orig["persona_config"]),
            scenarios=config.get("scenarios", orig["scenario_config"]),
            tenant_keys=tenant_keys,
        )
        # Update with rerun tracking
        await supabase._request(
            supabase.main_client, "PATCH", "/simulations",
            params={"id": f"eq.{new_sim_id}"},
            json={
                "previous_simulation_id": simulation_id,
                "is_edited_rerun": True,
            },
            label="sim_mark_edited_rerun",
        )
        return {"simulation_id": new_sim_id, "rerun_type": "edited"}
    else:
        # Exact rerun — copy plans from original
        new_row = {
            "tenant_id": orig["tenant_id"],
            "entity_id": orig["entity_id"],
            "entity_name": orig["entity_name"],
            "setter_key": orig["setter_key"],
            "name": orig["name"] + suffix,
            "persona_config": orig["persona_config"],
            "scenario_config": orig["scenario_config"],
            "conversation_plans": orig["conversation_plans"],
            "status": "planned",
            "total_conversations": orig["total_conversations"],
            "previous_simulation_id": simulation_id,
            "is_edited_rerun": False,
        }
        create_resp = await supabase._request(
            supabase.main_client, "POST", "/simulations",
            json=new_row,
            headers={"Prefer": "return=representation"},
            label="sim_create_rerun",
        )
        if create_resp.status_code >= 400:
            raise HTTPException(status_code=500, detail=f"Failed to create rerun: {create_resp.text}")
        new_sim_id = create_resp.json()[0]["id"]
        return {"simulation_id": new_sim_id, "rerun_type": "exact"}


@testing_router.post("/simulator/{simulation_id}/rerun-conversation")
async def simulator_rerun_conversation(simulation_id: str, body: RerunConversationRequest) -> dict[str, Any]:
    """Rerun a single conversation within a simulation. Runs as background task."""
    import asyncio
    from app.main import _resolve_tenant_ai_keys

    # Load simulation
    resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={"id": f"eq.{simulation_id}", "select": "status,entity_id,conversation_plans,conversation_results,scenario_config,setter_key,system_config_snapshot"},
        label="sim_load_for_conv_rerun",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Simulation not found")

    sim = data[0]
    if sim["status"] != "complete":
        raise HTTPException(status_code=409, detail=f"Can only rerun conversations in completed simulations (current: {sim['status']})")

    plans = sim.get("conversation_plans") or []
    plan = next((p for p in plans if p["id"] == body.conversation_plan_id), None)
    if not plan:
        raise HTTPException(status_code=404, detail=f"Conversation plan {body.conversation_plan_id} not found")

    tenant_keys = await _resolve_tenant_ai_keys(sim["entity_id"])

    # Run in background
    async def _run_conv_rerun():
        try:
            from app.testing.simulator import _plan_to_scenario, _build_conversation_result
            from app.testing.direct_runner import run_single_test

            entity_resp = await supabase._request(
                supabase.main_client, "GET", "/entities",
                params={"id": f"eq.{sim['entity_id']}", "select": "*"},
                label="sim_get_entity_conv_rerun",
            )
            entity_config = entity_resp.json()[0]

            # rerun_id is captured from the outer scope (set before task launch)
            scenario_config = sim.get("scenario_config") or {}
            total_turns = len(plan.get("turns", []))

            async def _on_rerun_progress(conversation: list[dict]) -> None:
                """Update turn progress on the placeholder card."""
                turns_done = sum(1 for e in conversation if e.get("role") == "turn")
                try:
                    fresh_resp = await supabase._request(
                        supabase.main_client, "GET", "/simulations",
                        params={"id": f"eq.{simulation_id}", "select": "conversation_results"},
                        label="sim_refresh_for_rerun_progress",
                    )
                    current = fresh_resp.json()[0].get("conversation_results") or []
                    for r in current:
                        if r.get("id") == rerun_id:
                            r["turns_completed"] = turns_done
                            r["total_turns"] = total_turns
                            break
                    await supabase._request(
                        supabase.main_client, "PATCH", "/simulations",
                        params={"id": f"eq.{simulation_id}"},
                        json={"conversation_results": current},
                        label="sim_store_rerun_progress",
                    )
                except Exception:
                    pass

            scenario = _plan_to_scenario(plan, scenario_config, sim["setter_key"])
            raw_result = await run_single_test(
                scenario=scenario,
                entity_config=entity_config,
                entity_id=sim["entity_id"],
                tenant_keys_override=tenant_keys,
                on_message=_on_rerun_progress,
            )

            conv_result = _build_conversation_result(raw_result, plan)
            conv_result["rerun_of"] = body.conversation_plan_id
            conv_result["id"] = rerun_id

            # Re-fetch current results (may have changed)
            fresh = await supabase._request(
                supabase.main_client, "GET", "/simulations",
                params={"id": f"eq.{simulation_id}", "select": "conversation_results"},
                label="sim_refresh_for_append",
            )
            existing_results = fresh.json()[0].get("conversation_results") or []
            # Replace partial with final result
            existing_results = [r for r in existing_results if r.get("id") != rerun_id]
            existing_results.append(conv_result)

            await supabase._request(
                supabase.main_client, "PATCH", "/simulations",
                params={"id": f"eq.{simulation_id}"},
                json={
                    "conversation_results": existing_results,
                    "ai_analysis": None,
                    "ai_analysis_cost": None,
                },
                label="sim_append_rerun_conv",
            )

            # Re-run compliance
            try:
                from app.testing.compliance import run_compliance
                await run_compliance(
                    simulation_id=simulation_id,
                    conversation_results=existing_results,
                    system_config=sim.get("system_config_snapshot") or {},
                    setter_key=sim["setter_key"],
                    tenant_keys=tenant_keys,
                )
            except Exception as e:
                logger.warning("CONV_RERUN | compliance failed: %s", e)

            logger.info("CONV_RERUN | complete | sim=%s | conv=%s", simulation_id, body.conversation_plan_id)
        except Exception as e:
            logger.error("CONV_RERUN | failed: %s", e, exc_info=True)

    # Immediately store an "ongoing" placeholder so the frontend shows the card right away
    rerun_id = f"{body.conversation_plan_id}_rerun_{int(datetime.now(timezone.utc).timestamp())}"
    existing_results = sim.get("conversation_results") or []
    total_turns = len(plan.get("turns", []))
    placeholder = {
        "id": rerun_id,
        "persona": plan["persona"],
        "scenario_type": plan["scenario_type"],
        "scenario_label": plan.get("scenario_label", ""),
        "outcome": "ongoing",
        "rerun_of": body.conversation_plan_id,
        "turns_completed": 0,
        "total_turns": total_turns,
        "turns": 0, "duration_ms": 0, "cost": 0,
        "messages": [],
        "decisions": {}, "tools": [], "variables": [],
    }
    existing_results.append(placeholder)
    await supabase._request(
        supabase.main_client, "PATCH", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        json={"conversation_results": existing_results},
        label="sim_store_rerun_placeholder",
    )

    asyncio.create_task(_run_conv_rerun())
    return {"status": "started", "conversation_plan_id": body.conversation_plan_id, "rerun_id": rerun_id}


@testing_router.delete("/simulator/{simulation_id}")
async def simulator_delete(simulation_id: str) -> dict[str, str]:
    """Delete a simulation. Allowed when status is planned, generating, complete, or failed."""
    # Load simulation to check status
    resp = await supabase._request(
        supabase.main_client, "GET", "/simulations",
        params={"id": f"eq.{simulation_id}", "select": "status,test_contact_ids,entity_id"},
        label="sim_get_for_delete",
    )
    data = resp.json()
    if not data:
        raise HTTPException(status_code=404, detail="Simulation not found")

    sim = data[0]
    allowed = {"planned", "generating", "complete", "failed"}
    if sim["status"] not in allowed:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete simulation with status '{sim['status']}'. Wait for it to finish.",
        )

    # Cleanup test contacts if they exist (leads, bookings, call_logs — NOT tool_executions or chat)
    if sim.get("test_contact_ids"):
        try:
            await cleanup_test_contacts(
                sim["test_contact_ids"],
                entity_id=sim["entity_id"],
                chat_table="",  # no chat history was written
                skip_tool_executions=True,  # keep tool_executions for inspector
            )
        except Exception as e:
            logger.warning("SIM_DELETE | cleanup warning: %s", e)

    # Delete the simulation row
    await supabase._request(
        supabase.main_client, "DELETE", "/simulations",
        params={"id": f"eq.{simulation_id}"},
        label="sim_delete",
    )

    return {"status": "deleted", "id": simulation_id}
