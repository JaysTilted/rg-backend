"""Workflow run tracker — fire-and-forget persistence to workflow_runs table.

Usage:
    tracker = WorkflowTracker("reply", entity_id=eid, ghl_contact_id=cid)

    async with tracker.stage("classification"):
        result = await classify(...)
        tracker.set_decisions({"classification": result})

    tracker.set_token_usage(ctx.token_usage)
    await tracker.save()

Every workflow wraps its execution with a tracker. The tracker records stages
(with timing), decisions, LLM call details, and writes one row to workflow_runs
at the end. save() NEVER raises — logging failures must not block workflows.
"""

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from app.services.supabase_client import supabase

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class WorkflowTracker:
    """Tracks a single workflow execution for the workflow_runs table."""

    def __init__(
        self,
        workflow_type: str,
        *,
        entity_id: str | None = None,
        ghl_contact_id: str | None = None,
        lead_id: str | None = None,
        parent_run_id: str | None = None,
        trigger_source: str = "webhook",
        mode: str = "production",
    ) -> None:
        # Pre-generate UUID v4 — same type Supabase uses (gen_random_uuid()).
        # Available before save() so callers can link chat messages to this run.
        self.run_id: str = str(uuid.uuid4())

        self._workflow_type = workflow_type
        self._entity_id = entity_id
        self._ghl_contact_id = ghl_contact_id
        self._lead_id = lead_id
        self._parent_run_id = parent_run_id
        self._trigger_source = trigger_source
        self._mode = mode

        self._stages: list[dict[str, Any]] = []
        self._decisions: dict[str, Any] = {}
        self._metadata: dict[str, Any] = {}
        self._error: str | None = None
        self._status: str | None = None
        self._system_config_snapshot: dict[str, Any] | None = None
        self._runtime_context: dict[str, Any] | None = None

        # AI cost tracking (replaces ai_usage_log)
        self._total_cost: float = 0
        self._prompt_tokens: int = 0
        self._completion_tokens: int = 0
        self._total_tokens: int = 0
        self._call_count: int = 0
        self._llm_calls: list[dict[str, Any]] = []

        self._start_time = perf_counter()

    # =========================================================================
    # STAGE TRACKING
    # =========================================================================

    @asynccontextmanager
    async def stage(self, name: str):
        """Context manager that tracks stage timing and status.

        Re-raises exceptions so workflow error handling is unaffected.
        Records error on the stage if an exception occurs.
        """
        stage_entry: dict[str, Any] = {
            "name": name,
            "status": "running",
            "started_at": _utcnow_iso(),
        }
        self._stages.append(stage_entry)
        t0 = perf_counter()
        try:
            yield stage_entry
            stage_entry["status"] = "success"
        except Exception as e:
            stage_entry["status"] = "error"
            stage_entry["error"] = str(e)[:500]
            raise
        finally:
            stage_entry["duration_ms"] = int((perf_counter() - t0) * 1000)

    # =========================================================================
    # DATA SETTERS
    # =========================================================================

    def set_decisions(self, decisions: dict[str, Any]) -> None:
        """Merge decision data into the decisions dict."""
        self._decisions.update(decisions)

    def set_metadata(self, key: str, value: Any) -> None:
        """Set a single metadata key."""
        self._metadata[key] = value

    def set_status(self, status: str) -> None:
        """Explicitly set run status (running/success/error/skipped)."""
        self._status = status

    def set_error(self, msg: str) -> None:
        """Record an error message. Also sets status to 'error'."""
        self._error = str(msg)[:2000]
        self._status = "error"

    def set_lead_id(self, lead_id: str) -> None:
        """Set lead_id (often resolved mid-pipeline, not at start)."""
        self._lead_id = lead_id

    def set_parent_run_id(self, run_id: str) -> None:
        """Link this run to a parent run (for chain tracing)."""
        self._parent_run_id = run_id

    def set_system_config(self, config: dict[str, Any] | None, setter_key: str | None = None) -> None:
        """Snapshot the entity's system_config at the time of this run.

        If setter_key is provided, filters the setters dict to only include
        the setter that was actually used — strips irrelevant setters to
        keep the snapshot focused and smaller.
        """
        if not config:
            self._system_config_snapshot = config
            return

        snapshot = dict(config)
        setters = snapshot.get("setters")
        if setters and isinstance(setters, dict) and setter_key:
            # Keep only the setter that was used
            matched = setters.get(setter_key)
            if matched:
                snapshot["setters"] = {setter_key: matched}
                snapshot["_active_setter_key"] = setter_key
            # else: setter_key didn't match any — keep all (safety fallback)
        elif setters and isinstance(setters, dict) and not setter_key:
            # No setter_key — find the default setter
            for key, val in setters.items():
                if isinstance(val, dict) and val.get("is_default"):
                    snapshot["setters"] = {key: val}
                    snapshot["_active_setter_key"] = key
                    break

        self._system_config_snapshot = snapshot

    def set_runtime_context(self, context: dict[str, Any]) -> None:
        """Store runtime variables that were injected into LLM prompts for this run."""
        self._runtime_context = context

    def set_entity_id(self, entity_id: str) -> None:
        """Set entity_id (for workflows that resolve it after init)."""
        self._entity_id = entity_id

    def set_ghl_contact_id(self, ghl_contact_id: str) -> None:
        """Set GHL contact ID (if resolved after init)."""
        self._ghl_contact_id = ghl_contact_id

    # =========================================================================
    # AI COST TRACKING
    # =========================================================================

    def set_token_usage(self, token_usage: Any) -> None:
        """Import from an existing TokenUsage accumulator.

        Extracts all per-call data including the provider field.
        Accepts TokenUsage dataclass or a summary dict.
        """
        from app.models import TokenUsage

        if isinstance(token_usage, TokenUsage):
            self._llm_calls = token_usage.calls
            self._total_cost = token_usage.total_cost()
            self._prompt_tokens = token_usage.prompt_tokens
            self._completion_tokens = token_usage.completion_tokens
            self._total_tokens = token_usage.total_tokens
            self._call_count = token_usage.call_count
        elif isinstance(token_usage, dict):
            self._llm_calls = token_usage.get("calls", [])
            self._total_cost = (
                token_usage["cost_usd"] if "cost_usd" in token_usage
                else token_usage["estimated_cost_usd"] if "estimated_cost_usd" in token_usage
                else token_usage.get("total_cost_usd", 0)
            )
            self._prompt_tokens = token_usage.get("prompt_tokens", 0)
            self._completion_tokens = token_usage.get("completion_tokens", 0)
            self._total_tokens = token_usage.get("total_tokens", 0)
            self._call_count = token_usage.get("call_count", 0)

    def add_llm_call(
        self,
        *,
        model: str,
        provider: str,
        label: str,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
        cost_usd: float = 0,
        latency_ms: int = 0,
    ) -> None:
        """Manually record one LLM call (for workflows not using TokenUsage)."""
        self._llm_calls.append({
            "model": model,
            "provider": provider,
            "label": label,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "cost_usd": round(cost_usd, 6),
            "latency_ms": latency_ms,
        })
        self._total_cost += cost_usd
        self._prompt_tokens += prompt_tokens
        self._completion_tokens += completion_tokens
        self._total_tokens += total_tokens
        self._call_count += 1

    # =========================================================================
    # PERSISTENCE
    # =========================================================================

    async def save(self) -> None:
        """Write to workflow_runs table. NEVER raises."""
        try:
            tenant_id = await self._resolve_tenant_id()
            if tenant_id is None:
                logger.error(
                    "WORKFLOW_TRACKER | no tenant_id resolved — run discarded | entity=%s | type=%s",
                    self._entity_id, self._workflow_type,
                )
                return

            status = self._status
            if status is None:
                status = "error" if self._error else "success"

            row: dict[str, Any] = {
                "id": self.run_id,
                "tenant_id": tenant_id,
                "entity_id": self._entity_id,
                "ghl_contact_id": self._ghl_contact_id,
                "lead_id": self._lead_id,
                "parent_run_id": self._parent_run_id,
                "workflow_type": self._workflow_type,
                "status": status,
                "trigger_source": self._trigger_source,
                "mode": self._mode,
                "stages": self._stages,
                "decisions": self._decisions,
                "error_message": self._error,
                "duration_ms": int((perf_counter() - self._start_time) * 1000),
                "total_cost": round(self._total_cost, 6),
                "prompt_tokens": self._prompt_tokens,
                "completion_tokens": self._completion_tokens,
                "total_tokens": self._total_tokens,
                "call_count": self._call_count,
                "llm_calls": self._llm_calls,
                "metadata": self._metadata,
                "system_config_snapshot": self._system_config_snapshot,
                "runtime_context": self._runtime_context,
            }

            # Strip None values to let DB defaults apply
            row = {k: v for k, v in row.items() if v is not None}

            resp = await supabase._request(
                supabase.main_client,
                "POST",
                "/workflow_runs",
                json=row,
                headers={"Prefer": "return=representation"},
                label="save_workflow_run",
            )
            if resp.status_code == 409:
                logger.warning(
                    "WORKFLOW_TRACKER | insert conflict | type=%s | run_id=%s | attempting patch",
                    self._workflow_type,
                    self.run_id,
                )
                patch_resp = await supabase._request(
                    supabase.main_client,
                    "PATCH",
                    "/workflow_runs",
                    params={"id": f"eq.{self.run_id}"},
                    json=row,
                    headers={"Prefer": "return=representation"},
                    label="save_workflow_run_conflict_patch",
                )
                if patch_resp.status_code >= 400:
                    logger.warning(
                        "WORKFLOW_TRACKER | patch after conflict failed | run_id=%s | status=%d | body=%s",
                        self.run_id,
                        patch_resp.status_code,
                        patch_resp.text[:500],
                    )
                    return
            elif resp.status_code >= 400:
                logger.warning(
                    "WORKFLOW_TRACKER | save failed | run_id=%s | status=%d | body=%s",
                    self.run_id,
                    resp.status_code,
                    resp.text[:500],
                )
                return
            logger.info(
                "WORKFLOW_TRACKER | saved | type=%s | entity=%s | status=%s | cost=%.6f | duration=%dms",
                self._workflow_type, self._entity_id, status,
                self._total_cost, row.get("duration_ms", 0),
            )
        except Exception:
            logger.warning("WORKFLOW_TRACKER | save failed (non-fatal)", exc_info=True)

    async def _resolve_tenant_id(self) -> str | None:
        """Get tenant_id from entity. Falls back to platform owner tenant."""
        try:
            if self._entity_id:
                resp = await supabase._request(
                    supabase.main_client,
                    "GET",
                    "/entities",
                    params={"id": f"eq.{self._entity_id}", "select": "tenant_id"},
                    label="tracker_resolve_tenant",
                )
                rows = resp.json()
                if rows and rows[0].get("tenant_id"):
                    return rows[0]["tenant_id"]

            # Fallback: platform owner tenant (for internal workflows)
            resp = await supabase._request(
                supabase.main_client,
                "GET",
                "/tenants",
                params={"is_platform_owner": "eq.true", "select": "id"},
                label="tracker_platform_tenant",
            )
            rows = resp.json()
            return rows[0]["id"] if rows else None
        except Exception:
            logger.warning("WORKFLOW_TRACKER | tenant resolution failed", exc_info=True)
            return None
