# n8n → Python Migration Tracker

> Generated: 2026-02-25
> Updated: 2026-02-28
> Status: **ALL WORKFLOWS MIGRATED** — deployment to production pending

---

## Migration Complete

All n8n workflows have been migrated to Python. Production backend is live at `https://api.bookmyleads.ai` (Hetzner/Coolify). Dr. K Beauty still on n8n — will migrate when ready.

### URL Structure

All endpoints live under `https://api.bookmyleads.ai`.

**Per-client webhooks** (triggered by GHL, client_id in path):
```
POST /webhook/{client_id}/reply              ← Text Engine (already done)
POST /webhook/{client_id}/followup           ← Text Engine (already done)
POST /webhook/{client_id}/missed-call        ← Missed Call Text-Back
POST /webhook/{client_id}/resolve-outreach   ← Outreach Resolver
POST /webhook/{client_id}/update-kb          ← Update Knowledge Base
POST /webhook/{client_id}/clean-chat-history ← Clean Chat History
```

In GHL, all of these use `{{custom_values.client_id}}` in the URL — same pattern as reply/followup.

**Scheduled / system workflows** (no GHL trigger, run internally):
```
POST /workflows/daily-reports                ← Daily Client Reports (cron)
```

These loop through all clients internally. Triggered by a scheduler (Prefect, cron, or a simple timer).

### Endpoint Summary

| Endpoint | Python File | Status |
|----------|-------------|--------|
| `POST /webhook/{client_id}/reply` | `app/text_engine/pipeline.py` | ✅ Migrated |
| `POST /webhook/{client_id}/followup` | `app/text_engine/followup.py` | ✅ Migrated |
| `POST /webhook/{client_id}/missed-call` | `app/workflows/missed_call_textback.py` | ✅ Migrated |
| `POST /webhook/{client_id}/resolve-outreach` | `app/workflows/outreach_resolver.py` | ✅ Migrated |
| `POST /workflows/daily-reports` | `app/workflows/daily_reports.py` | ✅ Migrated |
| `POST /webhook/{client_id}/update-kb` | `app/workflows/update_kb.py` | ✅ Migrated |
| `POST /webhook/{client_id}/clean-chat-history` | `app/workflows/clean_chat_history.py` | ✅ Migrated |

---

## Already Migrated (Done)

These n8n workflows are already fully implemented in Python. The **Cutover Action** column shows what still needs to be pointed at the new Python endpoints before the n8n workflow can be disabled.

| Workflow | Python Location | Cutover Action | Notes |
|----------|----------------|----------------|-------|
| Outreach Resolver | `app/workflows/outreach_resolver.py` | ✅ **Done** — GHL webhook updated | 50/50 tests passing. Checks if AI already replied to a drip outreach, skips if so. |
| Missed Call Text-Back | `app/workflows/missed_call_textback.py` | ✅ **Done** — GHL webhook updated. Body now sends `{id, name, email, phone, clientId}`. | 15/15 tests passing. Background processing via `asyncio.create_task()`. 15-min hard gate + AI gate for repeat callers. Gemini Flash temp 1.2. |
| Daily Client Reports | `app/workflows/daily_reports.py` | **None** — runs on internal scheduler (8:45 AM America/Chicago). Disable n8n schedule after confirming Python runs are clean. | 4 Supabase RPCs + cross-DB metrics. Claude AI digest (Mondays) + daily alerts. Per-client Slack channels. Manual trigger: `POST /workflows/daily-reports?dry_run=true`. |
| Update Knowledge Base | `app/workflows/update_kb.py` | **Agency Interface** → update KB create/update/delete in dashboard to POST to `{backend}/webhook/{client_id}/update-kb` instead of n8n webhook (`edd42465-d466-48a8-b66b-c69a7d5c3b8c`). Also update any scripts that call the n8n KB webhook. | Embedding pipeline: chunks (1500 chars, 50 overlap) → OpenAI `text-embedding-3-small` → `documents` table. Supports upsert + delete. |
| Clean Chat History | `app/workflows/clean_chat_history.py` | ✅ **Done** — GHL webhook updated. Body sends `{id, name, email, phone, clientId}`. **Agency Interface** still needs "Undo AI Response" button updated to POST to Python endpoint. | Simplified from n8n (deleted last 2 rows) to only delete most recent AI message. Atomic SQL DELETE with subquery. |
| V6 Text Engine (12 sub-workflows) | `app/text_engine/` | The entire conversation pipeline |
| Load Config (Clients) | `app/services/supabase_client.py` → `resolve_entity()` | Sub-workflow → shared function |
| knowledge_base_search | `app/tools/knowledge_base_search.py` | Agent tool |
| ghl_get_available_slots | `app/tools/get_available_slots.py` | Agent tool |
| ghl_book_appointment | `app/tools/book_appointment.py` | Agent tool |
| Transfer To Human | `app/tools/transfer_to_human.py` | Agent tool |
| ghl_get_appointments | `app/tools/get_appointments.py` | Legacy agent tool (file retained as dormant fallback, not registered in the live Python tool registry) |
| Media Analysis (Subworkflow) | `app/text_engine/attachments.py` + `app/tools/reanalyze_attachment.py` | Pipeline step + agent tool |
| ghl_update_appointment | `app/tools/update_appointment.py` | Agent tool |
| ghl_cancel_appointment | `app/tools/cancel_appointment.py` | Agent tool |

---

## Deprecated n8n Workflows

These n8n workflows are no longer needed — their functionality is either handled by Python or no longer required.

| Workflow | Status | Reason |
|----------|--------|--------|
| Log Manual Messages To ChatHistory | **DEPRECATED** | Replaced by Python `conversation_sync.py` |
| Log AI Calls | **DEPRECATED** | Replaced by Python `conversation_sync.py` |
| Transcribe & Log NON AI Calls | **DEPRECATED** | Replaced by Python `conversation_sync.py` |
| Manual Call Logging | **DEPRECATED** | Replaced by Python `conversation_sync.py` |
| Notify Staff | **DEPRECATED** | Was only for AI Voice (removed) |
| Appointment Reminder Logger | **DEPRECATED** | Replaced by GHL conversation sync |
| RVM Delivery | **REMOVED** | Feature discontinued — RVM removed from UI and backend. |
| Clay Refresh Webhook | Clay-specific | Low priority, stays on n8n for now |
| Clay Delete/Rerun Rows | Clay-specific | Low priority, stays on n8n for now |
| Error Workflow | n8n internal | n8n error handler — decommission with n8n |
| GHL Leads into SupaBase | GHL trigger | Simple DB insert, low priority — eventually Python or direct Supabase |

---

## How to Migrate a Workflow

1. **Find the n8n JSON** — either in `.tmp/` or fetch via API:
   ```bash
   python -c "
   from shared.supabase_utils import sb
   # Or fetch from n8n API directly
   "
   ```

2. **Read the n8n JSON** — understand triggers, nodes, and data flow

3. **Create the Python file** at `app/workflows/<name>.py`:
   - Use shared services (`ghl_client`, `supabase_client`, `postgres_client`, `ai_client`, `slack`)
   - No need to recreate sub-workflow calls — just call the Python function directly
   - Use decoupled shared components where applicable:
     - `run_conversation_sync()` from `app/text_engine/conversation_sync.py`
     - `format_timeline()` from `app/text_engine/timeline.py`
     - `run_webhook_post()` from `app/text_engine/delivery.py`

4. **Wrap the main function with `@flow`** (Prefect orchestration — MANDATORY):
   ```python
   from prefect import flow

   @flow(name="workflow-name-here", retries=0)
   async def my_workflow_function(...):
       ...
   ```
   Every migrated workflow MUST be a Prefect flow so it appears in the Prefect dashboard with full logging and execution tracking.

5. **Wire up in `app/main.py`** with Prefect tags:
   ```python
   from prefect import tags as prefect_tags

   # In the endpoint handler:
   set_request_context(client_id=client_id, trigger_type="my-type", contact_id=contact_id)
   try:
       run_tags = ["entity:{id}", "contact:{contact_id}", "type:my-type"]
       with prefect_tags(*run_tags):
           result = await my_workflow.with_options(name=f"my-type--{client_id[:8]}")(...)
   finally:
       clear_request_context()
   ```
   Tags enable filtering/grouping in the Prefect dashboard. The `set_request_context` / `clear_request_context` calls enable structured JSON logging with client/contact context in every log line.

6. **Use structured logging** throughout:
   ```python
   import logging
   logger = logging.getLogger(__name__)

   # Use PREFIX | key=value format for all log lines
   logger.info("WORKFLOW_NAME | step_description | key=%s value=%d", var1, var2)
   ```

7. **Run the post-migration checklist** (below)

8. **Update this file** — mark ✅ and add the Python path

---

## Post-Migration Checklist (MANDATORY)

Run through this for EVERY migrated workflow. Do not skip any step.

### GHL Webhook Updates
- [ ] **Switch GHL webhook URL** from n8n (`https://n8n.bookmyleads.ai/webhook/...`) to Python backend URL
- [ ] **Move ALL data from query parameters to POST body.** GHL workflows that previously sent data via query params (`?User_Id=...&ReplyChannel=SMS`) MUST be updated to send everything in the request body instead. Query params are fragile (URL encoding issues, length limits, visible in logs). The Python endpoints accept body-first with query param fallback for backwards compat, but all NEW/updated GHL workflows should use body only.
- [ ] **Verify the GHL workflow trigger** fires correctly (test with a real or test contact)
- [ ] **Verify response handling** — if GHL expects a specific response format, confirm Python returns it

### Python Endpoint Verification
- [ ] **Pydantic model defined** for the request body (validates all incoming fields)
- [ ] **Error handling** — endpoint returns proper HTTP status codes (not silent failures)
- [ ] **Logging** — structured logging with relevant context (client_id, contact_id, etc.)
- [ ] **Test mode support** — if applicable, respects "AI Test Mode" tag to avoid production side effects

### n8n Cleanup
- [ ] **Disable the n8n workflow** after Python version is confirmed working
- [ ] **Do NOT delete** the n8n workflow yet — keep as reference/rollback for 2 weeks

---

## Legend

- **⬜** = Not started
- **🔄** = In progress
- **✅** = Converted to Python
- **❌** = Stays on n8n
