# Claude Code Master Backend Handoff

This is the backend-only source of truth for handing `rg-backend` to another operator using Claude Code.

> Repo snapshot note: this clone does not include every guide path referenced later in this document. If a listed guide file is missing, use this handoff file, the bootstrap SQL, the starter system-config JSON, and the live code in `app/` as the source of truth.

Assumptions for this handoff:

- He already has the `rg-backend` repository.
- He is not using the frontend at all.
- He is using one Supabase project, not separate Main and Chat projects.
- Claude Code will be the primary coding/operator agent.
- The up-to-date chat history schema reference is `peak_fitness_gym_chat_history_e2e_test`.

This file is written to be directly useful to Claude Code.

## 1. What This Repo Actually Is

`rg-backend` is the operational brain of the system.

It does 3 main jobs:

1. Runs the Text Engine for inbound lead conversations.
2. Runs standalone workflows around outreach, bookings, missed calls, reactivation, KB sync, testing, and reporting.
3. Stores state in Supabase and syncs delivery/conversation state with GoHighLevel.

The live stack is:

- FastAPI app in `app/main.py`
- Prefect orchestration sidecar from Docker Compose
- Supabase for config/data/state
- GoHighLevel for CRM, messaging, calendars, contact records
- LLM providers through OpenRouter and direct fallback providers

Core folders:

- `app/main.py`: live HTTP routes
- `app/text_engine/`: reply pipeline, delivery, sync, timeline, booking context, follow-up logic
- `app/workflows/`: standalone workflows
- `app/tools/`: agent tools
- `app/services/`: Supabase, Postgres, GHL, AI, notifications, scheduler
- `app/testing/`: direct runner, simulator, sandbox

## 2. Read This First If You Are Claude Code

Read these in this order:

1. `CLAUDE_CODE_MASTER_BACKEND_HANDOFF.md`
2. `app/main.py`
3. `shared/api-guides/python-backend-guide.md`
4. `service-delivery/guides/python-text-engine.md`
5. `app/services/supabase_client.py`
6. `app/services/postgres_client.py`
7. `app/text_engine/data_loading.py`
8. `app/text_engine/model_resolver.py`

The code is more current than some docs. Trust live code first.
If one of the guide paths above is absent in this repo snapshot, skip it and continue with the remaining live-code files.

## 2.5 Documentation Trust Order

If written docs disagree, use this order:

1. live code in `app/`
2. `service-delivery/guides/system-config-schema.md`
3. workflow-specific guides in `service-delivery/guides/*workflow.md`
4. `service-delivery/guides/python-text-engine.md`
5. `shared/api-guides/python-backend-guide.md`
6. migration/history docs like `N8N_TO_PYTHON_MIGRATION.md`
7. old broad docs like `service-delivery/guides/supabase-database.md`

## 2.6 Best Supporting Docs

### Highest-value current docs

- `service-delivery/guides/system-config-schema.md`
- `service-delivery/guides/ghl-workflows.md`
- `service-delivery/guides/python-text-engine.md`
- `shared/api-guides/python-backend-guide.md`
- `service-delivery/guides/post-appointment-workflow.md`

### Workflow deep dives

- `service-delivery/guides/outreach-resolver-workflow.md`
- `service-delivery/guides/booking-logger-workflow.md`
- `service-delivery/guides/missed-call-textback-workflow.md`
- `service-delivery/guides/reactivation-workflow.md`
- `service-delivery/guides/utility-workflows.md`

## 3. Important Reality Checks

These matter because older docs and migration notes are not perfectly current.

### Live-code truths

- There is no live external follow-up webhook route in `app/main.py`.
- Follow-ups are now scheduled internally through `scheduled_messages` and fired by `app/workflows/message_scheduler_loop.py`.
- Reactivation is also Python-scheduled internally.
- The backend now uses `entities` and `tenants`, not the old `clients` / `personal_bots` runtime model.
- `system_config` is the real source of truth for setter behavior.

### Current mismatches I verified

- `pipeline_runs` is still referenced by legacy testing routes, but it does not exist in the current live main DB.
- Most active chat tables use the new flat schema, but there are still legacy/misaligned entity records in the current environment.
- For any new setup, standardize everything on the flat schema used by `peak_fitness_gym_chat_history_e2e_test`.

### What to ignore

- Any old docs centered on `clients`, `personal_bots`, or a required n8n follow-up webhook.
- Any legacy JSONB chat-history implementation for new work.
- Frontend-driven assumptions unless you explicitly decide to keep portal/data-chat/testing UI features.

## 4. One Supabase Project Setup

The code still expects two env var groups:

- `SUPABASE_MAIN_*`
- `SUPABASE_CHAT_*`

For his one-project setup, point both sets to the same Supabase project.

Use this pattern:

```env
SUPABASE_MAIN_URL=https://your-project.supabase.co
SUPABASE_MAIN_KEY=YOUR_SERVICE_ROLE_KEY

SUPABASE_CHAT_URL=https://your-project.supabase.co
SUPABASE_CHAT_KEY=YOUR_SERVICE_ROLE_KEY

DATABASE_URL=postgresql://...
DATABASE_CHAT_URL=postgresql://...
```

For a one-project setup:

- `SUPABASE_MAIN_URL` and `SUPABASE_CHAT_URL` should be identical.
- `SUPABASE_MAIN_KEY` and `SUPABASE_CHAT_KEY` should be identical.
- `DATABASE_URL` and `DATABASE_CHAT_URL` can be identical.

The repo logically separates "main" and "chat", but it does not require separate Supabase projects if the tables all live in one database.

Bootstrap SQL for this setup lives at:

- `rg-backend/BACKEND_ONLY_ONE_PROJECT_BOOTSTRAP.sql`

Starter `system_config` JSON lives at:

- `rg-backend/STARTER_SYSTEM_CONFIG_TEMPLATE.json`

## 5. Required Environment Variables

### Required for core runtime

```env
SUPABASE_MAIN_URL=
SUPABASE_MAIN_KEY=
SUPABASE_CHAT_URL=
SUPABASE_CHAT_KEY=
DATABASE_URL=
DATABASE_CHAT_URL=
API_AUTH_TOKEN=
OPENROUTER_API_KEY=
PREFECT_API_URL=http://prefect-server:4200/api
```

### Usually required in real usage

```env
OPENAI_API_KEY=
GOOGLE_GEMINI_API_KEY=
ANTHROPIC_API_KEY=
DEEPSEEK_API_KEY=
XAI_API_KEY=
```

Notes:

- `OPENROUTER_API_KEY` is the platform fallback key.
- Per-tenant AI keys are resolved from `tenants`.
- `OPENAI_API_KEY` matters for embeddings / KB tooling.
- Direct provider keys are used as fallbacks in `app/services/ai_client.py`.

### Signal House only if used

If `system_config.sms_provider = "signalhouse"`, these are also required:

```env
SIGNAL_HOUSE_API_KEY=
SIGNAL_HOUSE_AUTH_TOKEN=
```

Without those keys, the backend can still send through GHL, but Signal House delivery polling will not work correctly.

## 5.4 SMS Provider: What It Actually Does

This is important because the runtime behavior is more specific than the UI wording.

The value lives at:

- `entities.system_config.sms_provider`

Valid runtime values from the live code are:

- `signalhouse`
- `ghl_default`
- `imessage`
- `other`

### Important runtime truth

All SMS sends still go through the GHL API first.

`sms_provider` does not replace GHL as the send entry point.

What it changes is:

- how delivery confirmation is checked after send
- whether the backend treats the setup like an iMessage-style no-poll provider
- certain follow-up media behaviors for GIF/voice-note companion text

### Option 1: `ghl_default`

Recommended safest default for most setups.

Behavior:

- send through GHL API
- poll GHL message status endpoint for delivery confirmation

Use this when:

- he does not use Signal House
- he wants a straightforward GHL-native transport path

### Option 2: `signalhouse`

Advanced option only if Signal House is actually part of his delivery setup.

Behavior:

- send through GHL API
- then poll Signal House logs for delivery confirmation

Requirements:

- `SIGNAL_HOUSE_API_KEY`
- `SIGNAL_HOUSE_AUTH_TOKEN`
- a valid `business_phone`
- resolvable destination phone

Use this when:

- he specifically wants Signal House delivery confirmation behavior

### Option 3: `imessage`

This is the iMessage-enabled path.

Behavior:

- send through GHL API
- do not poll for delivery confirmation
- treat the client as an iMessage-style provider in follow-up media logic

Important effect:

- for GIFs and voice notes, the backend may send companion text on SMS/iMessage channels when `sms_provider = "imessage"`

Use this when:

- the business is actually running the iMessage-style setup this backend expects

### Option 4: `other`

Fallback / custom no-poll provider mode.

Behavior:

- send through GHL API
- do not poll for delivery confirmation

Use this when:

- he wants to bypass polling logic
- the provider does not fit the known cases above

### Recommended default

For a new owner with no special delivery stack:

- use `ghl_default`

Use `signalhouse` only if Signal House is definitely configured.

Use `imessage` only if the account is intentionally operating in the iMessage mode expected by the code.

### How Claude should set it

Inside `entities.system_config`:

```json
{
  "sms_provider": "ghl_default"
}
```

### How Claude should verify it

Query:

```sql
select id, name, system_config->>'sms_provider' as sms_provider
from public.entities;
```

Expected:

- one of `signalhouse`, `ghl_default`, `imessage`, `other`

## 5.5 Non-Negotiable External Prerequisites

Claude cannot successfully stand up a working production backend unless these real-world inputs exist:

- one Supabase project
- one real GoHighLevel sub-account
- that GHL sub-account's:
  - location id
  - API key
- at least one AI key:
  - either a tenant-level OpenRouter key
  - or a platform `OPENROUTER_API_KEY`
- one backend host for webhook targets:
  - local for testing
  - or deployed URL for production

If any of those are missing, Claude can still set up schema and config, but it cannot complete a real end-to-end production test.

## 5.6 Attachments Bucket Setup

The backend expects a storage bucket named `attachments`.

The bootstrap SQL creates it.

Expected bucket properties:

- bucket id: `attachments`
- bucket name: `attachments`
- public: `true`

Why public:

- the backend uploads media with the service-role key
- then stores public URLs in the chat/attachment pipeline
- reply and follow-up flows expect those URLs to be directly usable

What Claude should verify after bootstrap:

```sql
select id, name, public
from storage.buckets
where id = 'attachments';
```

Expected result:

- exactly one row
- `public = true`

## 5.7 Zero-Context Setup Procedure For Claude

If Claude is dropped into this repo with no outside context, this is the exact order it should follow.

### Step 1: create `.env`

Use `rg-backend/.env.example` as the starting point.

For the one-project setup:

- set `SUPABASE_MAIN_URL` and `SUPABASE_CHAT_URL` to the same project URL
- set `SUPABASE_MAIN_KEY` and `SUPABASE_CHAT_KEY` to the same service-role key
- set `DATABASE_URL` and `DATABASE_CHAT_URL` to the same Postgres connection string

### Step 2: run bootstrap SQL

Run:

- `rg-backend/BACKEND_ONLY_ONE_PROJECT_BOOTSTRAP.sql`

This creates:

- required tables
- required functions
- required triggers
- required bucket
- helper functions for chat table creation and entity creation

### Step 3: verify the schema exists

Claude should verify at minimum:

```sql
select tablename
from pg_tables
where schemaname = 'public'
  and tablename in (
    'tenants',
    'entities',
    'leads',
    'bookings',
    'call_logs',
    'outreach_templates',
    'scheduled_messages',
    'workflow_runs',
    'knowledge_base_articles',
    'documents',
    'attachments',
    'tool_executions'
  )
order by tablename;
```

### Step 4: create the first tenant

Example:

```sql
insert into public.tenants (
  name,
  is_platform_owner,
  openrouter_api_key
)
values (
  'My Tenant',
  true,
  'REPLACE_WITH_REAL_KEY'
)
returning id;
```

### Step 5: create the first entity and chat table

Use the helper created by the bootstrap:

```sql
select *
from public.create_entity_with_chat_table(
  'REPLACE_WITH_TENANT_UUID',
  'My Business',
  'client'
);
```

That creates:

- one `entities` row
- one canonical flat chat table
- all required chat-table indexes

### Step 6: update the entity with real business + GHL data

Claude should populate:

- `ghl_location_id`
- `ghl_api_key`
- `timezone`
- `business_phone`
- `business_schedule`
- `supported_languages`
- `system_config`

Example `business_schedule`:

```json
{
  "monday": { "enabled": true, "start": "09:00", "end": "17:00" },
  "tuesday": { "enabled": true, "start": "09:00", "end": "17:00" },
  "wednesday": { "enabled": true, "start": "09:00", "end": "17:00" },
  "thursday": { "enabled": true, "start": "09:00", "end": "17:00" },
  "friday": { "enabled": true, "start": "09:00", "end": "17:00" },
  "saturday": { "enabled": false, "start": "09:00", "end": "17:00" },
  "sunday": { "enabled": false, "start": "09:00", "end": "17:00" }
}
```

### Step 7: set `system_config`

Start from:

- `rg-backend/STARTER_SYSTEM_CONFIG_TEMPLATE.json`

Claude should:

1. load that file
2. customize it for the business
3. write the full JSON into `entities.system_config`

Minimum production expectation:

- one default setter
- valid bot persona
- real services/pricing
- booking config
- transfer config
- security config

### Step 8: set KB webhook DB settings

If he wants KB article edits to auto-trigger embedding sync through the backend, Claude must set:

```sql
alter database postgres set "app.settings.kb_webhook_base_url" = 'https://YOUR_BACKEND_HOST';
alter database postgres set "app.settings.api_auth_token" = 'YOUR_API_AUTH_TOKEN';
```

If these are not set:

- the KB trigger function still exists
- but it will no-op instead of posting to the backend

### Step 9: start the backend

Local Docker path:

```powershell
cd rg-backend
docker compose up -d
docker compose logs -f rg-backend
```

### Step 10: smoke test

Claude should run:

1. `GET /health`
2. one booking test
3. one call-event or missed-call test
4. one reply test
5. one outreach scheduling test if outreach mode 1 is enabled

### Step 11: verify database state

Claude should not stop at HTTP 200s.

It should verify table state after every test.

## 5.8 What Claude Must Verify After First Setup

After setup, Claude should confirm all of these are true:

- one tenant row exists
- one entity row exists
- `entities.chat_history_table_name` is not null
- the referenced chat table exists
- `entities.ghl_location_id` is set
- `entities.ghl_api_key` is set
- `entities.system_config` is set
- `storage.buckets` contains `attachments`
- `match_documents` exists
- `kb_articles_embedding_sync` trigger exists
- the backend returns `{"status":"ok"}` from `/health`

## 5.9 If GHL Is Not Ready Yet

If GHL credentials are not available yet, Claude should not pretend to run true end-to-end route tests.

Without real GHL credentials, these production paths are not fully testable:

- `/webhook/reply`
- `/webhook/log-booking`
- `/webhook/call-event`
- `/webhook/missed-call`
- `/webhook/human-activity`

Because those paths depend on:

- real contacts
- real location timezone lookups
- real conversation sync
- real delivery paths

If GHL is not ready, Claude should limit itself to:

- schema verification
- helper function verification
- health check
- entity/system_config seeding
- optional mock/testing-stack work only if that extra schema is intentionally added

### Optional by feature

```env
OPENROUTER_TESTING_KEY=
GHL_SNAPSHOT_API_KEY=
GHL_SNAPSHOT_LOCATION_ID=
RESEND_API_KEY=
RESEND_FROM_EMAIL=
SLACK_BOT_TOKEN=
PORTAL_JWT_SECRET=
FRONTEND_URL=
SIGNAL_HOUSE_API_KEY=
SIGNAL_HOUSE_AUTH_TOKEN=
```

Feature mapping:

- `OPENROUTER_TESTING_KEY`: testing routes
- `GHL_SNAPSHOT_*`: staff/tenant SMS notifications
- `RESEND_*`: notification emails
- `SLACK_BOT_TOKEN`: daily reports / error alerts
- `PORTAL_JWT_SECRET`, `FRONTEND_URL`: portal routes only
- `SIGNAL_HOUSE_*`: delivery verification path in `delivery_service.py`

## 6. What Routes Matter In A Backend-Only Setup

### Core production routes

- `GET /health`
- `POST /webhook/reply`
- `POST /webhook/resolve-outreach`
- `POST /webhook/log-booking`
- `POST /webhook/call-event`
- `POST /webhook/missed-call`
- `POST /webhook/human-activity`
- `POST /webhook/{entity_id}/update-kb`
- `POST /webhook/{entity_id}/clean-chat-history`

### Minimal webhook payload shapes

These are the minimum useful request bodies Claude should know.

#### Reply webhook

```json
{
  "payload": "how much is teeth whitening?",
  "userID": "ghl_contact_id",
  "userFullName": "Jane Doe",
  "userEmail": "jane@example.com",
  "userPhone": "+15555550123",
  "ReplyChannel": "SMS",
  "AgentType": "setter_1"
}
```

#### Outreach resolver webhook

```json
{
  "contact_id": "ghl_contact_id",
  "full_name": "Jane Doe",
  "phone": "+15555550123",
  "email": "jane@example.com",
  "location": {
    "id": "ghl_location_id",
    "name": "My Business",
    "address": "123 Main St",
    "city": "Austin",
    "state": "TX"
  },
  "customData": {
    "Lead Source": "Meta Ads",
    "Form Service Interest": "Botox",
    "Channel": "SMS"
  }
}
```

#### Booking logger webhook

```json
{
  "contact_id": "ghl_contact_id",
  "full_name": "Jane Doe",
  "phone": "+15555550123",
  "email": "jane@example.com",
  "location": {
    "id": "ghl_location_id"
  },
  "calendar": {
    "appointmentId": "ghl_appointment_id",
    "calendarName": "Main Calendar",
    "startTime": "2026-04-15T18:00:00Z",
    "endTime": "2026-04-15T18:30:00Z",
    "selectedTimezone": "America/Chicago",
    "status": "confirmed"
  }
}
```

#### Call-event or human-activity webhook

```json
{
  "contact_id": "ghl_contact_id",
  "full_name": "Jane Doe",
  "phone": "+15555550123",
  "email": "jane@example.com",
  "location": {
    "id": "ghl_location_id"
  },
  "direction": "inbound"
}
```

#### Update KB webhook

```json
{
  "title": "Botox FAQ",
  "content": "Write the full KB article content here.",
  "tag": "botox-faq",
  "action": "upsert",
  "kb_id": "article_uuid"
}
```

## 6.5 Testing Without UI

Because he is not using the frontend, testing should be thought of as a Claude Code operator workflow, not a browser workflow.

### The core idea

Claude Code should:

1. generate or edit test payloads/scripts in `.tmp/`
2. hit the backend routes directly
3. inspect database state after each run
4. summarize what changed and whether the behavior matched expectations

### Recommended default testing path

Use the required bootstrap schema only and test the real production routes directly.

That means:

- test `reply` by posting webhook payloads to `/webhook/reply`
- test outreach scheduling by posting form payloads to `/webhook/resolve-outreach`
- test bookings by posting calendar payloads to `/webhook/log-booking`
- test missed calls by posting call payloads to `/webhook/call-event` or `/webhook/missed-call`
- test bot pause by posting to `/webhook/human-activity`
- test KB sync by updating `knowledge_base_articles` or posting to `/webhook/{entity_id}/update-kb`

### What Claude should inspect after a test

For a normal reply test:

- `workflow_runs`
- the entity chat table
- `leads`
- `scheduled_messages` if a follow-up was created

For a booking test:

- `bookings`
- `scheduled_messages`
- `workflow_runs`

For a missed-call test:

- `workflow_runs`
- entity chat table
- `tool_executions`
- `scheduled_messages` if follow-up-after-textback is enabled

For outreach:

- `leads`
- `scheduled_messages`
- `workflow_runs`

### Basic local test loop

1. run the backend locally
2. send a webhook payload
3. query the affected tables
4. compare expected vs actual behavior

Useful local commands:

```powershell
cd rg-backend
docker compose up -d
docker compose logs -f rg-backend
```

### How to prompt Claude Code for real backend-only testing

Good prompts:

- `Run a reply smoke test for entity <uuid>. Use a fake inbound SMS payload, then inspect workflow_runs, leads, the chat table, and scheduled_messages. Summarize what happened.`
- `Run a missed-call test for entity <uuid> in test mode, then show me whether it texted back, transferred, or skipped.`
- `Run an outreach scheduling test for entity <uuid> using Form Service Interest = Botox. Show me the scheduled_messages rows created and the resolved template behavior.`
- `Run a booking logger test for entity <uuid> and show me the booking row plus any reminder/post-appointment rows that were created.`

### How Claude should test follow-ups

There is no external follow-up webhook anymore.

Follow-up testing should be done by:

1. causing the system to schedule a follow-up naturally through the reply path
2. inspecting `scheduled_messages`
3. optionally moving `due_at` to near-now for a controlled fire
4. watching `message_scheduler_loop` deliver it
5. confirming:
   - chat message inserted
   - `workflow_runs` written
   - next cadence position scheduled or cleared

### How Claude should test with scripts instead of curl

For repeatable tests, have Claude create `.tmp/*.py` scripts that:

- build the payload
- call the route with `httpx`
- wait if needed
- read the database afterward
- print a compact summary

That gives him deterministic operator scripts without needing any UI.

### About the repo’s built-in testing stack

The repo contains:

- Direct Test Runner
- Simulator
- Sandbox

Those are real and they can be run without a UI, but they are not part of the required bootstrap SQL in this handoff.

That means:

- the safest default is direct route testing with custom Claude-authored scripts
- if he later wants the richer simulator stack, Claude can add that schema and use the API routes directly

### How simulator testing works without UI

If he later installs the simulator schema, Claude can run the full simulator entirely over HTTP.

The typical no-UI flow is:

1. create a simulation
2. execute it
3. poll status
4. fetch full results
5. run AI analysis
6. rerun the whole simulation or a single conversation

The main routes are:

- `POST /testing/simulator/import-scenarios`
- `POST /testing/simulator/execute/{simulation_id}`
- `GET /testing/simulator/{simulation_id}/status`
- `GET /testing/simulator/{simulation_id}`
- `POST /testing/simulator/{simulation_id}/analyze`
- `POST /testing/simulator/{simulation_id}/rerun`
- `POST /testing/simulator/{simulation_id}/rerun-conversation`

### Best simulator pattern for Claude Code

If simulator support is installed, the best non-UI workflow is:

1. Claude writes a `test_scenarios` payload
2. Claude posts it to `POST /testing/simulator/import-scenarios`
3. Claude starts execution with `POST /testing/simulator/execute/{simulation_id}`
4. Claude polls `GET /testing/simulator/{simulation_id}/status`
5. Claude fetches the full object from `GET /testing/simulator/{simulation_id}`
6. Claude runs `POST /testing/simulator/{simulation_id}/analyze`
7. Claude summarizes failures, good paths, and rerun suggestions

That gives him the simulator behavior without any frontend.

### Important limitation for this handoff

The required bootstrap SQL in this repo does not create the simulator/direct-runner storage layer.

So for this handoff:

- route-level scripted testing is the default
- simulator is a later add-on if he decides he wants it

## 7. GHL Webhooks He Actually Needs

If he wants the backend to function in production with GoHighLevel, set up these incoming webhooks:

### Required

- Inbound lead message -> `POST /webhook/reply`
- Outreach/form resolution trigger -> `POST /webhook/resolve-outreach`
- Appointment create/update/cancel -> `POST /webhook/log-booking`
- Call details trigger -> `POST /webhook/call-event`
- Manual human message trigger -> `POST /webhook/human-activity`

### Key implementation detail

The backend can resolve the entity from `location.id` in the standard webhook payload.

That means:

- static endpoint URLs are fine
- the path does not need an entity id if `location.id` is present and mapped

### No longer needed

- A GHL-scheduled external follow-up webhook
- A separate reactivation webhook

Those are now internal Python scheduling flows.

### Best GHL workflow reference

Use `service-delivery/guides/ghl-workflows.md` as the best written reference for post-migration GHL wiring.

That doc gets the architecture right:

- GHL is the trigger layer
- GHL is the CRM/state-display layer
- Python is the intelligence layer
- Python is the scheduler
- Python is the normal delivery engine for generated replies

### GHL signup link

If he needs a fresh HighLevel account for this stack, use:

- `https://affiliates.gohighlevel.com/?fp_ref=refinedgrowth66&share=M0bNGtcvt52JWFeOam51`

## 7.5 Outreach Modes

Outreach is intentionally flexible in this backend.

He has two valid modes:

1. backend-managed outreach using `outreach_templates` plus `/webhook/resolve-outreach`
2. GHL-native outreach using HighLevel workflows/campaigns directly

Both work.

Both sync into conversation history.

Both can hand off cleanly into the reply pipeline when the lead responds.

### Mode 1: backend-managed outreach

This is the repo’s native outreach system.

The normal GHL workflow shape is:

1. a workflow like `1 - {service} Outreach` fires on form submission
2. GHL sets fields like Form Service Interest / Lead Source / Channel
3. GHL sends a standard webhook to `POST /webhook/resolve-outreach`
4. Python resolves the matching outreach template
5. Python schedules all positions into `scheduled_messages`
6. Python later delivers each position directly via GHL API

### What happens inside the backend

#### Step 1: route entry

`app/main.py` receives the form webhook at `/webhook/resolve-outreach`.

It:

- resolves the entity from `location.id`
- builds an `OutreachResolverBody`
- extracts:
  - `contact_id`
  - name / phone / email
  - `Form Service Interest`
  - `Lead Source`
  - `Channel`
  - location context

#### Step 2: template resolution

`app/workflows/outreach_resolver.py` does the business logic.

It:

- loads active rows from `outreach_templates`
- matches by `form_service_interest`
- picks A/B variant if B content exists
- resolves variables like:
  - `{{contact.first_name}}`
  - `{{contact.last_name}}`
  - `{{contact.name}}`
  - `{{contact.email}}`
  - `{{contact.phone}}`
  - `{{location.name}}`
  - appointment placeholders
- builds `resolved_positions`
- creates/updates the lead row
- stores unresolved attribution/snapshot data in `leads.outreach_variant`

Important implementation detail:

- outreach content now lives in `outreach_templates.positions` JSONB
- not in old `sms_1`...`sms_9` columns

Each resolved position can contain:

- `sms`
- `email_subject`
- `email_body`
- `media_url`
- `media_type`
- `media_transcript`
- `delay`
- `send_window`

#### Step 3: scheduling

`app/workflows/outreach_scheduler.py` schedules each position into `scheduled_messages`.

For each position it applies:

- parsed delay timing
- jitter
- send-window enforcement
- metadata snapshot for later delivery

The inserted rows use:

- `message_type = "outreach"`
- `source = "outreach"`
- `triggered_by = "form_submission"`

#### Step 4: delivery

`app/workflows/message_scheduler_loop.py` later fires those rows through `_fire_outreach()`.

That handler:

- loads entity config
- reads resolved content from `scheduled_messages.metadata`
- sends SMS via `DeliveryService.send_sms()`
- sends email via `DeliveryService.send_standalone_email()`
- supports media on the outreach message
- logs workflow execution in `workflow_runs`
- reschedules reactivation because outreach counts as activity

### Why backend-managed outreach is useful

This mode gives him:

- one place to define outreach content (`outreach_templates`)
- backend-controlled delay logic
- jitter and send-window control
- template variable resolution
- A/B routing
- centralized scheduling in `scheduled_messages`
- unified execution visibility in `workflow_runs`
- less dependence on long GHL drip workflows

### When backend-managed outreach is the better choice

Use this mode if he wants:

- Claude-friendly control over outreach content
- deeper backend ownership of automation
- exact scheduling logic in code
- easier future programmatic changes
- tighter inspection/debugging from Supabase + Claude Code

### Mode 2: GHL-native outreach

He can also skip the backend-managed outreach system entirely and let HighLevel deliver outreach by itself.

That means:

- use normal GHL workflows/campaigns/drips
- do not rely on `outreach_templates`
- do not rely on `/webhook/resolve-outreach`

This is valid.

The backend will still work for replies, bookings, missed calls, and conversation sync.

### Why GHL-native outreach still works with this backend

The key reason is `app/text_engine/conversation_sync.py`.

It pulls all GHL conversation messages and stores them into the chat table before the AI responds.

It classifies outbound GHL-originated messages as:

- `manual` when they were human/app messages
- `workflow` when they came from workflows, drips, reminders, campaigns, etc.

So even if outreach was sent entirely by GHL:

- the backend still sees those messages in history
- the AI still has context when the lead replies
- the conversation stays coherent

### What this means in practice

A lead can be warmed by a pure GHL drip.

Then the lead replies.

Then:

- GHL fires `/webhook/reply`
- Python syncs the prior GHL workflow messages
- Python sees the lead’s context
- Python continues the conversation naturally

That handoff works because conversation sync is not limited to Python-generated messages.

### Tradeoffs of GHL-native outreach

Advantages:

- simpler if he already lives inside GHL campaigns
- fewer backend moving parts
- easier if he wants outreach logic managed by GHL staff/users

Disadvantages:

- less backend visibility into scheduling intent
- less structured A/B handling from code
- less programmable send-window/jitter logic
- less centralized automation ownership

### Recommended fields to keep even in GHL-native outreach

If he uses GHL-native outreach, he should still keep these contact fields populated when possible:

- `Form Service Interest`
- `Lead Source`
- `Channel`
- `Agent Type`

That helps Python respond with the right context once the lead enters the live reply path.

### How all messages still sync

The important concept is:

- Python-generated replies are stored directly and later backfilled to match GHL ids
- GHL workflow messages are synced as `workflow`
- lead replies are synced as inbound/lead messages
- staff manual messages are synced as `manual`

So the conversation record stays usable even when outreach comes from mixed systems.

### Best recommendation

If he wants the repo’s full backend-owned automation model, use backend-managed outreach.

If he wants the fastest path with less backend ownership, use GHL-native outreach.

Both are legitimate.

The deciding question is:

- does he want Python to own outreach scheduling, or does he want GHL to own it?

### Practical note about this handoff

This bootstrap includes `outreach_templates` because backend-managed outreach is part of the repo’s native architecture.

But if he intentionally decides to run outreach entirely inside GHL, that table becomes much less important to his day-to-day operation.

## 8. Core Database Export For Backend-Only Use

If he wants the backend to run correctly, these are the main DB objects that matter.

### Must-have main tables

| Table | Why it matters |
|---|---|
| `entities` | Main runtime control plane per business/client |
| `tenants` | AI keys, tenant defaults, ownership |
| `leads` | Lead state |
| `bookings` | Appointment state |
| `call_logs` | Call context and summaries |
| `outreach_templates` | Outreach resolver + appointment reminder templates |
| `scheduled_messages` | Internal scheduler source of truth |
| `workflow_runs` | Execution tracking, costs, drill-down |
| `knowledge_base_articles` | KB source content |
| `documents` | Vector search store |

### Must-have chat objects

| Object | Why |
|---|---|
| one chat-history table per entity | conversation memory |
| `attachments` | media/link analysis and chat attachment linkage |
| `tool_executions` | agent tool audit trail |
| Storage bucket `attachments` | permanent media storage after GHL CDN download |

### Database-level objects that matter

| Object | Why |
|---|---|
| extension `vector` | `documents.embedding` + KB matching |
| extension `pg_net` | KB article trigger webhook pattern |
| function `match_documents` | KB similarity search RPC used by the backend |
| trigger `kb_articles_embedding_sync` | auto re-embed on KB article changes |
| trigger `trg_knowledge_base_articles_set_tenant` | tenant propagation on KB articles |

## 9. The Most Important Tables

### `entities`

This is the single most important backend table.

The backend reads entity rows to get:

- identity
- GHL credentials
- chat history table name
- timezone / business schedule
- supported languages
- `system_config`
- test overrides
- tenant linkage

Live columns include:

- `id`
- `tenant_id`
- `status`
- `entity_type`
- `name`
- `contact_name`
- `contact_email`
- `contact_phone`
- `ghl_location_id`
- `ghl_api_key`
- `ghl_domain`
- `chat_history_table_name`
- `business_phone`
- `business_schedule`
- `timezone`
- `average_booking_value`
- `supported_languages`
- `system_config`
- `test_config_overrides`

### `tenants`

This matters even in backend-only mode because AI key resolution is tenant-based.

Live columns include:

- `id`
- `name`
- `is_platform_owner`
- `max_clients`
- `openrouter_api_key`
- `google_ai_key`
- `openai_key`
- `anthropic_key`
- `deepseek_key`
- `xai_key`
- `notification_config`
- `default_setter_config`
- `status`

### `scheduled_messages`

This is the internal scheduler source of truth.

It powers:

- follow-ups
- reactivation
- human takeover expiry
- post-appointment automation
- appointment reminders

Key columns:

- `entity_id`
- `contact_id`
- `message_type`
- `position`
- `channel`
- `status`
- `due_at`
- `source`
- `triggered_by`
- `smart_reason`
- `metadata`
- `to_phone`

### `workflow_runs`

This is the unified execution log.

It stores:

- workflow type
- decisions
- stages
- LLM calls
- costs
- runtime context
- filtered system config snapshot

Key columns:

- `tenant_id`
- `entity_id`
- `ghl_contact_id`
- `lead_id`
- `workflow_type`
- `status`
- `trigger_source`
- `mode`
- `stages`
- `decisions`
- `error_message`
- `duration_ms`
- `total_cost`
- `prompt_tokens`
- `completion_tokens`
- `total_tokens`
- `call_count`
- `llm_calls`
- `metadata`
- `system_config_snapshot`
- `runtime_context`

## 10. Canonical Chat History Schema

Use `peak_fitness_gym_chat_history_e2e_test` as the template for all new chat tables.

Do not use the old JSONB `message` schema for new work.

### Canonical flat chat table columns

```sql
id                integer primary key generated by sequence
session_id        varchar not null
role              text not null
content           text not null default ''
source            text null
channel           text null
lead_id           uuid null
timestamp         timestamptz default now()
ghl_message_id    text null
attachment_ids    uuid[] default '{}'
workflow_run_id   text null
```

### Recommended indexes

At minimum:

- primary key on `id`
- index on `session_id`
- index on `(session_id, timestamp)`
- index on `timestamp`
- index on `lead_id`
- index on `role`

### Important semantics

- `session_id` = GHL contact id
- `role` is `human` or `ai`
- `source` distinguishes `AI`, `follow_up`, `manual`, `workflow`, `lead_reply`, `missed_call_textback`, `post_appointment`, etc.
- `attachment_ids` stores UUID references to `attachments.id`
- `workflow_run_id` links the message back to `workflow_runs.id`

### Recommended generic DDL

```sql
create table public.your_entity_chat_history (
  id serial primary key,
  session_id varchar(255) not null,
  role text not null,
  content text not null default '',
  source text,
  channel text,
  lead_id uuid,
  "timestamp" timestamptz default now(),
  ghl_message_id text,
  attachment_ids uuid[] default '{}'::uuid[],
  workflow_run_id text
);

create index idx_your_entity_chat_history_session_id
  on public.your_entity_chat_history (session_id);

create index idx_your_entity_chat_history_session_ts
  on public.your_entity_chat_history (session_id, "timestamp");

create index idx_your_entity_chat_history_ts
  on public.your_entity_chat_history ("timestamp");

create index idx_your_entity_chat_history_lead_id
  on public.your_entity_chat_history (lead_id);

create index idx_your_entity_chat_history_role
  on public.your_entity_chat_history (role);
```

## 11. `attachments` And `tool_executions`

### `attachments`

Live columns:

- `id uuid default gen_random_uuid()`
- `session_id text not null`
- `type text not null`
- `url text not null`
- `description text`
- `message_timestamp timestamptz`
- `created_at timestamptz default now()`
- `timestamp timestamptz default now()`
- `raw_analysis text`
- `"client/bot_id" text`
- `original_url text`
- `lead_id uuid`
- `ghl_message_id text`

Important indexes:

- unique `(session_id, url)`
- index on `session_id`
- index on `(session_id, type)`
- index on `ghl_message_id`
- index on `lead_id`

### `tool_executions`

Live columns:

- `id uuid default gen_random_uuid()`
- `session_id text`
- `client_id text not null`
- `tool_name text not null`
- `tool_input jsonb`
- `tool_output jsonb`
- `execution_id text`
- `created_at timestamptz default now()`
- `timestamp timestamptz default now()`
- `channel text`
- `test_mode boolean default false`
- `lead_id uuid`

Important note:

- The column is `client_id` even though the runtime now uses unified entities.
- The code still writes `client_id` in `tool_executions`.

## 12. How `system_config` Actually Works

This is the most important config object in the entire backend.

The backend compiles `system_config` into prompt/runtime sections in `app/text_engine/data_loading.py`.

The main idea:

- `system_config` root contains global backend behavior.
- `system_config.setters` contains one or more full setter definitions.
- Each setter is a complete persona and workflow bundle.
- The active setter is selected by `agent_type` / setter key.

### Root-level keys that matter

```jsonc
{
  "pause_bot_on_human_activity": true,
  "human_takeover_minutes": 15,
  "notifications": {
    "recipients": []
  },
  "sms_provider": "ghl_default",
  "test_prompt_overrides": {},
  "setters": {
    "setter_1": {}
  }
}
```

The more complete shape documented in `service-delivery/guides/system-config-schema.md` also allows:

- `conversation.reply_window`
- `conversation.debounce_window_seconds`
- `prep_instructions_prompt`
- `missed_call_textback`
- `auto_reactivation`
- richer `data_collection`

### What one setter looks like conceptually

```jsonc
{
  "name": "Main Setter",
  "is_default": true,
  "supported_languages": ["English"],
  "bot_persona": {},
  "services": {},
  "conversation": {
    "reply": {},
    "follow_up": {},
    "post_booking": {},
    "reply_window": {},
    "debounce_window_seconds": 90
  },
  "booking": {},
  "transfer": {},
  "security": {},
  "case_studies": [],
  "missed_call_textback": {},
  "auto_reactivation": {},
  "prep_instructions_prompt": "",
  "data_collection": {},
  "ai_models": {},
  "ai_temperatures": {}
}
```

## 13. `system_config` Breakdown By Section

### `bot_persona`

Compiled by `app/text_engine/bot_persona_compiler.py`.

Expected structure:

- `sections.identity`
- `sections.ai_disclosure`
- `sections.tone`
- `sections.name_usage`
- `sections.punctuation_style`
- `sections.humor`
- `sections.emojis`
- `sections.message_length`
- `sections.typos`
- `sections.skip_greetings`
- `sections.be_direct`
- `sections.mirror_style`
- `sections.stories_examples`
- `sections.validate_feelings`
- `sections.hype_celebrate`
- `sections.remember_details`
- `sections.casual_language`
- `sections.sarcasm`
- `sections.light_swearing`
- `sections.sentence_fragments`
- `sections.banned_phrases`
- `custom_sections`

This section controls tone and human impersonation style.

### `services`

Compiled by `app/text_engine/services_compiler.py`.

Expected structure:

```jsonc
{
  "services": [
    {
      "name": "Botox",
      "description": "What it is",
      "pricing": "How pricing works",
      "qualifications": [],
      "offers": [],
      "setter_keys": ["setter_1"]
    }
  ],
  "global_qualifications": [],
  "global_offers": [],
  "qualification_rules": "",
  "offers_deployment": {
    "style": "proactive",
    "prompts": {
      "proactive": "...",
      "reactive": "...",
      "conditional": "..."
    }
  }
}
```

This section drives:

- services injected into prompts
- pricing visibility
- qualification context
- offer visibility and filtering

### `conversation.reply`

Compiled by `app/text_engine/agent_compiler.py`.

Expected structure:

- `sections.agent_goal`
- `sections.role_context`
- `sections.lead_source`
- `sections.conversation_framework`
- `sections.booking_style`
- `sections.pricing_discussion`
- `sections.discovery_questions`
- `sections.max_questions`
- `sections.max_booking_pushes`
- behavior sections like:
  - `steer_toward_goal`
  - `confident_expert`
  - `fully_helped`
  - `always_moving_forward`
  - `push_back`
  - `proactive_tips`
  - `low_effort_responses`
  - `urgency`
  - `scarcity`
  - `returning_lead_rules`
  - `future_pacing`
  - `acknowledge_before_pivot`
  - `yes_and`
  - `paraphrase`
  - `accept_no`
  - `discover_timeline`
  - `allow_small_talk`
- `sections.objections`
- `sections.max_objection_retries`
- `sections.common_situations`
- `sections.post_booking`
- `custom_sections`
- `enabled_tools`
- `prompt`
- `media_items`

### `conversation.follow_up`

Compiled by `app/text_engine/followup_compiler.py`.

Expected structure:

- `sections.followup_context`
- `sections.skip_determination`
- `sections.skip_if_upcoming_appointment`
- `sections.skip_if_disqualified`
- `sections.appointment_context_window`
- `sections.positions_enabled`
- `sections.follow_up_positions`
- `sections.re_engagement_angles`
- `sections.max_offer_pushes`
- `sections.no_finality_language`
- `sections.tone_overrides`
- `sections.timing_followup_rules`
- `sections.appointment_context_rules`
- `sections.service_examples`
- `sections.banned_phrases`
- `sections.cadence_timing`
- `sections.timing_jitter_enabled`
- `sections.timing_jitter_percent`
- `mode`
- `templates`
- `static_instructions`
- `send_window`
- `custom_sections`
- `prompt`
- `media_items`

### `booking`

Compiled by `app/text_engine/booking_compiler.py`.

Expected structure:

```jsonc
{
  "booking_method": "conversational",
  "booking_window_days": 10,
  "calendars": [],
  "booking_links": [],
  "tool_rules": [],
  "link_rules": [],
  "custom_rules": []
}
```

Important child object fields:

- `calendars[].id`
- `calendars[].name`
- `calendars[].enabled`
- `calendars[].booking_mode`
- `calendars[].booking_window_days`
- `calendars[].appointment_length_minutes`
- `calendars[].description`
- `calendars[].services`
- `calendars[].setter_keys`
- `calendars[].booking_link_override`

### `transfer`

Compiled by `app/text_engine/transfer_compiler.py`.

Expected structure:

```jsonc
{
  "philosophy": "...",
  "scenarios": [],
  "do_not_transfer": [],
  "opt_out": {
    "phrases": {},
    "solicitor": {},
    "not_opt_out": {}
  }
}
```

This drives the transfer classifier, not the reply agent directly.

### `security`

Compiled by `app/text_engine/security_compiler.py`.

Expected structure:

```jsonc
{
  "protections": {
    "prompt_protection": {},
    "output_protection": {},
    "jailbreak_rejection": {}
  },
  "compliance_rules": [],
  "custom_compliance_rules": [],
  "term_replacements": []
}
```

This powers:

- prompt protection
- compliance pass
- deterministic term replacement

### `case_studies`

Compiled by `app/text_engine/case_studies_compiler.py`.

Expected structure:

- array of objects
- each object can include:
  - `title`
  - `details`
  - `video_url`
  - `services`
  - `deployment_style`
  - `deployment_prompt`
  - `media`
  - `setter_keys`
  - `enabled`

### `data_collection`

Used by `data_loading.py` and post-processing.

Expected structure:

```jsonc
{
  "fields": []
}
```

Each field is a custom extraction field the AI can populate.

### `missed_call_textback`

This section is defined in `service-delivery/guides/system-config-schema.md` and controls the missed call workflow.

Expected structure:

```jsonc
{
  "enabled": true,
  "cooldown_minutes": 15,
  "max_unanswered": 2,
  "delay_min_seconds": 60,
  "delay_max_seconds": 90,
  "opening": {
    "use_excuse": { "enabled": true, "prompt": "..." },
    "humorous": { "enabled": false, "prompt": "..." },
    "skip_missed_call_mention": { "enabled": false, "prompt": "..." }
  },
  "reply_behavior": {
    "tease": { "enabled": false, "prompt": "..." },
    "answer_voicemail_questions": { "enabled": true, "prompt": "..." }
  },
  "scheduling": "proactive",
  "search_knowledge_base": true,
  "custom_sections": [],
  "followup_after_textback": {
    "mode": "normal_cadence"
  }
}
```

Best deep-dive doc: `service-delivery/guides/missed-call-textback-workflow.md`

### `auto_reactivation`

This section controls the reactivation scheduler/workflow.

Expected structure:

```jsonc
{
  "enabled": true,
  "days": 45,
  "generous_stance": { "enabled": true, "prompt": "..." },
  "qual_block": {},
  "qual_allow": {},
  "message_content": {},
  "message_angle": {},
  "constraints": {},
  "custom_sections": []
}
```

Best deep-dive doc: `service-delivery/guides/reactivation-workflow.md`

### `conversation.reply_window` and `conversation.debounce_window_seconds`

These are easy to miss, but they exist in the documented schema and affect runtime timing behavior.

- `reply_window` controls when replies are allowed
- `debounce_window_seconds` controls how long the backend waits before processing a burst of inbound activity

### `ai_models` and `ai_temperatures`

These override model/temperature per LLM call key.

Call keys currently used by the backend:

- `reply_agent`
- `reply_media`
- `transfer_detection`
- `response_determination`
- `contact_extraction`
- `stop_detection`
- `pipeline_management`
- `reply_security`
- `qualification`
- `email_formatting`
- `media_analysis`
- `link_synthesis`
- `call_summarization`
- `followup_text`
- `followup_media`
- `followup_determination`
- `smart_scheduler`
- `followup_security`
- `followup_email_formatting`
- `missed_call_text`
- `missed_call_gate`
- `reactivation_sms`
- `reactivation_p1_only`
- `reactivation_qual`
- `service_matcher`
- `reactivation_security`
- `message_splitter`
- `post_appointment_determination`
- `post_appointment_generation`
- `data_chat`

Resolution order is:

1. setter-level override in `system_config`
2. tenant-level default in `tenants.default_setter_config`
3. hardcoded fallback in `app/text_engine/model_resolver.py`

## 14. Minimal Seed Data He Needs

If he is standing this up from scratch, create at least:

1. One `tenants` row
2. One `entities` row linked to that tenant
3. One chat table whose name matches `entities.chat_history_table_name`
4. `attachments`
5. `tool_executions`
6. `scheduled_messages`
7. `workflow_runs`
8. `leads`
9. `bookings`
10. `call_logs`
11. `outreach_templates`
12. `knowledge_base_articles`
13. `documents`

This handoff intentionally excludes optional and legacy tables.

## 15.5 Workflow Documentation Map

If he wants deeper written docs for a subsystem, use these:

| Subsystem | Best doc |
|---|---|
| Text reply pipeline | `service-delivery/guides/python-text-engine.md` |
| System config shape | `service-delivery/guides/system-config-schema.md` |
| GHL workflow wiring | `service-delivery/guides/ghl-workflows.md` |
| Outreach resolver | `service-delivery/guides/outreach-resolver-workflow.md` |
| Booking logger | `service-delivery/guides/booking-logger-workflow.md` |
| Missed call text-back | `service-delivery/guides/missed-call-textback-workflow.md` |
| Post-appointment | `service-delivery/guides/post-appointment-workflow.md` |
| Utility workflows | `service-delivery/guides/utility-workflows.md` |

For broad database docs, use `service-delivery/guides/supabase-database.md` carefully. It is useful, but parts of it are historical and predate the current `entities`/`tenants` runtime.

Also note:

- `service-delivery/guides/system-config-schema.md` is the strongest written spec for JSON shape.
- some workflow guides still contain old wording or old table names in examples; use them for behavior, not for blindly copy-pasting schema assumptions.

## 16. Known Sharp Edges

- `pipeline_runs` is referenced in testing code but missing from the current live DB.
- Not every legacy doc matches current code.
- The current real environment still contains some old chat-table inconsistencies.
- For any fresh handoff, standardize on:
  - unified `entities` / `tenants`
  - one Supabase project
  - flat chat schema only
  - `system_config.setters` as the source of truth

## 17. Final Operating Rule For Claude Code

If you are Claude Code working on this repo for the new owner, operate with these assumptions:

- no frontend dependency by default
- one Supabase project
- canonical chat schema = `peak_fitness_gym_chat_history_e2e_test`
- `entities` + `tenants` are the runtime control plane
- `system_config` is the behavioral source of truth

If you need to change runtime behavior, inspect live code in:

- `app/main.py`
- `app/text_engine/data_loading.py`
- `app/text_engine/pipeline.py`
- `app/services/message_scheduler.py`
- `app/workflows/message_scheduler_loop.py`
- `app/services/supabase_client.py`
- `app/services/postgres_client.py`

That is the shortest path to staying accurate.
