# iron-setter

AI SMS reply agent for Iron Automations (formerly `rg-backend` / `setter`).

This repo runs:
- inbound conversation handling through the text engine
- standalone workflows for bookings, KB sync, missed calls, reactivation, and reporting
- FastAPI + Prefect orchestration backed by Supabase and GoHighLevel

## Backend-Only One-Project Quickstart

This repo is set up for the backend-only handoff flow where a single Supabase project is used for both the logical `main` and `chat` surfaces, with separate schemas inside that shared project.

1. Run `make init` to create a local `.env` from `.env.example` if it does not exist yet.
2. Fill in the required keys.
3. Point both `SUPABASE_MAIN_*` and `SUPABASE_CHAT_*` at the same Supabase project.
4. Set `SUPABASE_MAIN_SCHEMA` and `SUPABASE_CHAT_SCHEMA` to the schemas you want this backend to own inside that shared project.
5. For local Docker development from this machine, start the DB tunnel with `make db-tunnel` and point both `DATABASE_URL` and `DATABASE_CHAT_URL` at `host.docker.internal:65432`.
6. Set `AZURE_OPENAI_API_KEY` and keep `AZURE_OPENAI_MODEL=gpt-4.1` unless you have a reason to override it.
7. Render or adapt the bootstrap SQL for your chosen schemas, then run the result in the Supabase SQL editor.
8. Create an initial tenant, then create an entity and chat-history table with the generated helper function.
9. Seed `entities.system_config` from [STARTER_SYSTEM_CONFIG_TEMPLATE.json](/home/jay/iron-setter/STARTER_SYSTEM_CONFIG_TEMPLATE.json:1).
10. Run `make doctor` to validate the handoff files and `.env`.
11. Run `make doctor-online` after the real values are in place.
12. Start the stack with `docker compose up --build`.
Default local host port is `8001`; override with `RG_BACKEND_PORT=...` if needed.

## Key Files

- [CLAUDE_CODE_MASTER_BACKEND_HANDOFF.md](/home/jay/iron-setter/CLAUDE_CODE_MASTER_BACKEND_HANDOFF.md:1): backend-only operator handoff and code-reading order
- [BACKEND_ONLY_ONE_PROJECT_BOOTSTRAP.sql](/home/jay/iron-setter/BACKEND_ONLY_ONE_PROJECT_BOOTSTRAP.sql:1): required schema, helpers, storage bucket, RPC, and KB trigger for the one-project setup
- [STARTER_SYSTEM_CONFIG_TEMPLATE.json](/home/jay/iron-setter/STARTER_SYSTEM_CONFIG_TEMPLATE.json:1): starter `entities.system_config` payload
- [scripts/doctor.py](/home/jay/iron-setter/scripts/doctor.py:1): validates `.env`, bootstrap assets, one-project assumptions, and optional online/runtime checks
- [scripts/bootstrap_entity.sql](/home/jay/iron-setter/scripts/bootstrap_entity.sql:1): minimal seed example for a fresh tenant + entity
- [scripts/render_bootstrap_sql.py](/home/jay/iron-setter/scripts/render_bootstrap_sql.py:1): renders schema-aware bootstrap SQL for shared Supabase deployments
- [scripts/render_seed_sql.py](/home/jay/iron-setter/scripts/render_seed_sql.py:1): renders full tenant/entity/system-config seed SQL from the starter JSON
- [scripts/tunnel_shared_db.sh](/home/jay/iron-setter/scripts/tunnel_shared_db.sh:1): opens a local SSH tunnel to the shared Supabase DB so Docker on this machine can reach it via `host.docker.internal:65432`
- [app/main.py](/home/jay/iron-setter/app/main.py:1): HTTP routes and app startup
- [app/services/supabase_client.py](/home/jay/iron-setter/app/services/supabase_client.py:1): main/chat REST client behavior
- [app/services/postgres_client.py](/home/jay/iron-setter/app/services/postgres_client.py:1): direct SQL for chat history, attachments, and tool logs

## Repo Layout

- `app/text_engine/` — reply pipeline, delivery, sync, timeline, booking context, follow-up logic
- `app/workflows/` — standalone workflows
- `app/services/` — Supabase, Postgres, GHL, AI, notifications, scheduler
- `app/tools/` — agent tools for booking, KB search, transfer, and attachment reanalysis
- `app/testing/` — simulator, sandbox, and direct runner surfaces

## Notes

- The public repo snapshot does not include every guide path referenced by the imported handoff doc. When a referenced guide is missing, use the handoff doc plus the live code and bootstrap files in this repo.
- For v1, preserve the source data model. Do not translate this backend onto another app's schema before you have parity working.
- The repo is now schema-aware: PostgREST requests bind to `SUPABASE_MAIN_SCHEMA` / `SUPABASE_CHAT_SCHEMA`, and asyncpg pools use those schemas as their search path.
- `openai/*` model IDs are intended to run through Azure GPT-4.1 first, with OpenRouter kept as fallback rather than primary.
- `make init` materializes a local `.env` from the example without overwriting an existing file.
- `make doctor` is the offline readiness check. `make doctor-online` also probes Supabase and validates `docker compose config`.
- `make seed-sql-example` prints a ready-to-edit SQL transaction that inserts a tenant, creates an entity with a canonical chat-history table, and seeds `system_config`.
- `make db-tunnel` opens the local SSH tunnel needed for Docker on this machine to reach the shared Supabase Postgres instance.
