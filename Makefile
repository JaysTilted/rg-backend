.PHONY: init build up logs health deploy restart stop doctor doctor-online seed-sql-example bootstrap-sql-example db-tunnel

# Create a local .env from the one-project example if it doesn't exist yet
init:
	@test -f .env || cp .env.example .env

# Full rebuild (only needed when dependencies change)
build:
	docker compose build --no-cache rg-backend

# Restart container (hot-reload handles most code changes automatically)
up:
	docker compose up -d --force-recreate rg-backend

# Follow logs in real-time
logs:
	docker compose logs -f rg-backend --tail 50

# Check backend health
health:
	@curl -s http://localhost:$${RG_BACKEND_PORT:-8001}/health | python -m json.tool

# Validate handoff files, .env coverage, and one-project assumptions
doctor:
	@python3 scripts/doctor.py

# Validate the same, plus Supabase/Docker probes
doctor-online:
	@python3 scripts/doctor.py --online --docker

# Render example tenant/entity seed SQL from the starter system-config template
seed-sql-example:
	@python3 scripts/render_seed_sql.py --tenant-name "Example Tenant" --entity-name "Example Business"

# Render schema-aware bootstrap SQL for a shared Supabase project
bootstrap-sql-example:
	@python3 scripts/render_bootstrap_sql.py --main-schema rg_backend --chat-schema rg_chat

# Start an SSH tunnel from localhost:65432 to the shared Supabase DB on the VPS
db-tunnel:
	@bash scripts/tunnel_shared_db.sh

# Full deploy cycle: build + restart + wait + health check
deploy:
	$(MAKE) build && $(MAKE) up && sleep 8 && $(MAKE) health

# Quick restart without rebuild
restart:
	docker compose restart rg-backend

# Stop all services
stop:
	docker compose down
