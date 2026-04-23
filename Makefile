.PHONY: init build up logs health deploy restart stop doctor doctor-online seed-sql-example bootstrap-sql-example db-tunnel miro-sync-conversation-agent miro-auth-url miro-token-refresh miro-token-info render-presentation-png e2e e2e-one

# Create a local .env from the one-project example if it doesn't exist yet
init:
	@test -f .env || cp .env.example .env

# Full rebuild (only needed when dependencies change)
build:
	docker compose build --no-cache iron-setter

# Restart container (hot-reload handles most code changes automatically)
up:
	docker compose up -d --force-recreate iron-setter

# Follow logs in real-time
logs:
	docker compose logs -f iron-setter --tail 50

# Check backend health
health:
	@curl -s http://localhost:$${RG_BACKEND_PORT:-8001}/health | python -m json.tool

# Run the E2E framework against the live backend (uses MockGHLClient — no real SMS)
e2e:
	@API_AUTH_TOKEN="$$(grep '^API_AUTH_TOKEN=' .env | cut -d= -f2-)" python3 -m scripts.e2e.run

# Run a single scenario by id (make e2e-one ID=03-price-question-early)
e2e-one:
	@API_AUTH_TOKEN="$$(grep '^API_AUTH_TOKEN=' .env | cut -d= -f2-)" python3 -m scripts.e2e.run --only $(ID)

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

# Render a readable PNG directly from Mermaid CLI for presentation surfaces like Miro
render-presentation-png:
	@PUPPETEER_EXECUTABLE_PATH="$$HOME/.cache/puppeteer/chrome-headless-shell/linux-131.0.6778.204/chrome-headless-shell-linux64/chrome-headless-shell" \
	npx -y -p @mermaid-js/mermaid-cli@10.9.1 mmdc -i docs/visuals/conversation-agent-current.mmd -o docs/visuals/conversation-agent-current.direct.png -t neutral -b white -w 1400 -H 1600

# Sync the validated conversation-agent SVG to Miro using REST API
miro-sync-conversation-agent:
	@$(MAKE) render-presentation-png
	@python3 scripts/sync_mermaid_to_miro.py --slug conversation-agent-current --asset docs/visuals/conversation-agent-current.direct.png

# Print the Miro OAuth authorization URL for the configured app
miro-auth-url:
	@python3 scripts/miro_oauth.py auth-url

# Refresh the configured Miro REST access token
miro-token-refresh:
	@python3 scripts/miro_oauth.py refresh

# Inspect the configured Miro REST access token
miro-token-info:
	@python3 scripts/miro_oauth.py token-info

# Full deploy cycle: build + restart + wait + health check
deploy:
	$(MAKE) build && $(MAKE) up && sleep 8 && $(MAKE) health

# Quick restart without rebuild
restart:
	docker compose restart iron-setter

# Stop all services
stop:
	docker compose down
