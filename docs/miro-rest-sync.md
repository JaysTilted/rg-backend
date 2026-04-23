# Miro REST Sync

This repo uses `Mermaid` as the source of truth and can mirror rendered SVGs to `Miro` with:

`scripts/sync_mermaid_to_miro.py`

## Required inputs

- `MIRO_ACCESS_TOKEN` or a refreshable OAuth setup
- `MIRO_BOARD_ID` or `MIRO_BOARD_URL`

You can set these in the shell or `.env` before running the script.

For a proper long-term setup, configure a Miro Developer app and set:

- `MIRO_CLIENT_ID`
- `MIRO_CLIENT_SECRET`
- `MIRO_REDIRECT_URI`
- `MIRO_REFRESH_TOKEN`

Then use:

```bash
make miro-auth-url
python3 scripts/miro_oauth.py exchange --code 'PASTE_CODE_HERE'
make miro-token-refresh
make miro-token-info
```

## Current visual

The current conversation-agent visual is rendered as a presentation PNG first, then synced:

```bash
make render-presentation-png
python3 scripts/sync_mermaid_to_miro.py \
  --slug conversation-agent-current \
  --asset docs/visuals/conversation-agent-current.direct.png
```

## Behavior

- reads the current manifest entry from `docs/mermaid-miro-sync-manifest.yaml`
- resolves board ID from `--board-id`, `--board-url`, env vars, or manifest URL
- optionally deletes the prior Miro item ID
- uploads the latest rendered image asset as a Miro image item
- updates `miro_board_url`, `miro_item_id`, `status`, and `last_synced_at`

## Dry run

Use `--dry-run` to verify resolved inputs without calling the API:

```bash
python3 scripts/sync_mermaid_to_miro.py \
  --slug conversation-agent-current \
  --svg docs/visuals/conversation-agent-current.svg \
  --board-url https://miro.com/app/board/uXjVEXAMPLE=/ \
  --replace-existing \
  --dry-run
```

## Important auth note

The `Miro MCP` OAuth token is **not** the same thing as a `Miro REST API` bearer token for this workflow.

This sync path requires a proper Miro Developer app OAuth token obtained through the REST OAuth flow.
