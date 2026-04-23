---
name: iron-setter-conversation-agent-visuals
description: Use when the user wants a visual, diagram, map, architecture picture, or current configuration overview of the iron-setter conversation agent. Read the real webhook, pipeline, data-loading, and agent files, generate conservative Mermaid, validate it with Mermaid CLI, write the result into Obsidian, and keep Mermaid and Miro sync metadata updated for the visual.
---

# iron-setter conversation agent Visuals

Use this skill when the user asks for:
- a visual of the conversation agent
- a map of how reply handling works
- the current agent configuration
- a Mermaid diagram for the iron-setter text engine
- something "put in Obsidian" for this system

## Source of truth files

Read these first:

- `app/main.py`
  Focus on `/webhook/reply` and the debounce handoff.
- `app/text_engine/pipeline.py`
  Focus on the reply pipeline stages.
- `app/text_engine/data_loading.py`
  Focus on `load_data()` and `_compile_system_config()`.
- `app/text_engine/agent.py`
  Focus on `select_reply_media()` and `call_agent()`.
- `STARTER_SYSTEM_CONFIG_TEMPLATE.json`
  Use this for the current configured setter, tools, timing, booking, follow-up, missed-call, and reactivation defaults.
- `README.md`
  Use for repo-level framing only.

## Output standard

Default output is a Mermaid `flowchart TD` with:

1. webhook entrypoint
2. debounce behavior
3. reply pipeline stages
4. tool loop
5. current config snapshot

Keep diagrams conservative:

- explicit node IDs
- simple subgraphs
- minimal styling
- no cutting-edge Mermaid syntax unless required

## Validation

If the user wants a tested artifact, validate with Mermaid CLI:

```bash
PUPPETEER_EXECUTABLE_PATH="$HOME/.cache/puppeteer/chrome-headless-shell/linux-131.0.6778.204/chrome-headless-shell-linux64/chrome-headless-shell" \
npx -y -p @mermaid-js/mermaid-cli@10.9.1 mmdc -i input.mmd -o output.svg
```

If local render fails because Chrome is missing, install the expected browser revision and retry.

## Sync script

For direct Miro sync, use:

`python3 scripts/sync_mermaid_to_miro.py`

Required live inputs:
- `MIRO_ACCESS_TOKEN`
- `MIRO_BOARD_ID` or `MIRO_BOARD_URL`

Current fast path for this repo:

```bash
python3 scripts/sync_mermaid_to_miro.py \
  --slug conversation-agent-current \
  --svg docs/visuals/conversation-agent-current.svg \
  --replace-existing
```

## Obsidian target

Write visuals to:

`10 Active/13 Products/RG Backend/Visuals/`

Use this note structure:

```markdown
# {Title}

Short context sentence.

## Visual

![[{file-name}.svg]]

## Source

```mermaid
{diagram}
```

## Notes

- Mermaid in this note is the source of truth.
- Ask Codex to update this note directly from code changes.
```

## Delivery contract

When asked for a visual:

1. Create or update a `.mmd` file under `docs/visuals/` in `iron-setter`.
2. Render an `.svg`.
3. Copy the `.svg` into the vault beside the note.
4. Write the Obsidian note with the embedded SVG plus Mermaid source.
5. Update `docs/mermaid-miro-sync-manifest.yaml` for the visual.
6. If Miro REST auth is available, sync the visual to Miro with `scripts/sync_mermaid_to_miro.py` and update the manifest status/URL.
7. If Miro tooling or auth is unavailable, leave the manifest at `pending_miro_sync` and say so explicitly.
8. Read the note back once to verify the embed and source landed correctly.

## Mermaid Miro rule

`Mermaid` remains the structural source of truth.

`Miro` is the human-facing canvas.

Do not claim the visual workflow is complete if the Miro sync state is unknown.
