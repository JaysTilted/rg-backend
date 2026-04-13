"""Entity override registry — lists entity IDs that have override files.

When an entity needs custom behavior (a different tool, a tweaked prompt,
a skipped pipeline step), add their ID here.

The override file must be named by the entity ID:
    app/clients/overrides/{entity_id}.py

Entities NOT listed here get 100% default behavior — no overrides.

Usage:
    # In any pipeline step that supports overrides:
    from app.clients.base import get_override

    custom_tools = get_override(ctx.entity_id, "tools")
    if custom_tools:
        tool_defs = custom_tools(agent_type, tool_defs)
"""

# Entity IDs that have override files in app/clients/overrides/
# Example:
#   "4c7fa58c-f10d-4b5f-9ecb-3898aa8d7368",
#
# This means app/clients/overrides/4c7fa58c-f10d-4b5f-9ecb-3898aa8d7368.py exists.

CLIENT_OVERRIDES: set[str] = {
    # No overrides yet — all entities use default behavior.
    # Add entity IDs here as entity-specific needs arise.
}
