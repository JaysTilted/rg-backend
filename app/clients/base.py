"""Entity override loader — looks up and calls per-entity overrides.

This is the only function pipeline code needs to interact with.
It handles the lookup, import, and caching of override modules.

Override files live in app/clients/overrides/{entity_id}.py and can
define any combination of override functions. See overrides/_example.py
for the full list of supported override functions.

Usage in pipeline code:

    from app.clients.base import get_override

    # Check if this entity has a custom tool set
    tools_override = get_override(ctx.entity_id, "tools")
    if tools_override:
        tool_defs = tools_override(agent_type, tool_defs)
    # else: use default tool_defs as-is
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from types import ModuleType
from typing import Callable

from app.clients.registry import CLIENT_OVERRIDES

logger = logging.getLogger(__name__)

# Cache loaded modules so we don't re-import on every request
_module_cache: dict[str, ModuleType] = {}

# Directory where override files live
_OVERRIDES_DIR = Path(__file__).resolve().parent / "overrides"


def get_override(entity_id: str, override_name: str) -> Callable | None:
    """Get a specific override function for an entity, or None if not defined.

    Args:
        entity_id: The entity's UUID (from Supabase / webhook URL).
        override_name: The name of the override function to look up.
            Convention: override functions are named override_{name}.
            e.g., override_name="tools" → looks for override_tools()

    Returns:
        The override function if the entity has one, or None.
    """
    if entity_id not in CLIENT_OVERRIDES:
        return None

    # Load and cache the module
    if entity_id not in _module_cache:
        file_path = _OVERRIDES_DIR / f"{entity_id}.py"
        if not file_path.exists():
            logger.warning(
                "Entity %s is in registry but override file not found",
                entity_id[:8],
            )
            return None
        try:
            # Use spec_from_file_location instead of import_module because
            # UUIDs contain hyphens, which Python treats as minus signs in
            # dotted module paths. File-based loading has no such restriction.
            spec = importlib.util.spec_from_file_location(
                f"override_{entity_id}", file_path
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            _module_cache[entity_id] = mod
        except Exception:
            logger.warning(
                "Failed to load override file for entity %s",
                entity_id[:8],
                exc_info=True,
            )
            return None

    mod = _module_cache[entity_id]
    func = getattr(mod, f"override_{override_name}", None)
    return func
