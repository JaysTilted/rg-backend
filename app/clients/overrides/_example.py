"""Example client override file.

To create overrides for a client:

1. Copy this file into the same folder and name it with the
   client's UUID. For example:
       4c7fa58c-f10d-4b5f-9ecb-3898aa8d7368.py

2. Add that same UUID to the set in app/clients/registry.py:
       CLIENT_OVERRIDES: set[str] = {
           "4c7fa58c-f10d-4b5f-9ecb-3898aa8d7368",
       }

Only define the override functions you need. Everything else
stays default. Delete the ones you don't use.
"""

from __future__ import annotations
from typing import Any


# --- TOOL OVERRIDES ---
# Called from: agent.py (tool loading step)
# Hook name: get_override(client_id, "tools")

def override_tools(agent_type: str, default_tools: list[dict]) -> list[dict]:
    """Add, remove, or replace tools for this client.

    Args:
        agent_type: Setter key (e.g., "setter_1", "setter_2")
        default_tools: The default tool definitions list

    Returns:
        Modified tool definitions list
    """
    # Example: add a custom tool for this client
    # default_tools.append(MY_CUSTOM_TOOL_DEF)

    # Example: remove knowledge base search for this client
    # default_tools = [t for t in default_tools if t["function"]["name"] != "knowledge_base_search"]

    return default_tools


# --- CLASSIFICATION OVERRIDES ---
# Called from: classification.py (transfer/opt-out gate)
# Hook name: get_override(client_id, "classification")

def override_classification(ctx: Any) -> str | None:
    """Override the transfer/opt-out classification for this client.

    Args:
        ctx: PipelineContext

    Returns:
        "human", "opt_out", or None to use default classification
    """
    # Example: auto-transfer Spanish messages for this client
    # if "spanish" in ctx.message.lower():
    #     return "human"
    return None


# --- PROMPT OVERRIDES ---
# Called from: agent.py (system prompt building)
# Hook name: get_override(client_id, "system_prompt")

def override_system_prompt(system_prompt: str, ctx: Any) -> str:
    """Modify the agent's system prompt for this client.

    Args:
        system_prompt: The fully assembled default system prompt
        ctx: PipelineContext

    Returns:
        Modified system prompt
    """
    # Example: append extra instructions
    # system_prompt += "\n\nALWAYS mention the free consultation offer."
    return system_prompt


# --- PIPELINE STEP OVERRIDES ---
# Called from: pipeline.py (step execution)
# Hook name: get_override(client_id, "skip_steps")

def override_skip_steps() -> list[str]:
    """Return a list of pipeline step names to skip for this client.

    Valid step names:
        "process_attachments", "sync_conversation", "load_booking_context",
        "classification_gate", "post_agent_processing"

    Returns:
        List of step names to skip (empty = run all)
    """
    # Example: skip attachment processing for this client
    # return ["process_attachments"]
    return []


# --- DELIVERY OVERRIDES ---
# Called from: delivery.py (message splitting)
# Hook name: get_override(client_id, "max_splits")

def override_max_splits() -> int:
    """Override max message splits for this client.

    Default is 5. Some clients may want fewer splits.

    Returns:
        Maximum number of split messages (1-5)
    """
    return 5
