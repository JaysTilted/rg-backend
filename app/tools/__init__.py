"""Agent tools — async functions that agents can call via function calling.

Each tool corresponds to a former n8n sub-workflow. They're exposed to agents
as OpenAI-format function definitions and called when the agent invokes them.
"""

from app.tools.book_appointment import book_appointment, BOOK_APPOINTMENT_DEF
from app.tools.cancel_appointment import cancel_appointment, CANCEL_APPOINTMENT_DEF
from app.tools.get_available_slots import get_available_slots, GET_AVAILABLE_SLOTS_DEF
from app.tools.knowledge_base_search import knowledge_base_search, KNOWLEDGE_BASE_SEARCH_DEF
from app.tools.mark_branch import mark_branch, MARK_BRANCH_DEF
from app.tools.reanalyze_attachment import reanalyze_attachment, REANALYZE_ATTACHMENT_DEF
from app.tools.transfer_to_human import transfer_to_human, TRANSFER_TO_HUMAN_DEF
from app.tools.update_appointment import update_appointment, UPDATE_APPOINTMENT_DEF

# Tool registry — maps function name to (handler, definition)
# NOTE: get_appointments removed — event IDs are auto-injected into the
# Appointment Status section of the system prompt via booking.py.  The agent
# reads the Event ID directly from context for reschedule/cancel operations.
TOOL_REGISTRY: dict = {
    "knowledge_base_search": (knowledge_base_search, KNOWLEDGE_BASE_SEARCH_DEF),
    "get_available_slots": (get_available_slots, GET_AVAILABLE_SLOTS_DEF),
    "book_appointment": (book_appointment, BOOK_APPOINTMENT_DEF),
    "update_appointment": (update_appointment, UPDATE_APPOINTMENT_DEF),
    "cancel_appointment": (cancel_appointment, CANCEL_APPOINTMENT_DEF),
    "transfer_to_human": (transfer_to_human, TRANSFER_TO_HUMAN_DEF),
    "reanalyze_attachment": (reanalyze_attachment, REANALYZE_ATTACHMENT_DEF),
    "mark_branch": (mark_branch, MARK_BRANCH_DEF),
}

# All available tool definitions — setters filter via enabled_tools list
ALL_TOOL_DEFS = [
    KNOWLEDGE_BASE_SEARCH_DEF,
    GET_AVAILABLE_SLOTS_DEF,
    BOOK_APPOINTMENT_DEF,
    UPDATE_APPOINTMENT_DEF,
    CANCEL_APPOINTMENT_DEF,
    TRANSFER_TO_HUMAN_DEF,
    REANALYZE_ATTACHMENT_DEF,
    MARK_BRANCH_DEF,
]
