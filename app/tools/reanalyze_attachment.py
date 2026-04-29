"""Tool: re-analyze a previously processed attachment with a specific question.

Matches n8n Media Analysis Subworkflow (reanalysis mode):
1. Agent sends attachment_type + attachment_index + question
2. Tool queries DB for the attachment by type + index
3. Downloads from Supabase Storage URL (permanent, never expires)
4. Runs Gemini analysis with SPECIFIC QUESTION prepended to full extraction prompt
5. Logs to tool_executions table
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from app.text_engine.attachments import _ANALYSIS_PROMPTS
from app.services.ai_client import analyze_media
from app.services.postgres_client import postgres

logger = logging.getLogger(__name__)

REANALYZE_ATTACHMENT_DEF = {
    "type": "function",
    "function": {
        "name": "reanalyze_attachment",
        "description": (
            "Re-analyze a previously sent attachment (image, video, audio, document, link) "
            "with a specific follow-up question. Use when you need more details about "
            "an attachment the lead sent. For links, this re-fetches the web page. "
            "Reference attachments by type and number (e.g., 'Image 1', 'Link 1')."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The specific question to answer about this attachment",
                },
                "attachment_type": {
                    "type": "string",
                    "description": "The type of attachment to analyze: 'image', 'video', 'audio', 'document', or 'link'",
                },
                "attachment_index": {
                    "type": "integer",
                    "description": "The attachment number to analyze (1 for first Image 1, 2 for Image 2, etc.)",
                },
            },
            "required": ["question", "attachment_type", "attachment_index"],
        },
    },
}


async def reanalyze_attachment(
    question: str,
    attachment_type: str,
    attachment_index: int,
    contact_id: str = "",
    entity_id: str = "",
    channel: str = "",
    is_test_mode: bool = False,
    lead_id: str | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Re-analyze an attachment by querying DB and running Gemini with specific question."""
    att_type = attachment_type.lower().strip()
    if att_type not in ("image", "video", "audio", "document", "link"):
        return {"error": f"Invalid attachment_type: {attachment_type}. Must be image, video, audio, document, or link."}

    if not contact_id or not entity_id:
        return {"error": "Missing contact_id or entity_id for attachment lookup."}

    # 1. Query DB for the attachment (matches n8n Lookup Attachment URL node)
    try:
        record = await postgres.get_attachment_by_type_index(
            contact_id, att_type, int(attachment_index), entity_id
        )
    except Exception as e:
        logger.warning("DB lookup failed for %s %d: %s", att_type, attachment_index, e)
        return {"error": f"Failed to look up attachment: {str(e)}"}

    if not record:
        return {
            "error": f"Attachment not found - {att_type} {attachment_index}",
            "combined_analysis": f"Error: Attachment not found - {att_type} {attachment_index}",
        }

    # 2. Branch: links use Jina re-fetch, media uses Gemini binary reanalysis
    if att_type == "link":
        return await _reanalyze_link(
            question=question,
            record=record,
            attachment_index=attachment_index,
            contact_id=contact_id,
            entity_id=entity_id,
            channel=channel,
            is_test_mode=is_test_mode,
            lead_id=lead_id,
        )

    url = record.get("url", "")
    if not url:
        return {"error": f"No URL found for {att_type} {attachment_index}"}

    # 3. Download from Supabase Storage URL (permanent)
    try:
        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "TextEngine/1.0"},
        ) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return {"error": f"Failed to download attachment (HTTP {resp.status_code})"}
            mime_type = resp.headers.get("content-type", "application/octet-stream").split(";")[0].strip()
            file_data = resp.content
    except Exception as e:
        logger.warning("Download failed for reanalyze: %s", e)
        return {"error": f"Download failed: {str(e)}"}

    # 4. Analyze with Gemini — prepend specific question to full extraction prompt
    base_prompt = _ANALYSIS_PROMPTS.get(att_type, _ANALYSIS_PROMPTS["image"])
    prompt = f"SPECIFIC QUESTION: {question}\n\n{base_prompt}"

    try:
        analysis = await analyze_media(file_data, mime_type, prompt)
    except Exception as e:
        logger.warning("Reanalyze Gemini call failed: %s", e, exc_info=True)
        return {"error": f"Analysis failed: {str(e)}"}

    result = {
        "combined_analysis": analysis,
        "attachment_type": att_type,
        "attachment_index": attachment_index,
    }

    # 5. Log to tool_executions (matches n8n Log Tool Execution node)
    try:
        await postgres.log_tool_execution({
            "session_id": contact_id,
            "client_id": entity_id,
            "tool_name": "reanalyze_attachment",
            "tool_input": json.dumps({
                "question": question,
                "attachment_type": att_type,
                "attachment_index": attachment_index,
            }),
            "tool_output": json.dumps({"combined_analysis": analysis}),
            "channel": channel,
            "execution_id": None,
            "test_mode": is_test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log reanalyze tool execution", exc_info=True)

    return result


async def _reanalyze_link(
    question: str,
    record: dict[str, Any],
    attachment_index: int,
    contact_id: str,
    entity_id: str,
    channel: str,
    is_test_mode: bool,
    lead_id: str | None,
) -> dict[str, Any]:
    """Re-fetch a link URL via Jina and answer a specific question about its content."""
    from app.text_engine.attachments import _fetch_link_content
    from app.services.ai_client import classify

    url = record.get("url", "") or record.get("original_url", "")
    if not url:
        return {"error": "No URL found for this link attachment"}

    # Re-fetch the page via Jina
    page_text = ""
    try:
        page_text, _final_url = await _fetch_link_content(url)
    except Exception as e:
        logger.warning("Reanalyze link fetch failed: %s", e)

    # Fall back to stored raw_analysis if re-fetch fails
    if not page_text:
        page_text = record.get("raw_analysis", "")

    if not page_text:
        return {"error": "Could not fetch or read the linked page", "combined_analysis": "Page content unavailable"}

    # Answer the specific question using page content
    try:
        result = await classify(
            prompt=(
                f"SPECIFIC QUESTION: {question}\n\n"
                f"URL: {url}\n\n"
                f"Page content:\n{page_text[:6000]}"
            ),
            schema={
                "type": "object",
                "properties": {
                    "answer": {"type": "string", "description": "Answer to the question based on page content"},
                },
                "required": ["answer"],
            },
            model="openai/gpt-4.1",
            temperature=0.3,
            system_prompt=(
                "<role>\n"
                "You are analyzing a web page to answer a specific question.\n"
                "</role>\n"
                "<rules>\n"
                "Only answer based on what's actually on the page. "
                "If the answer isn't on the page, say so.\n"
                "</rules>"
            ),
            label="link_reanalyze",
        )
        analysis = result.get("answer", "Could not determine answer from page content")
    except Exception as e:
        logger.warning("Reanalyze link AI call failed: %s", e, exc_info=True)
        return {"error": f"Analysis failed: {str(e)}"}

    result_data = {
        "combined_analysis": analysis,
        "attachment_type": "link",
        "attachment_index": attachment_index,
    }

    # Log to tool_executions
    try:
        await postgres.log_tool_execution({
            "session_id": contact_id,
            "client_id": entity_id,
            "tool_name": "reanalyze_attachment",
            "tool_input": json.dumps({
                "question": question,
                "attachment_type": "link",
                "attachment_index": attachment_index,
            }),
            "tool_output": json.dumps({"combined_analysis": analysis}),
            "channel": channel,
            "execution_id": None,
            "test_mode": is_test_mode,
            "lead_id": lead_id,
        })
    except Exception:
        logger.warning("Failed to log reanalyze tool execution", exc_info=True)

    return result_data
