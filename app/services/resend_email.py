"""Resend transactional email service.

Simple HTTP POST to Resend API. Used for all email notifications
(client staff alerts + tenant owner alerts).
"""

from __future__ import annotations

import logging
from html import escape
from typing import Any

import httpx

from app.config import settings
from app.services.ghl_links import build_ghl_contact_url

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.resend.com/emails"


def _wrap_html(body_html: str, footer_text: str = "") -> str:
    """Wrap body HTML in a clean, minimal email template."""
    footer_html = f'<p style="color:#999;font-size:12px;margin-top:24px;">{footer_text}</p>' if footer_text else ""
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }}
    .container {{ max-width: 560px; margin: 0 auto; padding: 24px; }}
    .divider {{ border: none; border-top: 1px solid #e5e5e5; margin: 20px 0; }}
    .label {{ color: #999; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px; }}
    .value {{ font-size: 14px; margin: 0 0 12px 0; }}
    h2 {{ font-size: 18px; font-weight: 600; margin: 0 0 16px 0; }}
  </style>
</head>
<body>
  <div class="container">
    {body_html}
    {footer_html}
  </div>
</body>
</html>"""


async def send_email(
    to: str,
    subject: str,
    body_html: str,
    footer_text: str = "You're receiving this because you have email alerts enabled.",
    from_name: str = "",
) -> dict[str, Any]:
    """Send an email via Resend API.

    Args:
        to: Recipient email address.
        subject: Email subject line.
        body_html: Inner HTML content (will be wrapped in template).
        footer_text: Optional footer text. Empty string to omit.
        from_name: Override display name (e.g., tenant name). Uses env default if empty.

    Returns:
        {"status": "sent"} or {"status": "failed", "error": "..."}.
    """
    if not settings.resend_api_key:
        logger.warning("RESEND | no API key configured — skipping email to %s", to)
        return {"status": "failed", "error": "Resend API key not configured"}

    html = _wrap_html(body_html, footer_text)

    # Build from address — use tenant name if provided, otherwise env default
    if from_name:
        from_addr = f"{from_name} <noreply@send.bookmyleads.ai>"
    else:
        from_addr = settings.resend_from_email

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _BASE_URL,
                headers={
                    "Authorization": f"Bearer {settings.resend_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": from_addr,
                    "to": [to],
                    "subject": subject,
                    "html": html,
                },
            )

        if resp.status_code >= 300:
            error = resp.text[:200]
            logger.warning("RESEND | failed | to=%s | status=%d | %s", to, resp.status_code, error)
            return {"status": "failed", "error": error}

        logger.info("RESEND | sent | to=%s | subject=%s", to, subject)
        return {"status": "sent"}

    except Exception as e:
        logger.warning("RESEND | error | to=%s | %s", to, e)
        return {"status": "failed", "error": str(e)[:200]}


# ── Pre-built notification email builders ──

def build_transfer_email(
    contact_name: str,
    contact_phone: str,
    reason: str = "",
    contact_email: str = "",
    client_name: str = "",
    include_client: bool = True,
    ghl_location_id: str = "",
    ghl_contact_id: str = "",
    ghl_domain: str = "",
) -> tuple[str, str]:
    """Build subject + body HTML for a transfer notification.

    Args:
        include_client: True for tenant-level (shows client name), False for client staff.
        ghl_location_id: GHL location ID for contact link.
        ghl_contact_id: GHL contact ID for contact link.

    Returns:
        (subject, body_html)
    """
    if include_client:
        subject = f"Transfer Alert — {client_name}"
        header = "Transfer Alert"
    else:
        subject = f"Human Needed — {contact_name}"
        header = "Human Needed"

    parts = [f'<h2>{header}</h2>']
    if include_client and client_name:
        parts.append(f'<p class="label">Client</p><p class="value">{client_name}</p>')
    parts.append(f'<p class="label">Lead</p><p class="value">{contact_name}</p>')
    parts.append(f'<p class="label">Phone</p><p class="value">{contact_phone}</p>')
    if contact_email:
        parts.append(f'<p class="label">Email</p><p class="value">{contact_email}</p>')
    if reason:
        parts.append(f'<hr class="divider"><p class="label">Reason</p><p class="value">{reason}</p>')

    # GHL contact link
    ghl_url = build_ghl_contact_url(ghl_location_id, ghl_contact_id, ghl_domain)
    if ghl_url:
        parts.append(f'<hr class="divider"><p><a href="{ghl_url}" style="color:#0066cc;font-size:13px;">View Contact in GHL</a></p>')

    return subject, "\n".join(parts)


def build_booking_email(
    contact_name: str,
    contact_phone: str,
    status: str = "",
    client_name: str = "",
    include_client: bool = True,
) -> tuple[str, str]:
    """Build subject + body HTML for a booking notification."""
    subject = f"New Booking — {client_name}" if include_client else f"New Booking — {contact_name}"

    parts = ['<h2>New Booking</h2>']
    if include_client and client_name:
        parts.append(f'<p class="label">Client</p><p class="value">{client_name}</p>')
    parts.append(f'<p class="label">Name</p><p class="value">{contact_name}</p>')
    parts.append(f'<p class="label">Phone</p><p class="value">{contact_phone}</p>')
    if status:
        parts.append(f'<p class="label">Status</p><p class="value">{status}</p>')

    return subject, "\n".join(parts)


def build_new_lead_email(
    contact_name: str,
    contact_phone: str,
    service: str = "",
    client_name: str = "",
    include_client: bool = True,
) -> tuple[str, str]:
    """Build subject + body HTML for a new lead notification."""
    subject = f"New Lead — {client_name}" if include_client else f"New Lead — {contact_name}"

    parts = ['<h2>New Lead</h2>']
    if include_client and client_name:
        parts.append(f'<p class="label">Client</p><p class="value">{client_name}</p>')
    parts.append(f'<p class="label">Name</p><p class="value">{contact_name}</p>')
    parts.append(f'<p class="label">Phone</p><p class="value">{contact_phone}</p>')
    if service:
        parts.append(f'<p class="label">Interest</p><p class="value">{service}</p>')

    return subject, "\n".join(parts)


def build_missed_call_email(
    contact_name: str,
    contact_phone: str,
    textback_active: bool = False,
    client_name: str = "",
    include_client: bool = True,
) -> tuple[str, str]:
    """Build subject + body HTML for a missed call notification."""
    subject = f"Missed Call — {client_name}" if include_client else f"Missed Call — {contact_name}"

    parts = ['<h2>Missed Call</h2>']
    if include_client and client_name:
        parts.append(f'<p class="label">Client</p><p class="value">{client_name}</p>')
    parts.append(f'<p class="label">Name</p><p class="value">{contact_name}</p>')
    parts.append(f'<p class="label">Phone</p><p class="value">{contact_phone}</p>')
    parts.append('<hr class="divider">')
    if textback_active:
        parts.append('<p style="color:#16a34a;">AI has reached out and will follow up.</p>')
    else:
        parts.append('<p style="color:#dc2626;font-weight:600;">Call this lead back as soon as possible.</p>')

    return subject, "\n".join(parts)


def build_change_request_email(
    topic: str,
    description: str,
    client_name: str = "",
    include_client: bool = True,
) -> tuple[str, str]:
    """Build subject + body HTML for a change request notification."""
    safe_topic = escape(topic or "General")
    safe_description = escape(description or "").replace("\n", "<br>")
    safe_client_name = escape(client_name or "Client")

    subject = f"Change Request - {client_name}" if include_client and client_name else "Change Request"

    parts = ['<h2>Change Request</h2>']
    if include_client and client_name:
        parts.append(f'<p class="label">Client</p><p class="value">{safe_client_name}</p>')
    parts.append(f'<p class="label">Topic</p><p class="value">{safe_topic}</p>')
    parts.append(f'<hr class="divider"><p class="label">Request</p><p class="value">{safe_description}</p>')

    return subject, "\n".join(parts)
