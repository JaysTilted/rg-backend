"""Backward compatibility — all functions moved to app.services.mattermost."""
from app.services.mattermost import (  # noqa: F401
    create_channel as create_slack_channel,
    find_channel_by_name,
    invite_to_channel,
    notify_error,
    post_message as post_slack_message,
)
