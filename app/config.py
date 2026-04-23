"""Environment variables and constants."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Supabase
    supabase_main_url: str = ""
    supabase_main_key: str = ""
    supabase_main_schema: str = "public"
    supabase_chat_url: str = ""
    supabase_chat_key: str = ""
    supabase_chat_schema: str = "public"
    database_url: str = ""
    database_chat_url: str = ""

    # AI
    openrouter_api_key: str = ""          # All production traffic (clients + personal bots + workflows)
    openrouter_testing_key: str = ""      # Test runner only
    azure_openai_api_key: str = ""
    azure_openai_base_url: str = "https://ironclaw.openai.azure.com/openai/v1"
    azure_openai_model: str = "gpt-4.1"
    google_gemini_api_key: str = ""
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    deepseek_api_key: str = ""
    xai_api_key: str = ""

    # Slack (legacy — kept for backward compat, unused if Mattermost is configured)
    slack_bot_token: str = ""

    # Mattermost
    mattermost_url: str = ""
    mattermost_bot_token: str = ""
    mattermost_team_name: str = ""

    # Giphy
    giphy_api_key: str = ""

    # Prefect
    prefect_api_url: str = "http://localhost:4200/api"

    # GHL Snapshot (agency account — used for staff notifications)
    ghl_snapshot_api_key: str = ""
    ghl_snapshot_location_id: str = ""

    # Resend (transactional email)
    resend_api_key: str = ""
    resend_from_email: str = "Iron Automations <noreply@send.ironautomations.com>"

    # Auth
    api_auth_token: str = ""

    # Portal auth
    portal_jwt_secret: str = ""
    frontend_url: str = "https://app.ironautomations.com"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
