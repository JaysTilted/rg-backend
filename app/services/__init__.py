"""Service clients — external API wrappers with connection pooling."""

from app.services.supabase_client import supabase
from app.services.postgres_client import postgres
from app.services.ghl_client import GHLClient, start_ghl_pool, stop_ghl_pool
from app.services.ai_client import classify, extract, chat, start_ai_clients
from app.services.signal_house_client import start_signal_house_pool, stop_signal_house_pool
from app.services.delivery_service import DeliveryService, DeliveryResult, SplitDeliveryResult

__all__ = [
    "supabase",
    "postgres",
    "GHLClient",
    "start_ghl_pool",
    "stop_ghl_pool",
    "classify",
    "extract",
    "chat",
    "start_ai_clients",
    "start_signal_house_pool",
    "stop_signal_house_pool",
    "DeliveryService",
    "DeliveryResult",
    "SplitDeliveryResult",
]
