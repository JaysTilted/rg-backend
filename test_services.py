"""Test service clients against real data (read-only).

Run inside the iron-setter container:
  docker exec iron-setter-iron-setter-1 python /app/test_services.py
"""

import asyncio
import json
import sys

# Fix for the app module imports
sys.path.insert(0, "/app")

from app.services.supabase_client import supabase
from app.services.postgres_client import postgres
from app.services.ai_client import start_ai_clients, classify


async def test_supabase():
    """Test Supabase client — resolve_entity, leads, call logs."""
    print("\n=== SUPABASE CLIENT ===")
    await supabase.start()

    # Discover all entities
    from app.config import settings
    import httpx

    async with httpx.AsyncClient(
        base_url=f"{settings.supabase_main_url}/rest/v1",
        headers={
            "apikey": settings.supabase_main_key,
            "Authorization": f"Bearer {settings.supabase_main_key}",
        },
    ) as client:
        resp = await client.get("/entities", params={
            "select": "id,name,entity_type,chat_history_table_name",
            "is_active": "eq.true",
            "order": "name.asc",
        })
        entities = resp.json()

    clients = [e for e in entities if e.get("entity_type") == "client"]
    internals = [e for e in entities if e.get("entity_type") == "internal"]

    print(f"Found {len(clients)} clients, {len(internals)} internal entities")
    for e in entities:
        print(f"  [{e['entity_type']}] {e['name']} (id={e['id'][:8]}...)")

    # Test resolve_entity with first available entity
    if entities:
        test_id = entities[0]["id"]
        print(f"\n--- resolve_entity({test_id[:8]}...) ---")
        config, is_client = await supabase.resolve_entity(test_id)
        print(f"  is_client: {is_client}")
        print(f"  name: {config.get('name')}")
        print(f"  chat_history_table_name: {config.get('chat_history_table_name')}")
        print(f"  ghl_location_id: {config.get('ghl_location_id', 'N/A')}")

    # Test resolve_entity with a bogus ID
    print(f"\n--- resolve_entity('bogus-id') ---")
    try:
        await supabase.resolve_entity("00000000-0000-0000-0000-000000000000")
        print("  ERROR: should have raised ValueError")
    except ValueError as e:
        print(f"  Correctly raised ValueError: {e}")

    # Test get_lead and get_call_logs (with dummy contact, just verify query executes)
    if entities:
        entity = entities[0]
        logs = await supabase.get_call_logs(
            contact_id="test", entity_id=entity["id"], is_client=True, limit=1
        )
        print(f"\nget_call_logs (dummy contact): {len(logs)} rows")

    await supabase.stop()
    # Return first available entity for postgres tests
    if entities:
        return {
            "id": entities[0]["id"],
            "chat_history_table_name": entities[0].get("chat_history_table_name"),
            "is_client": entities[0].get("entity_type") == "client",
        }
    return None


async def test_postgres(entity_info: dict):
    """Test Postgres client — read chat history."""
    print("\n=== POSTGRES CLIENT ===")
    await postgres.start()

    table = entity_info.get("chat_history_table_name")
    if not table:
        print("No chat_history_table_name, skipping")
        await postgres.stop()
        return

    # Test with dummy contact — verify query executes
    history = await postgres.get_chat_history(table, "test-contact-id", limit=1)
    print(f"get_chat_history('{table}', 'test-contact-id'): {len(history)} messages")

    # Get a real session_id from the table
    rows = await postgres.chat_pool.fetch(
        f'SELECT DISTINCT session_id FROM "{table}" LIMIT 1'
    )
    if rows:
        real_session = rows[0]["session_id"]
        history = await postgres.get_chat_history(table, real_session, limit=3)
        print(f"\nget_chat_history('{table}', '{real_session[:12]}...'): {len(history)} messages")
        if history:
            msg = history[0]
            print(f"  Latest: type={msg['message'].get('type')}, "
                  f"content={str(msg['message'].get('content', ''))[:80]}...")

    # Test outreach check
    exists = await postgres.check_outreach_exists(table, "test-contact-id")
    print(f"\ncheck_outreach_exists('test-contact-id'): {exists}")

    # Test attachments query
    attachments = await postgres.get_attachments("test-contact-id", entity_info["id"])
    print(f"get_attachments('test-contact-id'): {len(attachments)} attachments")

    await postgres.stop()


async def test_ai():
    """Test AI client — simple classification call."""
    print("\n=== AI CLIENT ===")
    start_ai_clients()

    schema = {
        "type": "object",
        "properties": {
            "sentiment": {
                "type": "string",
                "enum": ["positive", "negative", "neutral"],
            },
            "confidence": {
                "type": "number",
            },
        },
        "required": ["sentiment", "confidence"],
    }

    result = await classify(
        prompt="The user said: 'Yeah I'd love to come in for a consultation!'",
        schema=schema,
        model="flash",
        temperature=0.3,
    )
    print(f"classify result: {json.dumps(result, indent=2)}")


async def main():
    print("Testing service clients against real data (READ-ONLY)")
    print("=" * 60)

    entity_info = await test_supabase()
    if entity_info:
        await test_postgres(entity_info)
    await test_ai()

    print("\n" + "=" * 60)
    print("All tests complete.")


if __name__ == "__main__":
    asyncio.run(main())
