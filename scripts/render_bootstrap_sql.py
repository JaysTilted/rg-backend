#!/usr/bin/env python3
"""Render shared-Supabase bootstrap SQL with separate main/chat schemas."""

from __future__ import annotations

import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "BACKEND_ONLY_ONE_PROJECT_BOOTSTRAP.sql"


MAIN_OBJECTS = (
    "tenants",
    "entities",
    "leads",
    "bookings",
    "call_logs",
    "outreach_templates",
    "scheduled_messages",
    "workflow_runs",
    "knowledge_base_articles",
    "documents",
)

CHAT_OBJECTS = (
    "attachments",
    "tool_executions",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render schema-aware bootstrap SQL.")
    parser.add_argument("--main-schema", required=True, help="Schema for core runtime tables.")
    parser.add_argument("--chat-schema", required=True, help="Schema for chat-history runtime tables.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    main_schema = args.main_schema
    chat_schema = args.chat_schema
    sql = SOURCE.read_text(encoding="utf-8")

    prelude = [
        f"create schema if not exists {main_schema};",
        f"create schema if not exists {chat_schema};",
    ]
    sql = sql.replace(
        "set search_path = public, extensions;",
        "\n".join(prelude) + f"\nset search_path = {main_schema}, {chat_schema}, public, extensions;",
        1,
    )

    for name in MAIN_OBJECTS:
        sql = sql.replace(f"public.{name}", f"{main_schema}.{name}")
    for name in CHAT_OBJECTS:
        sql = sql.replace(f"public.{name}", f"{chat_schema}.{name}")

    sql = sql.replace(
        "create or replace function public.create_chat_history_table",
        f"create or replace function {chat_schema}.create_chat_history_table",
    )
    sql = sql.replace(
        "create or replace function public.create_entity_with_chat_table",
        f"create or replace function {main_schema}.create_entity_with_chat_table",
    )
    sql = sql.replace(
        "create or replace function public.set_updated_at()",
        f"create or replace function {main_schema}.set_updated_at()",
    )
    sql = sql.replace(
        "create or replace function public.set_tenant_id_from_entity()",
        f"create or replace function {main_schema}.set_tenant_id_from_entity()",
    )
    sql = sql.replace(
        "create or replace function public.match_documents(",
        f"create or replace function {main_schema}.match_documents(",
    )
    sql = sql.replace(
        "create or replace function public.sync_kb_embeddings()",
        f"create or replace function {main_schema}.sync_kb_embeddings()",
    )
    sql = sql.replace(
        "execute function public.set_updated_at();",
        f"execute function {main_schema}.set_updated_at();",
    )
    sql = sql.replace(
        "execute function public.set_tenant_id_from_entity();",
        f"execute function {main_schema}.set_tenant_id_from_entity();",
    )
    sql = sql.replace(
        "execute function public.sync_kb_embeddings();",
        f"execute function {main_schema}.sync_kb_embeddings();",
    )

    sql = sql.replace(
        "perform public.create_chat_history_table(v_chat_table_name);",
        f"perform {chat_schema}.create_chat_history_table(v_chat_table_name);",
    )
    sql = sql.replace(
        'create table if not exists public.%I (',
        f'create table if not exists {chat_schema}.%I (',
    )
    sql = sql.replace(
        "from public.create_entity_with_chat_table(",
        f"from {main_schema}.create_entity_with_chat_table(",
    )
    sql = sql.replace(
        "'/webhook/' || v_entity_id::text || '/update-kb'",
        "'/webhook/' || v_entity_id::text || '/update-kb'",
    )

    print(sql)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
