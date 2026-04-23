#!/usr/bin/env python3
"""Render tenant/entity bootstrap SQL from the starter system-config template."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = ROOT / "STARTER_SYSTEM_CONFIG_TEMPLATE.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render seed SQL for a fresh iron-setter entity.")
    parser.add_argument("--tenant-name", required=True, help="Tenant name to insert.")
    parser.add_argument("--entity-name", required=True, help="Business/entity name to create.")
    parser.add_argument(
        "--entity-type",
        default="client",
        choices=("client", "internal"),
        help="Entity type for public.entities.entity_type.",
    )
    parser.add_argument(
        "--template",
        default=str(TEMPLATE_PATH),
        help="Path to the starter system-config JSON.",
    )
    return parser.parse_args()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> int:
    args = parse_args()
    template_path = Path(args.template)
    config = json.loads(template_path.read_text(encoding="utf-8"))
    config_json = json.dumps(config, ensure_ascii=True)

    tenant_name = sql_literal(args.tenant_name)
    entity_name = sql_literal(args.entity_name)
    entity_type = sql_literal(args.entity_type)
    config_literal = sql_literal(config_json)

    print(
        f"""begin;

with new_tenant as (
  insert into public.tenants (name, is_platform_owner)
  values ({tenant_name}, true)
  returning id
),
new_entity as (
  select *
  from public.create_entity_with_chat_table(
    (select id from new_tenant),
    {entity_name},
    {entity_type}
  )
)
update public.entities
set system_config = {config_literal}::jsonb
where id = (select entity_id from new_entity);

select
  t.id as tenant_id,
  e.id as entity_id,
  e.chat_history_table_name
from public.tenants t
join public.entities e on e.tenant_id = t.id
where t.name = {tenant_name}
order by t.created_at desc, e.created_at desc
limit 1;

commit;"""
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
