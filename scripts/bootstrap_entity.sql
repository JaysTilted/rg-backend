-- Example seed flow for a fresh one-project deployment.
-- Replace the placeholder values before running.

-- 1. Create a tenant.
insert into public.tenants (name, is_platform_owner)
values ('Example Tenant', true)
returning id;

-- 2. Create an entity and its canonical flat chat-history table.
select *
from public.create_entity_with_chat_table(
  '00000000-0000-0000-0000-000000000000',
  'Example Business'
);

-- 3. After creating the entity, update entities.system_config using the JSON
--    from STARTER_SYSTEM_CONFIG_TEMPLATE.json.
