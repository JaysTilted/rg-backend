-- Backend-only / one-Supabase-project bootstrap for rg-backend
-- Run this in the Supabase SQL editor for the single project he will use.
--
-- What this creates:
-- - required core backend tables only
-- - required extensions
-- - required KB vector match RPC
-- - required tenant propagation helpers/triggers
-- - required KB webhook trigger
-- - required attachments storage bucket
-- - helper to create canonical chat history tables
-- - helper to create an entity + its chat history table together
--
-- What this intentionally does NOT create:
-- - optional frontend/admin tables
-- - legacy clients/personal_bots tables
-- - optional analytics/testing/data-chat schema

set search_path = public, extensions;

create schema if not exists extensions;
create extension if not exists pgcrypto with schema extensions;
create extension if not exists vector with schema extensions;
create extension if not exists pg_net with schema extensions;

-- ============================================================================
-- CORE TABLES
-- ============================================================================

create table if not exists public.tenants (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  logo_url text,
  tier text default 'tier_1',
  max_clients integer,
  is_platform_owner boolean default false,
  openrouter_api_key text,
  google_ai_key text,
  xai_key text,
  deepseek_key text,
  openai_key text,
  anthropic_key text,
  notification_config jsonb default '{"sms": null, "email": null, "events": ["transfer_to_human"], "in_app": true}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  subscription_amount numeric default 0,
  status text not null default 'active',
  archived_at timestamptz,
  archived_by uuid,
  default_setter_config jsonb,
  constraint tenants_status_check check (status in ('active', 'archived'))
);

create index if not exists idx_tenants_status on public.tenants (status);

create table if not exists public.entities (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null default 'client',
  name varchar,
  contact_name varchar,
  contact_email varchar,
  contact_phone varchar,
  logo_url text,
  journey_stage varchar not null default 'Onboarding',
  created_at timestamptz default now(),
  ghl_location_id varchar,
  ghl_api_key text,
  ghl_domain text default 'https://app.gohighlevel.com',
  chat_history_table_name text,
  business_phone text,
  business_schedule jsonb,
  timezone text,
  average_booking_value numeric default 0,
  supported_languages jsonb,
  system_config jsonb,
  test_config_overrides jsonb,
  notes text,
  slack_report_channel_id text,
  tenant_id uuid not null references public.tenants(id),
  status text not null default 'active',
  archived_at timestamptz,
  archived_by uuid,
  billing_config jsonb default '{"model": "tiered", "manual_rate": 0, "ai_direct_rate": 30, "ai_assisted_rate": 10}'::jsonb,
  constraint entities_entity_type_check check (entity_type in ('client', 'internal')),
  constraint entities_status_check check (status in ('active', 'archived'))
);

create index if not exists ent_entities_entity_type on public.entities (entity_type);
create index if not exists ent_entities_journey_stage on public.entities (journey_stage);
create index if not exists idx_entities_status on public.entities (status);
create index if not exists idx_entities_tenant_id on public.entities (tenant_id);
create index if not exists idx_entities_status_tenant on public.entities (tenant_id, status);

create table if not exists public.leads (
  id uuid primary key default gen_random_uuid(),
  entity_id uuid not null references public.entities(id),
  ghl_contact_id text not null,
  source text,
  created_at timestamptz default now(),
  contact_name text,
  contact_phone text,
  contact_email text,
  form_interest text,
  qualification_status text not null default 'undetermined',
  qualification_notes jsonb,
  last_reply_datetime timestamptz,
  smart_followup_timer jsonb,
  last_reply_channel text,
  outreach_variant jsonb,
  extracted_data jsonb default '{}'::jsonb,
  tenant_id uuid not null references public.tenants(id) on update cascade
);

create index if not exists ent_leads_created_at on public.leads (created_at);
create index if not exists ent_leads_entity_contact_created on public.leads (entity_id, ghl_contact_id, created_at desc);
create index if not exists ent_leads_entity_created on public.leads (entity_id, created_at);
create index if not exists ent_leads_entity_id on public.leads (entity_id);
create index if not exists ent_leads_entity_source on public.leads (entity_id, source);
create index if not exists ent_leads_form_interest on public.leads (form_interest) where form_interest is not null;
create index if not exists ent_leads_ghl_contact_id on public.leads (ghl_contact_id);
create index if not exists ent_leads_qualification on public.leads (qualification_status) where qualification_status = 'qualified';
create index if not exists ent_leads_reply_channel on public.leads (last_reply_channel);
create index if not exists ent_leads_source on public.leads (source);
create index if not exists idx_leads_tenant_id on public.leads (tenant_id);
create unique index if not exists ent_uq_leads_entity_contact_interest
  on public.leads (entity_id, ghl_contact_id, form_interest)
  where form_interest is not null;

create table if not exists public.bookings (
  id uuid primary key default gen_random_uuid(),
  entity_id uuid not null references public.entities(id),
  ghl_contact_id text not null,
  ghl_appointment_id text,
  ghl_calendar_name text,
  booked_at timestamptz not null default now(),
  appointment_datetime timestamptz,
  created_at timestamptz not null default now(),
  lead_source text,
  status text not null default 'confirmed',
  after_hours boolean,
  appointment_timezone text,
  booking_channel text,
  interest text,
  lead_id uuid references public.leads(id),
  ai_booked_directly boolean default false,
  booking_type text,
  tenant_id uuid not null references public.tenants(id) on update cascade
);

create index if not exists ent_bookings_after_hours on public.bookings (after_hours) where after_hours = true;
create index if not exists ent_bookings_appointment_dt on public.bookings (appointment_datetime);
create index if not exists ent_bookings_booked_at on public.bookings (booked_at);
create index if not exists ent_bookings_channel on public.bookings (booking_channel);
create index if not exists ent_bookings_created_at on public.bookings (created_at);
create index if not exists ent_bookings_entity_booked on public.bookings (entity_id, booked_at);
create index if not exists ent_bookings_entity_created on public.bookings (entity_id, created_at);
create index if not exists ent_bookings_entity_id on public.bookings (entity_id);
create index if not exists ent_bookings_ghl_contact_id on public.bookings (ghl_contact_id);
create index if not exists ent_bookings_lead on public.bookings (lead_id) where lead_id is not null;
create index if not exists ent_bookings_lead_source on public.bookings (lead_source);
create index if not exists ent_bookings_status on public.bookings (status);
create index if not exists idx_bookings_tenant_id on public.bookings (tenant_id);

create table if not exists public.call_logs (
  id uuid primary key default gen_random_uuid(),
  entity_id uuid not null references public.entities(id),
  lead_id uuid references public.leads(id),
  ghl_contact_id text,
  direction text not null,
  status text not null,
  from_number text,
  to_number text,
  duration_seconds integer,
  transcript text,
  summary text,
  created_at timestamptz default now(),
  after_hours boolean default false,
  user_sentiment text,
  ghl_message_id text,
  tenant_id uuid not null references public.tenants(id) on update cascade,
  workflow_run_id text,
  call_cost numeric,
  constraint call_logs_direction_check check (direction in ('inbound', 'outbound')),
  constraint call_logs_status_check check (status in ('answered', 'completed', 'no_answer', 'voicemail', 'busy', 'failed'))
);

create index if not exists ent_call_logs_created_desc on public.call_logs (created_at desc);
create index if not exists ent_call_logs_direction on public.call_logs (direction);
create index if not exists ent_call_logs_entity_created_dir on public.call_logs (entity_id, created_at, direction);
create index if not exists ent_call_logs_entity_dir_created on public.call_logs (entity_id, direction, created_at desc);
create index if not exists ent_call_logs_entity_id on public.call_logs (entity_id);
create index if not exists ent_call_logs_ghl_contact_id on public.call_logs (ghl_contact_id);
create index if not exists ent_call_logs_lead on public.call_logs (lead_id) where lead_id is not null;
create index if not exists ent_call_logs_phone_from on public.call_logs (from_number);
create index if not exists ent_call_logs_phone_to on public.call_logs (to_number);
create index if not exists ent_call_logs_sentiment on public.call_logs (user_sentiment);
create index if not exists ent_call_logs_status on public.call_logs (status);
create index if not exists idx_call_logs_tenant_id on public.call_logs (tenant_id);

create table if not exists public.outreach_templates (
  id uuid primary key default gen_random_uuid(),
  form_service_interest text not null,
  is_active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  is_appointment_template boolean default false,
  positions jsonb default '[]'::jsonb,
  timing_config jsonb default '{}'::jsonb,
  calendar_id text,
  calendar_name text,
  entity_id uuid not null references public.entities(id),
  tenant_id uuid not null references public.tenants(id) on update cascade,
  setter_key text
);

create index if not exists ent_outreach_entity_id on public.outreach_templates (entity_id);
create index if not exists idx_outreach_templates_tenant_id on public.outreach_templates (tenant_id);

create table if not exists public.scheduled_messages (
  id uuid primary key default gen_random_uuid(),
  entity_id uuid not null references public.entities(id),
  contact_id text not null,
  message_type text not null,
  position integer,
  channel text not null default 'SMS',
  status text not null default 'pending',
  due_at timestamptz not null,
  source text not null default 'cadence',
  triggered_by text,
  smart_reason text,
  fired_at timestamptz,
  result text,
  result_reason text,
  cancelled_at timestamptz,
  cancel_reason text,
  metadata jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  to_phone text,
  tenant_id uuid not null references public.tenants(id) on update cascade
);

create index if not exists ent_scheduled_messages_entity_id on public.scheduled_messages (entity_id);
create index if not exists idx_scheduled_messages_tenant_id on public.scheduled_messages (tenant_id);
create index if not exists idx_sm_contact_pending on public.scheduled_messages (entity_id, contact_id) where status = 'pending';
create index if not exists idx_sm_entity_stats on public.scheduled_messages (entity_id, status, created_at);
create index if not exists idx_sm_pending_due on public.scheduled_messages (due_at) where status = 'pending';
create unique index if not exists idx_sm_unique_pending
  on public.scheduled_messages (entity_id, contact_id, message_type, coalesce(position, 0))
  where status = 'pending';

create table if not exists public.workflow_runs (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references public.tenants(id),
  entity_id uuid references public.entities(id),
  ghl_contact_id text,
  lead_id uuid references public.leads(id) on delete set null,
  parent_run_id uuid references public.workflow_runs(id) on delete set null,
  workflow_type text not null,
  status text not null default 'running',
  trigger_source text not null default 'webhook',
  mode text not null default 'production',
  stages jsonb default '[]'::jsonb,
  decisions jsonb default '{}'::jsonb,
  error_message text,
  duration_ms integer default 0,
  total_cost numeric default 0,
  prompt_tokens integer default 0,
  completion_tokens integer default 0,
  total_tokens integer default 0,
  call_count integer default 0,
  llm_calls jsonb default '[]'::jsonb,
  metadata jsonb default '{}'::jsonb,
  created_at timestamptz not null default now(),
  system_config_snapshot jsonb,
  runtime_context jsonb,
  constraint workflow_runs_status_check check (status in ('running', 'success', 'error', 'skipped')),
  constraint workflow_runs_mode_check check (mode in ('production', 'test', 'chat')),
  constraint workflow_runs_trigger_source_check check (trigger_source in ('webhook', 'scheduled', 'manual', 'chain', 'ui'))
);

create index if not exists idx_wr_contact on public.workflow_runs (ghl_contact_id, created_at desc);
create index if not exists idx_wr_entity_date_status_type on public.workflow_runs (entity_id, created_at desc, status, workflow_type);
create index if not exists idx_wr_parent on public.workflow_runs (parent_run_id) where parent_run_id is not null;
create index if not exists idx_wr_status_error on public.workflow_runs (status) where status = 'error';
create index if not exists idx_wr_tenant_created on public.workflow_runs (tenant_id, created_at desc);

create table if not exists public.knowledge_base_articles (
  id uuid primary key default gen_random_uuid(),
  entity_id uuid not null references public.entities(id),
  tenant_id uuid not null references public.tenants(id) on update cascade,
  title text not null,
  content text,
  tag text not null,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index if not exists ent_kb_articles_entity_id on public.knowledge_base_articles (entity_id);
create index if not exists idx_kb_articles_tenant_id on public.knowledge_base_articles (tenant_id);

create table if not exists public.documents (
  id bigserial primary key,
  content text,
  metadata jsonb,
  embedding vector(1536),
  article_id uuid references public.knowledge_base_articles(id) on delete cascade,
  entity_id uuid not null references public.entities(id),
  tenant_id uuid not null references public.tenants(id) on update cascade
);

create index if not exists idx_documents_article_id on public.documents (article_id);
create index if not exists idx_documents_entity_id on public.documents (entity_id);
create index if not exists idx_documents_tenant_id on public.documents (tenant_id);
create index if not exists documents_embedding_idx
  on public.documents using ivfflat (embedding vector_cosine_ops) with (lists = 100);

create table if not exists public.attachments (
  id uuid primary key default gen_random_uuid(),
  session_id text not null,
  type text not null,
  url text not null,
  description text,
  message_timestamp timestamptz,
  created_at timestamptz default now(),
  "timestamp" timestamptz default now(),
  raw_analysis text,
  "client/bot_id" text,
  original_url text,
  lead_id uuid,
  ghl_message_id text,
  unique (session_id, url)
);

create index if not exists idx_attachments_ghl_message_id on public.attachments (ghl_message_id);
create index if not exists idx_attachments_lead_id on public.attachments (lead_id);
create index if not exists idx_attachments_session_id on public.attachments (session_id);
create index if not exists idx_attachments_session_type on public.attachments (session_id, type);

create table if not exists public.tool_executions (
  id uuid primary key default gen_random_uuid(),
  session_id text,
  client_id text not null,
  tool_name text not null,
  tool_input jsonb,
  tool_output jsonb,
  execution_id text,
  created_at timestamptz default now(),
  "timestamp" timestamptz default now(),
  channel text,
  test_mode boolean default false,
  lead_id uuid
);

create index if not exists idx_tool_executions_channel on public.tool_executions (channel);
create index if not exists idx_tool_executions_client on public.tool_executions (client_id);
create index if not exists idx_tool_executions_client_ts on public.tool_executions (client_id, "timestamp");
create index if not exists idx_tool_executions_created_at on public.tool_executions (created_at);
create index if not exists idx_tool_executions_lead_id on public.tool_executions (lead_id);
create index if not exists idx_tool_executions_session on public.tool_executions (session_id);
create index if not exists idx_tool_executions_tool on public.tool_executions (tool_name);
create index if not exists idx_tool_executions_tool_name on public.tool_executions (tool_name);

-- ============================================================================
-- STORAGE
-- ============================================================================

insert into storage.buckets (id, name, public)
values ('attachments', 'attachments', true)
on conflict (id) do update
set name = excluded.name,
    public = excluded.public;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create or replace function public.set_tenant_id_from_entity()
returns trigger
language plpgsql
as $$
begin
  if new.entity_id is not null and new.tenant_id is null then
    select e.tenant_id into new.tenant_id
    from public.entities e
    where e.id = new.entity_id;
  end if;
  return new;
end;
$$;

create or replace function public.create_chat_history_table(p_table_name text)
returns void
language plpgsql
as $$
declare
  v_hash text;
begin
  if p_table_name is null or btrim(p_table_name) = '' then
    raise exception 'p_table_name is required';
  end if;

  v_hash := substr(md5(p_table_name), 1, 8);

  execute format(
    'create table if not exists public.%I (
      id serial primary key,
      session_id varchar(255) not null,
      role text not null,
      content text not null default '''',
      source text,
      channel text,
      lead_id uuid,
      "timestamp" timestamptz default now(),
      ghl_message_id text,
      attachment_ids uuid[] default ''{}''::uuid[],
      workflow_run_id text
    )',
    p_table_name
  );

  execute format('create index if not exists %I on public.%I (session_id)', 'idx_ch_' || v_hash || '_sid', p_table_name);
  execute format('create index if not exists %I on public.%I (session_id, "timestamp")', 'idx_ch_' || v_hash || '_st', p_table_name);
  execute format('create index if not exists %I on public.%I ("timestamp")', 'idx_ch_' || v_hash || '_ts', p_table_name);
  execute format('create index if not exists %I on public.%I (lead_id)', 'idx_ch_' || v_hash || '_lid', p_table_name);
  execute format('create index if not exists %I on public.%I (role)', 'idx_ch_' || v_hash || '_role', p_table_name);
end;
$$;

create or replace function public.create_entity_with_chat_table(
  p_tenant_id uuid,
  p_name text,
  p_entity_type text default 'client',
  p_chat_history_table_name text default null
)
returns table(entity_id uuid, chat_history_table_name text)
language plpgsql
as $$
declare
  v_chat_table_name text;
begin
  if p_tenant_id is null then
    raise exception 'p_tenant_id is required';
  end if;

  if p_name is null or btrim(p_name) = '' then
    raise exception 'p_name is required';
  end if;

  v_chat_table_name := coalesce(
    nullif(p_chat_history_table_name, ''),
    lower(regexp_replace(p_name, '[^a-zA-Z0-9]+', '_', 'g')) || '_chat_history_' || substr(replace(gen_random_uuid()::text, '-', ''), 1, 8)
  );

  perform public.create_chat_history_table(v_chat_table_name);

  insert into public.entities (
    tenant_id,
    name,
    entity_type,
    chat_history_table_name,
    status
  )
  values (
    p_tenant_id,
    p_name,
    p_entity_type,
    v_chat_table_name,
    'active'
  )
  returning public.entities.id, public.entities.chat_history_table_name
  into entity_id, chat_history_table_name;

  return next;
end;
$$;

create or replace function public.match_documents(
  query_embedding vector,
  match_count integer default null,
  filter jsonb default '{}'::jsonb,
  p_entity_id uuid default null
)
returns table(id bigint, content text, metadata jsonb, similarity double precision)
language plpgsql
as $$
begin
  return query
  select
    documents.id,
    documents.content,
    documents.metadata,
    1 - (documents.embedding <=> query_embedding) as similarity
  from public.documents
  where
    (p_entity_id is null or documents.entity_id = p_entity_id)
    and (filter = '{}'::jsonb or documents.metadata @> filter)
  order by documents.embedding <=> query_embedding
  limit match_count;
end;
$$;

create or replace function public.sync_kb_embeddings()
returns trigger
language plpgsql
as $$
declare
  v_base_url text := nullif(current_setting('app.settings.kb_webhook_base_url', true), '');
  v_auth_token text := nullif(current_setting('app.settings.api_auth_token', true), '');
  v_entity_id uuid := coalesce(new.entity_id, old.entity_id);
  v_headers jsonb := '{"Content-Type":"application/json"}'::jsonb;
  v_body jsonb;
begin
  if v_entity_id is null then
    return coalesce(new, old);
  end if;

  if v_base_url is null then
    return coalesce(new, old);
  end if;

  if v_auth_token is not null then
    v_headers := v_headers || jsonb_build_object('Authorization', 'Bearer ' || v_auth_token);
  end if;

  v_body := jsonb_build_object(
    'title', coalesce(new.title, old.title, ''),
    'content', coalesce(new.content, old.content, ''),
    'tag', coalesce(new.tag, old.tag, ''),
    'action', case when tg_op = 'DELETE' then 'delete' else 'upsert' end,
    'kb_id', coalesce(new.id, old.id)
  );

  perform net.http_post(
    url := rtrim(v_base_url, '/') || '/webhook/' || v_entity_id::text || '/update-kb',
    body := v_body,
    headers := v_headers,
    timeout_milliseconds := 5000
  );

  return coalesce(new, old);
end;
$$;

-- ============================================================================
-- UPDATED_AT TRIGGERS
-- ============================================================================

drop trigger if exists trg_tenants_set_updated_at on public.tenants;
create trigger trg_tenants_set_updated_at
before update on public.tenants
for each row
execute function public.set_updated_at();

drop trigger if exists trg_outreach_templates_set_updated_at on public.outreach_templates;
create trigger trg_outreach_templates_set_updated_at
before update on public.outreach_templates
for each row
execute function public.set_updated_at();

drop trigger if exists trg_scheduled_messages_set_updated_at on public.scheduled_messages;
create trigger trg_scheduled_messages_set_updated_at
before update on public.scheduled_messages
for each row
execute function public.set_updated_at();

drop trigger if exists trg_knowledge_base_articles_set_updated_at on public.knowledge_base_articles;
create trigger trg_knowledge_base_articles_set_updated_at
before update on public.knowledge_base_articles
for each row
execute function public.set_updated_at();

-- ============================================================================
-- TENANT PROPAGATION TRIGGERS
-- ============================================================================

drop trigger if exists trg_leads_set_tenant on public.leads;
create trigger trg_leads_set_tenant
before insert or update on public.leads
for each row
execute function public.set_tenant_id_from_entity();

drop trigger if exists trg_bookings_set_tenant on public.bookings;
create trigger trg_bookings_set_tenant
before insert or update on public.bookings
for each row
execute function public.set_tenant_id_from_entity();

drop trigger if exists trg_call_logs_set_tenant on public.call_logs;
create trigger trg_call_logs_set_tenant
before insert or update on public.call_logs
for each row
execute function public.set_tenant_id_from_entity();

drop trigger if exists trg_outreach_templates_set_tenant on public.outreach_templates;
create trigger trg_outreach_templates_set_tenant
before insert or update on public.outreach_templates
for each row
execute function public.set_tenant_id_from_entity();

drop trigger if exists trg_scheduled_messages_set_tenant on public.scheduled_messages;
create trigger trg_scheduled_messages_set_tenant
before insert or update on public.scheduled_messages
for each row
execute function public.set_tenant_id_from_entity();

drop trigger if exists trg_workflow_runs_set_tenant on public.workflow_runs;
create trigger trg_workflow_runs_set_tenant
before insert or update on public.workflow_runs
for each row
execute function public.set_tenant_id_from_entity();

drop trigger if exists trg_knowledge_base_articles_set_tenant on public.knowledge_base_articles;
create trigger trg_knowledge_base_articles_set_tenant
before insert or update on public.knowledge_base_articles
for each row
execute function public.set_tenant_id_from_entity();

drop trigger if exists trg_documents_set_tenant on public.documents;
create trigger trg_documents_set_tenant
before insert or update on public.documents
for each row
execute function public.set_tenant_id_from_entity();

-- ============================================================================
-- KB EMBEDDING WEBHOOK TRIGGER
-- ============================================================================

drop trigger if exists kb_articles_embedding_sync on public.knowledge_base_articles;
create trigger kb_articles_embedding_sync
after insert or update or delete on public.knowledge_base_articles
for each row
execute function public.sync_kb_embeddings();

-- ============================================================================
-- POST-RUN NOTES
-- ============================================================================

-- 1. To enable automatic KB webhook sync, set these once after replacing values:
--    alter database postgres set "app.settings.kb_webhook_base_url" = 'https://your-backend-host';
--    alter database postgres set "app.settings.api_auth_token" = 'your_api_auth_token';
--
-- 2. Create a tenant:
--    insert into public.tenants (name, is_platform_owner) values ('My Tenant', true) returning id;
--
-- 3. Create an entity + chat table together:
--    select * from public.create_entity_with_chat_table('<tenant_uuid>', 'My Business');
--
-- 4. Set entities.system_config before using the reply pipeline.
--    This bootstrap creates schema, not your business-specific prompt/config payloads.
