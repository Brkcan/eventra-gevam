create table if not exists events (
  event_id text primary key,
  customer_id text not null,
  event_type text not null,
  ts timestamptz not null,
  payload jsonb not null,
  source text not null
);

create index if not exists idx_events_customer_type_ts
  on events (customer_id, event_type, ts desc);

create index if not exists idx_events_customer_ts
  on events (customer_id, ts desc);

create index if not exists idx_events_ts_brin
  on events using brin (ts);

create table if not exists customer_profiles (
  customer_id text primary key,
  segment text,
  attributes jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists action_log (
  action_id text primary key,
  event_id text not null,
  customer_id text not null,
  journey_id text,
  journey_version int,
  journey_node_id text,
  channel text not null,
  status text not null,
  message text not null,
  created_at timestamptz not null default now()
);

create table if not exists external_call_log (
  id text primary key,
  instance_id text not null,
  journey_id text not null,
  journey_version int not null,
  customer_id text not null,
  journey_node_id text not null,
  method text not null,
  url text not null,
  status_code int not null default 0,
  result_type text not null,
  reason text not null,
  response_json jsonb,
  created_at timestamptz not null default now()
);

create table if not exists journeys (
  journey_id text not null,
  version int not null,
  name text not null,
  status text not null default 'published',
  graph_json jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (journey_id, version)
);

create table if not exists journey_instances (
  instance_id text primary key,
  journey_id text not null,
  journey_version int not null,
  customer_id text not null,
  state text not null,
  current_node text not null,
  started_at timestamptz not null,
  due_at timestamptz,
  completed_at timestamptz,
  last_event_id text,
  context_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists uq_journey_instance_active
  on journey_instances (journey_id, customer_id)
  where state in ('waiting', 'waiting_manual', 'active', 'processing');

create index if not exists idx_journey_instances_due
  on journey_instances (state, due_at);

create index if not exists idx_journey_instances_journey_state_due
  on journey_instances (journey_id, state, due_at);

create index if not exists idx_journey_instances_customer_updated
  on journey_instances (customer_id, updated_at desc);

create index if not exists idx_journey_instances_updated
  on journey_instances (updated_at desc);

create index if not exists idx_external_call_log_lookup
  on external_call_log (journey_id, journey_version, customer_id, created_at desc);

create table if not exists event_dlq (
  id text primary key,
  event_id text,
  customer_id text,
  event_type text,
  source_topic text not null,
  error_message text not null,
  raw_payload jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_event_dlq_created
  on event_dlq (created_at desc);

create table if not exists journey_instance_transitions (
  id text primary key,
  instance_id text not null,
  journey_id text not null,
  journey_version int not null,
  customer_id text not null,
  from_state text,
  to_state text not null,
  from_node text,
  to_node text,
  reason text,
  event_id text,
  metadata_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_instance_transitions_lookup
  on journey_instance_transitions (instance_id, created_at desc);

create index if not exists idx_instance_transitions_customer
  on journey_instance_transitions (customer_id, created_at desc);

create table if not exists consumed_events (
  consumer_group text not null,
  event_id text not null,
  consumed_at timestamptz not null default now(),
  primary key (consumer_group, event_id)
);

create table if not exists edge_capacity_usage (
  journey_id text not null,
  journey_version int not null,
  edge_id text not null,
  window_type text not null,
  window_start timestamptz not null,
  used_count int not null default 0,
  updated_at timestamptz not null default now(),
  primary key (journey_id, journey_version, edge_id, window_type, window_start)
);

create index if not exists idx_edge_capacity_lookup
  on edge_capacity_usage (journey_id, journey_version, edge_id, window_type, window_start desc);
