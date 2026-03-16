import crypto from 'node:crypto';
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { Kafka, Partitioners } from 'kafkajs';
import { createClient } from 'redis';
import pg from 'pg';
import { z } from 'zod';

dotenv.config({ path: '../../.env' });

const port = Number(process.env.PORT || 3001);
const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const kafkaClientId = process.env.KAFKA_CLIENT_ID || 'eventra-api';
const postgresUrl = process.env.POSTGRES_URL || 'postgresql://eventra:eventra@localhost:5432/eventra';
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

const EventInputSchema = z.object({
  event_id: z.string().optional(),
  customer_id: z.string().min(1),
  event_type: z.string().min(1),
  ts: z.string().datetime().optional(),
  payload: z.record(z.any()).default({}),
  source: z.string().default('api')
});

const JourneyCreateSchema = z.object({
  journey_id: z.string().min(1),
  version: z.number().int().positive().default(1),
  name: z.string().min(1),
  status: z.enum(['draft', 'published', 'archived']).default('published'),
  folder_path: z.string().min(1).default('Workspace'),
  graph_json: z.record(z.any())
});

const JourneyCloneSchema = z.object({
  source_version: z.number().int().positive(),
  target_version: z.number().int().positive().optional(),
  name: z.string().min(1).optional(),
  status: z.enum(['draft', 'published', 'archived']).default('published')
});

const JourneyMoveFolderSchema = z.object({
  version: z.number().int().positive().optional(),
  target_folder_path: z.string().min(1)
});

const CustomerProfileSchema = z.object({
  segment: z.string().nullable().optional(),
  attributes: z.record(z.any()).default({})
});

const CustomerCreateSchema = z.object({
  customer_id: z.string().min(1),
  segment: z.string().nullable().optional(),
  attributes: z.record(z.any()).default({})
});

const ReleaseControlUpdateSchema = z.object({
  version: z.number().int().positive().optional(),
  rollout_percent: z.number().int().min(0).max(100).optional(),
  release_paused: z.boolean().optional()
});

const GlobalPauseUpdateSchema = z.object({
  enabled: z.boolean()
});

const ManualWaitReleaseSchema = z.object({
  journey_id: z.string().min(1),
  version: z.number().int().positive().optional(),
  wait_node_id: z.string().min(1).optional(),
  count: z.number().int().positive().max(10000).default(10),
  released_by: z.string().min(1).optional()
});

const CatalogueEventTypeSchema = z.object({
  event_type: z.string().min(1),
  description: z.string().default(''),
  owner: z.string().default(''),
  version: z.number().int().positive().default(1),
  required_fields: z.array(z.string()).default([]),
  schema_json: z.record(z.any()).default({}),
  sample_payload: z.record(z.any()).default({}),
  is_active: z.boolean().default(true)
});

const CatalogueSegmentSchema = z.object({
  segment_key: z.string().min(1),
  display_name: z.string().default(''),
  rule_expression: z.string().default(''),
  description: z.string().default(''),
  is_active: z.boolean().default(true)
});

const CatalogueTemplateSchema = z.object({
  template_id: z.string().min(1),
  channel: z.enum(['email', 'sms', 'push']).default('email'),
  subject: z.string().default(''),
  body: z.string().default(''),
  variables: z.array(z.string()).default([]),
  is_active: z.boolean().default(true)
});

const CatalogueEndpointSchema = z.object({
  endpoint_id: z.string().min(1),
  method: z.enum(['GET', 'POST', 'PUT', 'PATCH', 'DELETE']).default('POST'),
  url: z.string().url(),
  headers: z.record(z.any()).default({}),
  timeout_ms: z.number().int().min(100).max(60000).default(5000),
  description: z.string().default(''),
  is_active: z.boolean().default(true)
});

const kafka = new Kafka({ clientId: kafkaClientId, brokers: kafkaBrokers });
const admin = kafka.admin();
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});
const pgClient = new pg.Client({ connectionString: postgresUrl });
const redis = createClient({ url: redisUrl });

function parseLimit(value, defaultValue = 100, maxValue = 500) {
  const n = Number(value ?? defaultValue);
  if (!Number.isFinite(n) || n <= 0) return defaultValue;
  return Math.min(Math.floor(n), maxValue);
}

function parseOffset(value) {
  const n = Number(value ?? 0);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

function parseJsonSafe(raw, fallback = null) {
  try {
    return JSON.parse(String(raw ?? ''));
  } catch {
    return fallback;
  }
}

async function resolveJourneyVersion(journeyId, versionRaw) {
  if (versionRaw !== undefined && versionRaw !== null && String(versionRaw).trim() !== '') {
    const parsed = Number(versionRaw);
    if (!Number.isInteger(parsed) || parsed <= 0) {
      throw new Error('invalid version');
    }
    return parsed;
  }

  const latest = await pgClient.query(
    `select version
     from journeys
     where journey_id = $1
     order by version desc
     limit 1`,
    [journeyId]
  );
  if (latest.rowCount === 0) {
    throw new Error('journey not found');
  }
  return Number(latest.rows[0].version);
}

async function resolveWaitNodeId(journeyId, version, waitNodeIdRaw) {
  const journey = await pgClient.query(
    `select graph_json
     from journeys
     where journey_id = $1 and version = $2
     limit 1`,
    [journeyId, version]
  );
  if (journey.rowCount === 0) {
    throw new Error('journey version not found');
  }

  const nodes = Array.isArray(journey.rows[0]?.graph_json?.nodes) ? journey.rows[0].graph_json.nodes : [];
  const waitNodes = nodes.filter((node) => {
    const nodeKind = node?.data?.node_kind || node?.type;
    return nodeKind === 'wait';
  });

  if (waitNodes.length === 0) {
    throw new Error('wait node not found');
  }

  if (waitNodeIdRaw) {
    const matched = waitNodes.find((node) => String(node?.id) === String(waitNodeIdRaw));
    if (!matched) {
      throw new Error('wait_node_id not found in journey');
    }
    return String(matched.id);
  }

  const firstManual = waitNodes.find((node) => Boolean(node?.data?.manual_release));
  return String(firstManual?.id || waitNodes[0].id);
}

async function ensureKafkaTopics() {
  await admin.connect();
  try {
    const existingTopics = new Set(await admin.listTopics());
    const missingTopics = ['event.raw', 'action.triggered', 'event.dlq'].filter(
      (topic) => !existingTopics.has(topic)
    );

    if (missingTopics.length === 0) {
      return;
    }

    await admin.createTopics({
      topics: missingTopics.map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1
      })),
      waitForLeaders: true
    });
  } catch (error) {
    // Topic zaten varsa veya broker auto-create devredeyse startup'i durdurmayalim.
    console.warn('Kafka topic ensure warning:', error.message);
  } finally {
    await admin.disconnect();
  }
}

async function ensureSchema() {
  await pgClient.query(`
    create table if not exists events (
      event_id text primary key,
      customer_id text not null,
      event_type text not null,
      ts timestamptz not null,
      payload jsonb not null,
      source text not null
    )
  `);
  await pgClient.query(`
    create index if not exists idx_events_customer_type_ts
      on events (customer_id, event_type, ts desc)
  `);
  await pgClient.query(`
    create index if not exists idx_events_customer_ts
      on events (customer_id, ts desc)
  `);
  await pgClient.query(`
    create index if not exists idx_events_ts_brin
      on events using brin (ts)
  `);

  await pgClient.query(`
    create table if not exists customer_profiles (
      customer_id text primary key,
      segment text,
      attributes jsonb not null default '{}'::jsonb,
      updated_at timestamptz not null default now()
    )
  `);

  await pgClient.query(`
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
    )
  `);

  await pgClient.query(
    `alter table action_log add column if not exists journey_id text`
  );
  await pgClient.query(
    `alter table action_log add column if not exists journey_version int`
  );
  await pgClient.query(
    `alter table action_log add column if not exists journey_node_id text`
  );

  await pgClient.query(`
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
    )
  `);

  await pgClient.query(`
    create index if not exists idx_external_call_log_lookup
      on external_call_log (journey_id, journey_version, customer_id, created_at desc)
  `);

  await pgClient.query(`
    create table if not exists journeys (
      journey_id text not null,
      version int not null,
      name text not null,
      status text not null default 'published',
      folder_path text not null default 'Workspace',
      graph_json jsonb not null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      primary key (journey_id, version)
    )
  `);
  await pgClient.query(
    `alter table journeys add column if not exists folder_path text not null default 'Workspace'`
  );

  await pgClient.query(`
    create table if not exists journey_folders (
      folder_path text primary key,
      created_at timestamptz not null default now()
    )
  `);

  await pgClient.query(`
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
    )
  `);

  await pgClient.query(`
    drop index if exists uq_journey_instance_active
  `);
  await pgClient.query(`
    create unique index uq_journey_instance_active
      on journey_instances (journey_id, customer_id)
      where state in ('waiting', 'waiting_manual', 'active', 'processing')
  `);

  await pgClient.query(`
    create index if not exists idx_journey_instances_due
      on journey_instances (state, due_at)
  `);
  await pgClient.query(`
    create index if not exists idx_journey_instances_journey_state_due
      on journey_instances (journey_id, state, due_at)
  `);
  await pgClient.query(`
    create index if not exists idx_journey_instances_customer_updated
      on journey_instances (customer_id, updated_at desc)
  `);
  await pgClient.query(`
    create index if not exists idx_journey_instances_updated
      on journey_instances (updated_at desc)
  `);

  await pgClient.query(`
    create table if not exists event_dlq (
      id text primary key,
      event_id text,
      customer_id text,
      event_type text,
      source_topic text not null,
      error_message text not null,
      raw_payload jsonb,
      created_at timestamptz not null default now()
    )
  `);
  await pgClient.query(`
    create index if not exists idx_event_dlq_created
      on event_dlq (created_at desc)
  `);

  await pgClient.query(`
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
    )
  `);
  await pgClient.query(`
    create index if not exists idx_instance_transitions_lookup
      on journey_instance_transitions (instance_id, created_at desc)
  `);
  await pgClient.query(`
    create index if not exists idx_instance_transitions_customer
      on journey_instance_transitions (customer_id, created_at desc)
  `);

  await pgClient.query(`
    create table if not exists consumed_events (
      consumer_group text not null,
      event_id text not null,
      consumed_at timestamptz not null default now(),
      primary key (consumer_group, event_id)
    )
  `);

  await pgClient.query(`
    create table if not exists edge_capacity_usage (
      journey_id text not null,
      journey_version int not null,
      edge_id text not null,
      window_type text not null,
      window_start timestamptz not null,
      used_count int not null default 0,
      updated_at timestamptz not null default now(),
      primary key (journey_id, journey_version, edge_id, window_type, window_start)
    )
  `);
  await pgClient.query(`
    create index if not exists idx_edge_capacity_lookup
      on edge_capacity_usage (journey_id, journey_version, edge_id, window_type, window_start desc)
  `);

  await pgClient.query(`
    create table if not exists journey_release_controls (
      journey_id text not null,
      journey_version int not null,
      rollout_percent int not null default 100,
      release_paused boolean not null default false,
      updated_at timestamptz not null default now(),
      primary key (journey_id, journey_version)
    )
  `);

  await pgClient.query(`
    create table if not exists runtime_controls (
      key text primary key,
      value_json jsonb not null default '{}'::jsonb,
      updated_at timestamptz not null default now()
    )
  `);

  await pgClient.query(`
    create table if not exists catalogue_event_types (
      event_type text primary key,
      description text not null default '',
      owner text not null default '',
      version int not null default 1,
      required_fields jsonb not null default '[]'::jsonb,
      schema_json jsonb not null default '{}'::jsonb,
      sample_payload jsonb not null default '{}'::jsonb,
      is_active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);
  await pgClient.query(
    `alter table catalogue_event_types add column if not exists owner text not null default ''`
  );
  await pgClient.query(
    `alter table catalogue_event_types add column if not exists version int not null default 1`
  );
  await pgClient.query(
    `alter table catalogue_event_types add column if not exists required_fields jsonb not null default '[]'::jsonb`
  );
  await pgClient.query(
    `alter table catalogue_event_types add column if not exists schema_json jsonb not null default '{}'::jsonb`
  );

  await pgClient.query(`
    create table if not exists catalogue_segments (
      segment_key text primary key,
      display_name text not null default '',
      rule_expression text not null default '',
      description text not null default '',
      is_active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);
  await pgClient.query(`
    create table if not exists catalogue_templates (
      template_id text primary key,
      channel text not null,
      subject text not null default '',
      body text not null default '',
      variables jsonb not null default '[]'::jsonb,
      is_active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);
  await pgClient.query(`
    create table if not exists catalogue_endpoints (
      endpoint_id text primary key,
      method text not null,
      url text not null,
      headers jsonb not null default '{}'::jsonb,
      timeout_ms int not null default 5000,
      description text not null default '',
      is_active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);

  await pgClient.query(
    `insert into runtime_controls (key, value_json)
     values ('global_pause', '{"enabled": false}'::jsonb)
     on conflict (key) do nothing`
  );

  await pgClient.query(
    `insert into journeys (journey_id, version, name, status, graph_json)
     values ($1, $2, $3, $4, $5)
     on conflict (journey_id, version) do nothing`,
    [
      'cart_abandonment_v1',
      1,
      'Cart Abandonment 30m',
      'published',
      {
        nodes: [
          { id: 'trigger', type: 'trigger', event_type: 'cart_add' },
          { id: 'wait', type: 'wait', duration_minutes: 30 },
          { id: 'condition', type: 'condition', check: 'purchase_exists' },
          { id: 'action', type: 'action', channel: 'email' }
        ]
      }
    ]
  );
}

async function bootstrap() {
  await Promise.all([producer.connect(), pgClient.connect(), redis.connect()]);
  await ensureKafkaTopics();

  await ensureSchema();
}

function normalizeEvent(input) {
  const parsed = EventInputSchema.parse(input);
  return {
    event_id: parsed.event_id || crypto.randomUUID(),
    customer_id: parsed.customer_id,
    event_type: parsed.event_type,
    ts: parsed.ts || new Date().toISOString(),
    payload: parsed.payload,
    source: parsed.source
  };
}

const app = express();
app.use(cors());
app.use(express.json());

app.get('/health', async (_req, res) => {
  try {
    await pgClient.query('select 1');
    res.status(200).json({ status: 'ok' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/dashboard/kpi', async (_req, res) => {
  try {
    const [eventsResult, activeResult, completedResult, actionResult] = await Promise.all([
      pgClient.query(
        `select
           count(*) filter (where ts >= now() - interval '1 hour')::int as events_1h,
           count(*) filter (where ts >= now() - interval '24 hour')::int as events_24h
         from events`
      ),
      pgClient.query(
        `select state, count(*)::int as cnt
         from journey_instances
         where state in ('waiting', 'waiting_manual', 'processing')
         group by state`
      ),
      pgClient.query(
        `select count(*)::int as completed_24h
         from journey_instances
         where state = 'completed'
           and completed_at >= now() - interval '24 hour'`
      ),
      pgClient.query(
        `select
           count(*) filter (where status = 'failed')::int as failed_24h,
           count(*) filter (where status <> 'failed')::int as success_24h
         from action_log
         where created_at >= now() - interval '24 hour'`
      )
    ]);

    const activeByState = { waiting: 0, waiting_manual: 0, processing: 0 };
    for (const row of activeResult.rows) {
      activeByState[row.state] = Number(row.cnt || 0);
    }
    const activeTotal = activeByState.waiting + activeByState.waiting_manual + activeByState.processing;

    const success24h = Number(actionResult.rows[0]?.success_24h || 0);
    const failed24h = Number(actionResult.rows[0]?.failed_24h || 0);
    const actionTotal24h = success24h + failed24h;
    const successRate24h = actionTotal24h > 0 ? Number(((success24h / actionTotal24h) * 100).toFixed(2)) : 0;
    const failureRate24h = actionTotal24h > 0 ? Number(((failed24h / actionTotal24h) * 100).toFixed(2)) : 0;

    res.status(200).json({
      status: 'ok',
      item: {
        events_1h: Number(eventsResult.rows[0]?.events_1h || 0),
        events_24h: Number(eventsResult.rows[0]?.events_24h || 0),
        active_instances: {
          ...activeByState,
          total: activeTotal
        },
        completed_journeys_24h: Number(completedResult.rows[0]?.completed_24h || 0),
        actions_24h: {
          success: success24h,
          failed: failed24h,
          total: actionTotal24h,
          success_rate_pct: successRate24h,
          failure_rate_pct: failureRate24h
        }
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/dashboard/journey-performance', async (_req, res) => {
  try {
    const result = await pgClient.query(
      `with trigger_stats as (
         select journey_id, journey_version, count(*)::int as triggered_24h
         from journey_instances
         where started_at >= now() - interval '24 hour'
         group by journey_id, journey_version
       ),
       completion_stats as (
         select journey_id, journey_version, count(*)::int as completed_24h
         from journey_instances
         where completed_at >= now() - interval '24 hour'
         group by journey_id, journey_version
       ),
       fail_stats as (
         select journey_id, journey_version, count(*)::int as failed_actions_24h
         from action_log
         where created_at >= now() - interval '24 hour'
           and status = 'failed'
         group by journey_id, journey_version
       ),
       wait_stats as (
         select
           journey_id,
           journey_version,
           avg(extract(epoch from (processing_at - wait_at)))::numeric(12,2) as avg_wait_seconds
         from (
           select
             journey_id,
             journey_version,
             instance_id,
             min(created_at) filter (where to_state in ('waiting', 'waiting_manual')) as wait_at,
             min(created_at) filter (where to_state = 'processing') as processing_at
           from journey_instance_transitions
           where created_at >= now() - interval '24 hour'
           group by journey_id, journey_version, instance_id
         ) t
         where wait_at is not null and processing_at is not null and processing_at >= wait_at
         group by journey_id, journey_version
       )
       select
         j.journey_id,
         j.version as journey_version,
         j.name,
         coalesce(ts.triggered_24h, 0)::int as triggered_24h,
         coalesce(cs.completed_24h, 0)::int as completed_24h,
         coalesce(fs.failed_actions_24h, 0)::int as failed_actions_24h,
         coalesce(ws.avg_wait_seconds, 0)::numeric(12,2) as avg_wait_seconds
       from journeys j
       left join trigger_stats ts
         on ts.journey_id = j.journey_id and ts.journey_version = j.version
       left join completion_stats cs
         on cs.journey_id = j.journey_id and cs.journey_version = j.version
       left join fail_stats fs
         on fs.journey_id = j.journey_id and fs.journey_version = j.version
       left join wait_stats ws
         on ws.journey_id = j.journey_id and ws.journey_version = j.version
       where j.status in ('published', 'draft')
       order by triggered_24h desc, failed_actions_24h desc, j.updated_at desc
       limit 200`
    );

    const rows = result.rows.map((row) => ({
      journey_id: row.journey_id,
      journey_version: Number(row.journey_version),
      name: row.name || row.journey_id,
      triggered_24h: Number(row.triggered_24h || 0),
      completed_24h: Number(row.completed_24h || 0),
      failed_actions_24h: Number(row.failed_actions_24h || 0),
      avg_wait_seconds: Number(row.avg_wait_seconds || 0)
    }));

    const topByEvents = [...rows]
      .sort((a, b) => b.triggered_24h - a.triggered_24h)
      .slice(0, 5);
    const topByFailures = [...rows]
      .sort((a, b) => b.failed_actions_24h - a.failed_actions_24h)
      .slice(0, 5);

    res.status(200).json({
      status: 'ok',
      item: {
        journeys: rows,
        top_by_events: topByEvents,
        top_by_failures: topByFailures
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/dashboard/cache-health', async (_req, res) => {
  try {
    const summaryResult = await pgClient.query(
      `with latest as (
         select
           j.dataset_key,
           max(r.started_at) as last_run_at,
           max(r.started_at) filter (where r.status = 'success') as last_success_at
         from cache_loader_jobs j
         left join cache_loader_runs r on r.job_id = j.id
         group by j.dataset_key
       ),
       latest_success as (
         select distinct on (j.dataset_key)
           j.dataset_key,
           r.row_count,
           r.started_at
         from cache_loader_jobs j
         join cache_loader_runs r on r.job_id = j.id
         where r.status = 'success'
         order by j.dataset_key, r.started_at desc
       )
       select
         (select count(*)::int from latest) as datasets_total,
         (select count(*)::int from latest where last_success_at >= date_trunc('day', now())) as datasets_loaded_today,
         (select count(*)::int from cache_loader_runs where status = 'success' and started_at >= date_trunc('day', now())) as success_runs_today,
         coalesce((select sum(row_count)::int from latest_success), 0) as total_rows_last_success`
    );

    const itemsResult = await pgClient.query(
      `with latest_success as (
         select distinct on (j.dataset_key)
           j.dataset_key,
           r.started_at as last_success_at,
           r.row_count as last_success_row_count
         from cache_loader_jobs j
         join cache_loader_runs r on r.job_id = j.id
         where r.status = 'success'
         order by j.dataset_key, r.started_at desc
       ),
       latest_run as (
         select distinct on (j.dataset_key)
           j.dataset_key,
           r.started_at as last_run_at,
           r.status as last_run_status,
           r.error_text as last_error
         from cache_loader_jobs j
         left join cache_loader_runs r on r.job_id = j.id
         order by j.dataset_key, r.started_at desc nulls last
       )
       select
         k.dataset_key,
         lr.last_run_at,
         lr.last_run_status,
         ls.last_success_at,
         coalesce(ls.last_success_row_count, 0)::int as last_success_row_count,
         coalesce(ls.last_success_at >= date_trunc('day', now()), false) as loaded_today,
         lr.last_error
       from (select distinct dataset_key from cache_loader_jobs) k
       left join latest_run lr on lr.dataset_key = k.dataset_key
       left join latest_success ls on ls.dataset_key = k.dataset_key
       order by k.dataset_key asc`
    );

    res.status(200).json({
      status: 'ok',
      item: {
        summary: summaryResult.rows[0] || {
          datasets_total: 0,
          datasets_loaded_today: 0,
          success_runs_today: 0,
          total_rows_last_success: 0
        },
        items: itemsResult.rows || []
      }
    });
  } catch (error) {
    if (error?.code === '42P01') {
      res.status(200).json({
        status: 'ok',
        item: {
          summary: {
            datasets_total: 0,
            datasets_loaded_today: 0,
            success_runs_today: 0,
            total_rows_last_success: 0
          },
          items: []
        }
      });
      return;
    }
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/management/global-pause', async (_req, res) => {
  try {
    const result = await pgClient.query(
      `select value_json
       from runtime_controls
       where key = 'global_pause'
       limit 1`
    );
    const enabled = Boolean(result.rows[0]?.value_json?.enabled);
    res.status(200).json({ status: 'ok', item: { enabled } });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.put('/management/global-pause', async (req, res) => {
  try {
    const payload = GlobalPauseUpdateSchema.parse(req.body);
    await pgClient.query(
      `insert into runtime_controls (key, value_json, updated_at)
       values ('global_pause', $1::jsonb, now())
       on conflict (key)
       do update set value_json = excluded.value_json, updated_at = now()`,
      [JSON.stringify({ enabled: payload.enabled })]
    );
    res.status(200).json({ status: 'ok', item: { enabled: payload.enabled } });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/management/release-controls', async (_req, res) => {
  try {
    const result = await pgClient.query(
      `with latest as (
         select j.*,
                row_number() over (partition by j.journey_id order by j.version desc) as rn
         from journeys j
       )
       select
         l.journey_id,
         l.version as journey_version,
         l.name,
         l.status,
         coalesce(rc.rollout_percent, 100)::int as rollout_percent,
         coalesce(rc.release_paused, false) as release_paused,
         coalesce(rc.updated_at, l.updated_at) as updated_at
       from latest l
       left join journey_release_controls rc
         on rc.journey_id = l.journey_id and rc.journey_version = l.version
       where l.rn = 1
       order by l.journey_id asc`
    );
    res.status(200).json({ status: 'ok', items: result.rows });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.put('/management/release-controls/:journeyId', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const payload = ReleaseControlUpdateSchema.parse(req.body);
    const version = await resolveJourneyVersion(journeyId, payload.version);

    const existing = await pgClient.query(
      `select rollout_percent, release_paused
       from journey_release_controls
       where journey_id = $1 and journey_version = $2
       limit 1`,
      [journeyId, version]
    );
    const rolloutPercent = payload.rollout_percent ?? Number(existing.rows[0]?.rollout_percent ?? 100);
    const releasePaused = payload.release_paused ?? Boolean(existing.rows[0]?.release_paused ?? false);

    await pgClient.query(
      `insert into journey_release_controls
        (journey_id, journey_version, rollout_percent, release_paused, updated_at)
       values ($1, $2, $3, $4, now())
       on conflict (journey_id, journey_version)
       do update set
         rollout_percent = excluded.rollout_percent,
         release_paused = excluded.release_paused,
         updated_at = now()`,
      [journeyId, version, rolloutPercent, releasePaused]
    );

    res.status(200).json({
      status: 'ok',
      item: {
        journey_id: journeyId,
        journey_version: version,
        rollout_percent: rolloutPercent,
        release_paused: releasePaused
      }
    });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/catalogues/summary', async (_req, res) => {
  try {
    const [events, templates, endpoints, segments] = await Promise.all([
      pgClient.query(
        `select count(*)::int as total,
                count(*) filter (where is_active = true)::int as active_total
         from catalogue_event_types`
      ),
      pgClient.query(
        `select count(*)::int as total,
                count(*) filter (where is_active = true)::int as active_total
         from catalogue_templates`
      ),
      pgClient.query(
        `select count(*)::int as total,
                count(*) filter (where is_active = true)::int as active_total
         from catalogue_endpoints`
      ),
      pgClient.query(
        `select count(*)::int as total,
                count(*) filter (where is_active = true)::int as active_total
         from catalogue_segments`
      )
    ]);

    res.status(200).json({
      status: 'ok',
      item: {
        event_types: events.rows[0] || { total: 0, active_total: 0 },
        templates: templates.rows[0] || { total: 0, active_total: 0 },
        endpoints: endpoints.rows[0] || { total: 0, active_total: 0 },
        segments: segments.rows[0] || { total: 0, active_total: 0 }
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/catalogues/cache-datasets', async (_req, res) => {
  try {
    const result = await pgClient.query(
      `select distinct dataset_key
       from cache_loader_jobs
       where enabled = true
       order by dataset_key asc`
    );
    const items = await Promise.all(
      result.rows.map(async (row) => {
        const datasetKey = row.dataset_key;
        const meta = await redis.hGetAll(`cache:dataset:${datasetKey}:meta`);
        let columns = [];
        if (meta?.columns_json) {
          columns = parseJsonSafe(meta.columns_json, []);
        }
        if (!Array.isArray(columns)) {
          columns = [];
        }
        let columnTypes = {};
        if (meta?.column_types_json) {
          columnTypes = parseJsonSafe(meta.column_types_json, {});
        }
        if (!columnTypes || typeof columnTypes !== 'object' || Array.isArray(columnTypes)) {
          columnTypes = {};
        }
        return {
          dataset_key: datasetKey,
          key_column: meta?.key_column || null,
          row_count: Number(meta?.row_count || 0),
          version: meta?.version || null,
          updated_at: meta?.updated_at || null,
          columns,
          column_types: columnTypes
        };
      })
    );
    res.status(200).json({
      status: 'ok',
      items
    });
  } catch (error) {
    if (error?.code === '42P01') {
      res.status(200).json({ status: 'ok', items: [] });
      return;
    }
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/catalogues/event-types', async (_req, res) => {
  try {
    const q = _req.query.q?.toString();
    const limit = parseLimit(_req.query.limit, 100, 500);
    const offset = parseOffset(_req.query.offset);
    const values = [];
    let where = '';

    if (q) {
      values.push(`%${q}%`);
      where = `where event_type ilike $1 or description ilike $1`;
    }

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from catalogue_event_types
       ${where}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select event_type, description, owner, version, required_fields, schema_json,
              sample_payload, is_active, created_at, updated_at
       from catalogue_event_types
       ${where}
       order by updated_at desc, event_type asc
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/catalogues/event-types', async (req, res) => {
  try {
    const payload = CatalogueEventTypeSchema.parse(req.body);
    await pgClient.query(
      `insert into catalogue_event_types
        (event_type, description, owner, version, required_fields, schema_json, sample_payload, is_active, updated_at)
       values ($1, $2, $3, $4, $5, $6, $7, $8, now())
       on conflict (event_type)
       do update set
         description = excluded.description,
         owner = excluded.owner,
         version = excluded.version,
         required_fields = excluded.required_fields,
         schema_json = excluded.schema_json,
         sample_payload = excluded.sample_payload,
         is_active = excluded.is_active,
         updated_at = now()`,
      [
        payload.event_type,
        payload.description,
        payload.owner,
        payload.version,
        payload.required_fields,
        payload.schema_json,
        payload.sample_payload,
        payload.is_active
      ]
    );
    res.status(200).json({ status: 'ok', event_type: payload.event_type });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.put('/catalogues/event-types/:eventType', async (req, res) => {
  try {
    const pathKey = req.params.eventType;
    const payload = CatalogueEventTypeSchema.parse({ ...req.body, event_type: pathKey });
    const result = await pgClient.query(
      `update catalogue_event_types
       set description = $2,
           owner = $3,
           version = $4,
           required_fields = $5,
           schema_json = $6,
           sample_payload = $7,
           is_active = $8,
           updated_at = now()
       where event_type = $1`,
      [
        payload.event_type,
        payload.description,
        payload.owner,
        payload.version,
        payload.required_fields,
        payload.schema_json,
        payload.sample_payload,
        payload.is_active
      ]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'event_type not found' });
      return;
    }
    res.status(200).json({ status: 'ok', event_type: payload.event_type });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/catalogues/segments', async (_req, res) => {
  try {
    const q = _req.query.q?.toString();
    const limit = parseLimit(_req.query.limit, 100, 500);
    const offset = parseOffset(_req.query.offset);
    const values = [];
    let where = '';

    if (q) {
      values.push(`%${q}%`);
      where = `where segment_key ilike $1 or display_name ilike $1 or description ilike $1`;
    }

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from catalogue_segments
       ${where}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select segment_key, display_name, rule_expression, description, is_active, created_at, updated_at
       from catalogue_segments
       ${where}
       order by updated_at desc, segment_key asc
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/catalogues/segments', async (req, res) => {
  try {
    const payload = CatalogueSegmentSchema.parse(req.body);
    await pgClient.query(
      `insert into catalogue_segments
        (segment_key, display_name, rule_expression, description, is_active, updated_at)
       values ($1, $2, $3, $4, $5, now())
       on conflict (segment_key)
       do update set
         display_name = excluded.display_name,
         rule_expression = excluded.rule_expression,
         description = excluded.description,
         is_active = excluded.is_active,
         updated_at = now()`,
      [
        payload.segment_key,
        payload.display_name,
        payload.rule_expression,
        payload.description,
        payload.is_active
      ]
    );
    res.status(200).json({ status: 'ok', segment_key: payload.segment_key });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.put('/catalogues/segments/:segmentKey', async (req, res) => {
  try {
    const payload = CatalogueSegmentSchema.parse({ ...req.body, segment_key: req.params.segmentKey });
    const result = await pgClient.query(
      `update catalogue_segments
       set display_name = $2,
           rule_expression = $3,
           description = $4,
           is_active = $5,
           updated_at = now()
       where segment_key = $1`,
      [
        payload.segment_key,
        payload.display_name,
        payload.rule_expression,
        payload.description,
        payload.is_active
      ]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'segment not found' });
      return;
    }
    res.status(200).json({ status: 'ok', segment_key: payload.segment_key });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.delete('/catalogues/segments/:segmentKey', async (req, res) => {
  try {
    const result = await pgClient.query(
      `delete from catalogue_segments
       where segment_key = $1`,
      [req.params.segmentKey]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'segment not found' });
      return;
    }
    res.status(200).json({ status: 'ok' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.delete('/catalogues/event-types/:eventType', async (req, res) => {
  try {
    const result = await pgClient.query(
      `delete from catalogue_event_types
       where event_type = $1`,
      [req.params.eventType]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'event_type not found' });
      return;
    }
    res.status(200).json({ status: 'ok' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/catalogues/templates', async (_req, res) => {
  try {
    const q = _req.query.q?.toString();
    const limit = parseLimit(_req.query.limit, 100, 500);
    const offset = parseOffset(_req.query.offset);
    const values = [];
    let where = '';

    if (q) {
      values.push(`%${q}%`);
      where = `where template_id ilike $1 or body ilike $1 or subject ilike $1`;
    }

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from catalogue_templates
       ${where}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select template_id, channel, subject, body, variables, is_active, created_at, updated_at
       from catalogue_templates
       ${where}
       order by updated_at desc, template_id asc
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/catalogues/templates', async (req, res) => {
  try {
    const payload = CatalogueTemplateSchema.parse(req.body);
    await pgClient.query(
      `insert into catalogue_templates
        (template_id, channel, subject, body, variables, is_active, updated_at)
       values ($1, $2, $3, $4, $5, $6, now())
       on conflict (template_id)
       do update set
         channel = excluded.channel,
         subject = excluded.subject,
         body = excluded.body,
         variables = excluded.variables,
         is_active = excluded.is_active,
         updated_at = now()`,
      [
        payload.template_id,
        payload.channel,
        payload.subject,
        payload.body,
        payload.variables,
        payload.is_active
      ]
    );
    res.status(200).json({ status: 'ok', template_id: payload.template_id });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.put('/catalogues/templates/:templateId', async (req, res) => {
  try {
    const payload = CatalogueTemplateSchema.parse({ ...req.body, template_id: req.params.templateId });
    const result = await pgClient.query(
      `update catalogue_templates
       set channel = $2,
           subject = $3,
           body = $4,
           variables = $5,
           is_active = $6,
           updated_at = now()
       where template_id = $1`,
      [
        payload.template_id,
        payload.channel,
        payload.subject,
        payload.body,
        payload.variables,
        payload.is_active
      ]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'template not found' });
      return;
    }
    res.status(200).json({ status: 'ok', template_id: payload.template_id });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.delete('/catalogues/templates/:templateId', async (req, res) => {
  try {
    const result = await pgClient.query(
      `delete from catalogue_templates
       where template_id = $1`,
      [req.params.templateId]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'template not found' });
      return;
    }
    res.status(200).json({ status: 'ok' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/catalogues/endpoints', async (_req, res) => {
  try {
    const q = _req.query.q?.toString();
    const limit = parseLimit(_req.query.limit, 100, 500);
    const offset = parseOffset(_req.query.offset);
    const values = [];
    let where = '';

    if (q) {
      values.push(`%${q}%`);
      where = `where endpoint_id ilike $1 or url ilike $1 or description ilike $1`;
    }

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from catalogue_endpoints
       ${where}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select endpoint_id, method, url, headers, timeout_ms, description, is_active, created_at, updated_at
       from catalogue_endpoints
       ${where}
       order by updated_at desc, endpoint_id asc
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/catalogues/endpoints', async (req, res) => {
  try {
    const payload = CatalogueEndpointSchema.parse(req.body);
    await pgClient.query(
      `insert into catalogue_endpoints
        (endpoint_id, method, url, headers, timeout_ms, description, is_active, updated_at)
       values ($1, $2, $3, $4, $5, $6, $7, now())
       on conflict (endpoint_id)
       do update set
         method = excluded.method,
         url = excluded.url,
         headers = excluded.headers,
         timeout_ms = excluded.timeout_ms,
         description = excluded.description,
         is_active = excluded.is_active,
         updated_at = now()`,
      [
        payload.endpoint_id,
        payload.method,
        payload.url,
        payload.headers,
        payload.timeout_ms,
        payload.description,
        payload.is_active
      ]
    );
    res.status(200).json({ status: 'ok', endpoint_id: payload.endpoint_id });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.put('/catalogues/endpoints/:endpointId', async (req, res) => {
  try {
    const payload = CatalogueEndpointSchema.parse({ ...req.body, endpoint_id: req.params.endpointId });
    const result = await pgClient.query(
      `update catalogue_endpoints
       set method = $2,
           url = $3,
           headers = $4,
           timeout_ms = $5,
           description = $6,
           is_active = $7,
           updated_at = now()
       where endpoint_id = $1`,
      [
        payload.endpoint_id,
        payload.method,
        payload.url,
        payload.headers,
        payload.timeout_ms,
        payload.description,
        payload.is_active
      ]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'endpoint not found' });
      return;
    }
    res.status(200).json({ status: 'ok', endpoint_id: payload.endpoint_id });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.delete('/catalogues/endpoints/:endpointId', async (req, res) => {
  try {
    const result = await pgClient.query(
      `delete from catalogue_endpoints
       where endpoint_id = $1`,
      [req.params.endpointId]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'endpoint not found' });
      return;
    }
    res.status(200).json({ status: 'ok' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/ingest', async (req, res) => {
  try {
    const event = normalizeEvent(req.body);

    const insertResult = await pgClient.query(
      `insert into events (event_id, customer_id, event_type, ts, payload, source)
       values ($1, $2, $3, $4, $5, $6)
       on conflict (event_id) do nothing
       returning event_id`,
      [event.event_id, event.customer_id, event.event_type, event.ts, event.payload, event.source]
    );
    const isDuplicate = insertResult.rowCount === 0;

    if (isDuplicate) {
      res.status(200).json({
        status: 'duplicate_ignored',
        event_id: event.event_id
      });
      return;
    }

    await redis.hSet(`customer:${event.customer_id}`, {
      last_event_type: event.event_type,
      last_event_ts: event.ts
    });

    await producer.send({
      topic: 'event.raw',
      messages: [{ key: event.customer_id, value: JSON.stringify(event) }]
    });

    res.status(202).json({ status: 'accepted', event_id: event.event_id });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.post('/journeys', async (req, res) => {
  try {
    const input = JourneyCreateSchema.parse(req.body);
    await pgClient.query(
      `insert into journey_folders (folder_path)
       values ($1)
       on conflict (folder_path) do nothing`,
      [input.folder_path]
    );
    await pgClient.query(
      `insert into journeys (journey_id, version, name, status, folder_path, graph_json)
       values ($1, $2, $3, $4, $5, $6)
       on conflict (journey_id, version)
       do update set
         name = excluded.name,
         status = excluded.status,
         folder_path = excluded.folder_path,
         graph_json = excluded.graph_json,
         updated_at = now()`,
      [input.journey_id, input.version, input.name, input.status, input.folder_path, input.graph_json]
    );

    res.status(200).json({ status: 'ok', journey_id: input.journey_id, version: input.version });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/journeys', async (_req, res) => {
  try {
    const req = _req;
    const limit = parseLimit(req.query.limit, 100, 500);
    const offset = parseOffset(req.query.offset);
    const status = req.query.status?.toString();
    const journeyId = req.query.journey_id?.toString();

    const sortByRaw = req.query.sort_by?.toString() || 'journey_id';
    const sortOrderRaw = (req.query.sort_order?.toString() || 'asc').toLowerCase();
    const sortByMap = {
      journey_id: 'journey_id',
      version: 'version',
      created_at: 'created_at',
      updated_at: 'updated_at',
      status: 'status'
    };
    const sortBy = sortByMap[sortByRaw] || 'journey_id';
    const sortOrder = sortOrderRaw === 'desc' ? 'desc' : 'asc';

    const conditions = [];
    const values = [];

    if (status && ['draft', 'published', 'archived'].includes(status)) {
      values.push(status);
      conditions.push(`status = $${values.length}`);
    }

    if (journeyId) {
      values.push(`%${journeyId}%`);
      conditions.push(`journey_id ilike $${values.length}`);
    }

    const whereClause = conditions.length > 0 ? `where ${conditions.join(' and ')}` : '';

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from journeys
       ${whereClause}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select journey_id, version, name, status, folder_path, graph_json, created_at, updated_at
       from journeys
       ${whereClause}
       order by ${sortBy} ${sortOrder}, version desc
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: {
        limit,
        offset,
        total,
        has_more: offset + result.rows.length < total
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/journey-folders', async (_req, res) => {
  try {
    const result = await pgClient.query(
      `select folder_path, created_at
       from journey_folders
       order by folder_path asc`
    );
    res.status(200).json({ status: 'ok', items: result.rows });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/journey-folders', async (req, res) => {
  try {
    const folderPath = String(req.body?.folder_path || '').trim();
    if (!folderPath) {
      res.status(400).json({ status: 'error', message: 'folder_path is required' });
      return;
    }

    await pgClient.query(
      `insert into journey_folders (folder_path)
       values ($1)
       on conflict (folder_path) do nothing`,
      [folderPath]
    );

    res.status(201).json({ status: 'ok', folder_path: folderPath });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.delete('/journeys/:journeyId', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const versionRaw = req.query.version?.toString();

    if (versionRaw) {
      const version = Number(versionRaw);
      if (!Number.isInteger(version) || version <= 0) {
        res.status(400).json({ status: 'error', message: 'invalid version' });
        return;
      }

      const deletedJourney = await pgClient.query(
        `delete from journeys
         where journey_id = $1 and version = $2`,
        [journeyId, version]
      );

      await pgClient.query(
        `delete from journey_instances
         where journey_id = $1 and journey_version = $2`,
        [journeyId, version]
      );

      if (deletedJourney.rowCount === 0) {
        res.status(404).json({ status: 'error', message: 'journey version not found' });
        return;
      }

      res.status(200).json({
        status: 'ok',
        deleted: { journey_id: journeyId, version }
      });
      return;
    }

    const deletedJourneys = await pgClient.query(
      `delete from journeys
       where journey_id = $1`,
      [journeyId]
    );

    await pgClient.query(
      `delete from journey_instances
       where journey_id = $1`,
      [journeyId]
    );

    if (deletedJourneys.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'journey not found' });
      return;
    }

    res.status(200).json({
      status: 'ok',
      deleted: { journey_id: journeyId, versions_count: deletedJourneys.rowCount }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/journeys/:journeyId/clone-version', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const payload = JourneyCloneSchema.parse(req.body);

    const source = await pgClient.query(
      `select journey_id, version, name, folder_path, graph_json
       from journeys
       where journey_id = $1 and version = $2
       limit 1`,
      [journeyId, payload.source_version]
    );

    if (source.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'source journey version not found' });
      return;
    }

    let targetVersion = payload.target_version;
    if (!targetVersion) {
      const maxVersion = await pgClient.query(
        `select coalesce(max(version), 0)::int as max_version
         from journeys
         where journey_id = $1`,
        [journeyId]
      );
      targetVersion = (maxVersion.rows[0]?.max_version || 0) + 1;
    }

    await pgClient.query(
      `insert into journey_folders (folder_path)
       values ($1)
       on conflict (folder_path) do nothing`,
      [source.rows[0].folder_path || 'Workspace']
    );

    const targetName = payload.name || source.rows[0].name;
    await pgClient.query(
      `insert into journeys (journey_id, version, name, status, folder_path, graph_json)
       values ($1, $2, $3, $4, $5, $6)`,
      [
        journeyId,
        targetVersion,
        targetName,
        payload.status,
        source.rows[0].folder_path || 'Workspace',
        source.rows[0].graph_json
      ]
    );

    res.status(201).json({
      status: 'ok',
      item: {
        journey_id: journeyId,
        source_version: payload.source_version,
        version: targetVersion,
        name: targetName,
        status: payload.status
      }
    });
  } catch (error) {
    if (error?.code === '23505') {
      res.status(409).json({ status: 'error', message: 'target version already exists' });
      return;
    }
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.patch('/journeys/:journeyId/move-folder', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const payload = JourneyMoveFolderSchema.parse(req.body);
    const folderPath = payload.target_folder_path.trim();

    await pgClient.query(
      `insert into journey_folders (folder_path)
       values ($1)
       on conflict (folder_path) do nothing`,
      [folderPath]
    );

    const result = payload.version
      ? await pgClient.query(
          `update journeys
           set folder_path = $1, updated_at = now()
           where journey_id = $2 and version = $3`,
          [folderPath, journeyId, payload.version]
        )
      : await pgClient.query(
          `update journeys
           set folder_path = $1, updated_at = now()
           where journey_id = $2`,
          [folderPath, journeyId]
        );

    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'journey not found' });
      return;
    }

    res.status(200).json({
      status: 'ok',
      moved: {
        journey_id: journeyId,
        version: payload.version || null,
        target_folder_path: folderPath,
        count: result.rowCount
      }
    });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.post('/customers', async (req, res) => {
  try {
    const payload = CustomerCreateSchema.parse(req.body);
    await pgClient.query(
      `insert into customer_profiles (customer_id, segment, attributes, updated_at)
       values ($1, $2, $3, now())`,
      [payload.customer_id, payload.segment ?? null, payload.attributes]
    );

    res.status(201).json({ status: 'ok', customer_id: payload.customer_id });
  } catch (error) {
    if (error?.code === '23505') {
      res.status(409).json({
        status: 'error',
        message: 'customer already exists (insert-only endpoint)'
      });
      return;
    }
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.put('/customers/:customerId/profile', async (req, res) => {
  try {
    const customerId = req.params.customerId;
    const payload = CustomerProfileSchema.parse(req.body);

    await pgClient.query(
      `insert into customer_profiles (customer_id, segment, attributes, updated_at)
       values ($1, $2, $3, now())
       on conflict (customer_id)
       do update set
         segment = excluded.segment,
         attributes = excluded.attributes,
         updated_at = now()`,
      [customerId, payload.segment ?? null, payload.attributes]
    );

    res.status(200).json({ status: 'ok', customer_id: customerId });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/customers/:customerId/profile', async (req, res) => {
  try {
    const customerId = req.params.customerId;
    const result = await pgClient.query(
      `select customer_id, segment, attributes, updated_at
       from customer_profiles
       where customer_id = $1`,
      [customerId]
    );

    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'profile not found' });
      return;
    }

    res.status(200).json({ status: 'ok', item: result.rows[0] });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/customers', async (req, res) => {
  try {
    const segment = req.query.segment?.toString();
    const q = req.query.q?.toString();
    const limit = parseLimit(req.query.limit, 100, 500);
    const offset = parseOffset(req.query.offset);
    const sortOrder = (req.query.sort_order?.toString() || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';

    const conditions = [];
    const values = [];
    if (segment) {
      values.push(segment);
      conditions.push(`segment = $${values.length}`);
    }
    if (q) {
      values.push(`%${q}%`);
      conditions.push(`customer_id ilike $${values.length}`);
    }
    const whereClause = conditions.length > 0 ? `where ${conditions.join(' and ')}` : '';

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from customer_profiles
       ${whereClause}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select customer_id, segment, attributes, updated_at
       from customer_profiles
       ${whereClause}
       order by updated_at ${sortOrder}
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: {
        limit,
        offset,
        total,
        has_more: offset + result.rows.length < total
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/actions', async (req, res) => {
  try {
    const customerId = req.query.customer_id?.toString();
    const journeyId = req.query.journey_id?.toString();
    const channel = req.query.channel?.toString();
    const actionStatus = req.query.status?.toString();
    const limit = parseLimit(req.query.limit, 100, 500);
    const offset = parseOffset(req.query.offset);
    const sortOrder = (req.query.sort_order?.toString() || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';

    const conditions = [];
    const values = [];
    if (customerId) {
      values.push(customerId);
      conditions.push(`customer_id = $${values.length}`);
    }
    if (journeyId) {
      values.push(journeyId);
      conditions.push(`journey_id = $${values.length}`);
    }
    if (channel) {
      values.push(channel);
      conditions.push(`channel = $${values.length}`);
    }
    if (actionStatus) {
      values.push(actionStatus);
      conditions.push(`status = $${values.length}`);
    }

    const whereClause = conditions.length > 0 ? `where ${conditions.join(' and ')}` : '';

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from action_log
       ${whereClause}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select action_id, event_id, customer_id, journey_id, journey_version, journey_node_id,
              channel, status, message, created_at
       from action_log
       ${whereClause}
       order by created_at ${sortOrder}
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: {
        limit,
        offset,
        total,
        has_more: offset + result.rows.length < total
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/external-calls', async (req, res) => {
  try {
    const customerId = req.query.customer_id?.toString();
    const journeyId = req.query.journey_id?.toString();
    const instanceId = req.query.instance_id?.toString();
    const resultType = req.query.result_type?.toString();
    const limit = parseLimit(req.query.limit, 100, 500);
    const offset = parseOffset(req.query.offset);
    const sortOrder = (req.query.sort_order?.toString() || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';

    const conditions = [];
    const values = [];

    if (customerId) {
      values.push(customerId);
      conditions.push(`customer_id = $${values.length}`);
    }
    if (journeyId) {
      values.push(journeyId);
      conditions.push(`journey_id = $${values.length}`);
    }
    if (instanceId) {
      values.push(instanceId);
      conditions.push(`instance_id = $${values.length}`);
    }
    if (resultType) {
      values.push(resultType);
      conditions.push(`result_type = $${values.length}`);
    }

    const whereClause = conditions.length > 0 ? `where ${conditions.join(' and ')}` : '';

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from external_call_log
       ${whereClause}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select id, instance_id, journey_id, journey_version, customer_id, journey_node_id,
              method, url, status_code, result_type, reason, response_json, created_at
       from external_call_log
       ${whereClause}
       order by created_at ${sortOrder}
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: {
        limit,
        offset,
        total,
        has_more: offset + result.rows.length < total
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/journey-instances', async (req, res) => {
  try {
    const customerId = req.query.customer_id?.toString();
    const limit = Math.min(Number(req.query.limit || 50), 200);

    const result = customerId
      ? await pgClient.query(
          `select instance_id, journey_id, journey_version, customer_id, state, current_node,
                  started_at, due_at, completed_at, last_event_id, context_json, updated_at
           from journey_instances
           where customer_id = $1
           order by updated_at desc
           limit $2`,
          [customerId, limit]
        )
      : await pgClient.query(
          `select instance_id, journey_id, journey_version, customer_id, state, current_node,
                  started_at, due_at, completed_at, last_event_id, context_json, updated_at
           from journey_instances
           order by updated_at desc
           limit $1`,
          [limit]
        );

    res.status(200).json({ status: 'ok', items: result.rows });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/journey-instance-transitions', async (req, res) => {
  try {
    const instanceId = req.query.instance_id?.toString();
    const customerId = req.query.customer_id?.toString();
    const journeyId = req.query.journey_id?.toString();
    const limit = parseLimit(req.query.limit, 100, 500);
    const offset = parseOffset(req.query.offset);

    const conditions = [];
    const values = [];
    if (instanceId) {
      values.push(instanceId);
      conditions.push(`instance_id = $${values.length}`);
    }
    if (customerId) {
      values.push(customerId);
      conditions.push(`customer_id = $${values.length}`);
    }
    if (journeyId) {
      values.push(journeyId);
      conditions.push(`journey_id = $${values.length}`);
    }
    const whereClause = conditions.length > 0 ? `where ${conditions.join(' and ')}` : '';

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from journey_instance_transitions
       ${whereClause}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select id, instance_id, journey_id, journey_version, customer_id, from_state, to_state,
              from_node, to_node, reason, event_id, metadata_json, created_at
       from journey_instance_transitions
       ${whereClause}
       order by created_at desc
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: {
        limit,
        offset,
        total,
        has_more: offset + result.rows.length < total
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/manual-wait-queue', async (req, res) => {
  try {
    const journeyId = req.query.journey_id?.toString();
    if (!journeyId) {
      res.status(400).json({ status: 'error', message: 'journey_id is required' });
      return;
    }
    const version = await resolveJourneyVersion(journeyId, req.query.version?.toString());
    const waitNodeId = await resolveWaitNodeId(journeyId, version, req.query.wait_node_id?.toString());
    const limit = parseLimit(req.query.limit, 200, 1000);
    const offset = parseOffset(req.query.offset);

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from journey_instances
       where journey_id = $1
         and journey_version = $2
         and state = 'waiting_manual'
         and current_node = $3`,
      [journeyId, version, waitNodeId]
    );
    const total = countResult.rows[0]?.total || 0;

    const result = await pgClient.query(
      `select instance_id, customer_id, state, current_node, started_at, due_at, updated_at, context_json
       from journey_instances
       where journey_id = $1
         and journey_version = $2
         and state = 'waiting_manual'
         and current_node = $3
       order by started_at asc
       limit $4
       offset $5`,
      [journeyId, version, waitNodeId, limit, offset]
    );

    res.status(200).json({
      status: 'ok',
      journey_id: journeyId,
      journey_version: version,
      wait_node_id: waitNodeId,
      items: result.rows,
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.post('/manual-wait-release', async (req, res) => {
  try {
    const payload = ManualWaitReleaseSchema.parse(req.body);
    const version = await resolveJourneyVersion(payload.journey_id, payload.version);
    const waitNodeId = await resolveWaitNodeId(payload.journey_id, version, payload.wait_node_id);

    await pgClient.query('begin');
    const released = await pgClient.query(
      `with picked as (
         select instance_id
         from journey_instances
         where journey_id = $1
           and journey_version = $2
           and state = 'waiting_manual'
           and current_node = $3
         order by started_at asc
         limit $4
         for update skip locked
       )
       update journey_instances ji
       set state = 'waiting',
           due_at = now(),
           updated_at = now(),
           context_json = ji.context_json || $5::jsonb
       from picked
       where ji.instance_id = picked.instance_id
       returning ji.instance_id, ji.customer_id, ji.last_event_id`,
      [
        payload.journey_id,
        version,
        waitNodeId,
        payload.count,
        JSON.stringify({
          manual_release_at: new Date().toISOString(),
          manual_release_by: payload.released_by || 'ui',
          manual_release_count: payload.count
        })
      ]
    );

    for (const item of released.rows) {
      await pgClient.query(
        `insert into journey_instance_transitions
          (id, instance_id, journey_id, journey_version, customer_id, from_state, to_state, from_node, to_node, reason, event_id, metadata_json)
         values
          ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
        [
          crypto.randomUUID(),
          item.instance_id,
          payload.journey_id,
          version,
          item.customer_id,
          'waiting_manual',
          'waiting',
          waitNodeId,
          waitNodeId,
          'manual_release',
          item.last_event_id || null,
          {
            released_by: payload.released_by || 'ui',
            requested_count: payload.count
          }
        ]
      );
    }
    await pgClient.query('commit');

    res.status(200).json({
      status: 'ok',
      journey_id: payload.journey_id,
      journey_version: version,
      wait_node_id: waitNodeId,
      released_count: released.rowCount
    });
  } catch (error) {
    await pgClient.query('rollback').catch(() => {});
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/dlq-events', async (req, res) => {
  try {
    const limit = parseLimit(req.query.limit, 100, 500);
    const offset = parseOffset(req.query.offset);
    const result = await pgClient.query(
      `select id, event_id, customer_id, event_type, source_topic, error_message, raw_payload, created_at
       from event_dlq
       order by created_at desc
       limit $1
       offset $2`,
      [limit, offset]
    );
    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: { limit, offset, has_more: result.rows.length === limit }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/edge-capacity-usage', async (req, res) => {
  try {
    const journeyId = req.query.journey_id?.toString();
    const edgeId = req.query.edge_id?.toString();
    const windowType = req.query.window_type?.toString();
    const limit = parseLimit(req.query.limit, 100, 500);
    const offset = parseOffset(req.query.offset);

    const conditions = [];
    const values = [];
    if (journeyId) {
      values.push(journeyId);
      conditions.push(`journey_id = $${values.length}`);
    }
    if (edgeId) {
      values.push(edgeId);
      conditions.push(`edge_id = $${values.length}`);
    }
    if (windowType) {
      values.push(windowType);
      conditions.push(`window_type = $${values.length}`);
    }
    const whereClause = conditions.length > 0 ? `where ${conditions.join(' and ')}` : '';

    const countResult = await pgClient.query(
      `select count(*)::int as total
       from edge_capacity_usage
       ${whereClause}`,
      values
    );
    const total = countResult.rows[0]?.total || 0;

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select journey_id, journey_version, edge_id, window_type, window_start, used_count, updated_at
       from edge_capacity_usage
       ${whereClause}
       order by updated_at desc
       limit $${listValues.length - 1}
       offset $${listValues.length}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows,
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

bootstrap()
  .then(() => {
    app.listen(port, () => {
      console.log(`Eventra API listening on port ${port}`);
    });
  })
  .catch((error) => {
    console.error('Bootstrap failed:', error);
    process.exit(1);
  });
