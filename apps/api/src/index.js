import crypto from 'node:crypto';
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { Kafka, Partitioners } from 'kafkajs';
import { createClient } from 'redis';
import { z } from 'zod';
import { explainJourney, generateJourneyDraft, reviseJourney } from './lib/llm.js';
import { describeDatabaseTarget, resolvePrimaryDatabaseConfig } from '../../../lib/database-config.mjs';
import { createDatabaseClient } from '../../../lib/database-runtime.mjs';

dotenv.config({ path: '../../.env' });

const port = Number(process.env.PORT || 3001);
const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const kafkaClientId = process.env.KAFKA_CLIENT_ID || 'eventra-api';
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const primaryDb = resolvePrimaryDatabaseConfig();
const javaPluginRunnerUrl = String(
  process.env.JAVA_PLUGIN_RUNNER_URL || 'http://127.0.0.1:3030'
).replace(/\/+$/, '');

console.info(
  `[bootstrap] api primary database vendor=${primaryDb.vendor} target=${describeDatabaseTarget(
    primaryDb
  )}`
);

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

const JourneyApprovalRequestSchema = z.object({
  version: z.number().int().positive().optional(),
  requested_by: z.string().min(1).default('ui'),
  note: z.string().default('')
});

const JourneyApprovalDecisionSchema = z.object({
  version: z.number().int().positive().optional(),
  reviewed_by: z.string().min(1).default('ui'),
  note: z.string().default('')
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

const AiGenerateJourneySchema = z.object({
  prompt: z.string().min(10),
  context: z
    .object({
      existingJourney: z.record(z.any()).nullable().optional()
    })
    .optional()
});

const AiExplainJourneySchema = z.object({
  journey: z.object({
    journey_id: z.string().min(1).optional(),
    version: z.number().int().positive().optional(),
    name: z.string().min(1).optional(),
    status: z.string().optional(),
    folder_path: z.string().optional(),
    graph_json: z.object({
      nodes: z.array(z.record(z.any())).default([]),
      edges: z.array(z.record(z.any())).default([])
    })
  })
});

const AiReviseJourneySchema = z.object({
  instruction: z.string().min(5),
  journey: z.object({
    journey_id: z.string().min(1).optional(),
    version: z.number().int().positive().optional(),
    name: z.string().min(1).optional(),
    status: z.string().optional(),
    folder_path: z.string().optional(),
    graph_json: z.object({
      nodes: z.array(z.record(z.any())).default([]),
      edges: z.array(z.record(z.any())).default([])
    })
  })
});

const kafka = new Kafka({ clientId: kafkaClientId, brokers: kafkaBrokers });
const admin = kafka.admin();
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});
let pgClient = null;
const redis = createClient({ url: redisUrl });

function createPgClient() {
  return createDatabaseClient(primaryDb);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(label, fn, attempts = 20, delayMs = 3000) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const message = error?.message || String(error);
      console.warn(`[bootstrap] ${label} failed (${attempt}/${attempts}): ${message}`);
      if (attempt < attempts) {
        await sleep(delayMs);
      }
    }
  }
  throw lastError;
}

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

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 5000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const body = await response.json().catch(() => ({}));
    return { response, body };
  } finally {
    clearTimeout(timeout);
  }
}

const isOracle = primaryDb.vendor === 'oracle';

function oneRowClause() {
  return isOracle ? 'fetch first 1 rows only' : 'limit 1';
}

function paginationClause(limitIndex, offsetIndex) {
  return isOracle
    ? `offset $${offsetIndex} rows fetch next $${limitIndex} rows only`
    : `limit $${limitIndex} offset $${offsetIndex}`;
}

function caseInsensitiveLike(columnRef, bindIndex) {
  return `lower(${columnRef}) like lower($${bindIndex})`;
}

function normalizeJsonField(value, fallback) {
  if (value === null || value === undefined) {
    return fallback;
  }
  if (typeof value === 'string') {
    return parseJsonSafe(value, fallback);
  }
  return value;
}

function normalizeRowJsonFields(row, fields) {
  const next = { ...row };
  for (const field of fields) {
    if (!(field in next)) {
      continue;
    }
    const fallback = Array.isArray(next[field]) ? [] : {};
    next[field] = normalizeJsonField(next[field], fallback);
  }
  return next;
}

function dateHoursAgo(hours) {
  return new Date(Date.now() - hours * 60 * 60 * 1000);
}

function dateDaysAgo(days) {
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000);
}

function oracleFetchRowsClause(limit) {
  return `fetch first ${Math.max(1, Number(limit) || 1)} rows only`;
}

async function insertEventIfAbsent(event) {
  const existing = await pgClient.query(
    `select event_id
     from events
     where event_id = $1
     ${oneRowClause()}`,
    [event.event_id]
  );
  if (existing.rowCount > 0) {
    return false;
  }
  await pgClient.query(
    `insert into events (event_id, customer_id, event_type, ts, payload, source)
     values ($1, $2, $3, $4, $5, $6)`,
    [event.event_id, event.customer_id, event.event_type, event.ts, event.payload, event.source]
  );
  return true;
}

async function ensureFolderExists(folderPath) {
  await pgClient.query(
    `merge into journey_folders jf
     using (select $1 as folder_path from dual) src
     on (jf.folder_path = src.folder_path)
     when not matched then
       insert (folder_path, created_at) values (src.folder_path, current_timestamp)`,
    [folderPath]
  );
}

async function upsertJourneyApproval({
  journeyId,
  version,
  state,
  requestedBy = null,
  requestedNote = '',
  reviewedBy = null,
  reviewedNote = ''
}) {
  const existing = await pgClient.query(
    `select journey_id
     from journey_approvals
     where journey_id = $1 and journey_version = $2
     ${oneRowClause()}`,
    [journeyId, version]
  );

  if (existing.rowCount === 0) {
    await pgClient.query(
      `insert into journey_approvals
        (journey_id, journey_version, state, requested_by, requested_note, requested_at,
         reviewed_by, reviewed_note, reviewed_at, created_at, updated_at)
       values
        ($1, $2, $3, $4, $5, $6, $7, $8, $9, current_timestamp, current_timestamp)`,
      [
        journeyId,
        version,
        state,
        state === 'pending' ? requestedBy : null,
        state === 'pending' ? requestedNote : '',
        state === 'pending' ? new Date() : null,
        state === 'approved' || state === 'rejected' ? reviewedBy : null,
        state === 'approved' || state === 'rejected' ? reviewedNote : '',
        state === 'approved' || state === 'rejected' ? new Date() : null
      ]
    );
    return;
  }

  if (state === 'pending') {
    await pgClient.query(
      `update journey_approvals
       set state = 'pending',
           requested_by = $3,
           requested_note = $4,
           requested_at = current_timestamp,
           reviewed_by = null,
           reviewed_note = '',
           reviewed_at = null,
           updated_at = current_timestamp
       where journey_id = $1 and journey_version = $2`,
      [journeyId, version, requestedBy, requestedNote]
    );
    return;
  }

  await pgClient.query(
    `update journey_approvals
     set state = $3,
         reviewed_by = $4,
         reviewed_note = $5,
         reviewed_at = current_timestamp,
         updated_at = current_timestamp
     where journey_id = $1 and journey_version = $2`,
    [journeyId, version, state, reviewedBy, reviewedNote]
  );
}

async function upsertJourney(input) {
  await pgClient.query(
    `merge into journeys j
     using (
       select
         $1 as journey_id,
         $2 as version,
         $3 as name,
         $4 as status,
         $5 as folder_path,
         $6 as graph_json
       from dual
     ) src
     on (j.journey_id = src.journey_id and j.version = src.version)
     when matched then update set
       j.name = src.name,
       j.status = src.status,
       j.folder_path = src.folder_path,
       j.graph_json = src.graph_json,
       j.updated_at = current_timestamp
     when not matched then
       insert (journey_id, version, name, status, folder_path, graph_json, created_at, updated_at)
       values (
         src.journey_id,
         src.version,
         src.name,
         src.status,
         src.folder_path,
         src.graph_json,
         current_timestamp,
         current_timestamp
       )`,
    [
      input.journey_id,
      input.version,
      input.name,
      input.status,
      input.folder_path,
      JSON.stringify(input.graph_json)
    ]
  );
}

async function upsertGlobalPause(enabled) {
  const payload = JSON.stringify({ enabled });
  await pgClient.query(
    `merge into runtime_controls rc
     using (select 'global_pause' as key, $1 as value_json from dual) src
     on (rc.key = src.key)
     when matched then update set rc.value_json = src.value_json, rc.updated_at = current_timestamp
     when not matched then
       insert (key, value_json, updated_at) values (src.key, src.value_json, current_timestamp)`,
    [payload]
  );
}

async function upsertCatalogueRecord(kind, payload) {
  if (kind === 'event_type') {
    await pgClient.query(
      `merge into catalogue_event_types t
       using (
         select $1 event_type, $2 description, $3 owner, $4 version, $5 required_fields,
                $6 schema_json, $7 sample_payload, $8 is_active
         from dual
       ) src
       on (t.event_type = src.event_type)
       when matched then update set
         t.description = src.description,
         t.owner = src.owner,
         t.version = src.version,
         t.required_fields = src.required_fields,
         t.schema_json = src.schema_json,
         t.sample_payload = src.sample_payload,
         t.is_active = src.is_active,
         t.updated_at = current_timestamp
       when not matched then
         insert (
           event_type, description, owner, version, required_fields, schema_json, sample_payload,
           is_active, created_at, updated_at
         ) values (
           src.event_type, src.description, src.owner, src.version, src.required_fields, src.schema_json,
           src.sample_payload, src.is_active, current_timestamp, current_timestamp
         )`,
      [
        payload.event_type,
        payload.description,
        payload.owner,
        payload.version,
        JSON.stringify(payload.required_fields),
        JSON.stringify(payload.schema_json),
        JSON.stringify(payload.sample_payload),
        payload.is_active ? 1 : 0
      ]
    );
    return;
  }

  if (kind === 'segment') {
    await pgClient.query(
      `merge into catalogue_segments t
       using (
         select $1 segment_key, $2 display_name, $3 rule_expression, $4 description, $5 is_active
         from dual
       ) src
       on (t.segment_key = src.segment_key)
       when matched then update set
         t.display_name = src.display_name,
         t.rule_expression = src.rule_expression,
         t.description = src.description,
         t.is_active = src.is_active,
         t.updated_at = current_timestamp
       when not matched then
         insert (
           segment_key, display_name, rule_expression, description, is_active, created_at, updated_at
         ) values (
           src.segment_key, src.display_name, src.rule_expression, src.description,
           src.is_active, current_timestamp, current_timestamp
         )`,
      [
        payload.segment_key,
        payload.display_name,
        payload.rule_expression,
        payload.description,
        payload.is_active ? 1 : 0
      ]
    );
    return;
  }

  if (kind === 'template') {
    await pgClient.query(
      `merge into catalogue_templates t
       using (
         select $1 template_id, $2 channel, $3 subject, $4 body, $5 variables, $6 is_active from dual
       ) src
       on (t.template_id = src.template_id)
       when matched then update set
         t.channel = src.channel,
         t.subject = src.subject,
         t.body = src.body,
         t.variables = src.variables,
         t.is_active = src.is_active,
         t.updated_at = current_timestamp
       when not matched then
         insert (
           template_id, channel, subject, body, variables, is_active, created_at, updated_at
         ) values (
           src.template_id, src.channel, src.subject, src.body, src.variables, src.is_active,
           current_timestamp, current_timestamp
         )`,
      [
        payload.template_id,
        payload.channel,
        payload.subject,
        payload.body,
        JSON.stringify(payload.variables),
        payload.is_active ? 1 : 0
      ]
    );
    return;
  }

  if (kind === 'endpoint') {
    await pgClient.query(
      `merge into catalogue_endpoints t
       using (
         select $1 endpoint_id, $2 method, $3 url, $4 headers, $5 timeout_ms, $6 description, $7 is_active
         from dual
       ) src
       on (t.endpoint_id = src.endpoint_id)
       when matched then update set
         t.method = src.method,
         t.url = src.url,
         t.headers = src.headers,
         t.timeout_ms = src.timeout_ms,
         t.description = src.description,
         t.is_active = src.is_active,
         t.updated_at = current_timestamp
       when not matched then
         insert (
           endpoint_id, method, url, headers, timeout_ms, description, is_active, created_at, updated_at
         ) values (
           src.endpoint_id, src.method, src.url, src.headers, src.timeout_ms, src.description,
           src.is_active, current_timestamp, current_timestamp
         )`,
      [
        payload.endpoint_id,
        payload.method,
        payload.url,
        JSON.stringify(payload.headers),
        payload.timeout_ms,
        payload.description,
        payload.is_active ? 1 : 0
      ]
    );
    return;
  }
}

async function upsertCustomerProfile(customerId, payload) {
  await pgClient.query(
    `merge into customer_profiles cp
     using (
       select $1 customer_id, $2 segment, $3 attributes from dual
     ) src
     on (cp.customer_id = src.customer_id)
     when matched then update set
       cp.segment = src.segment,
       cp.attributes = src.attributes,
       cp.updated_at = current_timestamp
     when not matched then
       insert (customer_id, segment, attributes, updated_at)
       values (src.customer_id, src.segment, src.attributes, current_timestamp)`,
    [customerId, payload.segment ?? null, JSON.stringify(payload.attributes)]
  );
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
     ${oneRowClause()}`,
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
     ${oneRowClause()}`,
    [journeyId, version]
  );
  if (journey.rowCount === 0) {
    throw new Error('journey version not found');
  }

  const graph = normalizeJsonField(journey.rows[0]?.graph_json, {});
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
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

async function loadAiCatalogContext() {
  const [eventTypes, templates, segments, endpoints, cacheDatasets] = await Promise.all([
    pgClient.query(
      `select event_type
       from catalogue_event_types
       where is_active = true
       order by event_type asc
       fetch first 100 rows only`
    ),
    pgClient.query(
      `select template_id, channel
       from catalogue_templates
       where is_active = true
       order by template_id asc
       fetch first 100 rows only`
    ),
    pgClient.query(
      `select segment_key
       from catalogue_segments
       where is_active = true
       order by segment_key asc
       fetch first 100 rows only`
    ),
    pgClient.query(
      `select endpoint_id, method
       from catalogue_endpoints
       where is_active = true
       order by endpoint_id asc
       fetch first 100 rows only`
    ),
    pgClient.query(
      `select distinct dataset_key
       from cache_loader_jobs
       order by dataset_key asc
       fetch first 100 rows only`
    )
  ]);

  return {
    eventTypes: eventTypes.rows.map((row) => row.event_type).filter(Boolean),
    templates: templates.rows.map((row) => ({
      template_id: row.template_id,
      channel: row.channel
    })),
    segments: segments.rows.map((row) => row.segment_key).filter(Boolean),
    endpoints: endpoints.rows.map((row) => ({
      endpoint_id: row.endpoint_id,
      method: row.method
    })),
    cacheDatasets: cacheDatasets.rows.map((row) => ({
      dataset_key: row.dataset_key
    }))
  };
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
  return;
}

async function bootstrap() {
  await withRetry('database connect', async () => {
    const client = createPgClient();
    try {
      await client.connect();
      pgClient = client;
    } catch (error) {
      await client.end().catch(() => {});
      throw error;
    }
  });
  await withRetry('redis connect', () => redis.connect());
  await withRetry('kafka producer connect', () => producer.connect());
  await withRetry('kafka topic ensure', () => ensureKafkaTopics());

  console.warn(
    '[bootstrap] api automatic schema bootstrap is skipped for Oracle. Apply Oracle DDL manually before serving traffic.'
  );
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

app.get('/plugin-actions', async (_req, res) => {
  try {
    const { response, body } = await fetchJsonWithTimeout(
      `${javaPluginRunnerUrl}/plugins/actions`,
      {},
      4500
    );
    if (!response.ok) {
      throw new Error(body.message || `plugin runner status ${response.status}`);
    }
    res.status(200).json({
      status: 'ok',
      items: Array.isArray(body.items) ? body.items : []
    });
  } catch (error) {
    res.status(502).json({
      status: 'error',
      message: `plugin runner unavailable: ${error.message}`
    });
  }
});

app.post('/ai/generate-journey', async (req, res) => {
  try {
    const input = AiGenerateJourneySchema.parse(req.body || {});
    const catalogs = await loadAiCatalogContext();
    const result = await generateJourneyDraft({
      prompt: input.prompt,
      catalogs,
      existingJourney: input.context?.existingJourney || null
    });

    res.status(200).json({
      status: 'ok',
      provider: result.provider,
      validation: result.validation,
      draft: {
        ...result.draft,
        version: 1
      }
    });
  } catch (error) {
    if (error instanceof z.ZodError) {
      res.status(400).json({
        status: 'error',
        message: error.issues?.[0]?.message || 'invalid request'
      });
      return;
    }
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/ai/explain-journey', async (req, res) => {
  try {
    const input = AiExplainJourneySchema.parse(req.body || {});
    const catalogs = await loadAiCatalogContext();
    const result = await explainJourney({
      journey: input.journey,
      catalogs
    });

    res.status(200).json({
      status: 'ok',
      provider: result.provider,
      explanation: result.explanation
    });
  } catch (error) {
    if (error instanceof z.ZodError) {
      res.status(400).json({
        status: 'error',
        message: error.issues?.[0]?.message || 'invalid request'
      });
      return;
    }
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/ai/revise-journey', async (req, res) => {
  try {
    const input = AiReviseJourneySchema.parse(req.body || {});
    const catalogs = await loadAiCatalogContext();
    const result = await reviseJourney({
      instruction: input.instruction,
      journey: input.journey,
      catalogs
    });

    res.status(200).json({
      status: 'ok',
      provider: result.provider,
      validation: result.validation,
      draft: {
        ...result.draft,
        version: 1
      }
    });
  } catch (error) {
    if (error instanceof z.ZodError) {
      res.status(400).json({
        status: 'error',
        message: error.issues?.[0]?.message || 'invalid request'
      });
      return;
    }
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/dashboard/kpi', async (_req, res) => {
  try {
    const oneHourAgo = dateHoursAgo(1);
    const oneDayAgo = dateDaysAgo(1);
    const [events1h, events24h, activeResult, completedResult, failedResult, successResult] =
      await Promise.all([
        pgClient.query(`select count(*) as total from events where ts >= $1`, [oneHourAgo]),
        pgClient.query(`select count(*) as total from events where ts >= $1`, [oneDayAgo]),
        pgClient.query(
          `select state, count(*) as cnt
           from journey_instances
           where state in ('waiting', 'waiting_manual', 'processing')
           group by state`
        ),
        pgClient.query(
          `select count(*) as completed_24h
           from journey_instances
           where state = 'completed'
             and completed_at >= $1`,
          [oneDayAgo]
        ),
        pgClient.query(
          `select count(*) as failed_24h
           from action_log
           where created_at >= $1
             and status = 'failed'`,
          [oneDayAgo]
        ),
        pgClient.query(
          `select count(*) as success_24h
           from action_log
           where created_at >= $1
             and status <> 'failed'`,
          [oneDayAgo]
        )
      ]);

    const activeByState = { waiting: 0, waiting_manual: 0, processing: 0 };
    for (const row of activeResult.rows) {
      activeByState[row.state] = Number(row.cnt || 0);
    }
    const activeTotal = activeByState.waiting + activeByState.waiting_manual + activeByState.processing;

    const success24h = Number(successResult.rows[0]?.success_24h || 0);
    const failed24h = Number(failedResult.rows[0]?.failed_24h || 0);
    const actionTotal24h = success24h + failed24h;
    const successRate24h = actionTotal24h > 0 ? Number(((success24h / actionTotal24h) * 100).toFixed(2)) : 0;
    const failureRate24h = actionTotal24h > 0 ? Number(((failed24h / actionTotal24h) * 100).toFixed(2)) : 0;

    res.status(200).json({
      status: 'ok',
      item: {
        events_1h: Number(events1h.rows[0]?.total || 0),
        events_24h: Number(events24h.rows[0]?.total || 0),
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
    const oneDayAgo = dateDaysAgo(1);
    const [journeysResult, triggeredResult, completedResult, failedResult, transitionsResult] =
      await Promise.all([
        pgClient.query(
          `select journey_id, version as journey_version, name, updated_at
           from journeys
           where status in ('published', 'draft')
           order by updated_at desc
           ${isOracle ? 'fetch first 200 rows only' : 'limit 200'}`
        ),
        pgClient.query(
          `select journey_id, journey_version, count(*) as triggered_24h
           from journey_instances
           where started_at >= $1
           group by journey_id, journey_version`,
          [oneDayAgo]
        ),
        pgClient.query(
          `select journey_id, journey_version, count(*) as completed_24h
           from journey_instances
           where completed_at >= $1
           group by journey_id, journey_version`,
          [oneDayAgo]
        ),
        pgClient.query(
          `select journey_id, journey_version, count(*) as failed_actions_24h
           from action_log
           where created_at >= $1
             and status = 'failed'
           group by journey_id, journey_version`,
          [oneDayAgo]
        ),
        pgClient.query(
          `select journey_id, journey_version, instance_id, to_state, created_at
           from journey_instance_transitions
           where created_at >= $1
             and to_state in ('waiting', 'waiting_manual', 'processing')`,
          [oneDayAgo]
        )
      ]);

    const triggeredMap = new Map(
      triggeredResult.rows.map((row) => [
        `${row.journey_id}::${row.journey_version}`,
        Number(row.triggered_24h || 0)
      ])
    );
    const completedMap = new Map(
      completedResult.rows.map((row) => [
        `${row.journey_id}::${row.journey_version}`,
        Number(row.completed_24h || 0)
      ])
    );
    const failedMap = new Map(
      failedResult.rows.map((row) => [
        `${row.journey_id}::${row.journey_version}`,
        Number(row.failed_actions_24h || 0)
      ])
    );

    const waitStatsByJourney = new Map();
    for (const row of transitionsResult.rows) {
      const key = `${row.journey_id}::${row.journey_version}::${row.instance_id}`;
      const createdAt = new Date(row.created_at).getTime();
      if (Number.isNaN(createdAt)) {
        continue;
      }
      const current =
        waitStatsByJourney.get(key) || { journeyKey: `${row.journey_id}::${row.journey_version}` };
      if ((row.to_state === 'waiting' || row.to_state === 'waiting_manual') && !current.waitAt) {
        current.waitAt = createdAt;
      }
      if (row.to_state === 'processing' && !current.processingAt) {
        current.processingAt = createdAt;
      }
      waitStatsByJourney.set(key, current);
    }

    const avgWaitByJourney = new Map();
    for (const item of waitStatsByJourney.values()) {
      if (!item.waitAt || !item.processingAt || item.processingAt < item.waitAt) {
        continue;
      }
      const deltaSeconds = (item.processingAt - item.waitAt) / 1000;
      const current = avgWaitByJourney.get(item.journeyKey) || { total: 0, count: 0 };
      current.total += deltaSeconds;
      current.count += 1;
      avgWaitByJourney.set(item.journeyKey, current);
    }

    const rows = journeysResult.rows.map((row) => {
      const journeyKey = `${row.journey_id}::${row.journey_version}`;
      const avgWait = avgWaitByJourney.get(journeyKey);
      return {
        journey_id: row.journey_id,
        journey_version: Number(row.journey_version),
        name: row.name || row.journey_id,
        triggered_24h: triggeredMap.get(journeyKey) || 0,
        completed_24h: completedMap.get(journeyKey) || 0,
        failed_actions_24h: failedMap.get(journeyKey) || 0,
        avg_wait_seconds:
          avgWait && avgWait.count > 0 ? Number((avgWait.total / avgWait.count).toFixed(2)) : 0
      };
    });

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
    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    const [jobsResult, runsResult] = await Promise.all([
      pgClient.query(`select id, dataset_key from cache_loader_jobs`),
      pgClient.query(
        `select job_id, started_at, status, row_count, error_text
         from cache_loader_runs
         order by started_at desc`
      )
    ]);

    const jobToDataset = new Map(jobsResult.rows.map((row) => [row.id, row.dataset_key]));
    const datasetMap = new Map();
    let successRunsToday = 0;

    for (const run of runsResult.rows) {
      const datasetKey = jobToDataset.get(run.job_id);
      if (!datasetKey) {
        continue;
      }
      const startedAt = run.started_at ? new Date(run.started_at) : null;
      const dataset =
        datasetMap.get(datasetKey) || {
          dataset_key: datasetKey,
          last_run_at: null,
          last_run_status: null,
          last_success_at: null,
          last_success_row_count: 0,
          loaded_today: false,
          last_error: null
        };

      if (!dataset.last_run_at) {
        dataset.last_run_at = run.started_at || null;
        dataset.last_run_status = run.status || null;
        dataset.last_error = run.error_text || null;
      }

      if (run.status === 'success') {
        if (startedAt && startedAt >= startOfDay) {
          successRunsToday += 1;
        }
        if (!dataset.last_success_at) {
          dataset.last_success_at = run.started_at || null;
          dataset.last_success_row_count = Number(run.row_count || 0);
          dataset.loaded_today = Boolean(startedAt && startedAt >= startOfDay);
        }
      }

      datasetMap.set(datasetKey, dataset);
    }

    for (const row of jobsResult.rows) {
      if (!datasetMap.has(row.dataset_key)) {
        datasetMap.set(row.dataset_key, {
          dataset_key: row.dataset_key,
          last_run_at: null,
          last_run_status: null,
          last_success_at: null,
          last_success_row_count: 0,
          loaded_today: false,
          last_error: null
        });
      }
    }

    const items = [...datasetMap.values()].sort((a, b) => a.dataset_key.localeCompare(b.dataset_key));
    const summary = {
      datasets_total: items.length,
      datasets_loaded_today: items.filter((item) => item.loaded_today).length,
      success_runs_today: successRunsToday,
      total_rows_last_success: items.reduce((sum, item) => sum + Number(item.last_success_row_count || 0), 0)
    };

    res.status(200).json({
      status: 'ok',
      item: {
        summary,
        items
      }
    });
  } catch (error) {
    if (error?.code === '42P01' || error?.errorNum === 942) {
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
       ${oneRowClause()}`
    );
    const valueJson = normalizeJsonField(result.rows[0]?.value_json, {});
    const enabled = Boolean(valueJson?.enabled);
    res.status(200).json({ status: 'ok', item: { enabled } });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.put('/management/global-pause', async (req, res) => {
  try {
    const payload = GlobalPauseUpdateSchema.parse(req.body);
    await upsertGlobalPause(payload.enabled);
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
         coalesce(rc.rollout_percent, 100) as rollout_percent,
         coalesce(rc.release_paused, 0) as release_paused,
         coalesce(rc.updated_at, l.updated_at) as updated_at
       from latest l
       left join journey_release_controls rc
         on rc.journey_id = l.journey_id and rc.journey_version = l.version
       where l.rn = 1
       order by l.journey_id asc`
    );
    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) => ({
        ...row,
        journey_version: Number(row.journey_version || 0),
        rollout_percent: Number(row.rollout_percent || 100),
        release_paused: Boolean(Number(row.release_paused || 0))
      }))
    });
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
       ${oneRowClause()}`,
      [journeyId, version]
    );
    const rolloutPercent = payload.rollout_percent ?? Number(existing.rows[0]?.rollout_percent ?? 100);
    const releasePaused = payload.release_paused ?? Boolean(existing.rows[0]?.release_paused ?? false);

    await pgClient.query(
      `merge into journey_release_controls rc
       using (
         select $1 journey_id, $2 journey_version, $3 rollout_percent, $4 release_paused from dual
       ) src
       on (rc.journey_id = src.journey_id and rc.journey_version = src.journey_version)
       when matched then update set
         rc.rollout_percent = src.rollout_percent,
         rc.release_paused = src.release_paused,
         rc.updated_at = current_timestamp
       when not matched then
         insert (journey_id, journey_version, rollout_percent, release_paused, updated_at)
         values (src.journey_id, src.journey_version, src.rollout_percent, src.release_paused, current_timestamp)`,
      [journeyId, version, rolloutPercent, releasePaused ? 1 : 0]
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
    const summaryQuery = (tableName) =>
      pgClient.query(
        `select
           count(*) as total,
           sum(case when is_active = ${isOracle ? 1 : 'true'} then 1 else 0 end) as active_total
         from ${tableName}`
      );

    const [events, templates, endpoints, segments] = await Promise.all([
      summaryQuery('catalogue_event_types'),
      summaryQuery('catalogue_templates'),
      summaryQuery('catalogue_endpoints'),
      summaryQuery('catalogue_segments')
    ]);

    res.status(200).json({
      status: 'ok',
      item: {
        event_types: {
          total: Number(events.rows[0]?.total || 0),
          active_total: Number(events.rows[0]?.active_total || 0)
        },
        templates: {
          total: Number(templates.rows[0]?.total || 0),
          active_total: Number(templates.rows[0]?.active_total || 0)
        },
        endpoints: {
          total: Number(endpoints.rows[0]?.total || 0),
          active_total: Number(endpoints.rows[0]?.active_total || 0)
        },
        segments: {
          total: Number(segments.rows[0]?.total || 0),
          active_total: Number(segments.rows[0]?.active_total || 0)
        }
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
      where = `where ${caseInsensitiveLike('event_type', 1)} or ${caseInsensitiveLike('description', 1)}`;
    }

    const countResult = await pgClient.query(
      `select count(*) as total
       from catalogue_event_types
       ${where}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select event_type, description, owner, version, required_fields, schema_json,
              sample_payload, is_active, created_at, updated_at
       from catalogue_event_types
       ${where}
       order by updated_at desc, event_type asc
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) =>
        normalizeRowJsonFields(
          {
            ...row,
            version: Number(row.version || 0),
            is_active: Boolean(row.is_active)
          },
          ['required_fields', 'schema_json', 'sample_payload']
        )
      ),
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/catalogues/event-types', async (req, res) => {
  try {
    const payload = CatalogueEventTypeSchema.parse(req.body);
    await upsertCatalogueRecord('event_type', payload);
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
           updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
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
      where = `where ${caseInsensitiveLike('segment_key', 1)} or ${caseInsensitiveLike(
        'display_name',
        1
      )} or ${caseInsensitiveLike('description', 1)}`;
    }

    const countResult = await pgClient.query(
      `select count(*) as total
       from catalogue_segments
       ${where}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select segment_key, display_name, rule_expression, description, is_active, created_at, updated_at
       from catalogue_segments
       ${where}
       order by updated_at desc, segment_key asc
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) => ({ ...row, is_active: Boolean(row.is_active) })),
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/catalogues/segments', async (req, res) => {
  try {
    const payload = CatalogueSegmentSchema.parse(req.body);
    await upsertCatalogueRecord('segment', payload);
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
           updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
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
      where = `where ${caseInsensitiveLike('template_id', 1)} or ${caseInsensitiveLike(
        'body',
        1
      )} or ${caseInsensitiveLike('subject', 1)}`;
    }

    const countResult = await pgClient.query(
      `select count(*) as total
       from catalogue_templates
       ${where}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select template_id, channel, subject, body, variables, is_active, created_at, updated_at
       from catalogue_templates
       ${where}
       order by updated_at desc, template_id asc
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) =>
        normalizeRowJsonFields(
          {
            ...row,
            is_active: Boolean(row.is_active)
          },
          ['variables']
        )
      ),
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/catalogues/templates', async (req, res) => {
  try {
    const payload = CatalogueTemplateSchema.parse(req.body);
    await upsertCatalogueRecord('template', payload);
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
           updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
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
      where = `where ${caseInsensitiveLike('endpoint_id', 1)} or ${caseInsensitiveLike(
        'url',
        1
      )} or ${caseInsensitiveLike('description', 1)}`;
    }

    const countResult = await pgClient.query(
      `select count(*) as total
       from catalogue_endpoints
       ${where}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select endpoint_id, method, url, headers, timeout_ms, description, is_active, created_at, updated_at
       from catalogue_endpoints
       ${where}
       order by updated_at desc, endpoint_id asc
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) =>
        normalizeRowJsonFields(
          {
            ...row,
            timeout_ms: Number(row.timeout_ms || 0),
            is_active: Boolean(row.is_active)
          },
          ['headers']
        )
      ),
      meta: { limit, offset, total, has_more: offset + result.rows.length < total }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.post('/catalogues/endpoints', async (req, res) => {
  try {
    const payload = CatalogueEndpointSchema.parse(req.body);
    await upsertCatalogueRecord('endpoint', payload);
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
           updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
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
    const inserted = await insertEventIfAbsent(event);
    const isDuplicate = !inserted;

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
    if (input.status === 'published') {
      const approval = await pgClient.query(
        `select state
         from journey_approvals
         where journey_id = $1 and journey_version = $2
         ${oneRowClause()}`,
        [input.journey_id, input.version]
      );
      const state = String(approval.rows[0]?.state || '');
      if (state !== 'approved') {
        res.status(403).json({
          status: 'error',
          message: 'publish requires approval: request and approve this version first'
        });
        return;
      }
    }
    await ensureFolderExists(input.folder_path);
    await upsertJourney(input);

    res.status(200).json({ status: 'ok', journey_id: input.journey_id, version: input.version });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/journeys/:journeyId/approval', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const version = await resolveJourneyVersion(journeyId, req.query.version);
    const result = await pgClient.query(
      `select journey_id, journey_version, state, requested_by, requested_note, requested_at,
              reviewed_by, reviewed_note, reviewed_at, updated_at
       from journey_approvals
       where journey_id = $1 and journey_version = $2
       ${oneRowClause()}`,
      [journeyId, version]
    );
    res.status(200).json({
      status: 'ok',
      item:
        result.rows[0] || {
          journey_id: journeyId,
          journey_version: version,
          state: 'none',
          requested_by: null,
          requested_note: '',
          requested_at: null,
          reviewed_by: null,
          reviewed_note: '',
          reviewed_at: null,
          updated_at: null
        }
    });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.post('/journeys/:journeyId/request-approval', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const payload = JourneyApprovalRequestSchema.parse(req.body || {});
    const version = await resolveJourneyVersion(journeyId, payload.version);
    const exists = await pgClient.query(
      `select 1
       from journeys
       where journey_id = $1 and version = $2
       ${oneRowClause()}`,
      [journeyId, version]
    );
    if (exists.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'journey version not found' });
      return;
    }

    await upsertJourneyApproval({
      journeyId,
      version,
      state: 'pending',
      requestedBy: payload.requested_by,
      requestedNote: payload.note || ''
    });

    await pgClient.query(
      `update journeys
       set status = 'draft', updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
       where journey_id = $1 and version = $2`,
      [journeyId, version]
    );

    res.status(200).json({
      status: 'ok',
      item: { journey_id: journeyId, journey_version: version, state: 'pending' }
    });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.post('/journeys/:journeyId/approve', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const payload = JourneyApprovalDecisionSchema.parse(req.body || {});
    const version = await resolveJourneyVersion(journeyId, payload.version);

    await upsertJourneyApproval({
      journeyId,
      version,
      state: 'approved',
      reviewedBy: payload.reviewed_by,
      reviewedNote: payload.note || ''
    });

    await pgClient.query(
      `update journeys
       set status = 'published', updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
       where journey_id = $1 and version = $2`,
      [journeyId, version]
    );

    res.status(200).json({
      status: 'ok',
      item: { journey_id: journeyId, journey_version: version, state: 'approved' }
    });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.post('/journeys/:journeyId/reject', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const payload = JourneyApprovalDecisionSchema.parse(req.body || {});
    const version = await resolveJourneyVersion(journeyId, payload.version);

    await upsertJourneyApproval({
      journeyId,
      version,
      state: 'rejected',
      reviewedBy: payload.reviewed_by,
      reviewedNote: payload.note || ''
    });

    await pgClient.query(
      `update journeys
       set status = 'draft', updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
       where journey_id = $1 and version = $2`,
      [journeyId, version]
    );

    res.status(200).json({
      status: 'ok',
      item: { journey_id: journeyId, journey_version: version, state: 'rejected' }
    });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/journeys/:journeyId/diff', async (req, res) => {
  try {
    const journeyId = req.params.journeyId;
    const fromVersion = Number(req.query.from_version);
    const toVersion = Number(req.query.to_version);
    if (!Number.isInteger(fromVersion) || fromVersion <= 0) {
      res.status(400).json({ status: 'error', message: 'invalid from_version' });
      return;
    }
    if (!Number.isInteger(toVersion) || toVersion <= 0) {
      res.status(400).json({ status: 'error', message: 'invalid to_version' });
      return;
    }
    if (fromVersion === toVersion) {
      res.status(400).json({ status: 'error', message: 'versions must be different' });
      return;
    }

    const versions = await pgClient.query(
      `select version, graph_json
       from journeys
       where journey_id = $1 and version in ($2, $3)`,
      [journeyId, fromVersion, toVersion]
    );
    if (versions.rowCount !== 2) {
      res.status(404).json({ status: 'error', message: 'one or both versions not found' });
      return;
    }

    const byVersion = new Map(versions.rows.map((row) => [Number(row.version), row.graph_json || {}]));
    const fromGraph = byVersion.get(fromVersion) || {};
    const toGraph = byVersion.get(toVersion) || {};
    const fromNodes = Array.isArray(fromGraph.nodes) ? fromGraph.nodes : [];
    const toNodes = Array.isArray(toGraph.nodes) ? toGraph.nodes : [];
    const fromEdges = Array.isArray(fromGraph.edges) ? fromGraph.edges : [];
    const toEdges = Array.isArray(toGraph.edges) ? toGraph.edges : [];

    const fromNodeMap = new Map(fromNodes.map((node) => [String(node.id), node]));
    const toNodeMap = new Map(toNodes.map((node) => [String(node.id), node]));
    const fromEdgeMap = new Map(fromEdges.map((edge) => [String(edge.id), edge]));
    const toEdgeMap = new Map(toEdges.map((edge) => [String(edge.id), edge]));

    const canonicalNode = (node) =>
      JSON.stringify({
        id: node?.id || '',
        type: node?.type || null,
        data: node?.data || {}
      });
    const canonicalEdge = (edge) =>
      JSON.stringify({
        id: edge?.id || '',
        source: edge?.source || '',
        target: edge?.target || '',
        data: edge?.data || {}
      });

    const addedNodeIds = [...toNodeMap.keys()].filter((id) => !fromNodeMap.has(id));
    const removedNodeIds = [...fromNodeMap.keys()].filter((id) => !toNodeMap.has(id));
    const changedNodeIds = [...toNodeMap.keys()].filter(
      (id) => fromNodeMap.has(id) && canonicalNode(fromNodeMap.get(id)) !== canonicalNode(toNodeMap.get(id))
    );

    const addedEdgeIds = [...toEdgeMap.keys()].filter((id) => !fromEdgeMap.has(id));
    const removedEdgeIds = [...fromEdgeMap.keys()].filter((id) => !toEdgeMap.has(id));
    const changedEdgeIds = [...toEdgeMap.keys()].filter(
      (id) => fromEdgeMap.has(id) && canonicalEdge(fromEdgeMap.get(id)) !== canonicalEdge(toEdgeMap.get(id))
    );

    res.status(200).json({
      status: 'ok',
      item: {
        journey_id: journeyId,
        from_version: fromVersion,
        to_version: toVersion,
        summary: {
          from_nodes: fromNodes.length,
          to_nodes: toNodes.length,
          from_edges: fromEdges.length,
          to_edges: toEdges.length,
          added_nodes: addedNodeIds.length,
          removed_nodes: removedNodeIds.length,
          changed_nodes: changedNodeIds.length,
          added_edges: addedEdgeIds.length,
          removed_edges: removedEdgeIds.length,
          changed_edges: changedEdgeIds.length
        },
        detail: {
          added_node_ids: addedNodeIds,
          removed_node_ids: removedNodeIds,
          changed_node_ids: changedNodeIds,
          added_edge_ids: addedEdgeIds,
          removed_edge_ids: removedEdgeIds,
          changed_edge_ids: changedEdgeIds
        }
      }
    });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
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
      conditions.push(`${caseInsensitiveLike('journey_id', values.length)}`);
    }

    const whereClause = conditions.length > 0 ? `where ${conditions.join(' and ')}` : '';

    const countResult = await pgClient.query(
      `select count(*) as total
       from journeys
       ${whereClause}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select journey_id, version, name, status, folder_path, graph_json, created_at, updated_at
       from journeys
       ${whereClause}
       order by ${sortBy} ${sortOrder}, version desc
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) =>
        normalizeRowJsonFields(
          {
            ...row,
            version: Number(row.version || 0)
          },
          ['graph_json']
        )
      ),
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

    await ensureFolderExists(folderPath);

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
       ${oneRowClause()}`,
      [journeyId, payload.source_version]
    );

    if (source.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'source journey version not found' });
      return;
    }

    let targetVersion = payload.target_version;
    if (!targetVersion) {
      const maxVersion = await pgClient.query(
        `select coalesce(max(version), 0) as max_version
         from journeys
         where journey_id = $1`,
        [journeyId]
      );
      targetVersion = (maxVersion.rows[0]?.max_version || 0) + 1;
    }

    await ensureFolderExists(source.rows[0].folder_path || 'Workspace');

    const targetName = payload.name || source.rows[0].name;
    await pgClient.query(
      `insert into journeys (journey_id, version, name, status, folder_path, graph_json${isOracle ? ', created_at, updated_at' : ''})
       values ($1, $2, $3, $4, $5, $6${isOracle ? ', current_timestamp, current_timestamp' : ''})`,
      [
        journeyId,
        targetVersion,
        targetName,
        payload.status,
        source.rows[0].folder_path || 'Workspace',
        isOracle ? JSON.stringify(normalizeJsonField(source.rows[0].graph_json, {})) : source.rows[0].graph_json
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

    await ensureFolderExists(folderPath);

    const result = payload.version
      ? await pgClient.query(
          `update journeys
           set folder_path = $1, updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
           where journey_id = $2 and version = $3`,
          [folderPath, journeyId, payload.version]
        )
      : await pgClient.query(
          `update journeys
           set folder_path = $1, updated_at = ${isOracle ? 'current_timestamp' : 'now()'}
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
       values ($1, $2, $3, ${isOracle ? 'current_timestamp' : 'now()'})`,
      [payload.customer_id, payload.segment ?? null, isOracle ? JSON.stringify(payload.attributes) : payload.attributes]
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

    await upsertCustomerProfile(customerId, payload);

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

    res.status(200).json({
      status: 'ok',
      item: normalizeRowJsonFields(result.rows[0], ['attributes'])
    });
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
      conditions.push(`${caseInsensitiveLike('customer_id', values.length)}`);
    }
    const whereClause = conditions.length > 0 ? `where ${conditions.join(' and ')}` : '';

    const countResult = await pgClient.query(
      `select count(*) as total
       from customer_profiles
       ${whereClause}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select customer_id, segment, attributes, updated_at
       from customer_profiles
       ${whereClause}
       order by updated_at ${sortOrder}
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) => normalizeRowJsonFields(row, ['attributes'])),
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
      `select count(*) as total
       from action_log
       ${whereClause}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select action_id, event_id, customer_id, journey_id, journey_version, journey_node_id,
              channel, status, message, created_at
       from action_log
       ${whereClause}
       order by created_at ${sortOrder}
       ${paginationClause(listValues.length - 1, listValues.length)}`,
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
      `select count(*) as total
       from external_call_log
       ${whereClause}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select id, instance_id, journey_id, journey_version, customer_id, journey_node_id,
              method, url, status_code, result_type, reason, response_json, created_at
       from external_call_log
       ${whereClause}
       order by created_at ${sortOrder}
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) => normalizeRowJsonFields(row, ['response_json'])),
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
    const values = [];
    let whereClause = '';
    if (customerId) {
      values.push(customerId);
      whereClause = `where customer_id = $${values.length}`;
    }
    const listValues = [...values, limit, 0];
    const result = await pgClient.query(
      `select instance_id, journey_id, journey_version, customer_id, state, current_node,
              started_at, due_at, completed_at, last_event_id, context_json, updated_at
       from journey_instances
       ${whereClause}
       order by updated_at desc
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) => normalizeRowJsonFields(row, ['context_json']))
    });
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
      `select count(*) as total
       from journey_instance_transitions
       ${whereClause}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select id, instance_id, journey_id, journey_version, customer_id, from_state, to_state,
              from_node, to_node, reason, event_id, metadata_json, created_at
       from journey_instance_transitions
       ${whereClause}
       order by created_at desc
       ${paginationClause(listValues.length - 1, listValues.length)}`,
      listValues
    );

    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) => normalizeRowJsonFields(row, ['metadata_json'])),
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
      `select count(*) as total
       from journey_instances
       where journey_id = $1
         and journey_version = $2
         and state = 'waiting_manual'
         and current_node = $3`,
      [journeyId, version, waitNodeId]
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const result = await pgClient.query(
      `select instance_id, customer_id, state, current_node, started_at, due_at, updated_at, context_json
       from journey_instances
       where journey_id = $1
         and journey_version = $2
         and state = 'waiting_manual'
         and current_node = $3
       order by started_at asc
       ${paginationClause(4, 5)}`,
      [journeyId, version, waitNodeId, limit, offset]
    );

    res.status(200).json({
      status: 'ok',
      journey_id: journeyId,
      journey_version: version,
      wait_node_id: waitNodeId,
      items: result.rows.map((row) => normalizeRowJsonFields(row, ['context_json'])),
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
    let released;
    const releaseContext = {
      manual_release_at: new Date().toISOString(),
      manual_release_by: payload.released_by || 'ui',
      manual_release_count: payload.count
    };

    const picked = await pgClient.query(
      `select instance_id, customer_id, last_event_id
       from journey_instances
       where journey_id = $1
         and journey_version = $2
         and state = 'waiting_manual'
         and current_node = $3
       order by started_at asc
       ${oracleFetchRowsClause(payload.count)}`,
      [payload.journey_id, version, waitNodeId]
    );
    released = { rows: picked.rows, rowCount: picked.rows.length };
    for (const item of picked.rows) {
      const current = await pgClient.query(
        `select context_json
         from journey_instances
         where instance_id = $1
         ${oneRowClause()}`,
        [item.instance_id]
      );
      const mergedContext = {
        ...normalizeJsonField(current.rows[0]?.context_json, {}),
        ...releaseContext
      };
      await pgClient.query(
        `update journey_instances
         set state = 'waiting',
             due_at = current_timestamp,
             updated_at = current_timestamp,
             context_json = $2
         where instance_id = $1`,
        [item.instance_id, JSON.stringify(mergedContext)]
      );
    }

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
          isOracle
            ? JSON.stringify({
                released_by: payload.released_by || 'ui',
                requested_count: payload.count
              })
            : {
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
       ${paginationClause(1, 2)}`,
      [limit, offset]
    );
    res.status(200).json({
      status: 'ok',
      items: result.rows.map((row) => normalizeRowJsonFields(row, ['raw_payload'])),
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
      `select count(*) as total
       from edge_capacity_usage
       ${whereClause}`,
      values
    );
    const total = Number(countResult.rows[0]?.total || 0);

    const listValues = [...values, limit, offset];
    const result = await pgClient.query(
      `select journey_id, journey_version, edge_id, window_type, window_start, used_count, updated_at
       from edge_capacity_usage
       ${whereClause}
       order by updated_at desc
       ${paginationClause(listValues.length - 1, listValues.length)}`,
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
