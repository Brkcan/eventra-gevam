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
  graph_json: z.record(z.any())
});

const JourneyCloneSchema = z.object({
  source_version: z.number().int().positive(),
  target_version: z.number().int().positive().optional(),
  name: z.string().min(1).optional(),
  status: z.enum(['draft', 'published', 'archived']).default('published')
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

async function ensureKafkaTopics() {
  await admin.connect();
  try {
    const existingTopics = new Set(await admin.listTopics());
    const missingTopics = ['event.raw', 'action.triggered'].filter(
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
      graph_json jsonb not null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      primary key (journey_id, version)
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
    create unique index if not exists uq_journey_instance_active
      on journey_instances (journey_id, customer_id)
      where state in ('waiting', 'active', 'processing')
  `);

  await pgClient.query(`
    create index if not exists idx_journey_instances_due
      on journey_instances (state, due_at)
  `);

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

app.post('/ingest', async (req, res) => {
  try {
    const event = normalizeEvent(req.body);

    await pgClient.query(
      `insert into events (event_id, customer_id, event_type, ts, payload, source)
       values ($1, $2, $3, $4, $5, $6)`,
      [event.event_id, event.customer_id, event.event_type, event.ts, event.payload, event.source]
    );

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
      `insert into journeys (journey_id, version, name, status, graph_json)
       values ($1, $2, $3, $4, $5)
       on conflict (journey_id, version)
       do update set
         name = excluded.name,
         status = excluded.status,
         graph_json = excluded.graph_json,
         updated_at = now()`,
      [input.journey_id, input.version, input.name, input.status, input.graph_json]
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
      `select journey_id, version, name, status, graph_json, created_at, updated_at
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
      `select journey_id, version, name, graph_json
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

    const targetName = payload.name || source.rows[0].name;
    await pgClient.query(
      `insert into journeys (journey_id, version, name, status, graph_json)
       values ($1, $2, $3, $4, $5)`,
      [journeyId, targetVersion, targetName, payload.status, source.rows[0].graph_json]
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
