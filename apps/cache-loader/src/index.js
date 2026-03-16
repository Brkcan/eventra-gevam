import crypto from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import cron from 'node-cron';
import pg from 'pg';
import { createClient } from 'redis';
import { z } from 'zod';

dotenv.config({ path: '../../.env' });

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.resolve(__dirname, '../public');

const port = Number(process.env.CACHE_LOADER_PORT || 3010);
const metadataDbUrl =
  process.env.CACHE_LOADER_METADATA_DB_URL ||
  process.env.POSTGRES_URL ||
  'postgresql://eventra:eventra@localhost:5432/eventra';
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

const metadataPg = new pg.Client({ connectionString: metadataDbUrl });
const redis = createClient({ url: redisUrl });

const schedules = new Map();

const ConnectionSchema = z.object({
  name: z.string().min(1),
  host: z.string().min(1),
  port: z.number().int().min(1).max(65535).default(5432),
  database: z.string().min(1),
  username: z.string().min(1),
  password: z.string().min(1),
  ssl: z.boolean().default(false)
});

const JobSchema = z.object({
  name: z.string().min(1),
  connection_id: z.string().uuid(),
  dataset_key: z.string().min(1),
  sql_query: z.string().min(1),
  key_column: z.string().min(1),
  run_time: z.string().regex(/^([01]\d|2[0-3]):([0-5]\d)$/),
  timezone: z.string().min(1).default('Europe/Istanbul'),
  enabled: z.boolean().default(true)
});

const TestQuerySchema = z.object({
  connection_id: z.string().uuid(),
  sql_query: z.string().min(1),
  key_column: z.string().min(1),
  preview_limit: z.number().int().min(1).max(200).default(20)
});

function assertSelectQuery(sql) {
  const normalized = String(sql || '').trim();
  if (!/^select\b/i.test(normalized)) {
    throw new Error('Only SELECT queries are allowed');
  }
  if (/;\s*$/m.test(normalized)) {
    throw new Error('Trailing semicolon is not allowed');
  }
  if (/\b(insert|update|delete|drop|alter|create|truncate|grant|revoke)\b/i.test(normalized)) {
    throw new Error('Write/DDL keywords are not allowed');
  }
}

function toCronExpr(runTime) {
  const [hour, minute] = runTime.split(':').map((v) => Number(v));
  return `${minute} ${hour} * * *`;
}

function inferValueType(value) {
  if (value === null || value === undefined) return 'null';
  if (value instanceof Date) return 'date';
  if (Array.isArray(value)) return 'array';
  const t = typeof value;
  if (t === 'number') return Number.isInteger(value) ? 'integer' : 'number';
  if (t === 'boolean') return 'boolean';
  if (t === 'object') return 'object';
  return 'string';
}

async function ensureSchema() {
  await metadataPg.query(`
    create table if not exists cache_loader_connections (
      id uuid primary key,
      name text not null,
      host text not null,
      port int not null,
      database_name text not null,
      username text not null,
      password text not null,
      ssl_enabled boolean not null default false,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);

  await metadataPg.query(`
    create table if not exists cache_loader_jobs (
      id uuid primary key,
      name text not null,
      connection_id uuid not null references cache_loader_connections(id) on delete cascade,
      dataset_key text not null,
      sql_query text not null,
      key_column text not null,
      run_time text not null,
      timezone text not null,
      enabled boolean not null default true,
      last_run_at timestamptz,
      last_status text,
      last_error text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);

  await metadataPg.query(`
    create table if not exists cache_loader_runs (
      id uuid primary key,
      job_id uuid not null references cache_loader_jobs(id) on delete cascade,
      started_at timestamptz not null,
      finished_at timestamptz,
      status text not null,
      row_count int not null default 0,
      error_text text
    )
  `);
}

async function getConnectionById(connectionId) {
  const result = await metadataPg.query(
    `select id, name, host, port, database_name, username, password, ssl_enabled
     from cache_loader_connections
     where id = $1
     limit 1`,
    [connectionId]
  );
  if (result.rowCount === 0) {
    throw new Error('connection not found');
  }
  return result.rows[0];
}

async function runSelectPreview({ connection, sqlQuery, previewLimit }) {
  const sourceClient = new pg.Client({
    host: connection.host,
    port: Number(connection.port),
    database: connection.database_name,
    user: connection.username,
    password: connection.password,
    ssl: connection.ssl_enabled ? { rejectUnauthorized: false } : undefined
  });

  try {
    await sourceClient.connect();
    const query = `select * from (${sqlQuery}) as q limit ${Math.max(1, Number(previewLimit) || 20)}`;
    const result = await sourceClient.query(query);
    return result.rows || [];
  } finally {
    await sourceClient.end().catch(() => {});
  }
}

async function testConnection(connection) {
  const sourceClient = new pg.Client({
    host: connection.host,
    port: Number(connection.port),
    database: connection.database_name,
    user: connection.username,
    password: connection.password,
    ssl: connection.ssl_enabled ? { rejectUnauthorized: false } : undefined
  });

  const startedAt = Date.now();
  try {
    await sourceClient.connect();
    const probe = await sourceClient.query(
      `select current_database() as database_name, now() as server_now`
    );
    const elapsedMs = Date.now() - startedAt;
    return {
      ok: true,
      elapsed_ms: elapsedMs,
      database_name: probe.rows[0]?.database_name || connection.database_name,
      server_now: probe.rows[0]?.server_now || null
    };
  } finally {
    await sourceClient.end().catch(() => {});
  }
}

async function runJob(jobId, trigger = 'scheduler') {
  const jobResult = await metadataPg.query(
    `select id, name, connection_id, dataset_key, sql_query, key_column, run_time, timezone, enabled
     from cache_loader_jobs
     where id = $1
     limit 1`,
    [jobId]
  );

  if (jobResult.rowCount === 0) {
    throw new Error('job not found');
  }

  const job = jobResult.rows[0];
  if (!job.enabled && trigger === 'scheduler') {
    return { skipped: true, reason: 'disabled' };
  }

  assertSelectQuery(job.sql_query);

  const runId = crypto.randomUUID();
  await metadataPg.query(
    `insert into cache_loader_runs (id, job_id, started_at, status)
     values ($1, $2, now(), 'running')`,
    [runId, jobId]
  );

  const sourceConn = await getConnectionById(job.connection_id);
  const sourceClient = new pg.Client({
    host: sourceConn.host,
    port: Number(sourceConn.port),
    database: sourceConn.database_name,
    user: sourceConn.username,
    password: sourceConn.password,
    ssl: sourceConn.ssl_enabled ? { rejectUnauthorized: false } : undefined
  });

  try {
    await sourceClient.connect();
    const queryResult = await sourceClient.query(job.sql_query);
    const rows = queryResult.rows || [];

    const missingKey = rows.find((row) => !(job.key_column in row));
    if (missingKey) {
      throw new Error(`key_column not found in row: ${job.key_column}`);
    }

    const version = new Date().toISOString();
    const redisHashKey = `cache:dataset:${job.dataset_key}`;
    const pipeline = redis.multi();
    pipeline.del(redisHashKey);
    const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
    const columnTypes = {};
    if (rows.length > 0) {
      for (const col of columns) {
        columnTypes[col] = inferValueType(rows[0][col]);
      }
    }

    for (const row of rows) {
      const key = String(row[job.key_column]);
      pipeline.hSet(redisHashKey, key, JSON.stringify(row));
    }

    pipeline.hSet(`cache:dataset:${job.dataset_key}:meta`, {
      version,
      updated_at: version,
      row_count: String(rows.length),
      columns_json: JSON.stringify(columns),
      column_types_json: JSON.stringify(columnTypes),
      key_column: job.key_column,
      source_job_id: job.id
    });

    await pipeline.exec();

    await redis.publish(
      'cache.updated',
      JSON.stringify({
        dataset_key: job.dataset_key,
        version,
        row_count: rows.length,
        key_column: job.key_column,
        trigger,
        job_id: job.id
      })
    );

    await metadataPg.query(
      `update cache_loader_runs
       set finished_at = now(), status = 'success', row_count = $2
       where id = $1`,
      [runId, rows.length]
    );

    await metadataPg.query(
      `update cache_loader_jobs
       set last_run_at = now(),
           last_status = 'success',
           last_error = null,
           updated_at = now()
       where id = $1`,
      [job.id]
    );

    return { run_id: runId, row_count: rows.length, dataset_key: job.dataset_key, version };
  } catch (error) {
    await metadataPg.query(
      `update cache_loader_runs
       set finished_at = now(), status = 'failed', error_text = $2
       where id = $1`,
      [runId, String(error.message || error)]
    );

    await metadataPg.query(
      `update cache_loader_jobs
       set last_run_at = now(),
           last_status = 'failed',
           last_error = $2,
           updated_at = now()
       where id = $1`,
      [job.id, String(error.message || error)]
    );

    throw error;
  } finally {
    await sourceClient.end().catch(() => {});
  }
}

function unscheduleJob(jobId) {
  const task = schedules.get(jobId);
  if (task) {
    task.stop();
    schedules.delete(jobId);
  }
}

function scheduleJob(job) {
  unscheduleJob(job.id);
  if (!job.enabled) {
    return;
  }
  const expression = toCronExpr(job.run_time);
  const task = cron.schedule(
    expression,
    async () => {
      try {
        await runJob(job.id, 'scheduler');
      } catch (error) {
        console.error('[cache-loader] scheduled run failed', job.id, error.message);
      }
    },
    { timezone: job.timezone }
  );
  schedules.set(job.id, task);
}

async function refreshSchedules() {
  const result = await metadataPg.query(
    `select id, run_time, timezone, enabled
     from cache_loader_jobs`
  );
  const activeIds = new Set();

  for (const row of result.rows) {
    activeIds.add(row.id);
    scheduleJob(row);
  }

  for (const [jobId] of schedules) {
    if (!activeIds.has(jobId)) {
      unscheduleJob(jobId);
    }
  }
}

app.get('/health', async (_req, res) => {
  try {
    await metadataPg.query('select 1');
    res.status(200).json({ status: 'ok', scheduler_jobs: schedules.size });
  } catch (error) {
    res.status(500).json({ status: 'error', message: error.message });
  }
});

app.get('/connections', async (_req, res) => {
  const result = await metadataPg.query(
    `select id, name, host, port, database_name, username, ssl_enabled, created_at, updated_at
     from cache_loader_connections
     order by created_at desc`
  );
  res.status(200).json({ status: 'ok', items: result.rows });
});

app.post('/connections', async (req, res) => {
  try {
    const payload = ConnectionSchema.parse(req.body);
    const id = crypto.randomUUID();
    await metadataPg.query(
      `insert into cache_loader_connections
        (id, name, host, port, database_name, username, password, ssl_enabled)
       values ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        id,
        payload.name,
        payload.host,
        payload.port,
        payload.database,
        payload.username,
        payload.password,
        payload.ssl
      ]
    );
    res.status(201).json({ status: 'ok', id });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.delete('/connections/:id', async (req, res) => {
  const result = await metadataPg.query(
    `delete from cache_loader_connections where id = $1`,
    [req.params.id]
  );
  if (result.rowCount === 0) {
    res.status(404).json({ status: 'error', message: 'connection not found' });
    return;
  }
  res.status(200).json({ status: 'ok' });
});

app.post('/connections/:id/test', async (req, res) => {
  try {
    const connection = await getConnectionById(req.params.id);
    const result = await testConnection(connection);
    res.status(200).json({ status: 'ok', item: result });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/jobs', async (_req, res) => {
  const result = await metadataPg.query(
    `select j.id, j.name, j.connection_id, c.name as connection_name, j.dataset_key, j.sql_query,
            j.key_column, j.run_time, j.timezone, j.enabled, j.last_run_at, j.last_status, j.last_error,
            j.created_at, j.updated_at
     from cache_loader_jobs j
     join cache_loader_connections c on c.id = j.connection_id
     order by j.created_at desc`
  );
  res.status(200).json({ status: 'ok', items: result.rows });
});

app.post('/jobs', async (req, res) => {
  try {
    const payload = JobSchema.parse(req.body);
    assertSelectQuery(payload.sql_query);
    const id = crypto.randomUUID();

    await metadataPg.query(
      `insert into cache_loader_jobs
        (id, name, connection_id, dataset_key, sql_query, key_column, run_time, timezone, enabled)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        id,
        payload.name,
        payload.connection_id,
        payload.dataset_key,
        payload.sql_query,
        payload.key_column,
        payload.run_time,
        payload.timezone,
        payload.enabled
      ]
    );

    await refreshSchedules();
    res.status(201).json({ status: 'ok', id });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.post('/jobs/test-query', async (req, res) => {
  try {
    const payload = TestQuerySchema.parse(req.body);
    assertSelectQuery(payload.sql_query);
    const sourceConn = await getConnectionById(payload.connection_id);
    const rows = await runSelectPreview({
      connection: sourceConn,
      sqlQuery: payload.sql_query,
      previewLimit: payload.preview_limit
    });
    const hasKeyColumn = rows.length === 0 ? true : rows.every((row) => payload.key_column in row);
    const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

    res.status(200).json({
      status: 'ok',
      item: {
        preview_count: rows.length,
        columns,
        key_column: payload.key_column,
        key_column_exists: hasKeyColumn,
        sample_rows: rows
      }
    });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.put('/jobs/:id', async (req, res) => {
  try {
    const payload = JobSchema.parse(req.body);
    assertSelectQuery(payload.sql_query);

    const result = await metadataPg.query(
      `update cache_loader_jobs
       set name = $2,
           connection_id = $3,
           dataset_key = $4,
           sql_query = $5,
           key_column = $6,
           run_time = $7,
           timezone = $8,
           enabled = $9,
           updated_at = now()
       where id = $1`,
      [
        req.params.id,
        payload.name,
        payload.connection_id,
        payload.dataset_key,
        payload.sql_query,
        payload.key_column,
        payload.run_time,
        payload.timezone,
        payload.enabled
      ]
    );

    if (result.rowCount === 0) {
      res.status(404).json({ status: 'error', message: 'job not found' });
      return;
    }

    await refreshSchedules();
    res.status(200).json({ status: 'ok' });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.delete('/jobs/:id', async (req, res) => {
  const result = await metadataPg.query(
    `delete from cache_loader_jobs where id = $1`,
    [req.params.id]
  );

  if (result.rowCount === 0) {
    res.status(404).json({ status: 'error', message: 'job not found' });
    return;
  }

  unscheduleJob(req.params.id);
  res.status(200).json({ status: 'ok' });
});

app.post('/jobs/:id/run-now', async (req, res) => {
  try {
    const result = await runJob(req.params.id, 'manual');
    res.status(200).json({ status: 'ok', item: result });
  } catch (error) {
    res.status(400).json({ status: 'error', message: error.message });
  }
});

app.get('/runs', async (req, res) => {
  const jobId = req.query.job_id?.toString();
  const values = [];
  let where = '';
  if (jobId) {
    values.push(jobId);
    where = 'where r.job_id = $1';
  }

  const result = await metadataPg.query(
    `select r.id, r.job_id, j.name as job_name, r.started_at, r.finished_at, r.status, r.row_count, r.error_text
     from cache_loader_runs r
     join cache_loader_jobs j on j.id = r.job_id
     ${where}
     order by r.started_at desc
     limit 200`,
    values
  );

  res.status(200).json({ status: 'ok', items: result.rows });
});

app.use('/', express.static(publicDir));

async function start() {
  await metadataPg.connect();
  await redis.connect();
  await ensureSchema();
  await refreshSchedules();

  app.listen(port, () => {
    console.log(`[cache-loader] listening on :${port}`);
  });
}

start().catch((error) => {
  console.error('[cache-loader] bootstrap failed', error);
  process.exit(1);
});
