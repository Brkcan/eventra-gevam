import crypto from 'node:crypto';
import dotenv from 'dotenv';
import { Kafka, Partitioners } from 'kafkajs';
import nodemailer from 'nodemailer';
import pg from 'pg';

dotenv.config({ path: '../../.env' });

const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const postgresUrl = process.env.POSTGRES_URL || 'postgresql://eventra:eventra@localhost:5432/eventra';
const smtpHost = process.env.SMTP_HOST || '';
const smtpPort = Number(process.env.SMTP_PORT || 587);
const smtpSecure = String(process.env.SMTP_SECURE || 'false').toLowerCase() === 'true';
const smtpUser = process.env.SMTP_USER || '';
const smtpPass = process.env.SMTP_PASS || '';
const smtpFrom = process.env.SMTP_FROM || 'Eventra <no-reply@eventra.local>';
const emailDryRun = String(process.env.EMAIL_DRY_RUN || 'true').toLowerCase() === 'true';

const kafka = new Kafka({ clientId: 'eventra-rule-engine', brokers: kafkaBrokers });
const admin = kafka.admin();
const consumer = kafka.consumer({ groupId: 'eventra-rule-engine-v2' });
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});
const pgClient = new pg.Client({ connectionString: postgresUrl });
const mailTransporter =
  smtpHost && smtpUser && smtpPass
    ? nodemailer.createTransport({
        host: smtpHost,
        port: smtpPort,
        secure: smtpSecure,
        auth: { user: smtpUser, pass: smtpPass }
      })
    : null;

let runtimeJourneys = [];

async function resolveCustomerEmail(customerId) {
  const result = await pgClient.query(
    `select attributes->>'email' as email
     from customer_profiles
     where customer_id = $1
     limit 1`,
    [customerId]
  );

  return result.rows[0]?.email || null;
}

async function trySendEmail({ to, subject, text }) {
  if (!to) {
    return { status: 'failed', reason: 'missing_recipient_email' };
  }

  if (emailDryRun || !mailTransporter) {
    return { status: 'sent', reason: 'dry_run_or_smtp_not_configured' };
  }

  try {
    await mailTransporter.sendMail({
      from: smtpFrom,
      to,
      subject,
      text
    });
    return { status: 'sent', reason: 'smtp_sent' };
  } catch (error) {
    return { status: 'failed', reason: `smtp_error:${error.message}` };
  }
}

function getNodeKind(node) {
  return node?.data?.node_kind || node?.type || null;
}

function selectFirstById(nodes) {
  return [...nodes].sort((a, b) => String(a.id).localeCompare(String(b.id)))[0] || null;
}

function getConditionResultFromEdge(edge) {
  const raw = edge?.data?.condition_result || edge?.label || '';
  const normalized = String(raw).trim().toLowerCase();
  if (normalized === 'true' || normalized === 'yes') {
    return 'true';
  }
  if (normalized === 'false' || normalized === 'no') {
    return 'false';
  }
  return null;
}

function getEdgeType(edge) {
  const raw = edge?.data?.edge_type;
  if (raw) {
    return String(raw).toLowerCase();
  }
  return getConditionResultFromEdge(edge) || 'always';
}

function getEdgePriority(edge) {
  const raw = Number(edge?.data?.priority);
  if (Number.isFinite(raw)) {
    return raw;
  }
  return 100;
}

function getEdgeDelayMinutes(edge) {
  const raw = Number(edge?.data?.delay_minutes);
  if (Number.isFinite(raw) && raw >= 0) {
    return raw;
  }
  return 0;
}

function pickBestRoute(routes) {
  if (!routes || routes.length === 0) {
    return null;
  }
  return [...routes].sort((a, b) => {
    if (a.priority !== b.priority) {
      return a.priority - b.priority;
    }
    return String(a.edge_id || '').localeCompare(String(b.edge_id || ''));
  })[0];
}

function getPathValue(obj, path) {
  if (!obj || !path) {
    return undefined;
  }
  return String(path)
    .split('.')
    .reduce((acc, key) => (acc && Object.prototype.hasOwnProperty.call(acc, key) ? acc[key] : undefined), obj);
}

function setPathValue(obj, path, value) {
  if (!obj || !path) {
    return;
  }
  const keys = String(path).split('.');
  let cursor = obj;
  for (let i = 0; i < keys.length - 1; i += 1) {
    const key = keys[i];
    if (!Object.prototype.hasOwnProperty.call(cursor, key) || typeof cursor[key] !== 'object' || cursor[key] === null) {
      cursor[key] = {};
    }
    cursor = cursor[key];
  }
  cursor[keys[keys.length - 1]] = value;
}

function buildNormalizedHttpFields(httpResult, mappingRaw) {
  const mapping = parseJsonSafe(mappingRaw, {});
  if (!mapping || typeof mapping !== 'object' || Array.isArray(mapping)) {
    return {};
  }

  const source = {
    response: httpResult?.response || {},
    ok: Boolean(httpResult?.ok),
    status: Number(httpResult?.status || 0),
    type: String(httpResult?.type || ''),
    reason: String(httpResult?.reason || '')
  };

  const normalized = {};
  for (const [outputKey, sourcePathRaw] of Object.entries(mapping)) {
    if (!outputKey || typeof sourcePathRaw !== 'string') {
      continue;
    }
    const sourcePath = sourcePathRaw.trim();
    if (!sourcePath) {
      continue;
    }
    const value = getPathValue(source, sourcePath);
    if (value !== undefined) {
      setPathValue(normalized, outputKey, value);
    }
  }

  return normalized;
}

function parseLiteral(raw) {
  const value = String(raw).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    return value.slice(1, -1);
  }
  if (value === 'true') return true;
  if (value === 'false') return false;
  if (!Number.isNaN(Number(value))) return Number(value);
  return value;
}

function evaluateSimpleExpression(expression, context) {
  const trimmed = String(expression || '').trim();
  if (!trimmed) {
    return true;
  }

  const existsMatch = trimmed.match(/^(!)?\s*exists\(\s*([a-zA-Z_][\w.]*)\s*\)$/);
  if (existsMatch) {
    const [, negate, path] = existsMatch;
    const value = getPathValue(context, path);
    const exists = value !== undefined && value !== null;
    return negate ? !exists : exists;
  }

  const containsMatch = trimmed.match(/^([a-zA-Z_][\w.]*)\s+contains\s+(.+)$/);
  if (containsMatch) {
    const [, lhsPath, rhsRaw] = containsMatch;
    const lhs = getPathValue(context, lhsPath);
    const rhs = parseLiteral(rhsRaw);

    if (Array.isArray(lhs)) {
      return lhs.includes(rhs);
    }
    if (typeof lhs === 'string') {
      return lhs.includes(String(rhs));
    }
    return false;
  }

  const match = trimmed.match(/^([a-zA-Z_][\w.]*)\s*(>=|<=|==|!=|>|<)\s*(.+)$/);
  if (!match) {
    return false;
  }

  const [, lhsPath, operator, rhsRaw] = match;
  const lhs = getPathValue(context, lhsPath);
  const rhs = parseLiteral(rhsRaw);

  switch (operator) {
    case '>':
      return Number(lhs) > Number(rhs);
    case '>=':
      return Number(lhs) >= Number(rhs);
    case '<':
      return Number(lhs) < Number(rhs);
    case '<=':
      return Number(lhs) <= Number(rhs);
    case '==':
      if (typeof lhs === 'string' || typeof rhs === 'string') {
        return String(lhs) === String(rhs);
      }
      return lhs == rhs;
    case '!=':
      if (typeof lhs === 'string' || typeof rhs === 'string') {
        return String(lhs) !== String(rhs);
      }
      return lhs != rhs;
    default:
      return false;
  }
}

function parseJsonSafe(raw, fallback = {}) {
  if (!raw || typeof raw !== 'string') {
    return fallback;
  }
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function renderTemplate(template, vars) {
  return String(template || '').replace(/\{\{([\w.]+)\}\}/g, (_, key) =>
    Object.prototype.hasOwnProperty.call(vars, key) ? String(vars[key]) : ''
  );
}

async function executeHttpCall(httpConfig, instance, customerAttributes = {}) {
  if (!httpConfig?.url) {
    return { ok: false, type: 'error', reason: 'http_url_missing', status: 0, response: null };
  }

  const controller = new AbortController();
  const timeoutMs = Math.max(100, Number(httpConfig.timeout_ms || 5000));
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const headers = parseJsonSafe(httpConfig.headers_json, {});
    const renderedBody = renderTemplate(httpConfig.body_template, {
      customer_id: instance.customer_id,
      journey_id: instance.journey_id || '',
      ...Object.fromEntries(
        Object.entries(customerAttributes || {}).map(([key, value]) => [`attributes.${key}`, value])
      )
    });
    const method = (httpConfig.method || 'POST').toUpperCase();

    const requestInit = {
      method,
      headers,
      signal: controller.signal
    };

    if (method !== 'GET' && renderedBody) {
      requestInit.body = renderedBody;
      if (!requestInit.headers['content-type'] && !requestInit.headers['Content-Type']) {
        requestInit.headers['content-type'] = 'application/json';
      }
    }

    const response = await fetch(httpConfig.url, requestInit);
    const text = await response.text();
    const parsed = parseJsonSafe(text, { raw: text });
    const ok = response.ok;
    return {
      ok,
      type: ok ? 'success' : 'error',
      reason: ok ? 'http_ok' : `http_status_${response.status}`,
      status: response.status,
      response: parsed,
      attributes: customerAttributes
    };
  } catch (error) {
    const timeoutError = error?.name === 'AbortError';
    return {
      ok: false,
      type: timeoutError ? 'timeout' : 'error',
      reason: timeoutError ? 'http_timeout' : `http_error:${error.message}`,
      status: 0,
      response: null,
      attributes: customerAttributes
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function isRateLimited(route, journey, customerId) {
  const limit = Number(route?.rate_limit_per_day || 0);
  if (!Number.isFinite(limit) || limit <= 0 || !route?.edge_id) {
    return false;
  }

  const result = await pgClient.query(
    `select count(*)::int as cnt
     from edge_transition_log
     where journey_id = $1
       and journey_version = $2
       and edge_id = $3
       and customer_id = $4
       and triggered_at >= date_trunc('day', now())`,
    [journey.journey_id, journey.version, route.edge_id, customerId]
  );

  return (result.rows[0]?.cnt || 0) >= limit;
}

function findFirstActionNodeId(startNodeId, nodeMap, outgoingEdges) {
  if (!startNodeId || !nodeMap.has(startNodeId)) {
    return null;
  }

  const visited = new Set();
  let cursor = startNodeId;

  while (cursor && !visited.has(cursor)) {
    visited.add(cursor);
    const node = nodeMap.get(cursor);
    if (getNodeKind(node) === 'action') {
      return cursor;
    }

    const nextEdges = outgoingEdges.get(cursor) || [];
    if (nextEdges.length === 0) {
      return null;
    }

    cursor = nextEdges[0].target;
  }

  return null;
}

function normalizeJourneyDefinition(row) {
  const nodes = Array.isArray(row.graph_json?.nodes) ? row.graph_json.nodes : [];
  const edges = Array.isArray(row.graph_json?.edges) ? row.graph_json.edges : [];

  const nodeMap = new Map(nodes.map((node) => [node.id, node]));
  const outgoingEdges = new Map();

  for (const edge of edges) {
    if (!edge?.source || !edge?.target) {
      continue;
    }

    if (!outgoingEdges.has(edge.source)) {
      outgoingEdges.set(edge.source, []);
    }

    outgoingEdges.get(edge.source).push(edge);
  }

  for (const [source, edgeList] of outgoingEdges.entries()) {
    outgoingEdges.set(
      source,
      [...edgeList].sort((a, b) => String(a.id || '').localeCompare(String(b.id || '')))
    );
  }

  const triggerNode = selectFirstById(nodes.filter((node) => getNodeKind(node) === 'trigger'));
  if (!triggerNode) {
    return null;
  }

  const visited = new Set();
  const pathNodes = [];
  let cursorNode = triggerNode;

  while (cursorNode && !visited.has(cursorNode.id)) {
    pathNodes.push(cursorNode);
    visited.add(cursorNode.id);

    const nextEdges = outgoingEdges.get(cursorNode.id) || [];
    if (nextEdges.length === 0) {
      break;
    }

    const nextNode = nodeMap.get(nextEdges[0].target);
    cursorNode = nextNode || null;
  }

  const waitNode = pathNodes.find((node) => getNodeKind(node) === 'wait') || null;
  const httpCallNode = pathNodes.find((node) => getNodeKind(node) === 'http_call') || null;
  const conditionNode = pathNodes.find((node) => getNodeKind(node) === 'condition') || null;

  let defaultActionNodeId =
    pathNodes.find((node) => getNodeKind(node) === 'action')?.id ||
    findFirstActionNodeId(triggerNode.id, nodeMap, outgoingEdges);

  let actionOnTrueNodeId = null;
  let actionOnFalseNodeId = null;
  let trueRoutes = [];
  let falseRoutes = [];
  let alwaysRoutes = [];
  let timeoutRoutes = [];
  let errorRoutes = [];

  if (conditionNode) {
    const conditionEdges = outgoingEdges.get(conditionNode.id) || [];
    trueRoutes = conditionEdges
      .filter((edge) => getEdgeType(edge) === 'true')
      .map((edge) => ({
        edge_id: edge.id,
        edge_type: 'true',
        priority: getEdgePriority(edge),
        delay_minutes: getEdgeDelayMinutes(edge),
        expression: String(edge?.data?.expression || '').trim(),
        rate_limit_per_day: Number(edge?.data?.rate_limit_per_day || 0),
        action_node_id: findFirstActionNodeId(edge.target, nodeMap, outgoingEdges)
      }))
      .filter((route) => route.action_node_id);

    falseRoutes = conditionEdges
      .filter((edge) => getEdgeType(edge) === 'false')
      .map((edge) => ({
        edge_id: edge.id,
        edge_type: 'false',
        priority: getEdgePriority(edge),
        delay_minutes: getEdgeDelayMinutes(edge),
        expression: String(edge?.data?.expression || '').trim(),
        rate_limit_per_day: Number(edge?.data?.rate_limit_per_day || 0),
        action_node_id: findFirstActionNodeId(edge.target, nodeMap, outgoingEdges)
      }))
      .filter((route) => route.action_node_id);

    alwaysRoutes = conditionEdges
      .filter((edge) => getEdgeType(edge) === 'always')
      .map((edge) => ({
        edge_id: edge.id,
        edge_type: 'always',
        priority: getEdgePriority(edge),
        delay_minutes: getEdgeDelayMinutes(edge),
        expression: String(edge?.data?.expression || '').trim(),
        rate_limit_per_day: Number(edge?.data?.rate_limit_per_day || 0),
        action_node_id: findFirstActionNodeId(edge.target, nodeMap, outgoingEdges)
      }))
      .filter((route) => route.action_node_id);

    timeoutRoutes = conditionEdges
      .filter((edge) => getEdgeType(edge) === 'timeout')
      .map((edge) => ({
        edge_id: edge.id,
        edge_type: 'timeout',
        priority: getEdgePriority(edge),
        delay_minutes: getEdgeDelayMinutes(edge),
        expression: String(edge?.data?.expression || '').trim(),
        rate_limit_per_day: Number(edge?.data?.rate_limit_per_day || 0),
        action_node_id: findFirstActionNodeId(edge.target, nodeMap, outgoingEdges)
      }))
      .filter((route) => route.action_node_id);

    errorRoutes = conditionEdges
      .filter((edge) => getEdgeType(edge) === 'error')
      .map((edge) => ({
        edge_id: edge.id,
        edge_type: 'error',
        priority: getEdgePriority(edge),
        delay_minutes: getEdgeDelayMinutes(edge),
        expression: String(edge?.data?.expression || '').trim(),
        rate_limit_per_day: Number(edge?.data?.rate_limit_per_day || 0),
        action_node_id: findFirstActionNodeId(edge.target, nodeMap, outgoingEdges)
      }))
      .filter((route) => route.action_node_id);

    actionOnTrueNodeId = pickBestRoute(trueRoutes)?.action_node_id || null;
    actionOnFalseNodeId = pickBestRoute(falseRoutes)?.action_node_id || null;

    if (!actionOnFalseNodeId && conditionEdges.length === 1) {
      actionOnFalseNodeId = findFirstActionNodeId(conditionEdges[0].target, nodeMap, outgoingEdges);
    }

    if (!defaultActionNodeId) {
      defaultActionNodeId = actionOnFalseNodeId || actionOnTrueNodeId;
    }
  }

  if (!defaultActionNodeId && !actionOnTrueNodeId && !actionOnFalseNodeId) {
    return null;
  }

  const triggerEventType = triggerNode?.data?.event_type || triggerNode?.event_type || 'cart_add';
  const waitMinutes = Math.max(1, Number(waitNode?.data?.wait_minutes || 30));
  const conditionKey = conditionNode?.data?.condition_key || null;
  const conditionEventType = conditionNode?.data?.condition_event_type || null;
  const conditionSegmentValue = conditionNode?.data?.condition_segment_value || null;

  const actionNodeLookupId = defaultActionNodeId || actionOnFalseNodeId || actionOnTrueNodeId;
  const actionNode = actionNodeLookupId ? nodeMap.get(actionNodeLookupId) : null;

  return {
    journey_id: row.journey_id,
    version: row.version,
    trigger_event_type: triggerEventType,
    wait_minutes: waitMinutes,
    wait_node_id: waitNode?.id || triggerNode.id,
    condition_node_id: conditionNode?.id || null,
    condition_key: conditionKey,
    condition_event_type: conditionEventType,
    condition_segment_value: conditionSegmentValue,
    http_call: httpCallNode
      ? {
          node_id: httpCallNode.id,
          method: String(httpCallNode?.data?.http_method || 'POST').toUpperCase(),
          url: String(httpCallNode?.data?.http_url || '').trim(),
          headers_json: String(httpCallNode?.data?.http_headers_json || '{}'),
          body_template: String(httpCallNode?.data?.http_body_template || ''),
          timeout_ms: Math.max(100, Number(httpCallNode?.data?.http_timeout_ms || 5000)),
          response_mapping_json: String(httpCallNode?.data?.response_mapping_json || '{}')
        }
      : null,
    condition_routes: {
      true_routes: trueRoutes,
      false_routes: falseRoutes,
      always_routes: alwaysRoutes,
      timeout_routes: timeoutRoutes,
      error_routes: errorRoutes
    },
    action_on_true_node_id: actionOnTrueNodeId,
    action_on_false_node_id: actionOnFalseNodeId,
    default_action_node_id: defaultActionNodeId,
    action_channel: actionNode?.data?.channel || actionNode?.channel || 'email',
    action_template_id: actionNode?.data?.template_id || '',
    action_message: actionNode?.data?.label || 'Journey action triggered.',
    action_node_map: Object.fromEntries(
      nodes
        .filter((node) => getNodeKind(node) === 'action')
        .map((node) => [
          node.id,
          {
            channel: node?.data?.channel || node?.channel || 'email',
            template_id: node?.data?.template_id || '',
            message: node?.data?.label || 'Journey action triggered.'
          }
        ])
    )
  };
}

async function refreshRuntimeJourneys() {
  const result = await pgClient.query(
    `with ranked as (
       select *, row_number() over (partition by journey_id order by version desc) as rn
       from journeys
       where status = 'published'
     )
     select journey_id, version, graph_json
     from ranked
     where rn = 1`
  );

  runtimeJourneys = result.rows
    .map((row) => normalizeJourneyDefinition(row))
    .filter(Boolean);
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

  await pgClient.query(`alter table action_log add column if not exists journey_id text`);
  await pgClient.query(`alter table action_log add column if not exists journey_version int`);
  await pgClient.query(`alter table action_log add column if not exists journey_node_id text`);

  await pgClient.query(`
    create table if not exists edge_transition_log (
      id text primary key,
      journey_id text not null,
      journey_version int not null,
      edge_id text not null,
      customer_id text not null,
      triggered_at timestamptz not null default now()
    )
  `);

  await pgClient.query(`
    create index if not exists idx_edge_transition_daily
      on edge_transition_log (journey_id, journey_version, edge_id, customer_id, triggered_at desc)
  `);

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
}

async function createOrReplaceWaitingInstance(journey, event) {
  const existing = await pgClient.query(
    `select instance_id
     from journey_instances
     where journey_id = $1 and customer_id = $2 and state in ('waiting', 'active', 'processing')
     limit 1`,
    [journey.journey_id, event.customer_id]
  );

  if (existing.rowCount > 0) {
    await pgClient.query(
      `update journey_instances
       set state = 'waiting',
           current_node = $6,
           started_at = $1,
           due_at = $2::timestamptz + make_interval(mins => $3),
           last_event_id = $4,
           context_json = jsonb_build_object('trigger_event_id', $4, 'trigger_ts', $1),
           updated_at = now(),
           completed_at = null
       where instance_id = $5`,
      [
        event.ts,
        event.ts,
        journey.wait_minutes,
        event.event_id,
        existing.rows[0].instance_id,
        journey.wait_node_id
      ]
    );
    return;
  }

  await pgClient.query(
    `insert into journey_instances
      (instance_id, journey_id, journey_version, customer_id, state, current_node, started_at, due_at, last_event_id, context_json)
     values
      ($1, $2, $3, $4, 'waiting', $5, $6, $6::timestamptz + make_interval(mins => $7), $8, $9)`,
    [
      crypto.randomUUID(),
      journey.journey_id,
      journey.version,
      event.customer_id,
      journey.wait_node_id,
      event.ts,
      journey.wait_minutes,
      event.event_id,
      JSON.stringify({
        trigger_event_id: event.event_id,
        trigger_ts: event.ts,
        wait_node_id: journey.wait_node_id,
        action_node_id: journey.default_action_node_id
      })
    ]
  );
}

async function processEvent(event) {
  for (const journey of runtimeJourneys) {
    if (event.event_type === journey.trigger_event_type) {
      await createOrReplaceWaitingInstance(journey, event);
    }
  }
}

async function evaluateDueJourney(journey) {
  const claimed = await pgClient.query(
    `with due as (
       select instance_id
       from journey_instances
       where journey_id = $1
         and state = 'waiting'
         and due_at <= now()
       order by due_at asc
       limit 50
       for update skip locked
     )
     update journey_instances ji
     set state = 'processing', updated_at = now()
     from due
     where ji.instance_id = due.instance_id
     returning ji.instance_id, ji.customer_id, ji.started_at, ji.last_event_id, ji.context_json`,
    [journey.journey_id]
  );

  for (const instance of claimed.rows) {
    try {
      let conditionMatched = false;
      if (
        journey.condition_node_id &&
        journey.condition_event_type &&
        (journey.condition_key === 'purchase_exists' || journey.condition_key === 'event_exists')
      ) {
        const eventCheck = await pgClient.query(
          `select 1
           from events
           where customer_id = $1 and event_type = $2 and ts >= $3
           limit 1`,
          [instance.customer_id, journey.condition_event_type, instance.started_at]
        );
        conditionMatched = eventCheck.rowCount > 0;
      }
      if (
        journey.condition_node_id &&
        journey.condition_key === 'segment_match' &&
        journey.condition_segment_value
      ) {
        const segmentCheck = await pgClient.query(
          `select 1
           from customer_profiles
           where customer_id = $1 and segment = $2
           limit 1`,
          [instance.customer_id, journey.condition_segment_value]
        );
        conditionMatched = segmentCheck.rowCount > 0;
      }

      const triggerEventResult = await pgClient.query(
        `select payload
         from events
         where event_id = $1
         limit 1`,
        [instance.last_event_id]
      );
      const profileResult = await pgClient.query(
        `select attributes
         from customer_profiles
         where customer_id = $1
         limit 1`,
        [instance.customer_id]
      );
      const expressionContext = {
        payload: triggerEventResult.rows[0]?.payload || {},
        attributes: profileResult.rows[0]?.attributes || {},
        external: {}
      };
      const customerAttributes = expressionContext.attributes || {};
      let httpResult = null;
      if (journey.http_call?.url) {
        httpResult = await executeHttpCall(journey.http_call, instance, customerAttributes);
        const normalizedFields = buildNormalizedHttpFields(
          httpResult,
          journey.http_call.response_mapping_json
        );
        await pgClient.query(
          `insert into external_call_log
            (id, instance_id, journey_id, journey_version, customer_id, journey_node_id, method, url, status_code, result_type, reason, response_json)
           values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
          [
            crypto.randomUUID(),
            instance.instance_id,
            journey.journey_id,
            journey.version,
            instance.customer_id,
            journey.http_call.node_id || 'http_call',
            journey.http_call.method || 'POST',
            journey.http_call.url,
            Number(httpResult.status || 0),
            httpResult.type || 'error',
            httpResult.reason || 'unknown',
            httpResult.response || null
          ]
        );
        const httpNodeKey = journey.http_call.node_id || 'http_call';
        expressionContext.external[httpNodeKey] = {
          ok: httpResult.ok,
          type: httpResult.type,
          reason: httpResult.reason,
          status: httpResult.status,
          response: httpResult.response,
          normalized: normalizedFields
        };
        expressionContext.external.http = expressionContext.external[httpNodeKey];
      }

      const trueRoutes = journey.condition_routes?.true_routes || [];
      const falseRoutes = journey.condition_routes?.false_routes || [];
      const alwaysRoutes = journey.condition_routes?.always_routes || [];
      const timeoutRoutes = journey.condition_routes?.timeout_routes || [];
      const errorRoutes = journey.condition_routes?.error_routes || [];

      const filterAllowedRoutes = async (routes) => {
        const allowed = [];
        for (const route of routes) {
          if (!evaluateSimpleExpression(route.expression, expressionContext)) {
            continue;
          }
          const blocked = await isRateLimited(route, journey, instance.customer_id);
          if (blocked) {
            continue;
          }
          allowed.push(route);
        }
        return allowed;
      };

      const trueCandidates = await filterAllowedRoutes([...trueRoutes, ...alwaysRoutes]);
      const falseCandidates = await filterAllowedRoutes([...falseRoutes, ...alwaysRoutes]);
      const timeoutCandidates = await filterAllowedRoutes([...timeoutRoutes, ...alwaysRoutes]);
      const errorCandidates = await filterAllowedRoutes([...errorRoutes, ...alwaysRoutes]);

      let selectedRoute = null;
      if (journey.condition_node_id) {
        if (httpResult?.type === 'timeout') {
          selectedRoute = pickBestRoute(timeoutCandidates);
        } else if (httpResult?.type === 'error') {
          selectedRoute = pickBestRoute(errorCandidates);
        } else {
          selectedRoute = conditionMatched ? pickBestRoute(trueCandidates) : pickBestRoute(falseCandidates);
        }
      }

      let selectedActionNodeId = selectedRoute?.action_node_id || journey.default_action_node_id;
      let selectedDelayMinutes = selectedRoute?.delay_minutes || 0;

      if (!selectedActionNodeId) {
        selectedActionNodeId = journey.default_action_node_id;
      }

      const actionConfig = selectedActionNodeId
        ? journey.action_node_map?.[selectedActionNodeId] || null
        : null;

      const context = instance.context_json || {};
      const pendingActionNodeId = context.pending_action_node_id || null;
      const pendingDelayApplied = Boolean(context.pending_delay_applied);

      if (
        actionConfig &&
        selectedDelayMinutes > 0 &&
        (!pendingDelayApplied || pendingActionNodeId !== selectedActionNodeId)
      ) {
        await pgClient.query(
          `update journey_instances
           set state = 'waiting',
               current_node = $2,
               due_at = now() + make_interval(mins => $3),
               context_json = context_json || $4::jsonb,
               updated_at = now()
           where instance_id = $1`,
          [
            instance.instance_id,
            selectedActionNodeId,
            selectedDelayMinutes,
            JSON.stringify({
              pending_delay_applied: true,
              pending_action_node_id: selectedActionNodeId,
              pending_delay_minutes: selectedDelayMinutes
            })
          ]
        );
        continue;
      }

      if (!actionConfig) {
        await pgClient.query(
          `update journey_instances
           set state = 'completed',
               current_node = 'end',
               completed_at = now(),
               context_json = context_json || $2::jsonb,
               updated_at = now()
           where instance_id = $1`,
          [
            instance.instance_id,
            JSON.stringify({
              exit_reason: conditionMatched
                ? 'condition_true_no_action_path'
                : 'condition_false_no_action_path'
            })
          ]
        );
        continue;
      }

      const dispatchAction = async (actionNodeId, config) => {
        const action = {
          action_id: crypto.randomUUID(),
          event_id: instance.last_event_id,
          customer_id: instance.customer_id,
          journey_id: journey.journey_id,
          journey_version: journey.version,
          journey_node_id: actionNodeId,
          channel: config.channel,
          status: 'triggered',
          message: config.message,
          template_id: config.template_id
        };

        if (action.channel === 'email') {
          const email = await resolveCustomerEmail(action.customer_id);
          const sendResult = await trySendEmail({
            to: email,
            subject: action.message || `${journey.journey_id} notification`,
            text: action.message || 'Eventra notification'
          });
          action.status = sendResult.status;
          action.delivery_reason = sendResult.reason;
        }

        await pgClient.query(
          `insert into action_log (action_id, event_id, customer_id, journey_id, journey_version, journey_node_id, channel, status, message)
           values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
          [
            action.action_id,
            action.event_id,
            action.customer_id,
            action.journey_id,
            action.journey_version,
            action.journey_node_id,
            action.channel,
            action.status,
            action.delivery_reason
              ? `${action.message} [${action.delivery_reason}]`
              : action.message
          ]
        );

        if (action.status !== 'failed') {
          await producer.send({
            topic: 'action.triggered',
            messages: [{ key: action.customer_id, value: JSON.stringify(action) }]
          });
        }

        return action;
      };

      let finalActionNodeId = selectedActionNodeId;
      let finalAction = await dispatchAction(selectedActionNodeId, actionConfig);
      let finalRoute = selectedRoute;

      if (finalAction.status === 'failed' && errorRoutes.length > 0) {
        const failureErrorCandidates = await filterAllowedRoutes(errorRoutes);
        const errorRoute = pickBestRoute(failureErrorCandidates);
        const errorNodeId = errorRoute?.action_node_id || null;
        const errorConfig = errorNodeId ? journey.action_node_map?.[errorNodeId] || null : null;

        if (errorNodeId && errorConfig && errorNodeId !== selectedActionNodeId) {
          finalAction = await dispatchAction(errorNodeId, errorConfig);
          finalActionNodeId = errorNodeId;
          finalRoute = errorRoute;
        }
      }

      if (finalRoute?.edge_id) {
        await pgClient.query(
          `insert into edge_transition_log
            (id, journey_id, journey_version, edge_id, customer_id)
           values ($1, $2, $3, $4, $5)`,
          [
            crypto.randomUUID(),
            journey.journey_id,
            journey.version,
            finalRoute.edge_id,
            instance.customer_id
          ]
        );
      }

      await pgClient.query(
        `update journey_instances
         set state = 'completed',
             current_node = $2,
             completed_at = now(),
             context_json = context_json || $3::jsonb,
             updated_at = now()
         where instance_id = $1`,
        [
          instance.instance_id,
          finalActionNodeId,
            JSON.stringify({
              http_call: httpResult
                ? {
                    type: httpResult.type,
                    reason: httpResult.reason,
                    status: httpResult.status,
                    ok: httpResult.ok
                  }
                : null,
              exit_reason:
                finalAction.status === 'failed'
                  ? 'action_failed'
                  : httpResult?.type === 'timeout'
                    ? 'http_timeout_action'
                    : httpResult?.type === 'error'
                      ? 'http_error_action'
                  : conditionMatched
                    ? 'condition_true_action'
                    : 'condition_false_action'
            })
          ]
        );
    } catch (error) {
      console.error('Due journey processing failed:', error.message);
      await pgClient.query(
        `update journey_instances
         set state = 'waiting', updated_at = now()
         where instance_id = $1`,
        [instance.instance_id]
      );
    }
  }
}

async function evaluateDueJourneys() {
  await refreshRuntimeJourneys();
  for (const journey of runtimeJourneys) {
    await evaluateDueJourney(journey);
  }
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
    console.warn('Kafka topic ensure warning:', error.message);
  } finally {
    await admin.disconnect();
  }
}

async function run() {
  await pgClient.connect();
  await ensureSchema();
  await ensureKafkaTopics();
  await refreshRuntimeJourneys();

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'event.raw', fromBeginning: false });

  setInterval(() => {
    evaluateDueJourneys().catch((error) => {
      console.error('Scheduler error:', error.message);
    });
  }, 5000);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value?.toString();
      if (!raw) {
        return;
      }

      const event = JSON.parse(raw);
      await refreshRuntimeJourneys();
      await processEvent(event);
    }
  });

  console.log('Rule engine dynamic state machine is running');
}

run().catch((error) => {
  console.error('Rule engine failed:', error);
  process.exit(1);
});
