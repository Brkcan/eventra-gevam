import crypto from 'node:crypto';
import http from 'node:http';
import dotenv from 'dotenv';
import { Kafka, Partitioners } from 'kafkajs';
import nodemailer from 'nodemailer';
import pg from 'pg';
import { createClient } from 'redis';

dotenv.config({ path: '../../.env' });

const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const postgresUrl = process.env.POSTGRES_URL || 'postgresql://eventra:eventra@localhost:5432/eventra';
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const healthPort = Number(process.env.RULE_ENGINE_HEALTH_PORT || 3002);
const smtpHost = process.env.SMTP_HOST || '';
const smtpPort = Number(process.env.SMTP_PORT || 587);
const smtpSecure = String(process.env.SMTP_SECURE || 'false').toLowerCase() === 'true';
const smtpUser = process.env.SMTP_USER || '';
const smtpPass = process.env.SMTP_PASS || '';
const smtpFrom = process.env.SMTP_FROM || 'Eventra <no-reply@eventra.local>';
const emailDryRun = String(process.env.EMAIL_DRY_RUN || 'true').toLowerCase() === 'true';

const kafka = new Kafka({ clientId: 'eventra-rule-engine', brokers: kafkaBrokers });
const admin = kafka.admin();
const consumerGroupId = process.env.RULE_ENGINE_CONSUMER_GROUP || 'eventra-rule-engine-v2';
const consumer = kafka.consumer({ groupId: consumerGroupId });
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});
const pgClient = new pg.Client({ connectionString: postgresUrl });
const redisClient = createClient({ url: redisUrl });
const redisSubscriber = redisClient.duplicate();
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
let runtimeGlobalPause = false;
const inMemoryCacheDatasets = new Map();
const inMemoryCacheMeta = new Map();

async function loadDatasetIntoMemory(datasetKey) {
  const key = String(datasetKey || '').trim();
  if (!key) {
    return;
  }
  const hashKey = `cache:dataset:${key}`;
  const hash = await redisClient.hGetAll(hashKey);
  const itemMap = new Map();

  for (const [itemKey, rawValue] of Object.entries(hash || {})) {
    try {
      itemMap.set(itemKey, JSON.parse(rawValue));
    } catch {
      // ignore invalid row payloads
    }
  }

  const meta = await redisClient.hGetAll(`${hashKey}:meta`);
  inMemoryCacheDatasets.set(key, itemMap);
  inMemoryCacheMeta.set(key, meta || {});
}

async function refreshDatasetsFromJourneys(journeys) {
  const requiredKeys = Array.from(
    new Set(
      (journeys || [])
        .map((journey) => String(journey?.cache_lookup?.dataset_key || '').trim())
        .filter(Boolean)
    )
  );

  await Promise.all(requiredKeys.map((datasetKey) => loadDatasetIntoMemory(datasetKey)));
}

function getInMemoryCacheItem(datasetKey, lookupKey) {
  const dataset = inMemoryCacheDatasets.get(String(datasetKey || '').trim());
  if (!dataset) {
    return undefined;
  }
  return dataset.get(String(lookupKey || '').trim());
}

function getCacheHealthSnapshot() {
  return Array.from(inMemoryCacheDatasets.entries()).map(([datasetKey, itemMap]) => {
    const meta = inMemoryCacheMeta.get(datasetKey) || {};
    return {
      dataset_key: datasetKey,
      row_count: Number(meta.row_count || itemMap.size || 0),
      in_memory_count: itemMap.size,
      version: meta.version || null,
      updated_at: meta.updated_at || null,
      key_column: meta.key_column || null
    };
  });
}

function startHealthServer() {
  const server = http.createServer((req, res) => {
    const path = req.url || '/';

    if (path === '/health') {
      const body = JSON.stringify({
        status: 'ok',
        global_pause: runtimeGlobalPause,
        journeys_loaded: runtimeJourneys.length
      });
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      res.end(body);
      return;
    }

    if (path === '/health/cache') {
      const datasets = getCacheHealthSnapshot();
      const body = JSON.stringify({
        status: 'ok',
        dataset_count: datasets.length,
        items: datasets
      });
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      res.end(body);
      return;
    }

    res.writeHead(404, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ status: 'error', message: 'not_found' }));
  });

  server.listen(healthPort, () => {
    console.log(`Rule engine health server listening on :${healthPort}`);
  });
}

async function logInstanceTransition({
  instanceId,
  journeyId,
  journeyVersion,
  customerId,
  fromState,
  toState,
  fromNode,
  toNode,
  reason,
  eventId,
  metadata = {}
}) {
  await pgClient.query(
    `insert into journey_instance_transitions
      (id, instance_id, journey_id, journey_version, customer_id, from_state, to_state, from_node, to_node, reason, event_id, metadata_json)
     values
      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
    [
      crypto.randomUUID(),
      instanceId,
      journeyId,
      journeyVersion,
      customerId,
      fromState || null,
      toState,
      fromNode || null,
      toNode || null,
      reason || null,
      eventId || null,
      metadata
    ]
  );
}

async function markEventConsumed(eventId) {
  if (!eventId) {
    return true;
  }

  const result = await pgClient.query(
    `insert into consumed_events (consumer_group, event_id)
     values ($1, $2)
     on conflict (consumer_group, event_id) do nothing
     returning event_id`,
    [consumerGroupId, eventId]
  );
  return result.rowCount > 0;
}

async function pushToDlq({ rawPayload, errorMessage, parsedEvent }) {
  const item = {
    id: crypto.randomUUID(),
    event_id: parsedEvent?.event_id || null,
    customer_id: parsedEvent?.customer_id || null,
    event_type: parsedEvent?.event_type || null,
    source_topic: 'event.raw',
    error_message: String(errorMessage || 'unknown_error'),
    raw_payload: rawPayload ? parseJsonSafe(rawPayload, { raw: rawPayload }) : null
  };

  await pgClient.query(
    `insert into event_dlq
      (id, event_id, customer_id, event_type, source_topic, error_message, raw_payload)
     values ($1, $2, $3, $4, $5, $6, $7)`,
    [item.id, item.event_id, item.customer_id, item.event_type, item.source_topic, item.error_message, item.raw_payload]
  );

  await producer.send({
    topic: 'event.dlq',
    messages: [{ key: item.customer_id || item.event_id || item.id, value: JSON.stringify(item) }]
  });
}

function resolveEmailFromEventPayload(payload) {
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const directEmail = typeof payload.email === 'string' ? payload.email.trim() : '';
  if (directEmail) {
    return directEmail;
  }

  const toEmail = typeof payload.to === 'string' ? payload.to.trim() : '';
  if (toEmail) {
    return toEmail;
  }

  const nestedCustomerEmail =
    typeof payload.customer?.email === 'string' ? payload.customer.email.trim() : '';
  if (nestedCustomerEmail) {
    return nestedCustomerEmail;
  }

  return null;
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

function getEdgeMaxCustomersTotal(edge) {
  const raw = Number(edge?.data?.max_customers_total);
  if (Number.isFinite(raw) && raw > 0) {
    return Math.floor(raw);
  }
  return 0;
}

function getEdgeMaxCustomersPerDay(edge) {
  const raw = Number(edge?.data?.max_customers_per_day);
  if (Number.isFinite(raw) && raw > 0) {
    return Math.floor(raw);
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

function isCustomerIncludedInRollout(journey, customerId) {
  const rolloutPercent = Number(journey?.rollout_percent ?? 100);
  if (!Number.isFinite(rolloutPercent) || rolloutPercent >= 100) {
    return true;
  }
  if (rolloutPercent <= 0) {
    return false;
  }

  const hash = crypto.createHash('sha256').update(`${journey.journey_id}:${customerId}`).digest();
  const bucket = hash.readUInt32BE(0) % 100;
  return bucket < rolloutPercent;
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
    getPathValue(vars, key) !== undefined ? String(getPathValue(vars, key)) : ''
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

async function reserveEdgeCapacity(route, journey) {
  const totalLimit = Number(route?.max_customers_total || 0);
  const dailyLimit = Number(route?.max_customers_per_day || 0);
  if ((!Number.isFinite(totalLimit) || totalLimit <= 0) && (!Number.isFinite(dailyLimit) || dailyLimit <= 0)) {
    return { ok: true };
  }

  const reserve = async (windowType, windowStart, limit) => {
    if (!Number.isFinite(limit) || limit <= 0) {
      return true;
    }
    const result = await pgClient.query(
      `insert into edge_capacity_usage
        (journey_id, journey_version, edge_id, window_type, window_start, used_count, updated_at)
       values ($1, $2, $3, $4, $5, 1, now())
       on conflict (journey_id, journey_version, edge_id, window_type, window_start)
       do update set
         used_count = edge_capacity_usage.used_count + 1,
         updated_at = now()
       where edge_capacity_usage.used_count < $6
       returning used_count`,
      [journey.journey_id, journey.version, route.edge_id, windowType, windowStart, limit]
    );
    return result.rowCount > 0;
  };

  if (Number.isFinite(totalLimit) && totalLimit > 0) {
    const totalOk = await reserve('total', '1970-01-01T00:00:00Z', totalLimit);
    if (!totalOk) {
      return { ok: false, reason: 'edge_capacity_total_full' };
    }
  }

  if (Number.isFinite(dailyLimit) && dailyLimit > 0) {
    const dailyOk = await reserve('day', new Date().toISOString().slice(0, 10), dailyLimit);
    if (!dailyOk) {
      return { ok: false, reason: 'edge_capacity_daily_full' };
    }
  }

  return { ok: true };
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
  const cacheLookupNode = pathNodes.find((node) => getNodeKind(node) === 'cache_lookup') || null;
  const httpCallNode = pathNodes.find((node) => getNodeKind(node) === 'http_call') || null;
  const conditionNode = pathNodes.find((node) => getNodeKind(node) === 'condition') || null;
  const routeDecisionNodeId =
    conditionNode?.id || cacheLookupNode?.id || httpCallNode?.id || waitNode?.id || triggerNode.id;

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

  const decisionEdges = outgoingEdges.get(routeDecisionNodeId) || [];
  if (conditionNode) {
    const conditionEdges = decisionEdges;
    trueRoutes = conditionEdges
      .filter((edge) => getEdgeType(edge) === 'true')
      .map((edge) => ({
        edge_id: edge.id,
        edge_type: 'true',
        priority: getEdgePriority(edge),
        delay_minutes: getEdgeDelayMinutes(edge),
        expression: String(edge?.data?.expression || '').trim(),
        rate_limit_per_day: Number(edge?.data?.rate_limit_per_day || 0),
        max_customers_total: getEdgeMaxCustomersTotal(edge),
        max_customers_per_day: getEdgeMaxCustomersPerDay(edge),
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
        max_customers_total: getEdgeMaxCustomersTotal(edge),
        max_customers_per_day: getEdgeMaxCustomersPerDay(edge),
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
        max_customers_total: getEdgeMaxCustomersTotal(edge),
        max_customers_per_day: getEdgeMaxCustomersPerDay(edge),
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
        max_customers_total: getEdgeMaxCustomersTotal(edge),
        max_customers_per_day: getEdgeMaxCustomersPerDay(edge),
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
        max_customers_total: getEdgeMaxCustomersTotal(edge),
        max_customers_per_day: getEdgeMaxCustomersPerDay(edge),
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
  } else {
    alwaysRoutes = decisionEdges
      .map((edge) => ({
        edge_id: edge.id,
        edge_type: getEdgeType(edge),
        priority: getEdgePriority(edge),
        delay_minutes: getEdgeDelayMinutes(edge),
        expression: String(edge?.data?.expression || '').trim(),
        rate_limit_per_day: Number(edge?.data?.rate_limit_per_day || 0),
        max_customers_total: getEdgeMaxCustomersTotal(edge),
        max_customers_per_day: getEdgeMaxCustomersPerDay(edge),
        action_node_id: findFirstActionNodeId(edge.target, nodeMap, outgoingEdges)
      }))
      .filter((route) => route.action_node_id);
  }

  if (!defaultActionNodeId && !actionOnTrueNodeId && !actionOnFalseNodeId) {
    return null;
  }

  const triggerEventType = triggerNode?.data?.event_type || triggerNode?.event_type || 'cart_add';
  const waitMinutes = Math.max(1, Number(waitNode?.data?.wait_minutes || 30));
  const waitIsManual = Boolean(waitNode?.data?.manual_release);
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
    wait_is_manual: waitIsManual,
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
    cache_lookup: cacheLookupNode
      ? {
          node_id: cacheLookupNode.id,
          dataset_key: String(cacheLookupNode?.data?.cache_dataset_key || '').trim(),
          lookup_key_template: String(cacheLookupNode?.data?.cache_lookup_key_template || '{{customer_id}}').trim(),
          target_path: String(cacheLookupNode?.data?.cache_target_path || 'external.cache.item').trim(),
          on_miss: String(cacheLookupNode?.data?.cache_on_miss || 'continue').trim(),
          default_json: String(cacheLookupNode?.data?.cache_default_json || '{}')
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
    rollout_percent: Number(row.rollout_percent ?? 100),
    release_paused: Boolean(row.release_paused),
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
       select j.*,
              row_number() over (partition by j.journey_id order by j.version desc) as rn
       from journeys j
       where j.status = 'published'
     ),
     gp as (
       select coalesce((value_json->>'enabled')::boolean, false) as enabled
       from runtime_controls
       where key = 'global_pause'
       limit 1
     )
     select
       r.journey_id,
       r.version,
       r.graph_json,
       coalesce(rc.rollout_percent, 100)::int as rollout_percent,
       coalesce(rc.release_paused, false) as release_paused,
       coalesce((select enabled from gp), false) as global_pause
     from ranked r
     left join journey_release_controls rc
       on rc.journey_id = r.journey_id and rc.journey_version = r.version
     where r.rn = 1`
  );

  runtimeGlobalPause = Boolean(result.rows[0]?.global_pause);

  runtimeJourneys = result.rows
    .map((row) => normalizeJourneyDefinition(row))
    .filter(Boolean);

  await refreshDatasetsFromJourneys(runtimeJourneys);
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

  await pgClient.query(
    `insert into runtime_controls (key, value_json)
     values ('global_pause', '{"enabled": false}'::jsonb)
     on conflict (key) do nothing`
  );

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
}

async function createOrReplaceWaitingInstance(journey, event) {
  const existing = await pgClient.query(
    `select instance_id, state, current_node, journey_version
     from journey_instances
     where journey_id = $1 and customer_id = $2 and state in ('waiting', 'waiting_manual', 'active', 'processing')
     limit 1`,
    [journey.journey_id, event.customer_id]
  );

  if (existing.rowCount > 0) {
    const current = existing.rows[0];
    await pgClient.query(
      `update journey_instances
       set state = $7,
           current_node = $6,
           started_at = $1,
           due_at = case when $7 = 'waiting_manual' then null else $2::timestamptz + make_interval(mins => $3) end,
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
        current.instance_id,
        journey.wait_node_id,
        journey.wait_is_manual ? 'waiting_manual' : 'waiting'
      ]
    );
    await logInstanceTransition({
      instanceId: current.instance_id,
      journeyId: journey.journey_id,
      journeyVersion: current.journey_version || journey.version,
      customerId: event.customer_id,
      fromState: current.state,
      toState: journey.wait_is_manual ? 'waiting_manual' : 'waiting',
      fromNode: current.current_node,
      toNode: journey.wait_node_id,
      reason: journey.wait_is_manual ? 'trigger_event_replace_manual_wait' : 'trigger_event_replace_wait',
      eventId: event.event_id,
      metadata: { trigger_event_type: event.event_type, wait_minutes: journey.wait_minutes, wait_is_manual: journey.wait_is_manual }
    });
    return;
  }

  const newInstanceId = crypto.randomUUID();
  await pgClient.query(
    `insert into journey_instances
      (instance_id, journey_id, journey_version, customer_id, state, current_node, started_at, due_at, last_event_id, context_json)
     values
      ($1, $2, $3, $4, $10, $5, $6, case when $10 = 'waiting_manual' then null else $6::timestamptz + make_interval(mins => $7) end, $8, $9)`,
    [
      newInstanceId,
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
      }),
      journey.wait_is_manual ? 'waiting_manual' : 'waiting'
    ]
  );

  await logInstanceTransition({
    instanceId: newInstanceId,
    journeyId: journey.journey_id,
    journeyVersion: journey.version,
    customerId: event.customer_id,
    fromState: null,
    toState: journey.wait_is_manual ? 'waiting_manual' : 'waiting',
    fromNode: null,
    toNode: journey.wait_node_id,
    reason: journey.wait_is_manual ? 'trigger_event_new_manual_wait_instance' : 'trigger_event_new_instance',
    eventId: event.event_id,
    metadata: { trigger_event_type: event.event_type, wait_minutes: journey.wait_minutes, wait_is_manual: journey.wait_is_manual }
  });
}

async function processEvent(event) {
  if (runtimeGlobalPause) {
    return;
  }
  for (const journey of runtimeJourneys) {
    if (journey.release_paused) {
      continue;
    }
    if (!isCustomerIncludedInRollout(journey, event.customer_id)) {
      continue;
    }
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
     returning ji.instance_id, ji.customer_id, ji.started_at, ji.last_event_id, ji.context_json,
               ji.state as to_state, 'waiting'::text as from_state, ji.current_node, ji.journey_version`,
    [journey.journey_id]
  );

  for (const instance of claimed.rows) {
    try {
      await logInstanceTransition({
        instanceId: instance.instance_id,
        journeyId: journey.journey_id,
        journeyVersion: instance.journey_version || journey.version,
        customerId: instance.customer_id,
        fromState: instance.from_state,
        toState: instance.to_state,
        fromNode: instance.current_node,
        toNode: instance.current_node,
        reason: 'due_claimed_for_processing',
        eventId: instance.last_event_id
      });

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
      const triggerPayload = triggerEventResult.rows[0]?.payload || {};
      const expressionContext = {
        payload: triggerPayload,
        attributes: profileResult.rows[0]?.attributes || {},
        external: {}
      };
      const customerAttributes = expressionContext.attributes || {};
      let cacheMiss = false;
      if (journey.cache_lookup?.dataset_key) {
        const lookupKey = renderTemplate(journey.cache_lookup.lookup_key_template, {
          customer_id: instance.customer_id,
          payload: triggerPayload,
          attributes: customerAttributes
        });
        const cacheValue = getInMemoryCacheItem(journey.cache_lookup.dataset_key, lookupKey);
        if (cacheValue !== undefined) {
          setPathValue(expressionContext, journey.cache_lookup.target_path, cacheValue);
          expressionContext.external.cache = {
            dataset_key: journey.cache_lookup.dataset_key,
            lookup_key: lookupKey,
            hit: true
          };
        } else {
          cacheMiss = true;
          const onMiss = journey.cache_lookup.on_miss;
          if (onMiss === 'default') {
            const defaultValue = parseJsonSafe(journey.cache_lookup.default_json, {});
            setPathValue(expressionContext, journey.cache_lookup.target_path, defaultValue);
            expressionContext.external.cache = {
              dataset_key: journey.cache_lookup.dataset_key,
              lookup_key: lookupKey,
              hit: false,
              default_applied: true
            };
            cacheMiss = false;
          } else {
            expressionContext.external.cache = {
              dataset_key: journey.cache_lookup.dataset_key,
              lookup_key: lookupKey,
              hit: false
            };
          }
        }
      }

      if (cacheMiss && journey.cache_lookup?.on_miss === 'fail') {
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
              exit_reason: 'cache_miss_fail',
              cache_dataset_key: journey.cache_lookup.dataset_key
            })
          ]
        );
        await logInstanceTransition({
          instanceId: instance.instance_id,
          journeyId: journey.journey_id,
          journeyVersion: instance.journey_version || journey.version,
          customerId: instance.customer_id,
          fromState: 'processing',
          toState: 'completed',
          fromNode: instance.current_node,
          toNode: 'end',
          reason: 'cache_miss_fail',
          eventId: instance.last_event_id
        });
        continue;
      }

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

      const pickAllowedRouteWithCapacity = async (routes) => {
        const sorted = [...routes].sort((a, b) => {
          if (a.priority !== b.priority) {
            return a.priority - b.priority;
          }
          return String(a.edge_id || '').localeCompare(String(b.edge_id || ''));
        });

        for (const route of sorted) {
          if (!evaluateSimpleExpression(route.expression, expressionContext)) {
            continue;
          }
          const blocked = await isRateLimited(route, journey, instance.customer_id);
          if (blocked) {
            continue;
          }
          const reserved = await reserveEdgeCapacity(route, journey);
          if (!reserved.ok) {
            continue;
          }
          return route;
        }

        return null;
      };

      let selectedRoute = null;
      if (journey.condition_node_id) {
        if (httpResult?.type === 'timeout') {
          selectedRoute = await pickAllowedRouteWithCapacity([...timeoutRoutes, ...alwaysRoutes]);
        } else if (httpResult?.type === 'error') {
          selectedRoute = await pickAllowedRouteWithCapacity([...errorRoutes, ...alwaysRoutes]);
        } else {
          selectedRoute = conditionMatched
            ? await pickAllowedRouteWithCapacity([...trueRoutes, ...alwaysRoutes])
            : await pickAllowedRouteWithCapacity([...falseRoutes, ...alwaysRoutes]);
        }
      } else {
        selectedRoute = await pickAllowedRouteWithCapacity(alwaysRoutes);
      }

      let selectedActionNodeId = selectedRoute?.action_node_id || null;
      let selectedDelayMinutes = selectedRoute?.delay_minutes || 0;

      if (!selectedActionNodeId && !journey.condition_node_id && alwaysRoutes.length === 0) {
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
        await logInstanceTransition({
          instanceId: instance.instance_id,
          journeyId: journey.journey_id,
          journeyVersion: instance.journey_version || journey.version,
          customerId: instance.customer_id,
          fromState: 'processing',
          toState: 'waiting',
          fromNode: instance.current_node,
          toNode: selectedActionNodeId,
          reason: 'edge_delay_wait',
          eventId: instance.last_event_id,
          metadata: { delay_minutes: selectedDelayMinutes }
        });
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
        await logInstanceTransition({
          instanceId: instance.instance_id,
          journeyId: journey.journey_id,
          journeyVersion: instance.journey_version || journey.version,
          customerId: instance.customer_id,
          fromState: 'processing',
          toState: 'completed',
          fromNode: instance.current_node,
          toNode: 'end',
          reason: conditionMatched ? 'condition_true_no_action_path' : 'condition_false_no_action_path',
          eventId: instance.last_event_id
        });
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
          const email = resolveEmailFromEventPayload(triggerPayload);
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
        const errorRoute = await pickAllowedRouteWithCapacity(errorRoutes);
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
      await logInstanceTransition({
        instanceId: instance.instance_id,
        journeyId: journey.journey_id,
        journeyVersion: instance.journey_version || journey.version,
        customerId: instance.customer_id,
        fromState: 'processing',
        toState: 'completed',
        fromNode: instance.current_node,
        toNode: finalActionNodeId,
        reason:
          finalAction.status === 'failed'
            ? 'action_failed'
            : httpResult?.type === 'timeout'
              ? 'http_timeout_action'
              : httpResult?.type === 'error'
                ? 'http_error_action'
                : conditionMatched
                  ? 'condition_true_action'
                  : 'condition_false_action',
        eventId: instance.last_event_id,
        metadata: {
          http_type: httpResult?.type || null,
          http_status: Number(httpResult?.status || 0),
          route_edge_id: finalRoute?.edge_id || null
        }
      });
    } catch (error) {
      console.error('Due journey processing failed:', error.message);
      await pgClient.query(
        `update journey_instances
         set state = 'waiting', updated_at = now()
         where instance_id = $1`,
        [instance.instance_id]
      );
      await logInstanceTransition({
        instanceId: instance.instance_id,
        journeyId: journey.journey_id,
        journeyVersion: instance.journey_version || journey.version,
        customerId: instance.customer_id,
        fromState: 'processing',
        toState: 'waiting',
        fromNode: instance.current_node,
        toNode: instance.current_node,
        reason: 'processing_error_requeue',
        eventId: instance.last_event_id,
        metadata: { error: error.message }
      });
    }
  }
}

async function evaluateDueJourneys() {
  await refreshRuntimeJourneys();
  if (runtimeGlobalPause) {
    return;
  }
  for (const journey of runtimeJourneys) {
    if (journey.release_paused) {
      continue;
    }
    await evaluateDueJourney(journey);
  }
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
    console.warn('Kafka topic ensure warning:', error.message);
  } finally {
    await admin.disconnect();
  }
}

async function run() {
  await pgClient.connect();
  await redisClient.connect();
  await redisSubscriber.connect();
  await ensureSchema();
  await ensureKafkaTopics();
  await refreshRuntimeJourneys();
  startHealthServer();

  await redisSubscriber.subscribe('cache.updated', async (payload) => {
    const parsed = parseJsonSafe(payload, {});
    const datasetKey = String(parsed?.dataset_key || '').trim();
    if (!datasetKey) {
      return;
    }
    try {
      await loadDatasetIntoMemory(datasetKey);
    } catch (error) {
      console.error('Cache refresh failed:', datasetKey, error.message);
    }
  });

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

      let parsedEvent = null;
      try {
        parsedEvent = JSON.parse(raw);
        const isNew = await markEventConsumed(parsedEvent.event_id);
        if (!isNew) {
          return;
        }

        await refreshRuntimeJourneys();
        await processEvent(parsedEvent);
      } catch (error) {
        console.error('Event consume failed, pushed to DLQ:', error.message);
        try {
          await pushToDlq({
            rawPayload: raw,
            errorMessage: error.message,
            parsedEvent
          });
        } catch (dlqError) {
          console.error('DLQ write failed:', dlqError.message);
        }
      }
    }
  });

  console.log('Rule engine dynamic state machine is running');
}

run().catch((error) => {
  console.error('Rule engine failed:', error);
  process.exit(1);
});
