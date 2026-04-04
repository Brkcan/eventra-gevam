import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import dotenv from 'dotenv';
import express from 'express';
import { Kafka, logLevel } from 'kafkajs';
import { z } from 'zod';

dotenv.config({ path: '../../.env' });

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '../../..');
const writeFile = promisify(fs.writeFile);
const mkdir = promisify(fs.mkdir);

const GlobalConfigSchema = z.object({
  kafka: z.object({
    brokers: z.array(z.string().min(1)).min(1),
    target_topic: z.string().min(1).default('event.raw')
  }),
  runtime: z.object({
    reload_on_change: z.boolean().default(false),
    default_max_retries: z.number().int().positive().default(5),
    health_port: z.number().int().positive().default(3020),
    config_dir: z.string().min(1).default('config/listeners')
  })
});

const ListenerConfigSchema = z.object({
  listener_id: z.string().min(1),
  name: z.string().min(1),
  enabled: z.boolean().default(true),
  source_type: z.literal('kafka_topic'),
  source: z.object({
    brokers: z.array(z.string().min(1)).min(1).optional(),
    topic: z.string().min(1),
    group_id: z.string().min(1)
  }),
  filter: z
    .object({
      event_names: z.array(z.string().min(1)).default([])
    })
    .default({ event_names: [] }),
  debug: z
    .object({
      log_raw_payload: z.boolean().optional(),
      log_normalized_event: z.boolean().optional()
    })
    .default({}),
  mapping: z.object({
    mode: z.literal('declarative'),
    fields: z.record(z.string().min(1))
  }),
  output: z.object({
    topic: z.string().min(1).default('event.raw'),
    key_field: z.string().min(1).default('customer_id')
  }),
  retry: z
    .object({
      max_retries: z.number().int().positive().optional(),
      backoff_ms: z.number().int().positive().optional()
    })
    .default({})
});

const EventSchema = z.object({
  event_id: z.string().min(1),
  customer_id: z.string().min(1),
  event_type: z.string().min(1),
  ts: z.string().datetime(),
  payload: z.record(z.any()).default({}),
  source: z.string().min(1)
});

const PreviewNormalizeSchema = z.object({
  listener: ListenerConfigSchema,
  payload: z.record(z.any())
});

const PreviewConsumeSchema = z.object({
  listener: ListenerConfigSchema,
  timeout_ms: z.number().int().min(1000).max(60000).default(8000)
});

function readJsonFile(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJsonFile(filePath, payload) {
  return writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

function slugifyListenerId(raw) {
  return String(raw || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function setNestedValue(target, dottedPath, value) {
  const parts = String(dottedPath || '')
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean);

  if (parts.length === 0) {
    return;
  }

  let cursor = target;
  for (let index = 0; index < parts.length - 1; index += 1) {
    const key = parts[index];
    if (!cursor[key] || typeof cursor[key] !== 'object' || Array.isArray(cursor[key])) {
      cursor[key] = {};
    }
    cursor = cursor[key];
  }

  cursor[parts[parts.length - 1]] = value;
}

function getByJsonPath(input, jsonPath) {
  const raw = String(jsonPath || '').trim();
  if (!raw.startsWith('$.')) {
    return raw;
  }

  const parts = raw
    .slice(2)
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean);

  let cursor = input;
  for (const part of parts) {
    if (cursor === null || cursor === undefined || typeof cursor !== 'object') {
      return undefined;
    }
    cursor = cursor[part];
  }

  return cursor;
}

function buildEventFromMapping(message, listenerConfig) {
  const event = { payload: {} };
  for (const [targetField, sourcePath] of Object.entries(listenerConfig.mapping.fields || {})) {
    const value = getByJsonPath(message, sourcePath);
    if (value === undefined) {
      continue;
    }
    if (targetField.startsWith('payload.')) {
      setNestedValue(event.payload, targetField.replace(/^payload\./, ''), value);
      continue;
    }
    setNestedValue(event, targetField, value);
  }

  if (!event.ts) {
    event.ts = new Date().toISOString();
  }
  if (!event.payload || typeof event.payload !== 'object' || Array.isArray(event.payload)) {
    event.payload = {};
  }

  return EventSchema.parse(event);
}

function passesFilter(message, listenerConfig) {
  const eventNames = listenerConfig.filter?.event_names || [];
  if (eventNames.length === 0) {
    return true;
  }
  const currentName =
    String(message?.event?.name || message?.event_name || message?.eventType || '').trim();
  return eventNames.includes(currentName);
}

async function retryLoop(fn, maxRetries, backoffMs) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (error) {
      attempt += 1;
      if (attempt > maxRetries) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, backoffMs * attempt));
    }
  }
}

async function consumeOneKafkaMessage(listenerConfig, globalConfig, timeoutMs = 8000) {
  const brokers = listenerConfig.source.brokers?.length
    ? listenerConfig.source.brokers
    : globalConfig.kafka.brokers;
  const kafka = new Kafka({
    clientId: `listener-preview-${listenerConfig.listener_id}-${Date.now()}`,
    brokers,
    logLevel: logLevel.NOTHING
  });
  const consumer = kafka.consumer({
    groupId: `${listenerConfig.source.group_id}-preview-${Date.now()}`
  });

  let finished = false;

  try {
    await consumer.connect();
    await consumer.subscribe({ topic: listenerConfig.source.topic, fromBeginning: false });

    return await new Promise((resolve, reject) => {
      const timeout = setTimeout(async () => {
        if (finished) return;
        finished = true;
        try {
          await consumer.disconnect();
        } catch {
          // ignore
        }
        reject(new Error(`timeout waiting for sample message on ${listenerConfig.source.topic}`));
      }, timeoutMs);

      consumer
        .run({
          eachMessage: async ({ message }) => {
            if (finished) {
              return;
            }
            finished = true;
            clearTimeout(timeout);
            const rawValue = message.value?.toString('utf8') || '{}';
            try {
              const parsed = JSON.parse(rawValue);
              const matches = passesFilter(parsed, listenerConfig);
              const normalizedEvent = matches ? buildEventFromMapping(parsed, listenerConfig) : null;
              await consumer.disconnect();
              resolve({
                raw_payload: parsed,
                matches_filter: matches,
                normalized_event: normalizedEvent
              });
            } catch (error) {
              try {
                await consumer.disconnect();
              } catch {
                // ignore
              }
              reject(error);
            }
          }
        })
        .catch(async (error) => {
          if (finished) {
            return;
          }
          finished = true;
          clearTimeout(timeout);
          try {
            await consumer.disconnect();
          } catch {
            // ignore
          }
          reject(error);
        });
    });
  } catch (error) {
    try {
      await consumer.disconnect();
    } catch {
      // ignore
    }
    throw error;
  }
}

class ListenerHubRuntime {
  constructor({ globalConfig, listenerConfigs }) {
    this.globalConfig = globalConfig;
    this.listenerConfigs = listenerConfigs;
    this.listeners = new Map();
    this.consumerHandles = new Map();
    this.publisherKafka = new Kafka({
      clientId: 'eventra-listener-hub',
      brokers: globalConfig.kafka.brokers,
      logLevel: logLevel.NOTHING
    });
    this.publisher = this.publisherKafka.producer();
  }

  async start() {
    await this.publisher.connect();
    await this.loadListeners(this.listenerConfigs);
  }

  async loadListeners(listenerConfigs) {
    this.listenerConfigs = listenerConfigs;
    this.listeners.clear();

    for (const listenerConfig of listenerConfigs) {
      if (!listenerConfig.enabled) {
        this.listeners.set(listenerConfig.listener_id, {
          config: listenerConfig,
          status: 'disabled',
          source_connected: false,
          last_event_at: null,
          last_publish_at: null,
          success_count: 0,
          error_count: 0,
          last_error: null
        });
        continue;
      }

      this.startKafkaTopicListener(listenerConfig).catch((error) => {
        const current = this.listeners.get(listenerConfig.listener_id);
        if (current) {
          current.status = 'error';
          current.source_connected = false;
          current.last_error = error.message;
        }
      });
    }
  }

  async reload(listenerConfigs) {
    for (const [, handle] of this.consumerHandles) {
      try {
        await handle.consumer.disconnect();
      } catch {
        // ignore disconnect errors during reload
      }
    }
    this.consumerHandles.clear();
    await this.loadListeners(listenerConfigs);
  }

  async startKafkaTopicListener(listenerConfig) {
    const brokers = listenerConfig.source.brokers?.length
      ? listenerConfig.source.brokers
      : this.globalConfig.kafka.brokers;

    const kafka = new Kafka({
      clientId: `listener-hub-${listenerConfig.listener_id}`,
      brokers,
      logLevel: logLevel.NOTHING
    });
    const consumer = kafka.consumer({ groupId: listenerConfig.source.group_id });

    const state = {
      config: listenerConfig,
      status: 'starting',
      source_connected: false,
      last_event_at: null,
      last_publish_at: null,
      success_count: 0,
      error_count: 0,
      last_error: null,
      source_topic: listenerConfig.source.topic
    };
    this.listeners.set(listenerConfig.listener_id, state);
    this.consumerHandles.set(listenerConfig.listener_id, { consumer });

    await consumer.connect();
    await consumer.subscribe({ topic: listenerConfig.source.topic, fromBeginning: false });
    state.status = 'running';
    state.source_connected = true;

    consumer
      .run({
      eachMessage: async ({ message }) => {
        state.last_event_at = new Date().toISOString();
        const rawValue = message.value?.toString('utf8') || '{}';

        try {
          if (listenerConfig.debug?.log_raw_payload) {
            console.info(`[listener-hub:${listenerConfig.listener_id}] raw payload ${rawValue}`);
          }
          const parsed = JSON.parse(rawValue);
          if (!passesFilter(parsed, listenerConfig)) {
            return;
          }

          const event = buildEventFromMapping(parsed, listenerConfig);
          if (listenerConfig.debug?.log_normalized_event) {
            console.info(
              `[listener-hub:${listenerConfig.listener_id}] normalized event ${JSON.stringify(event)}`
            );
          }
          const maxRetries =
            listenerConfig.retry?.max_retries || this.globalConfig.runtime.default_max_retries;
          const backoffMs = listenerConfig.retry?.backoff_ms || 1000;

          await retryLoop(
            async () => {
              await this.publisher.send({
                topic: listenerConfig.output.topic || this.globalConfig.kafka.target_topic,
                messages: [
                  {
                    key: String(event[listenerConfig.output.key_field] || event.customer_id),
                    value: JSON.stringify(event)
                  }
                ]
              });
            },
            maxRetries,
            backoffMs
          );

          state.last_publish_at = new Date().toISOString();
          state.success_count += 1;
          state.last_error = null;
        } catch (error) {
          state.error_count += 1;
          state.last_error = error.message;
        }
      }
      })
      .catch((error) => {
        state.status = 'error';
        state.source_connected = false;
        state.last_error = error.message;
      });
  }

  snapshot() {
    return Array.from(this.listeners.values()).map((item) => ({
      listener_id: item.config.listener_id,
      name: item.config.name,
      enabled: item.config.enabled,
      source_type: item.config.source_type,
      status: item.status,
      source_connected: item.source_connected,
      source_topic: item.source_topic || null,
      last_event_at: item.last_event_at,
      last_publish_at: item.last_publish_at,
      success_count: item.success_count,
      error_count: item.error_count,
      last_error: item.last_error
    }));
  }
}

function loadGlobalConfig() {
  const globalConfigPath =
    process.env.LISTENER_HUB_CONFIG_PATH || path.resolve(repoRoot, 'config/listener-hub.json');
  return GlobalConfigSchema.parse(readJsonFile(globalConfigPath));
}

function loadListenerConfigs(globalConfig) {
  const configDir = path.resolve(repoRoot, globalConfig.runtime.config_dir);
  if (!fs.existsSync(configDir)) {
    return [];
  }

  return fs
    .readdirSync(configDir)
    .filter((fileName) => fileName.endsWith('.json'))
    .sort()
    .map((fileName) => ListenerConfigSchema.parse(readJsonFile(path.join(configDir, fileName))));
}

function getListenerConfigDir(globalConfig) {
  return path.resolve(repoRoot, globalConfig.runtime.config_dir);
}

async function saveListenerConfig(globalConfig, payload, { existingId = null } = {}) {
  const parsed = ListenerConfigSchema.parse(payload);
  const configDir = getListenerConfigDir(globalConfig);
  await mkdir(configDir, { recursive: true });
  const fileName = `${slugifyListenerId(parsed.listener_id)}.json`;
  const targetPath = path.join(configDir, fileName);

  if (existingId && existingId !== parsed.listener_id) {
    const previousPath = path.join(configDir, `${slugifyListenerId(existingId)}.json`);
    if (fs.existsSync(previousPath) && previousPath !== targetPath) {
      fs.unlinkSync(previousPath);
    }
  }

  await writeJsonFile(targetPath, parsed);
  return parsed;
}

async function deleteListenerConfig(globalConfig, listenerId) {
  const configDir = getListenerConfigDir(globalConfig);
  const targetPath = path.join(configDir, `${slugifyListenerId(listenerId)}.json`);
  if (fs.existsSync(targetPath)) {
    fs.unlinkSync(targetPath);
  }
}

async function startHealthApi(runtime, port) {
  const app = express();
  app.use((_req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (_req.method === 'OPTIONS') {
      res.status(204).end();
      return;
    }
    next();
  });
  app.use(express.json({ limit: '2mb' }));

  app.get('/health', (_req, res) => {
    res.status(200).json({
      status: 'ok',
      listeners_total: runtime.snapshot().length,
      listeners_running: runtime.snapshot().filter((item) => item.status === 'running').length
    });
  });

  app.get('/listeners', (_req, res) => {
    res.status(200).json({ status: 'ok', items: runtime.snapshot() });
  });

  app.get('/listeners/:listenerId/status', (req, res) => {
    const item = runtime.snapshot().find((entry) => entry.listener_id === req.params.listenerId);
    if (!item) {
      res.status(404).json({ status: 'error', message: 'listener not found' });
      return;
    }
    res.status(200).json({ status: 'ok', item });
  });

  app.get('/admin/configs', (_req, res) => {
    res.status(200).json({
      status: 'ok',
      items: runtime.listenerConfigs
    });
  });

  app.get('/admin/configs/:listenerId', (req, res) => {
    const item = runtime.listenerConfigs.find((entry) => entry.listener_id === req.params.listenerId);
    if (!item) {
      res.status(404).json({ status: 'error', message: 'listener config not found' });
      return;
    }
    res.status(200).json({ status: 'ok', item });
  });

  app.post('/admin/configs', async (req, res) => {
    try {
      const saved = await saveListenerConfig(runtime.globalConfig, req.body || {});
      const nextConfigs = loadListenerConfigs(runtime.globalConfig);
      await runtime.reload(nextConfigs);
      res.status(201).json({ status: 'ok', item: saved });
    } catch (error) {
      res.status(400).json({ status: 'error', message: error.message });
    }
  });

  app.put('/admin/configs/:listenerId', async (req, res) => {
    try {
      const saved = await saveListenerConfig(runtime.globalConfig, req.body || {}, {
        existingId: req.params.listenerId
      });
      const nextConfigs = loadListenerConfigs(runtime.globalConfig);
      await runtime.reload(nextConfigs);
      res.status(200).json({ status: 'ok', item: saved });
    } catch (error) {
      res.status(400).json({ status: 'error', message: error.message });
    }
  });

  app.delete('/admin/configs/:listenerId', async (req, res) => {
    try {
      await deleteListenerConfig(runtime.globalConfig, req.params.listenerId);
      const nextConfigs = loadListenerConfigs(runtime.globalConfig);
      await runtime.reload(nextConfigs);
      res.status(200).json({ status: 'ok' });
    } catch (error) {
      res.status(400).json({ status: 'error', message: error.message });
    }
  });

  app.post('/admin/reload', async (_req, res) => {
    try {
      const nextConfigs = loadListenerConfigs(runtime.globalConfig);
      await runtime.reload(nextConfigs);
      res.status(200).json({
        status: 'ok',
        listeners_total: runtime.snapshot().length
      });
    } catch (error) {
      res.status(400).json({ status: 'error', message: error.message });
    }
  });

  app.post('/admin/preview/normalize', async (req, res) => {
    try {
      const parsed = PreviewNormalizeSchema.parse(req.body || {});
      const matchesFilter = passesFilter(parsed.payload, parsed.listener);
      const normalizedEvent = matchesFilter
        ? buildEventFromMapping(parsed.payload, parsed.listener)
        : null;
      res.status(200).json({
        status: 'ok',
        item: {
          matches_filter: matchesFilter,
          normalized_event: normalizedEvent
        }
      });
    } catch (error) {
      res.status(400).json({ status: 'error', message: error.message });
    }
  });

  app.post('/admin/preview/consume-sample', async (req, res) => {
    try {
      const parsed = PreviewConsumeSchema.parse(req.body || {});
      const item = await consumeOneKafkaMessage(parsed.listener, runtime.globalConfig, parsed.timeout_ms);
      res.status(200).json({ status: 'ok', item });
    } catch (error) {
      res.status(400).json({ status: 'error', message: error.message });
    }
  });

  await new Promise((resolve) => {
    app.listen(port, () => {
      console.log(`[listener-hub] health api listening on :${port}`);
      resolve();
    });
  });
}

async function main() {
  const globalConfig = loadGlobalConfig();
  const listenerConfigs = loadListenerConfigs(globalConfig);
  const runtime = new ListenerHubRuntime({ globalConfig, listenerConfigs });

  console.info(
    `[listener-hub] starting with ${listenerConfigs.length} listener config(s), target topic=${globalConfig.kafka.target_topic}`
  );

  await runtime.start();
  await startHealthApi(runtime, globalConfig.runtime.health_port);
}

main().catch((error) => {
  console.error('[listener-hub] bootstrap failed', error);
  process.exit(1);
});
