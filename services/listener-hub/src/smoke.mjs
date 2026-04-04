import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

import { Kafka } from 'kafkajs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '../../..');

async function readJson(relativePath) {
  const fullPath = path.resolve(repoRoot, relativePath);
  return JSON.parse(await fs.readFile(fullPath, 'utf8'));
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const healthResponse = await fetch('http://127.0.0.1:3020/health');
  assert(healthResponse.ok, 'listener-hub health endpoint is not reachable on :3020');

  const listenerHubConfig = await readJson('config/listener-hub.json');
  const listenerConfig = await readJson('config/listeners/mobile-app-events.json');

  const brokers = listenerConfig.source?.brokers?.length
    ? listenerConfig.source.brokers
    : listenerHubConfig.kafka?.brokers || ['127.0.0.1:9092'];

  const inputTopic = listenerConfig.source?.topic;
  const outputTopic = listenerConfig.output?.topic || listenerHubConfig.kafka?.target_topic || 'event.raw';
  assert(inputTopic, 'listener source topic is missing');

  const kafka = new Kafka({
    clientId: `listener-hub-smoke-${Date.now()}`,
    brokers
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({
    groupId: `listener-hub-smoke-${Date.now()}`
  });

  const inputEvent = {
    meta: { id: `mobile-smoke-${Date.now()}` },
    user: { customer_id: 'cust-smoke-1' },
    event: { name: 'app_open', ts: new Date().toISOString() },
    context: { screen: 'home' },
    device: { platform: 'ios' }
  };

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: outputTopic, fromBeginning: false });

  let receivedPayload = null;
  let receiveResolve;
  let receiveReject;
  const receivePromise = new Promise((resolve, reject) => {
    receiveResolve = resolve;
    receiveReject = reject;
  });

  const timeout = setTimeout(async () => {
    receiveReject(new Error(`timeout waiting for message on ${outputTopic}`));
  }, 30000);

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const parsed = JSON.parse(message.value?.toString('utf8') || '{}');
        if (parsed.event_id === inputEvent.meta.id) {
          receivedPayload = parsed;
          clearTimeout(timeout);
          receiveResolve(parsed);
        }
      } catch (error) {
        clearTimeout(timeout);
        receiveReject(error);
      }
    }
  });

  await delay(3000);

  await producer.send({
    topic: inputTopic,
    messages: [
      {
        key: inputEvent.user.customer_id,
        value: JSON.stringify(inputEvent)
      }
    ]
  });

  await receivePromise;

  assert(receivedPayload, 'normalized event was not received');
  assert(receivedPayload.customer_id === inputEvent.user.customer_id, 'customer_id mapping mismatch');
  assert(receivedPayload.event_type === inputEvent.event.name, 'event_type mapping mismatch');
  assert(receivedPayload.source === 'mobile-app', 'source mapping mismatch');
  assert(receivedPayload.payload?.screen === inputEvent.context.screen, 'payload.screen mapping mismatch');
  assert(receivedPayload.payload?.platform === inputEvent.device.platform, 'payload.platform mapping mismatch');

  console.log(JSON.stringify({
    status: 'ok',
    input_topic: inputTopic,
    output_topic: outputTopic,
    normalized_event: receivedPayload
  }, null, 2));

  clearTimeout(timeout);
  await consumer.disconnect();
  await producer.disconnect();
}

main().catch(async (error) => {
  console.error(error.message);
  process.exit(1);
});
