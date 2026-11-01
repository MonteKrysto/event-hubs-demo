import { Kafka, logLevel } from 'kafkajs';
import { setTimeout as sleep } from 'node:timers/promises';
import { eventKey, validate } from './domain.js';

export const TOPIC = process.env.EVENTHUB_NAME || 'prior-authorization';

export function kafka(clientId = 'fiber-local-demo', retries = 6) {
  const protocol = process.env.KAFKA_SECURITY_PROTOCOL || 'SASL_PLAINTEXT';
  if (!['SASL_PLAINTEXT', 'SASL_SSL'].includes(protocol)) throw new Error('Use SASL_PLAINTEXT locally or SASL_SSL in Azure');
  return new Kafka({
    clientId,
    brokers: (process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:59092').split(','),
    ssl: protocol === 'SASL_SSL',
    sasl: {
      mechanism: 'plain',
      username: '$ConnectionString',
      password: process.env.EVENTHUB_CONNECTION_STRING || 'Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;',
    },
    logLevel: logLevel.ERROR,
    connectionTimeout: 2000,
    requestTimeout: 10000,
    retry: { retries, initialRetryTime: 300, maxRetryTime: 3000 },
  });
}

export async function waitReady(timeout = 90000, topic = TOPIC) {
  const deadline = Date.now() + timeout;
  let lastError;
  while (Date.now() < deadline) {
    const client = kafka('fiber-readiness', 0).admin();
    try {
      await client.connect();
      // Metadata request only. No topic creation or other management operations.
      const metadata = await client.fetchTopicMetadata({ topics: [topic] });
      const entity = metadata.topics.find((row) => row.name === topic);
      if (entity?.partitions.length) {
        console.log(`READY: Kafka endpoint exposes ${topic} with ${entity.partitions.length} partitions`);
        return;
      }
      lastError = 'No topic partitions';
    } catch (error) {
      lastError = error.message;
    } finally {
      await client.disconnect();
    }
    await sleep(1000);
  }
  throw new Error(`Kafka readiness timed out: ${lastError}. Check emulator logs and ACCEPT_EULA=Y.`);
}

export function producer() {
  return kafka('fiber-humata').producer({ allowAutoTopicCreation: false, idempotent: false, maxInFlightRequests: 1 });
}

export async function publish(events, { duplicate = false, topic = TOPIC } = {}) {
  if (!Array.isArray(events) || !events.length) throw new Error('Provide a nonempty JSON array of events');
  events.forEach(validate); // Validate the whole input before sending any messages.
  const client = producer();
  const receipts = [];
  try {
    await client.connect();
    for (const event of [...events, ...(duplicate ? [events.at(-1)] : [])]) {
      const acknowledgments = await client.send({ topic, acks: -1, messages: [{ key: eventKey(event), value: JSON.stringify(event) }] });
      for (const ack of acknowledgments) {
        const receipt = { topic: ack.topicName, partition: ack.partition, offset: ack.baseOffset };
        receipts.push(receipt);
        console.log(`BROKER ACK: ${JSON.stringify(receipt)}`);
      }
    }
    return receipts;
  } finally {
    await client.disconnect();
  }
}

export async function processMessage(history, consumer, { topic, partition, message }) {
  const result = await history.ingest(message.value, message.key, { topic, partition, offset: message.offset });
  // PostgreSQL commits first. Crashing here causes safe redelivery by event_id.
  // Offsets are 64-bit integers; do not round them through a JavaScript Number.
  await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
  console.log(`SINK: ${result} partition=${partition} offset=${message.offset}`);
}

export async function consume(history, { topic = TOPIC, groupId = process.env.KAFKA_GROUP_ID || 'r1dp-sink' } = {}) {
  const client = kafka('fiber-history-sink').consumer({ groupId, allowAutoTopicCreation: false, sessionTimeout: 30000 });
  const heartbeatName = `sink:${topic}:${groupId}`;
  let lastHeartbeat = 0;
  let heartbeatWrite = Promise.resolve();
  const reportHeartbeat = (status) => {
    if (status === 'connected' && Date.now() - lastHeartbeat < 2500) return;
    lastHeartbeat = Date.now();
    heartbeatWrite = heartbeatWrite.then(() => history.recordHeartbeat(heartbeatName, status))
      .catch((error) => console.error(`Receiver heartbeat: ${error.message}`));
  };
  reportHeartbeat('starting');
  client.on(client.events.GROUP_JOIN, () => reportHeartbeat('connected'));
  client.on(client.events.HEARTBEAT, () => reportHeartbeat('connected'));
  // Surface a crash to the CLI. Compose can restart it; uncommitted data replays.
  let finish;
  let fail;
  const done = new Promise((resolve, reject) => { finish = resolve; fail = reject; });
  done.catch(() => {}); // CLI attaches its handler after startup.
  client.on(client.events.CRASH, ({ payload }) => { reportHeartbeat('failed'); fail(payload.error); });
  try {
    await client.connect();
    await client.subscribe({ topics: [topic], fromBeginning: true });
    await client.run({ autoCommit: false, eachMessage: (message) => processMessage(history, client, message) });
  } catch (error) {
    await client.disconnect();
    reportHeartbeat('failed');
    await heartbeatWrite;
    throw error;
  }
  console.log(`SINK: consuming ${topic}; group=${groupId}`);
  let stopping;
  const stop = () => {
    stopping ??= (async () => {
      await client.stop(); await client.disconnect();
      reportHeartbeat('stopped'); await heartbeatWrite; finish();
    })();
    return stopping;
  };
  return { done, stop };
}
