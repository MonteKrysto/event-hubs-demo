import { parseArgs } from 'node:util';
import { writeFileSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';
import { ROOT, readJson, scenario } from './domain.js';
import { History } from './history.js';
import { consume, publish, waitReady } from './transport.js';

const help = `Synthetic Humata → Event Hubs (Kafka) → PostgreSQL history → Flow

Usage: node src/cli.js <command> [options]

  ready       Wait for authenticated Kafka topic metadata
  sink        Continuously receive Kafka events into PostgreSQL
  sample      Print a fresh synthetic four-event JSON array [--output FILE]
  produce     Publish saved scenario or JSON array [--file FILE] [--duplicate]
  inspect     Show events, receipts, rejected records, and Flow batches
  flow-pull   Run the daily-style Flow query [--crosswalk FILE] [--claims FILE]
  demo        Publish, await this run's receipts, pull, and verify [--timeout SECONDS]

PostgreSQL: DATABASE_URL; optional DB_SCHEMA (default public).
All data is synthetic. Flow recommendations require human review.`;

const show = (value) => console.log(JSON.stringify(value, null, 2));
const receiptKey = (topic, partition, offset) => JSON.stringify([topic, partition, String(offset)]);

async function main() {
  const { values, positionals } = parseArgs({ allowPositionals: true, options: {
    help: { type: 'boolean', short: 'h' }, output: { type: 'string' }, file: { type: 'string' },
    duplicate: { type: 'boolean' }, timeout: { type: 'string', default: '90' },
    crosswalk: { type: 'string', default: `${ROOT}fixtures/crosswalk.json` },
    claims: { type: 'string', default: `${ROOT}fixtures/claims.json` },
  } });
  const [command] = positionals;
  if (values.help || !command) { console.log(help); return; }
  if (!['ready', 'sink', 'sample', 'produce', 'inspect', 'flow-pull', 'demo'].includes(command) || positionals.length !== 1) throw new Error(help);
  if (command === 'sample') {
    const events = scenario();
    if (values.output) writeFileSync(values.output, `${JSON.stringify(events, null, 2)}\n`);
    show(events);
    return;
  }
  if (command === 'ready') { await waitReady(); return; }
  // Publishing custom events does not need a database connection.
  if (command === 'produce' && values.file) {
    await waitReady();
    await publish(readJson(values.file), { duplicate: values.duplicate });
    return;
  }
  const history = new History();
  try {
    await history.init();
    if (command === 'sink') {
      await waitReady();
      const sink = await consume(history);
      const stop = () => sink.stop().catch((error) => { console.error(error.message); process.exitCode = 1; });
      process.once('SIGTERM', stop);
      process.once('SIGINT', stop);
      try { await sink.done; }
      finally { process.removeListener('SIGTERM', stop); process.removeListener('SIGINT', stop); await sink.stop(); }
    } else if (command === 'produce') {
      await waitReady();
      await publish(await history.savedScenario(), { duplicate: values.duplicate });
    } else if (command === 'inspect') {
      show(await history.inspect());
    } else if (command === 'flow-pull') {
      show(await history.pull(readJson(values.crosswalk), readJson(values.claims)));
    } else if (command === 'demo') {
      const timeout = Number(values.timeout) * 1000;
      if (!Number.isFinite(timeout) || timeout <= 0) throw new Error('--timeout must be a positive number of seconds');
      await waitReady();
      const events = await history.savedScenario();
      const cursor = await history.receiptCursor();
      console.log('1. HUMATA: publishing four states plus a duplicate approval');
      const acknowledgments = await publish(events, { duplicate: true });
      if (acknowledgments.length !== 5) throw new Error('Expected five Kafka delivery acknowledgments');
      const expected = new Set(acknowledgments.map((row) => receiptKey(row.topic, row.partition, row.offset)));
      const deadline = Date.now() + timeout;
      while (true) {
        const received = new Set((await history.events()).map((row) => row.event_id));
        const receipts = await history.receiptsAfter(cursor);
        const accepted = new Set(receipts.filter((row) => row.result !== 'rejected').map((row) => receiptKey(row.topic, row.partition_id, row.kafka_offset)));
        if (events.every((event) => received.has(event.event_id)) && [...expected].every((key) => accepted.has(key))) break;
        if (Date.now() >= deadline) throw new Error('Sink did not record all delivery receipts. Check sink logs and inspect rejections.');
        await sleep(250);
      }
      console.log("2. HISTORY: all five deliveries received, four unique events persisted; triggering Flow's daily pull");
      const report = await history.pull(readJson(values.crosswalk), readJson(values.claims));
      show(report);
      const decision = report.decisions.find((row) => row.claim_id === 'CLAIM-DEMO-001');
      if (decision?.recommendation !== 'dispute_candidate' || decision.event_id !== events.at(-1).event_id) throw new Error('Flow did not interpret the expected approval');
      console.log('PASS: Kafka send + receive, PostgreSQL history, identity crosswalk, and Flow batch interpretation');
    }
  } finally {
    await history.close();
  }
}

main().catch((error) => { console.error(`ERROR: ${error.message}`); process.exitCode = 1; });
