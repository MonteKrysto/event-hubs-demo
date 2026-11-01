// Actual Kafka + PostgreSQL check, isolated from the default demo's topic and schema.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { eventKey, scenario } from '../src/domain.js';
import { History } from '../src/history.js';
import { consume, producer, publish, waitReady } from '../src/transport.js';

process.on('warning', (warning) => {
  if (warning.name === 'TimeoutNegativeWarning') throw warning;
});

const topic = 'prior-authorization-tests';
const run = randomUUID();
const schema = `acceptance_${run.replaceAll('-', '')}`;
const history = new History({ schema });
let sink;
try {
  await history.init();
  await waitReady(90000, topic);
  sink = await consume(history, { topic, groupId: `acceptance-${run}` });
  const events = scenario().map((event) => ({ ...event, source_encounter_id: `ACCEPTANCE-${run}` }));
  await publish([...events].reverse(), { duplicate: true, topic });
  const invalid = `INVALID-ACCEPTANCE-${run}`;
  const client = producer();
  try {
    await client.connect();
    await client.send({ topic, acks: -1, messages: [{ key: eventKey(events[0]), value: invalid }] });
  } finally {
    await client.disconnect();
  }
  let rows;
  let rejected;
  const deadline = Date.now() + 90000;
  while (Date.now() < deadline) {
    const snapshot = await history.inspect();
    rows = snapshot.events.filter((row) => row.payload.source_encounter_id === events[0].source_encounter_id);
    rejected = snapshot.rejections.filter((row) => Buffer.from(row.raw_base64, 'base64').toString() === invalid);
    if (rows.length === 4 && rejected.length) break;
    await Promise.race([sleep(250), sink.done]);
  }
  assert.equal(rows.length, 4, 'expected four unique stored events');
  assert.equal(rejected.length, 1, 'expected one invalid record in quarantine');
  assert.equal(new Set(rows.map((row) => row.partition_id)).size, 1);
  assert.deepEqual(rows.map((row) => row.payload.source_sequence), [4, 3, 2, 1]);
  const crosswalk = [{ tenant_id: 'demo-hospital', source_account_id: 'H-ACCOUNT-001', source_encounter_id: events[0].source_encounter_id,
    account_id: 'ACCEPTANCE-ACCOUNT', encounter_id: run }];
  const claims = [{ claim_id: run, tenant_id: 'demo-hospital', account_id: 'ACCEPTANCE-ACCOUNT', encounter_id: run,
    denial_reason: 'missing_authorization', service_date: '2026-10-15' }];
  const report = await history.pull(crosswalk, claims);
  assert.equal(report.decisions[0].event_id, events.at(-1).event_id);
  assert.equal(report.decisions[0].recommendation, 'dispute_candidate');
  console.log(JSON.stringify({ result: 'PASS', database: 'PostgreSQL', unique_events: rows.length,
    arrival_sequences: [4, 3, 2, 1], quarantined_invalid_records: rejected.length, flow_recommendation: 'dispute_candidate' }, null, 2));
} finally {
  if (sink) await sink.stop();
  await history.pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await history.close();
}
