import { beforeEach, afterEach, test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { ROOT, eventKey, readJson, scenario } from '../src/domain.js';
import { History } from '../src/history.js';

let history;
let schema;
const crosswalk = readJson(`${ROOT}fixtures/crosswalk.json`);
const claims = readJson(`${ROOT}fixtures/claims.json`);
const location = (offset = '0') => ({ topic: 'prior-authorization', partition: 0, offset });
const ingest = (event, offset) => history.ingest(Buffer.from(JSON.stringify(event)), Buffer.from(eventKey(event)), location(offset));

beforeEach(async () => {
  schema = `test_${randomUUID().replaceAll('-', '')}`;
  history = new History({ schema });
  await history.init();
});
afterEach(async () => {
  await history.pool.query(`DROP SCHEMA "${schema}" CASCADE`);
  await history.close();
});

test('PostgreSQL persists four unique events, five receipts, and a Flow batch across reconnects', async () => {
  const events = scenario();
  for (const [index, event] of events.entries()) assert.equal(await ingest(event, String(index)), 'stored');
  assert.equal(await ingest(events.at(-1), '4'), 'duplicate');
  await history.close();
  history = await new History({ schema }).init();
  const snapshot = await history.inspect();
  assert.equal(snapshot.events.length, 4);
  assert.equal(snapshot.receipts.length, 5);
  assert.equal(snapshot.receipts.at(-1).result, 'duplicate');
  assert.equal(snapshot.receipts.at(-1).kafka_offset, '4');
  const report = await history.pull(crosswalk, claims);
  assert.equal(report.decisions[0].recommendation, 'dispute_candidate');
  assert.equal((await history.inspect()).flow_batches.length, 1);
});

test('duplicate ID with changed content is quarantined', async () => {
  const approved = scenario().at(-1);
  await ingest(approved);
  assert.equal(await ingest({ ...approved, authorization_number: 'CONFLICT' }), 'rejected');
  const snapshot = await history.inspect();
  assert.equal(snapshot.events.length, 1);
  assert.match(snapshot.rejections[0].reason, /Conflicting payload/);
});

test('same source revision with different event IDs is quarantined', async () => {
  const event = scenario()[0];
  await ingest(event);
  assert.equal(await ingest({ ...event, event_id: randomUUID() }), 'rejected');
  assert.match((await history.inspect()).rejections[0].reason, /source sequence/);
});

test('concurrent copies insert one event and two durable receipts', async () => {
  const event = scenario()[0];
  const results = await Promise.all([ingest(event, '0'), ingest(event, '1')]);
  assert.deepEqual(results.sort(), ['duplicate', 'stored']);
  assert.equal((await history.events()).length, 1);
  assert.equal((await history.inspect()).receipts.length, 2);
});

test('JSON key order does not change event identity or payload equality', async () => {
  const event = scenario()[0];
  await ingest(event);
  assert.equal(await ingest(Object.fromEntries(Object.entries(event).reverse())), 'duplicate');
});

test('malformed JSON, invalid schema, wrong keys, tombstones, and invalid UTF-8 are quarantined', async () => {
  const event = scenario()[0];
  for (const [raw, key] of [
    [Buffer.from('broken json'), Buffer.from('key')],
    [Buffer.from('{}'), Buffer.from('key')],
    [Buffer.from(JSON.stringify(event)), Buffer.from('wrong-encounter')],
    [null, null],
    [Buffer.from([255]), Buffer.from('key')],
  ]) assert.equal(await history.ingest(raw, key, location()), 'rejected');
  const snapshot = await history.inspect();
  assert.equal(snapshot.rejections.length, 5);
  assert.equal(snapshot.events.length, 0);
  assert.ok(snapshot.receipts.every((row) => row.result === 'rejected'));
});

test('real database write failure propagates and creates no successful receipt', async () => {
  // A failing receipt insert must roll back the preceding event insert as well.
  await history.pool.query(`CREATE FUNCTION fail_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'simulated receipt storage failure'; END $$`);
  await history.pool.query('CREATE TRIGGER fail_receipt BEFORE INSERT ON receipts FOR EACH ROW EXECUTE FUNCTION fail_receipt()');
  await assert.rejects(ingest(scenario()[0]), /simulated receipt storage failure/);
  const snapshot = await history.inspect();
  assert.equal(snapshot.events.length, 0);
  assert.equal(snapshot.receipts.length, 0);
  assert.equal(snapshot.rejections.length, 0);
});

test('ambiguous crosswalk rolls back the batch', async () => {
  await ingest(scenario().at(-1));
  await assert.rejects(history.pull([...crosswalk, ...crosswalk], claims), /Ambiguous/);
  assert.equal((await history.inspect()).flow_batches.length, 0);
});

test('Flow only changes on an explicit pull', async () => {
  const before = await history.pull(crosswalk, claims);
  await ingest(scenario().at(-1));
  assert.equal((await history.inspect()).flow_batches.at(-1).report.decisions[0].recommendation, 'manual_review');
  const after = await history.pull(crosswalk, claims);
  assert.ok(BigInt(after.through_ingest_id) > BigInt(before.through_ingest_id));
  assert.equal(after.decisions[0].recommendation, 'dispute_candidate');
});

test('saved scenario IDs survive process reconnect and parallel initialization', async () => {
  const [first, second] = await Promise.all([history.savedScenario(), history.savedScenario()]);
  assert.deepEqual(first, second);
  await history.close();
  history = await new History({ schema }).init();
  assert.deepEqual(await history.savedScenario(), first);
});
