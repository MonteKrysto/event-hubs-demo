import { test } from 'node:test';
import assert from 'node:assert/strict';
import { processMessage } from '../src/transport.js';

const message = { topic: 'prior-authorization', partition: 1, message: { value: Buffer.from('{}'), key: Buffer.from('key'), offset: '9007199254740993' } };

test('Kafka commit follows the completed database transaction and preserves 64-bit offsets', async () => {
  const order = [];
  const history = { ingest: async () => { order.push('database-committed'); return 'stored'; } };
  const consumer = { commitOffsets: async (offsets) => {
    order.push('kafka-commit');
    assert.equal(offsets[0].offset, '9007199254740994');
  } };
  await processMessage(history, consumer, message);
  assert.deepEqual(order, ['database-committed', 'kafka-commit']);
});

test('database failure prevents Kafka offset commit', async () => {
  let committed = false;
  await assert.rejects(processMessage({ ingest: async () => { throw new Error('database unavailable'); } },
    { commitOffsets: async () => { committed = true; } }, message), /database unavailable/);
  assert.equal(committed, false);
});

test('a durably quarantined message is committed so the partition can progress', async () => {
  let committed = false;
  await processMessage({ ingest: async () => 'rejected' }, { commitOffsets: async () => { committed = true; } }, message);
  assert.equal(committed, true);
});
