import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createRequire } from 'node:module';
import { setImmediate as nextTurn } from 'node:timers/promises';

// Exercise the installed KafkaJS error handler, including our install-time patch.
const require = createRequire(import.meta.url);
const Connection = require('kafkajs/src/network/connection');
const { CONNECTION_STATUS } = require('kafkajs/src/network/connectionStatus');

class Socket extends EventEmitter {
  writableEnded = false;
  end() { this.writableEnded = true; return this; }
  unref() { return this; }
}

function fixture(t, { autoConnect = true, connectionTimeout = 1000 } = {}) {
  const entries = [];
  const sockets = [];
  const logger = { namespace() { return this; } };
  for (const level of ['debug', 'info', 'warn', 'error']) {
    logger[level] = (message, fields) => entries.push({ level, message, ...fields });
  }
  const connection = new Connection({
    host: 'test-broker', port: 9092, logger, connectionTimeout, requestTimeout: 10000,
    socketFactory: ({ onConnect }) => {
      const socket = new Socket();
      sockets.push(socket);
      if (autoConnect) queueMicrotask(onConnect);
      return socket;
    },
  });
  t.after(() => connection.disconnect());
  return { connection, entries, sockets, errors: () => entries.filter((entry) => entry.level === 'error') };
}

async function emitFailure(socket, code = 'ECONNRESET') {
  socket.emit('error', Object.assign(new Error(`read ${code}`), { code }));
  await nextTurn();
}

test('a reset after a drained local disconnect is DEBUG, not ERROR', async (t) => {
  const { connection, sockets, entries, errors } = fixture(t);
  await connection.connect();
  await connection.disconnect();
  await emitFailure(sockets[0]);
  assert.equal(errors().length, 0);
  assert.ok(entries.some((entry) => entry.level === 'debug' && entry.expected === true && entry.code === 'ECONNRESET'));
});

test('an active connection reset stays ERROR and rejects its in-flight request', async (t) => {
  const { connection, sockets, errors } = fixture(t);
  await connection.connect();
  let rejected;
  connection.requestQueue.inflight.set(1, { rejected: (error) => { rejected = error; } });
  await emitFailure(sockets[0]);
  assert.equal(errors().length, 1);
  assert.match(errors()[0].message, /ECONNRESET/);
  assert.equal(rejected.code, 'ECONNRESET');
});

test('starting shutdown does not hide a reset before the socket is locally ended', async (t) => {
  const { connection, sockets, errors } = fixture(t);
  await connection.connect();
  connection.connectionStatus = CONNECTION_STATUS.DISCONNECTING;
  await emitFailure(sockets[0]);
  assert.equal(errors().length, 1);
});

for (const queue of ['pending', 'inflight']) {
  test(`a reset with ${queue} requests stays ERROR even during local shutdown`, async (t) => {
    const { connection, sockets, errors } = fixture(t);
    await connection.connect();
    connection.connectionStatus = CONNECTION_STATUS.DISCONNECTING;
    sockets[0].end();
    let rejected = false;
    const request = { rejected: () => { rejected = true; } };
    if (queue === 'pending') connection.requestQueue.pending.push(request);
    else connection.requestQueue.inflight.set(1, request);
    await emitFailure(sockets[0]);
    assert.equal(errors().length, 1);
    assert.equal(rejected, true);
  });
}

for (const code of ['ECONNREFUSED', 'ETIMEDOUT', 'EACCES']) {
  test(`${code} stays ERROR even after local disconnect`, async (t) => {
    const { connection, sockets, errors } = fixture(t);
    await connection.connect();
    await connection.disconnect();
    await emitFailure(sockets[0], code);
    assert.equal(errors().length, 1);
    assert.match(errors()[0].message, new RegExp(code));
  });
}

test('a refused initial connection stays ERROR and rejects connect()', async (t) => {
  const { connection, sockets, errors } = fixture(t, { autoConnect: false });
  const failure = assert.rejects(connection.connect(), { code: 'ECONNREFUSED' });
  await emitFailure(sockets[0], 'ECONNREFUSED');
  await failure;
  assert.equal(errors().length, 1);
});

test('an initial connection timeout stays ERROR and rejects connect()', async (t) => {
  const { connection, errors } = fixture(t, { autoConnect: false, connectionTimeout: 10 });
  await assert.rejects(connection.connect(), /Connection timeout/);
  assert.equal(errors().length, 1);
});

test('reconnecting restores active-error logging', async (t) => {
  const { connection, sockets, errors } = fixture(t);
  await connection.connect();
  await connection.disconnect();
  await emitFailure(sockets[0]);
  await connection.connect();
  await emitFailure(sockets[1]);
  assert.equal(errors().length, 1);
});

test('disconnecting one client does not mute another client', async (t) => {
  const closing = fixture(t);
  const active = fixture(t);
  await closing.connection.connect();
  await active.connection.connect();
  await closing.connection.disconnect();
  await emitFailure(closing.sockets[0]);
  await emitFailure(active.sockets[0]);
  assert.equal(closing.errors().length, 0);
  assert.equal(active.errors().length, 1);
});
