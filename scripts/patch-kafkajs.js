// Scoped compatibility fixes for the pinned KafkaJS version:
// - Idle timer: https://github.com/tulios/kafkajs/issues/1751
// - Log socket resets after a completed local shutdown at DEBUG instead of ERROR.
// Fail installation if a future dependency change makes this patch inapplicable.
import { readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';

const require = createRequire(import.meta.url);
const packagePath = require.resolve('kafkajs/package.json');
const version = JSON.parse(readFileSync(packagePath, 'utf8')).version;
if (version !== '2.2.4') throw new Error('Review the KafkaJS compatibility patches before changing versions');
const path = join(dirname(packagePath), 'src/network/requestQueue/index.js');
const original = `      if (this.pending.length > 0) {
        scheduleAt = scheduleAt > 0 ? scheduleAt : CHECK_PENDING_REQUESTS_INTERVAL
      }
      this.throttleCheckTimeoutId = setTimeout`;
const replacement = `      if (this.pending.length > 0) {
        scheduleAt = scheduleAt > 0 ? scheduleAt : CHECK_PENDING_REQUESTS_INTERVAL
      } else if (scheduleAt <= 0) {
        return // No queued requests or active throttle: nothing to schedule.
      }
      this.throttleCheckTimeoutId = setTimeout`;
const source = readFileSync(path, 'utf8');
if (!source.includes(replacement)) {
  if (!source.includes(original)) throw new Error('KafkaJS timer source differs; review the compatibility patch');
  writeFileSync(path, source.replace(original, replacement));
}

// A client-level "disconnecting" flag is too broad: requests can still be draining.
// Only downgrade a reset once the socket has been locally ended with no work left.
// All request rejection, retry, and error propagation behavior remains unchanged.
const connectionPath = join(dirname(packagePath), 'src/network/connection.js');
const connectionOriginal = `        this.logError(error.message, { stack: e.stack })
        this.rejectRequests(error)`;
const connectionReplacement = `        const expectedDisconnect =
          e.code === 'ECONNRESET' &&
          this.socket?.writableEnded === true &&
          [CONNECTION_STATUS.DISCONNECTING, CONNECTION_STATUS.DISCONNECTED].includes(this.connectionStatus) &&
          this.requestQueue.pending.length === 0 &&
          this.requestQueue.inflight.size === 0

        if (expectedDisconnect) {
          this.logDebug('Connection reset after intentional disconnect', { code: e.code, expected: true })
        } else {
          this.logError(error.message, { stack: e.stack })
        }
        this.rejectRequests(error)`;
const connectionSource = readFileSync(connectionPath, 'utf8');
if (!connectionSource.includes(connectionReplacement)) {
  if (!connectionSource.includes(connectionOriginal)) throw new Error('KafkaJS connection source differs; review the logging patch');
  writeFileSync(connectionPath, connectionSource.replace(connectionOriginal, connectionReplacement));
}
