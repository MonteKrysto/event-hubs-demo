import { afterEach, beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { History } from "../src/history.js";
import { DemoService } from "../src/demo-service.js";
import { IntegrationDemo } from "../src/integration.js";
import { createWebApp } from "../src/web-server.js";
import { eventKey } from "../src/domain.js";

let history, service, web, integration, server, base, current, sent, publisher;
const approval = { status: "approved", authorization_number: "AUTH-WEBHOOK", valid_from: "2026-10-01", valid_to: "2026-10-31" };
beforeEach(async () => {
  history = await new History({ schema: `integration_${randomUUID().replaceAll("-", "")}` }).init();
  sent = [];
  publisher = async ([event]) => {
    sent.push(event);
    return [{ topic: "prior-authorization", partition: 0, offset: String(sent.length) }];
  };
  service = new DemoService(history, publisher);
  current = await service.activeScenario();
  web = createWebApp(service, { pageSize: 2 });
  server = web.app.listen(0, "127.0.0.1");
  await once(server, "listening");
  base = `http://127.0.0.1:${server.address().port}`;
  web.startWorkers(base, { automatic: false });
  integration = web.integration;
});
afterEach(async () => {
  await web?.close();
  server?.closeAllConnections();
  if (server) await new Promise((resolve) => server.close(resolve));
  if (history) {
    await history.pool.query(`DROP SCHEMA "${history.schema}" CASCADE`);
    await history.close();
  }
});
async function snapshot() { return (await service.snapshot()).integration; }
async function postWebhook(event, valid = true) {
  const raw = JSON.stringify(event);
  return fetch(`${base}/api/integration/webhook`, {
    method: "POST", headers: { "Content-Type": "application/json", "x-demo-signature": valid ? integration.signature(raw) : "bad" }, body: raw,
  });
}
async function receivePublished() {
  const results = [];
  for (const [index, event] of sent.entries()) results.push(await history.ingest(
    Buffer.from(JSON.stringify(event)), Buffer.from(eventKey(event)),
    { topic: "prior-authorization", partition: 0, offset: String(index) }));
  return results;
}
async function skip(input = approval) {
  await integration.controls(current.id, { skip_next_webhook: true });
  return (await service.send(current.id, input)).event;
}

test("real HTTP webhook commits inbox and outbox before Kafka publication; Flow still loads explicitly", async () => {
  const { event, webhook } = await service.send(current.id, approval);
  assert.equal(webhook.result, "accepted");
  assert.equal(sent.length, 0);
  assert.equal((await snapshot()).stats.pending, 1);
  assert.equal((await history.events()).length, 0);
  assert.equal((await service.snapshot()).publications[0].publication_state, "pending");
  await integration.pump();
  assert.equal(sent[0].event_id, event.event_id);
  assert.equal((await snapshot()).stats.pending, 0);
  assert.equal((await history.events()).length, 0, "only the Kafka receiver may write history");
  await receivePublished();
  assert.equal((await service.snapshot()).pull, null);
  await service.pull(current.id);
  assert.equal((await service.review(current.id)).decision.recommendation, "dispute_candidate");
});

test("HTTP 503 leaves the decision in Humata; restoration retries its original ID and revision", async () => {
  await integration.controls(current.id, { receiver_available: false });
  const { event, webhook } = await service.send(current.id, approval);
  assert.equal(webhook.result, "failed");
  let state = await snapshot();
  assert.equal(state.stats.received, 0);
  assert.equal(state.webhooks[0].state, "failed");
  assert.match(state.webhooks[0].last_error, /HTTP 503/);
  assert.equal((await service.snapshot()).publications.length, 1);
  await integration.controls(current.id, { receiver_available: true });
  await integration.tick();
  state = await snapshot();
  assert.equal(state.webhooks[0].state, "delivered");
  assert.equal(state.webhooks[0].attempts, 2);
  assert.equal(state.stats.received, 1);
  assert.equal(sent[0].event_id, event.event_id);
  assert.equal(sent[0].source_sequence, 1);
});

test("simultaneous webhook duplicates and API copies keep one inbox entry and one outgoing event", async () => {
  const event = await skip();
  const responses = await Promise.all(Array.from({ length: 4 }, () => postWebhook(event)));
  const outcomes = [];
  for (const response of responses) {
    assert.equal(response.status, 200);
    outcomes.push((await response.json()).result);
  }
  assert.equal(outcomes.filter((result) => result === "accepted").length, 1);
  assert.equal(outcomes.filter((result) => result === "duplicate").length, 3);
  const result = await integration.recover(current.id);
  assert.equal(result.duplicates, 1);
  assert.equal(result.recovered, 0);
  assert.equal((await snapshot()).stats.duplicates, 4);
  await integration.pump();
  assert.equal(sent.length, 1);
  assert.equal((await snapshot()).stats.received, 1);
});

test("a lost HTTP acknowledgment causes a safe retry after durable receipt", async () => {
  let loseResponse = true;
  integration.fetch = async (...args) => {
    const response = await fetch(...args);
    if (loseResponse && String(args[0]).endsWith("/webhook")) {
      loseResponse = false;
      await response.arrayBuffer();
      throw new Error("Connection lost after the receiver committed");
    }
    return response;
  };
  const { event, webhook } = await service.send(current.id, approval);
  assert.equal(webhook.result, "failed");
  assert.equal((await snapshot()).stats.received, 1);
  assert.equal((await integration.replay(current.id, event.event_id)).result, "duplicate");
  await integration.pump();
  assert.equal(sent.length, 1);
  assert.equal((await snapshot()).stats.duplicates, 1);
});

test("a deliberately skipped webhook is recovered through the actual HTTP API, with no direct history write", async () => {
  const event = await skip();
  assert.equal((await snapshot()).webhooks[0].attempts, 0);
  assert.equal((await snapshot()).webhooks[0].state, "skipped");
  assert.equal((await snapshot()).controls.skip_next_webhook, false);
  assert.equal((await snapshot()).stats.missing, 1);
  const result = await integration.recover(current.id);
  assert.equal(result.recovered, 1);
  assert.equal(result.cursor, "1");
  assert.equal(result.caught_up, true);
  assert.equal((await snapshot()).inbox[0].received_via, "api");
  assert.equal((await history.events()).length, 0);
  await integration.pump();
  assert.equal(sent[0].event_id, event.event_id);
  assert.deepEqual(await receivePublished(), ["stored"]);
  assert.equal((await snapshot()).stats.missing, 0);
  assert.equal((await integration.recover(current.id)).recovered, 0);
  assert.equal((await snapshot()).stats.received, 1);
});

test("API pagination resumes from the last committed page after a network failure", async () => {
  await integration.controls(current.id, { receiver_available: false });
  for (const status of ["required", "attempted", "submitted", "approved", "denied"])
    await service.send(current.id, { ...approval, status });
  let calls = 0;
  integration.fetch = (...args) => {
    if (++calls === 2) throw new Error("API connection interrupted on page two");
    return fetch(...args);
  };
  await assert.rejects(integration.recover(current.id), /interrupted/);
  assert.equal((await snapshot()).controls.recovery_cursor, "2");
  assert.equal((await snapshot()).stats.received, 2);
  assert.equal((await snapshot()).controls.last_recovery_at, null);
  integration.fetch = (...args) => fetch(...args);
  const resumed = await integration.recover(current.id);
  assert.equal(resumed.pages, 2);
  assert.equal(resumed.recovered, 3);
  assert.equal(resumed.cursor, "5");
  assert.equal((await snapshot()).stats.received, 5);
  assert.ok((await snapshot()).controls.last_recovery_at);
});

test("an outbox write failure rolls back the entire API page and its bookmark", async () => {
  await skip({ status: "required" });
  const second = await skip();
  // UUID is generated by the simulator, never supplied as SQL by a caller.
  await history.pool.query(`ALTER TABLE integration_outbox ADD CONSTRAINT simulated_disk_failure CHECK (event_id <> '${second.event_id}'::uuid)`);
  await assert.rejects(integration.recover(current.id), /simulated_disk_failure/);
  assert.equal((await snapshot()).controls.recovery_cursor, "0");
  assert.equal((await snapshot()).stats.received, 0);
  assert.equal((await snapshot()).stats.pending, 0);
  await history.pool.query("ALTER TABLE integration_outbox DROP CONSTRAINT simulated_disk_failure");
  assert.equal((await integration.recover(current.id)).recovered, 2);
  assert.equal((await snapshot()).controls.recovery_cursor, "2");
});

test("an unavailable API preserves its bookmark and can recover after restoration", async () => {
  await skip();
  await integration.controls(current.id, { api_available: false });
  await assert.rejects(integration.recover(current.id), /HTTP 503/);
  assert.equal((await snapshot()).controls.recovery_cursor, "0");
  assert.equal((await snapshot()).stats.received, 0);
  await integration.controls(current.id, { api_available: true });
  assert.equal((await integration.recover(current.id)).recovered, 1);
  assert.equal((await snapshot()).controls.recovery_error, null);
});

test("publishing pause accepts webhooks without publishing; a fresh worker drains the durable outbox", async () => {
  await integration.controls(current.id, { publisher_paused: true });
  await service.send(current.id, approval);
  await integration.pump();
  assert.equal(sent.length, 0);
  assert.equal((await snapshot()).stats.pending, 1);
  await integration.controls(current.id, { publisher_paused: false });
  const restarted = new IntegrationDemo(service);
  await restarted.pump();
  assert.equal(sent.length, 1);
  assert.equal((await snapshot()).stats.pending, 0);
});

test("real publication errors remain visible and retry the immutable event", async (t) => {
  const log = t.mock.method(console, "error", () => {});
  const { event } = await service.send(current.id, approval);
  service.publisher = async () => { throw new Error("Real broker connection failed"); };
  await integration.pump();
  assert.equal(log.mock.calls.length, 1);
  assert.equal((await snapshot()).stats.pending, 1);
  assert.equal((await service.snapshot()).publications[0].publication_state, "unconfirmed");
  service.publisher = publisher;
  await history.pool.query("UPDATE integration_outbox SET next_attempt_at=clock_timestamp()");
  await integration.pump();
  assert.deepEqual(sent, [event]);
  assert.equal((await snapshot()).stats.pending, 0);
});

test("crash after broker acceptance leaves a recoverable lease; Kafka replay still stores one history event", async () => {
  const { event } = await service.send(current.id, approval);
  const transaction = history.transaction.bind(history);
  let crash = false;
  service.publisher = async (events) => { const ack = await publisher(events); crash = true; return ack; };
  history.transaction = (...args) => {
    if (crash) throw new Error("Process stopped before saving broker acknowledgment");
    return transaction(...args);
  };
  try { await assert.rejects(integration.publishOne(), /Process stopped/); }
  finally { history.transaction = transaction; service.publisher = publisher; }
  assert.equal(sent.length, 1);
  await history.pool.query("UPDATE integration_outbox SET lease_until=clock_timestamp()-interval '1 second'");
  await new IntegrationDemo(service).pump();
  assert.deepEqual(sent.map((item) => item.event_id), [event.event_id, event.event_id]);
  assert.deepEqual(await receivePublished(), ["stored", "duplicate"]);
  assert.equal((await history.events()).length, 1);
});

test("recovering an older missed update cannot replace a newer loaded decision", async () => {
  const older = await skip({ status: "required" });
  await service.send(current.id, approval);
  await integration.pump();
  await receivePublished();
  await service.pull(current.id);
  assert.equal((await service.review(current.id)).decision.recommendation, "dispute_candidate");
  const recovered = await integration.recover(current.id);
  assert.equal(recovered.recovered, 1);
  await integration.pump();
  assert.equal(sent[1].event_id, older.event_id);
  await receivePublished();
  assert.equal((await service.snapshot()).pull.events.length, 1);
  await service.pull(current.id);
  assert.equal((await service.snapshot()).pull.events.length, 2);
  assert.equal((await service.review(current.id)).decision.recommendation, "dispute_candidate");
});

test("automatic API checks are optional and run on a 30-second schedule", async () => {
  await skip();
  await integration.tick();
  assert.equal((await snapshot()).stats.received, 0);
  await integration.controls(current.id, { automatic_recovery: true });
  let apiCalls = 0;
  integration.fetch = (...args) => { if (String(args[0]).includes("/changes")) apiCalls++; return fetch(...args); };
  await integration.tick();
  assert.equal((await snapshot()).stats.recovered, 1);
  assert.equal(apiCalls, 1);
  await integration.tick();
  assert.equal(apiCalls, 1);
  await history.pool.query("UPDATE integration_state SET last_recovery_attempt_at=clock_timestamp()-interval '31 seconds'");
  await integration.tick();
  assert.equal(apiCalls, 2);
});

test("signatures, source API credentials, conflicting revisions, and encounter isolation are enforced", async () => {
  const { event } = await service.send(current.id, approval);
  assert.equal((await postWebhook(event, false)).status, 401);
  assert.equal((await fetch(`${base}/api/humata/scenarios/${current.id}/changes`)).status, 401);
  assert.equal((await postWebhook({ ...event, authorization_number: "CONFLICT" })).status, 409);
  assert.equal((await postWebhook({ ...event, event_id: randomUUID() })).status, 409);
  assert.equal((await snapshot()).stats.received, 1);
  await integration.controls(current.id, { receiver_available: false });
  const other = await service.startScenario();
  await service.send(other.id, { status: "required" });
  assert.equal((await snapshot()).controls.receiver_available, true);
  assert.equal((await snapshot()).stats.received, 1);
  await integration.recover(other.id);
  await service.selectScenario(current.id);
  assert.equal((await snapshot()).controls.recovery_cursor, "0");
  assert.equal((await snapshot()).controls.receiver_available, false);
});
