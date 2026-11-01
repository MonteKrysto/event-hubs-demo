import { beforeEach, afterEach, test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { History } from "../src/history.js";
import { DemoService } from "../src/demo-service.js";
import { createWebApp } from "../src/web-server.js";
import { eventKey } from "../src/domain.js";

let history, service, schema, sent;
beforeEach(async () => {
  schema = `webtest_${randomUUID().replaceAll("-", "")}`;
  history = await new History({ schema }).init();
  sent = [];
  service = new DemoService(history, async ([event]) => {
    sent.push(event);
    return [
      {
        topic: "prior-authorization",
        partition: 0,
        offset: String(sent.length),
      },
    ];
  });
});
afterEach(async () => {
  await history.pool.query(`DROP SCHEMA "${schema}" CASCADE`);
  await history.close();
});
const approve = {
  status: "approved",
  authorization_number: "AUTH-UI-001",
  valid_from: "2026-10-01",
  valid_to: "2026-10-31",
};
async function receive(event) {
  return history.ingest(
    Buffer.from(JSON.stringify(event)),
    Buffer.from(eventKey(event)),
    { topic: "prior-authorization", partition: 0, offset: String(sent.length) },
  );
}

test("a shared scenario survives reconnect and starting a new one isolates identity without deleting history", async () => {
  const first = await service.activeScenario();
  assert.equal((await service.activeScenario()).id, first.id);
  await service.send(first.id, { status: "required" });
  await receive(sent[0]);
  const next = await service.startScenario();
  assert.notEqual(
    next.identity.source_encounter_id,
    first.identity.source_encounter_id,
  );
  const snapshot = await service.snapshot();
  assert.equal(snapshot.scenario.id, next.id);
  assert.equal(snapshot.events.length, 0);
  assert.equal(snapshot.pull, null);
  assert.equal((await history.events()).length, 1);
});

test("publication acknowledgment cannot bypass Kafka ingestion or make Flow data appear", async () => {
  const current = await service.activeScenario();
  await service.send(current.id, approve);
  const snapshot = await service.snapshot();
  assert.equal(snapshot.publications[0].publication_state, "published");
  assert.equal(snapshot.events.length, 0);
  assert.equal(snapshot.pull, null);
  await service.pull(current.id);
  assert.equal(
    (await service.review(current.id)).decision.recommendation,
    "manual_review",
  );
});

test("live ingestion does not change the last Flow pull and review uses only that snapshot", async () => {
  const current = await service.activeScenario();
  await service.send(current.id, approve);
  await receive(sent[0]);
  await service.pull(current.id);
  assert.equal(
    (await service.review(current.id)).decision.recommendation,
    "dispute_candidate",
  );
  await service.send(current.id, { status: "denied" });
  await receive(sent[1]);
  const before = await service.snapshot();
  assert.equal(before.events.length, 2);
  assert.equal(before.pull.events.length, 1);
  assert.equal(
    (await service.review(current.id)).decision.recommendation,
    "dispute_candidate",
  );
  await service.pull(current.id);
  assert.equal((await service.snapshot()).review, null);
  assert.equal(
    (await service.review(current.id)).decision.recommendation,
    "manual_review",
  );
});

test("bad approval details do not create or publish a partial event", async () => {
  const current = await service.activeScenario();
  for (const input of [
    { ...approve, authorization_number: "" },
    { ...approve, valid_from: "2026-11-01" },
    { ...approve, valid_to: "not-a-date" },
    { status: "invented" },
  ]) {
    await assert.rejects(service.send(current.id, input), { status: 400 });
  }
  assert.equal(sent.length, 0);
  assert.equal((await service.snapshot()).publications.length, 0);
});

test("concurrent actions have unique increasing source revisions", async () => {
  const current = await service.activeScenario();
  await Promise.all([
    service.send(current.id, { status: "required" }),
    service.send(current.id, { status: "submitted" }),
  ]);
  assert.deepEqual(sent.map((event) => event.source_sequence).sort(), [1, 2]);
  assert.equal(new Set(sent.map((event) => event.event_id)).size, 2);
});

test("an unconfirmed delivery remains visible and retries reuse the same event ID and revision", async () => {
  const current = await service.activeScenario();
  service.publisher = async () => {
    throw new Error("broker unavailable");
  };
  await assert.rejects(service.send(current.id, approve), { status: 502 });
  const failed = (await service.snapshot()).publications[0];
  assert.equal(failed.publication_state, "unconfirmed");
  service.publisher = async ([event]) => {
    sent.push(event);
    return [{ topic: "prior-authorization", partition: 0, offset: "5" }];
  };
  await service.retry(current.id, failed.event_id);
  assert.deepEqual(sent[0], failed.event);
  assert.equal((await service.snapshot()).publications.length, 1);
});

test("a missing broker acknowledgment never becomes a successful publication", async () => {
  const current = await service.activeScenario();
  service.publisher = async () => [];
  await assert.rejects(service.send(current.id, approve), { status: 502 });
  const snapshot = await service.snapshot();
  assert.equal(snapshot.publications[0].publication_state, "unconfirmed");
  assert.equal(snapshot.publications[0].acknowledgment, null);
});

test("review requires a pull and scenarios cannot borrow another encounter history", async () => {
  const old = await service.activeScenario();
  await service.send(old.id, approve);
  await receive(sent[0]);
  const current = await service.startScenario();
  await assert.rejects(service.review(current.id), { status: 409 });
  await service.pull(current.id);
  assert.equal(
    (await service.review(current.id)).decision.recommendation,
    "manual_review",
  );
});

test("approvals outside the claim service date are not dispute candidates", async () => {
  const current = await service.activeScenario();
  await service.send(current.id, {
    ...approve,
    valid_from: "2026-11-01",
    valid_to: "2026-11-30",
  });
  await receive(sent[0]);
  await service.pull(current.id);
  assert.equal(
    (await service.review(current.id)).decision.recommendation,
    "manual_review",
  );
});

test("HTTP actions enforce JSON, reject unknown scenarios, and stream live state", async () => {
  const web = createWebApp(service);
  const server = web.app.listen(0, "127.0.0.1");
  await once(server, "listening");
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    assert.equal(
      (await fetch(`${base}/api/scenarios`, { method: "POST", body: "{}" }))
        .status,
      415,
    );
    assert.equal(
      (
        await fetch(`${base}/api/scenarios`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Origin: "https://unrelated.example",
          },
          body: "{}",
        })
      ).status,
      403,
    );
    assert.equal(
      (
        await fetch(`${base}/api/scenarios`, {
          method: "POST",
          headers: { "Content-Type": "application/json", Origin: "malformed" },
          body: "{}",
        })
      ).status,
      403,
    );
    const state = await (await fetch(`${base}/api/state`)).json();
    assert.match(state.instance_id, /^[a-f0-9-]{36}$/);
    const next = await (await fetch(`${base}/api/state`)).json();
    assert.equal(next.instance_id, state.instance_id);
    assert.ok(next.revision > state.revision);
    const missing = await fetch(`${base}/api/scenarios/${randomUUID()}/pull`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    assert.equal(missing.status, 404);
    const controller = new AbortController();
    const stream = await fetch(`${base}/api/stream`, {
      signal: controller.signal,
    });
    assert.match(stream.headers.get("content-type"), /text\/event-stream/);
    const reader = stream.body.getReader();
    const chunk = new TextDecoder().decode((await reader.read()).value);
    assert.match(chunk, /event: state/);
    assert.ok(chunk.includes(state.scenario.id));
    controller.abort();
    await reader.cancel().catch(() => {});
  } finally {
    web.close();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
});
