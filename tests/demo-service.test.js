import { beforeEach, afterEach, test } from "node:test";
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { once } from "node:events";
import { History } from "../src/history.js";
import { DemoService } from "../src/demo-service.js";
import { createWebApp } from "../src/web-server.js";
import { eventKey, scenario } from "../src/domain.js";

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

test("sample encounters publish varied histories through the producer, preserve the active encounter, and can be selected independently", async () => {
  const original = await service.activeScenario();
  const result = await service.addSamples();
  assert.deepEqual(result, { encounters: 48, events_published: 126 });
  assert.equal(sent.length, 126);
  let state = await service.snapshot();
  assert.equal(state.scenario.id, original.id);
  assert.equal(state.encounters.length, 49);
  assert.equal(
    state.events.length,
    0,
    "publisher acknowledgment alone must never create received history",
  );
  assert.equal((await history.events()).length, 0);
  for (const event of sent) await receive(event);
  const cases = state.encounters.filter((row) => row.details.sample_key);
  const approval = cases.find(
    (row) => row.details.sample_key === "practice-v1-001",
  );
  const outside = cases.find(
    (row) => row.details.sample_key === "practice-v1-005",
  );
  const required = cases.find(
    (row) => row.details.sample_key === "practice-v1-002",
  );
  assert.equal(new Set(cases.map((row) => row.details.payer)).size, 4);
  assert.equal(new Set(cases.map((row) => row.details.procedure)).size, 6);
  assert.equal(cases.filter((row) => row.claim.status === "paid").length, 4);
  assert.equal(cases.filter((row) => row.claim.status === "pending").length, 4);
  for (const status of ["paid", "pending"]) {
    const current = cases.find((row) => row.claim.status === status);
    await service.selectScenario(current.id);
    await service.pull(current.id);
    await assert.rejects(service.review(current.id), { status: 409 });
    assert.equal((await service.snapshot()).review, null);
    assert.equal((await service.snapshot()).scenario.claim.denial_reason, null);
  }
  await service.selectScenario(approval.id);
  state = await service.snapshot();
  assert.equal(state.events.length, 4);
  assert.equal(
    state.pull,
    null,
    "selecting an encounter must not fabricate a Flow load",
  );
  await service.pull(approval.id);
  assert.equal(
    (await service.review(approval.id)).decision.recommendation,
    "dispute_candidate",
  );
  await service.selectScenario(outside.id);
  await service.pull(outside.id);
  assert.equal(
    (await service.review(outside.id)).decision.recommendation,
    "manual_review",
  );
  await service.selectScenario(required.id);
  await service.pull(required.id);
  assert.equal((await service.review(required.id)).decision.status, "required");
  assert.equal(
    (await service.snapshot()).scenario.claim.denial_reason,
    "missing_authorization",
  );
  await service.selectScenario(approval.id);
  assert.equal(
    (await service.snapshot()).review.decision.recommendation,
    "dispute_candidate",
  );
  assert.deepEqual(await service.addSamples(), {
    encounters: 48,
    events_published: 0,
  });
  assert.equal(
    sent.length,
    126,
    "adding the samples twice must not duplicate cases or decisions",
  );
  assert.equal((await service.snapshot()).scenario.id, approval.id);
  await assert.rejects(service.selectScenario(randomUUID()), { status: 404 });
});

test("sample preparation can resume a failed delivery using its original event instead of duplicating data", async () => {
  const original = await service.activeScenario();
  const publisher = service.publisher;
  service.publisher = async () => {
    throw new Error("broker unavailable");
  };
  await assert.rejects(service.addSamples(), { status: 502 });
  const attempts = (await history.pool.query("SELECT * FROM ui_publications"))
    .rows;
  assert.equal(attempts.length, 1);
  assert.equal(attempts[0].publication_state, "unconfirmed");
  service.publisher = publisher;
  await service.addSamples();
  assert.equal(sent[0].event_id, attempts[0].event_id);
  assert.equal(sent.length, 126);
  assert.equal((await service.snapshot()).scenario.id, original.id);
  assert.equal(
    (
      await history.pool.query(
        "SELECT COUNT(*)::int AS count FROM ui_scenarios",
      )
    ).rows[0].count,
    49,
  );
});

test("new evidence previews match each immutable decision and cannot be borrowed by another scenario", async () => {
  const current = await service.activeScenario();
  await service.send(current.id, {
    ...approve,
    authorization_number: "AUTH-CUSTOM",
    valid_from: "2026-10-10",
    valid_to: "2026-10-20",
  });
  await receive(sent[0]);
  const approval = await service.evidence(current.id, sent[0].event_id);
  assert.equal(approval.kind, "decision-document");
  assert.match(approval.content, /AUTH-CUSTOM/);
  assert.match(approval.content, /2026-10-10 through 2026-10-20/);
  assert.equal(
    createHash("sha256").update(approval.content).digest("hex"),
    sent[0].evidence.sha256,
  );
  await service.send(current.id, { status: "denied" });
  await receive(sent[1]);
  const denial = await service.evidence(current.id, sent[1].event_id);
  assert.match(denial.content, /decision: denied/);
  assert.match(denial.content, /does not grant an authorization/);
  assert.doesNotMatch(denial.content, /AUTH-CUSTOM|Coverage:/);
  assert.notEqual(sent[0].evidence.uri, sent[1].evidence.uri);
  await service.retry(current.id, sent[0].event_id);
  assert.deepEqual(
    await service.evidence(current.id, sent[0].event_id),
    approval,
  );
  const next = await service.startScenario();
  await assert.rejects(service.evidence(next.id, sent[0].event_id), {
    status: 404,
  });
  await assert.rejects(service.evidence(current.id, "invalid"), {
    status: 400,
  });
});

test("legacy evidence retains its original content and is explicitly identified as a reused fixture", async () => {
  const current = await service.activeScenario();
  const legacy = { ...scenario()[0], ...current.identity };
  delete legacy.account_id;
  delete legacy.encounter_id;
  await receive(legacy);
  const evidence = await service.evidence(current.id, legacy.event_id);
  assert.equal(evidence.kind, "legacy-fixture");
  assert.equal(evidence.status, "required");
  assert.equal(
    evidence.content,
    readFileSync(new URL("../fixtures/evidence.txt", import.meta.url), "utf8"),
  );
  assert.equal(
    (await service.snapshot()).events[0].payload.evidence.sha256,
    legacy.evidence.sha256,
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
    await service.send(state.scenario.id, approve);
    await receive(sent[0]);
    const evidenceResponse = await fetch(
      `${base}/api/scenarios/${state.scenario.id}/events/${sent[0].event_id}/evidence`,
    );
    assert.equal(evidenceResponse.status, 200);
    const document = await evidenceResponse.json();
    assert.equal(document.kind, "decision-document");
    assert.match(document.content, /AUTH-UI-001/);
    assert.equal(
      (
        await fetch(
          `${base}/api/scenarios/${state.scenario.id}/events/${randomUUID()}/evidence`,
        )
      ).status,
      404,
    );
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
