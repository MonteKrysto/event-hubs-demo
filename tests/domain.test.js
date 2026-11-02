import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  ROOT,
  eventKey,
  interpret,
  readJson,
  scenario,
  validate,
} from "../src/domain.js";

const crosswalk = readJson(`${ROOT}fixtures/crosswalk.json`);
const claims = readJson(`${ROOT}fixtures/claims.json`);

test("scenario validates and Flow matches approval, identity, and service date", () => {
  const events = scenario();
  events.forEach(validate);
  assert.equal(new Set(events.map(eventKey)).size, 1);
  const decision = interpret(events, crosswalk, claims).decisions[0];
  assert.equal(decision.recommendation, "dispute_candidate");
  assert.equal(decision.authorization_number, "AUTH-DEMO-2026");
  assert.equal(decision.event_id, events.at(-1).event_id);
});

test("latest source sequence wins over arrival order", () => {
  assert.equal(
    interpret(scenario().reverse(), crosswalk, claims).decisions[0].status,
    "approved",
  );
});

test("newer denial supersedes an older approval", () => {
  const events = scenario();
  events.push({
    ...events.at(-1),
    event_id: randomUUID(),
    source_sequence: 5,
    status: "denied",
  });
  assert.equal(
    interpret(events, crosswalk, claims).decisions[0].recommendation,
    "manual_review",
  );
});

test("missing and wrong-tenant crosswalks do not guess an identity", () => {
  for (const mapping of [
    [],
    [{ ...crosswalk[0], tenant_id: "other-hospital" }],
  ]) {
    const report = interpret(scenario(), mapping, claims);
    assert.equal(report.decisions[0].recommendation, "manual_review");
    assert.equal(report.unresolved_identities.length, 1);
  }
});

test("duplicate or many-to-one mappings fail instead of choosing arbitrarily", () => {
  assert.throws(
    () => interpret(scenario(), [...crosswalk, ...crosswalk], claims),
    /Ambiguous/,
  );
  const events = scenario();
  events.push({
    ...events.at(-1),
    event_id: randomUUID(),
    source_encounter_id: "OTHER",
  });
  assert.throws(
    () =>
      interpret(
        events,
        [...crosswalk, { ...crosswalk[0], source_encounter_id: "OTHER" }],
        claims,
      ),
    /Multiple source/,
  );
});

test("coverage date and denial reason must match", () => {
  for (const claim of [
    { ...claims[0], service_date: "2026-11-01" },
    { ...claims[0], denial_reason: "other" },
  ]) {
    assert.equal(
      interpret(scenario(), crosswalk, [claim]).decisions[0].recommendation,
      "manual_review",
    );
  }
  for (const service_date of ["2026-10-01", "2026-10-31"]) {
    assert.equal(
      interpret(scenario(), crosswalk, [{ ...claims[0], service_date }])
        .decisions[0].recommendation,
      "dispute_candidate",
    );
  }
});

test("paid and pending claims cannot become dispute candidates even when an approval exists", () => {
  for (const status of ["paid", "pending"]) {
    const decision = interpret(scenario(), crosswalk, [
      { ...claims[0], status, denial_reason: null },
    ]).decisions[0];
    assert.equal(decision.recommendation, "not_applicable");
    assert.equal(decision.claim_status, status);
    assert.equal(
      decision.status,
      undefined,
      "claim status must not be presented as an authorization status",
    );
    assert.match(decision.reason, /no denial to evaluate/);
  }
  const deniedClaim = interpret(scenario(), crosswalk, [
    { ...claims[0], status: "denied" },
  ]).decisions[0];
  assert.equal(deniedClaim.claim_status, "denied");
  assert.equal(deniedClaim.status, "approved");
});

test("coverage and eligibility denials explain why approval alone does not address the denial", () => {
  for (const denial_reason of ["service_not_covered", "eligibility"]) {
    const decision = interpret(scenario(), crosswalk, [
      { ...claims[0], status: "denied", denial_reason },
    ]).decisions[0];
    assert.equal(decision.recommendation, "manual_review");
    assert.match(decision.reason, /does not address that denial reason/);
  }
});

test("approval requires auth number, real dates, and ordered coverage", () => {
  const approved = scenario().at(-1);
  const { authorization_number, ...missingNumber } = approved;
  assert.throws(() => validate(missingNumber));
  assert.throws(() => validate({ ...approved, valid_from: "not-a-date" }));
  assert.throws(() => validate({ ...approved, valid_from: "2026-02-30" }));
  assert.throws(
    () => validate({ ...approved, valid_from: "2026-11-01" }),
    /on or before/,
  );
});

test("timestamps require a timezone and unsupported schema versions are rejected", () => {
  for (const occurred_at of ["not-a-time", "2026-10-01T10:00:00"]) {
    assert.throws(() => validate({ ...scenario()[0], occurred_at }));
  }
  assert.throws(() => validate({ ...scenario()[0], schema_version: 2 }));
});
