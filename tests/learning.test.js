import { test } from "node:test";
import assert from "node:assert/strict";
import { ROOT, interpret, readJson, scenario } from "../src/domain.js";
import {
  latestEvent,
  learningState,
  reviewChecks,
} from "../web/src/learning.ts";

const identity = readJson(`${ROOT}fixtures/crosswalk.json`)[0];
const claim = readJson(`${ROOT}fixtures/claims.json`)[0];
function initial() {
  return {
    scenario: { id: "demo", identity, claim },
    publications: [],
    events: [],
    pull: null,
    review: null,
  };
}
function publication(event, acknowledgment = { offset: "1" }) {
  return {
    event_id: event.event_id,
    event,
    publication_state: acknowledgment ? "published" : "pending",
    acknowledgment,
    created_at: new Date().toISOString(),
  };
}
function received(event) {
  return { event_id: event.event_id, payload: event };
}
function load(state) {
  state.pull = { id: "pull-1", events: structuredClone(state.events) };
}
function evaluate(state) {
  state.review = {
    pull_id: state.pull.id,
    decision: interpret(
      state.pull.events.map((row) => row.payload),
      [identity],
      [claim],
    ).decisions[0],
  };
}

test("guidance waits for real history, then asks for an explicit load and evaluation", () => {
  const state = initial();
  const approval = scenario().at(-1);
  assert.equal(learningState(state).next.action, "approve");
  state.publications = [publication(approval)];
  let journey = learningState(state);
  assert.equal(journey.next.action, "wait");
  assert.deepEqual(
    journey.stages.map((stage) => stage.done),
    [true, true, false, false, false],
  );
  state.events.push(received(approval));
  journey = learningState(state);
  assert.equal(journey.next.action, "load");
  assert.equal(journey.inFlow, false);
  load(state);
  assert.equal(learningState(state).next.action, "review");
  evaluate(state);
  journey = learningState(state);
  assert.equal(journey.next.action, "explore");
  assert.ok(journey.stages.every((stage) => stage.done));
});

test("a newer decision makes loaded data and its evaluation visibly stale even before delivery", () => {
  const state = initial();
  const approval = scenario().at(-1);
  state.publications.push(publication(approval));
  state.events.push(received(approval));
  load(state);
  evaluate(state);
  const denial = {
    ...approval,
    event_id: "new-event",
    source_sequence: 5,
    status: "denied",
  };
  state.publications.push(publication(denial, null));
  let journey = learningState(state);
  assert.deepEqual(
    journey.stages.map((stage) => stage.done),
    [true, false, false, false, false],
  );
  assert.equal(journey.reviewStale, true);
  assert.equal(journey.loaded.status, "approved");
  assert.equal(journey.next.action, "wait");
  state.events.push(received(denial));
  journey = learningState(state);
  assert.equal(journey.next.action, "load");
  assert.equal(journey.newHistory, 1);
  assert.equal(state.review.decision.recommendation, "dispute_candidate");
  assert.ok(reviewChecks(state).every((check) => check.result === "pass"));
  load(state);
  state.review = null;
  assert.equal(learningState(state).next.action, "review");
  evaluate(state);
  assert.equal(learningState(state).currentReview, true);
  assert.equal(state.review.decision.recommendation, "manual_review");
  assert.equal(reviewChecks(state)[1].result, "fail");
});

test("replaying an older event does not replace the latest source decision or stale an evaluation", () => {
  const state = initial();
  const events = scenario();
  state.events = [received(events[3]), received(events[0])];
  state.publications = [publication(events[0]), publication(events[3])];
  load(state);
  evaluate(state);
  assert.equal(latestEvent(state.events).event_id, events[3].event_id);
  assert.equal(learningState(state).reviewStale, false);
  assert.equal(learningState(state).latest.event_id, events[3].event_id);
  assert.equal(learningState(state).newHistory, 0);
});

test("unconfirmed or abandoned pending deliveries offer inspection, but actual receipt proves delivery", () => {
  const state = initial();
  const event = scenario()[0];
  const attempt = publication(event, null);
  state.publications.push(attempt);
  attempt.created_at = new Date(Date.now() - 60000).toISOString();
  assert.equal(learningState(state).next.action, "retry");
  attempt.created_at = new Date().toISOString();
  attempt.publication_state = "unconfirmed";
  assert.equal(learningState(state).next.action, "retry");
  state.events.push(received(event));
  assert.equal(learningState(state).next.action, "load");
  assert.equal(learningState(state).accepted, true);
});

test("the checklist distinguishes identity mismatch, date coverage, and a reference from reviewed evidence", () => {
  const state = initial();
  const event = scenario().at(-1);
  state.events = [received(event)];
  load(state);
  assert.ok(reviewChecks(state).every((check) => check.result === "pass"));
  assert.match(reviewChecks(state)[3].detail, /person still needs to review/);
  const loaded = state.pull.events[0].payload;
  loaded.valid_from = "2026-11-01";
  loaded.valid_to = "2026-11-30";
  assert.equal(reviewChecks(state)[2].result, "fail");
  loaded.source_encounter_id = "another-visit";
  assert.equal(reviewChecks(state)[0].result, "fail");
  delete loaded.evidence;
  assert.equal(reviewChecks(state)[3].result, "fail");
});

test("loaded authorization on a paid claim does not prompt denial evaluation", () => {
  const state = initial();
  state.scenario = {
    ...state.scenario,
    claim: { ...claim, status: "paid", denial_reason: null },
  };
  state.events = [received(scenario().at(-1))];
  load(state);
  assert.equal(learningState(state).next.action, "wait");
  assert.match(learningState(state).stages[4].text, /no denial/);
});

test("an evaluation of an empty load does not invent a newer decision or successful checks", () => {
  const state = initial();
  load(state);
  evaluate(state);
  assert.equal(learningState(state).reviewStale, false);
  assert.equal(learningState(state).next.action, "approve");
  assert.ok(
    reviewChecks(state).every((check) => check.result === "unavailable"),
  );
});
