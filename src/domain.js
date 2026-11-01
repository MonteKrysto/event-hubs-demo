import { createHash, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';

export const ROOT = fileURLToPath(new URL('../', import.meta.url));
export const readJson = (path) => JSON.parse(readFileSync(path, 'utf8'));
const ajv = new Ajv2020({ allErrors: true });
addFormats(ajv);
const validateSchema = ajv.compile(readJson(`${ROOT}schemas/prior-authorization.v1.json`));

export class InvalidEvent extends Error {}

export function validate(event) {
  if (!validateSchema(event)) {
    throw new InvalidEvent(ajv.errorsText(validateSchema.errors, { separator: '; ' }));
  }
  if (event.valid_from && event.valid_to && event.valid_from > event.valid_to) {
    throw new InvalidEvent('valid_from must be on or before valid_to');
  }
}

export function eventKey(event) {
  // JSON avoids ambiguous delimiters. A tenant's encounter stays on one partition.
  return JSON.stringify([event.tenant_id, event.source_encounter_id]);
}

export function scenario() {
  const digest = createHash('sha256').update(readFileSync(`${ROOT}fixtures/evidence.txt`)).digest('hex');
  return ['required', 'attempted', 'submitted', 'approved'].map((status, index) => {
    const event = {
      event_id: randomUUID(),
      event_type: 'humata.prior_authorization.state_changed',
      schema_version: 1,
      source: 'humata-demo',
      tenant_id: 'demo-hospital',
      source_account_id: 'H-ACCOUNT-001',
      source_encounter_id: 'H-ENC-001',
      source_sequence: index + 1,
      occurred_at: `2026-10-01T10:0${index + 1}:00+00:00`,
      published_at: new Date().toISOString(),
      status,
      evidence: { uri: `humata://evidence/${digest}`, sha256: digest },
    };
    if (status === 'approved') {
      Object.assign(event, { authorization_number: 'AUTH-DEMO-2026', valid_from: '2026-10-01', valid_to: '2026-10-31' });
    }
    validate(event);
    return event;
  });
}

const sourceIdentity = (row) => [row.tenant_id, row.source_account_id, row.source_encounter_id];
const flowIdentity = (row) => [row.tenant_id, row.account_id, row.encounter_id];

export function interpret(events, crosswalk, claims) {
  const mappings = new Map();
  for (const row of crosswalk) {
    const key = JSON.stringify(sourceIdentity(row));
    if (mappings.has(key)) throw new Error(`Ambiguous crosswalk for ${key}`);
    mappings.set(key, row);
  }
  const latest = new Map();
  for (const event of events) {
    const key = JSON.stringify(sourceIdentity(event));
    if (!latest.has(key) || event.source_sequence > latest.get(key).source_sequence) latest.set(key, event);
  }
  const unresolved = [];
  const canonical = new Map();
  for (const [key, event] of latest) {
    const mapping = mappings.get(key);
    if (!mapping) {
      unresolved.push({ event_id: event.event_id, reason: 'missing_crosswalk', source_identity: sourceIdentity(event) });
      continue;
    }
    const identity = JSON.stringify(flowIdentity(mapping));
    if (canonical.has(identity)) throw new Error(`Multiple source encounters map to ${identity}; manual identity resolution required`);
    canonical.set(identity, event);
  }
  const decisions = claims.map((claim) => {
    const event = canonical.get(JSON.stringify(flowIdentity(claim)));
    const decision = { ...claim, recommendation: 'manual_review', reason: 'No mapped authorization history' };
    if (event) {
      Object.assign(decision, { status: event.status, event_id: event.event_id, evidence: event.evidence, reason: `Latest authorization status is ${event.status}` });
      if (event.status === 'approved') {
        decision.authorization_number = event.authorization_number;
        if (event.valid_from <= claim.service_date && claim.service_date <= event.valid_to && claim.denial_reason === 'missing_authorization') {
          Object.assign(decision, { recommendation: 'dispute_candidate', reason: 'Approval covers the service date; review the source evidence before disputing' });
        } else {
          decision.reason = 'Approval does not cover this service date or denial reason';
        }
      }
    }
    return decision;
  });
  return { decisions, unresolved_identities: unresolved };
}
