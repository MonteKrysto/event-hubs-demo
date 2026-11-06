export type AuthStatus =
  | "not_required"
  | "required"
  | "attempted"
  | "submitted"
  | "approved"
  | "denied";
export type Event = {
  event_id: string;
  event_type: string;
  schema_version: number;
  source: string;
  tenant_id: string;
  source_account_id: string;
  source_encounter_id: string;
  source_sequence: number;
  occurred_at: string;
  published_at: string;
  status: AuthStatus;
  authorization_number?: string;
  valid_from?: string;
  valid_to?: string;
  evidence: { uri: string; sha256: string };
};
export type HistoryEvent = {
  ingest_id: string;
  event_id: string;
  received_at: string;
  payload: Event;
  topic: string;
  partition_id: number;
  kafka_offset: string;
};
export type Publication = {
  event_id: string;
  source_sequence: string;
  event: Event;
  publication_state: "pending" | "published" | "unconfirmed";
  acknowledgment: { topic: string; partition: number; offset: string } | null;
  error: string | null;
  created_at: string;
  updated_at: string;
};
export type Decision = {
  recommendation: "manual_review" | "dispute_candidate" | "not_applicable";
  reason: string;
  status?: AuthStatus;
  authorization_number?: string;
  event_id?: string;
  evidence?: { uri: string; sha256: string };
};
export type Snapshot = {
  revision: number;
  instance_id: string;
  scenario: {
    id: string;
    number: string;
    created_at: string;
    details?: {
      patient_name?: string;
      patient_id?: string;
      procedure?: string;
      procedure_code?: string;
      payer?: string;
      provider?: string;
      billed_amount?: number;
      denial_date?: string | null;
      claim_date?: string;
      paid_amount?: number | null;
      practice_case?: string;
      sample_key?: string;
      sample_seed_complete?: boolean;
      suggested_authorization?: {
        authorization_number: string;
        valid_from: string;
        valid_to: string;
      };
    };
    identity: {
      tenant_id: string;
      source_encounter_id: string;
      source_account_id: string;
      encounter_id: string;
      account_id: string;
    };
    claim: {
      claim_id: string;
      service_date: string;
      status?: "denied" | "paid" | "pending";
      denial_reason: string | null;
      tenant_id: string;
      account_id: string;
      encounter_id: string;
    };
  };
  encounters: Snapshot["scenario"][];
  publications: Publication[];
  events: HistoryEvent[];
  receipts: {
    id: string;
    event_id: string;
    result: string;
    processed_at: string;
  }[];
  pull: { id: string; events: HistoryEvent[]; pulled_at: string } | null;
  review: {
    id: string;
    pull_id: string;
    decision: Decision;
    reviewed_at: string;
  } | null;
  receiver: { status: string; last_seen: string | null };
  broker: { status: string; checked_at: string | null };
  database: { status: string };
  integration?: {
    controls: {
      receiver_available: boolean;
      publisher_paused: boolean;
      api_available: boolean;
      skip_next_webhook: boolean;
      automatic_recovery: boolean;
      recovery_cursor: string;
      last_recovery_at: string | null;
      last_recovery_attempt_at: string | null;
      recovery_error: string | null;
    };
    stats: { received: number; recovered: number; duplicates: number; pending: number; missing: number };
    inbox: { event_id: string; received_via: "webhook" | "api"; received_at: string }[];
    webhooks: { event_id: string; source_sequence: string; state: "queued" | "delivered" | "failed" | "skipped"; attempts: number; last_error: string | null }[];
    activity: { id: string; event_id: string | null; kind: string; message: string; created_at: string }[];
  };
};
