import {
  ArrowRight,
  Check,
  CheckCircle2,
  ClipboardCheck,
  Clock3,
  Database,
  HeartPulse,
  LoaderCircle,
  Radio,
  Search,
  ShieldCheck,
  XCircle,
} from "lucide-react";
import type { Snapshot } from "./types";
import { learningState, reviewChecks } from "./learning";

const authorizationLabels = {
  required: "Required — approval still needed",
  not_required: "Not required",
  attempted: "Attempt started — no approval yet",
  submitted: "Submitted — awaiting a decision",
  approved: "Approved",
  denied: "Denied",
};

export function AuthorizationComparison({ state }: { state: Snapshot }) {
  const journey = learningState(state);
  return (
    <section
      className="authorization-comparison"
      aria-label="Humata and Flow authorization statuses"
      aria-live="polite"
    >
      <div className="source-authorization">
        <span>HUMATA’S LATEST STATUS</span>
        <strong>
          {journey.latest
            ? authorizationLabels[journey.latest.status]
            : "No decision recorded"}
        </strong>
        <small>
          {journey.latest
            ? `Update ${journey.latest.source_sequence} · ${new Date(journey.latest.occurred_at).toLocaleString()}`
            : "Choose an authorization action in Humata."}
        </small>
      </div>
      <div
        className={`loaded-authorization ${journey.latest && !journey.inFlow ? "behind" : ""}`}
      >
        <span>LOADED IN FLOW</span>
        <strong>
          {journey.loaded
            ? authorizationLabels[journey.loaded.status]
            : "No authorization loaded"}
        </strong>
        <small>
          {journey.loaded
            ? `Update ${journey.loaded.source_sequence} · loaded ${new Date(state.pull!.pulled_at).toLocaleString()}`
            : "Use Load latest update to bring the decision into Flow."}
        </small>
      </div>
    </section>
  );
}

export function JourneyProgress({ state }: { state: Snapshot | null }) {
  if (!state) return null;
  const journey = learningState(state);
  const icons = [HeartPulse, Radio, Database, ArrowRight, ClipboardCheck];
  return (
    <section className="journey-section" aria-label="Latest update journey">
      <div className="journey-caption">
        <strong>
          {journey.latest
            ? `Following update ${journey.latest.source_sequence}`
            : "The event’s journey"}
        </strong>
        <span>
          {journey.loaded
            ? `Flow has loaded through update ${journey.loaded.source_sequence}.`
            : "Flow has not loaded authorization data yet."}
        </span>
      </div>
      <div className="pipeline" aria-label="Integration progress">
        {journey.stages.map((stage, index) => {
          const Icon = icons[index];
          return (
            <div
              className={`pipeline-stage ${stage.done ? "done" : ""} ${stage.attention ? "attention" : ""}`}
              key={stage.title}
            >
              <div className="pipeline-icon">
                <Icon size={17} />
              </div>
              <div>
                <strong>{stage.title}</strong>
                <span>{stage.text}</span>
              </div>
              {index < 4 && <ArrowRight size={14} className="pipeline-arrow" />}
            </div>
          );
        })}
      </div>
    </section>
  );
}

export function ActionFeedback({
  state,
  busy,
  onDetails,
}: {
  state: Snapshot | null;
  busy: string;
  onDetails: () => void;
}) {
  if (!state) return null;
  const journey = learningState(state);
  const titles = {
    required: "Authorization is required.",
    not_required: "No authorization is needed.",
    attempted: "Authorization attempt started.",
    submitted: "Authorization request submitted.",
    approved: "Authorization approved.",
    denied: "Authorization denied.",
  };
  const saving = Object.hasOwn(titles, busy);
  if (!journey.latest && !saving) return null;
  return (
    <div
      className={`action-feedback ${journey.deliveryUnconfirmed ? "unconfirmed" : ""}`}
      role="status"
    >
      {saving || (!journey.saved && !journey.deliveryUnconfirmed) ? (
        <LoaderCircle size={19} className="spin" />
      ) : (
        <CheckCircle2 size={19} />
      )}
      <div>
        <strong>
          {saving ? "Saving your update…" : titles[journey.latest!.status]}
        </strong>
        <p>
          {saving
            ? "The authorization update is being saved and shared."
            : journey.deliveryUnconfirmed
              ? "Your decision is saved in Humata, but sharing could not be confirmed."
              : journey.saved
                ? journey.inFlow
                  ? "Shared successfully. Flow has loaded this update."
                  : "Shared successfully. In Flow, choose Load latest update."
                : "Sharing this update… Flow can load it once delivery finishes."}
        </p>
        {(journey.deliveryUnconfirmed ||
          (!journey.saved && state.receiver.status !== "connected")) && (
          <button className="text-button" onClick={onDetails}>
            Check delivery
          </button>
        )}
      </div>
    </div>
  );
}

export function FlowFreshness({ state }: { state: Snapshot }) {
  const journey = learningState(state);
  if (!journey.latest || journey.inFlow) return null;
  return (
    <div className="flow-freshness" role="status">
      <Clock3 size={18} />
      <div>
        <strong>
          {journey.saved
            ? "New authorization update available"
            : "An update is being shared"}
        </strong>
        <p>
          {journey.saved
            ? journey.loaded
              ? "Flow still shows the earlier decision. Load the latest update to see the change."
              : "Load the latest update to see Humata’s decision here."
            : "Flow will keep its current information until sharing finishes and you load the update."}
        </p>
      </div>
    </div>
  );
}

export function ReviewExplanation({ state }: { state: Snapshot }) {
  if (
    !state.review ||
    state.review.decision.recommendation === "not_applicable"
  )
    return null;
  const journey = learningState(state);
  const candidate =
    state.review.decision.recommendation === "dispute_candidate";
  return (
    <section
      id="review-result"
      className={`review-result detailed-review ${journey.reviewStale ? "stale-review" : candidate ? "candidate" : "manual"}`}
      aria-live="polite"
    >
      <div className="review-result-top">
        <div className="result-icon">
          {candidate ? <ShieldCheck size={21} /> : <Search size={21} />}
        </div>
        <div>
          <span className="eyebrow">
            {journey.reviewStale
              ? "EARLIER DATA EVALUATED"
              : "EVALUATION COMPLETE"}
          </span>
          <h4>
            {candidate
              ? "Possible dispute — human review needed"
              : "Manual review needed"}
          </h4>
        </div>
      </div>
      {journey.reviewStale && (
        <p className="stale-review-note">
          A newer decision exists. This result still uses loaded update{" "}
          {journey.loaded?.source_sequence || "—"}. Load the saved history and
          evaluate again to use newer information.
        </p>
      )}
      <p>{state.review.decision.reason}</p>
      <details className="evaluation-explanation">
        <summary>Why this result?</summary>
        <ul className="review-checklist">
          {reviewChecks(state).map((check) => (
            <li className={check.result} key={check.label}>
              {check.result === "pass" ? (
                <CheckCircle2 size={17} />
              ) : check.result === "fail" ? (
                <XCircle size={17} />
              ) : (
                <Clock3 size={17} />
              )}
              <div>
                <strong>{check.label}</strong>
                <span>{check.detail}</span>
              </div>
            </li>
          ))}
        </ul>
      </details>
      <div className="human-next-step">
        <strong>Next step: review the evidence before taking action.</strong>
        <span>The claim remains denied; no appeal has been submitted.</span>
      </div>
      <small>
        Based on update {journey.loaded?.source_sequence || "—"} loaded at{" "}
        {new Date(state.pull!.pulled_at).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        })}
        .
      </small>
    </section>
  );
}
