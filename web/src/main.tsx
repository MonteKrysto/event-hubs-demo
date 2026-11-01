import { StrictMode, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  ArrowDownToLine,
  ArrowRight,
  ArrowUpRight,
  BellRing,
  Check,
  CheckCheck,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  CircleHelp,
  ClipboardCheck,
  Clock3,
  Database,
  FileCheck2,
  FileClock,
  FileText,
  Fingerprint,
  GitBranch,
  HeartPulse,
  LayoutDashboard,
  LoaderCircle,
  Radio,
  RefreshCw,
  Search,
  Send,
  ShieldCheck,
  Sparkles,
  Unplug,
  X,
  XCircle,
} from "lucide-react";
import type { AuthStatus, Publication, Snapshot } from "./types";
import "./styles.css";

const labels: Record<AuthStatus, string> = {
  not_required: "Not required",
  required: "Required",
  attempted: "Attempted",
  submitted: "Submitted",
  approved: "Approved",
  denied: "Denied",
};
const eventTitles: Record<AuthStatus, string> = {
  not_required: "Authorization not required",
  required: "Authorization required",
  attempted: "Authorization attempted",
  submitted: "Request submitted",
  approved: "Authorization approved",
  denied: "Authorization denied",
};
const time = (date: string) =>
  new Date(date).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
const date = (value: string) =>
  new Date(`${value.slice(0, 10)}T12:00:00`).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });

async function request<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(
    path,
    body === undefined
      ? undefined
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
  );
  const data = await res.json();
  if (!res.ok)
    throw new Error(data.error || "The request could not be completed.");
  return data as T;
}

function Status({ status }: { status?: AuthStatus }) {
  return (
    <span className={`status ${status || "empty"}`}>
      <span />
      {status ? labels[status] : "No decision yet"}
    </span>
  );
}

function Empty({
  icon: Icon = Radio,
  title,
  children,
}: {
  icon?: typeof Radio;
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="empty-state">
      <div className="empty-icon">
        <Icon size={21} />
      </div>
      <strong>{title}</strong>
      <p>{children}</p>
    </div>
  );
}

function App() {
  const [state, setState] = useState<Snapshot | null>(null);
  const [view, setView] = useState(
    location.pathname === "/humata"
      ? "humata"
      : location.pathname === "/flow"
        ? "flow"
        : "overview",
  );
  const [live, setLive] = useState(false);
  const [historyAvailable, setHistoryAvailable] = useState(true);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState("");
  const [guide, setGuide] = useState(false);
  const [evidence, setEvidence] = useState<string | null>(null);
  const [authNumber, setAuthNumber] = useState("AUTH-DEMO-2026");
  const [validFrom, setValidFrom] = useState("2026-10-01");
  const [validTo, setValidTo] = useState("2026-10-31");
  const [expanded, setExpanded] = useState<string | null>(null);
  const modalTrigger = useRef<HTMLElement | null>(null);

  useEffect(() => {
    const receive = (next: Snapshot) => {
      setState((current) =>
        !current || current.instance_id !== next.instance_id || next.revision >= current.revision ? next : current,
      );
      setHistoryAvailable(true);
    };
    request<Snapshot>("/api/state")
      .then(receive)
      .catch((e: Error) => setError(e.message));
    const stream = new EventSource("/api/stream");
    stream.onopen = () => setLive(true);
    stream.onerror = () => setLive(false);
    stream.addEventListener("state", (event) =>
      receive(JSON.parse((event as MessageEvent<string>).data) as Snapshot),
    );
    stream.addEventListener("unavailable", () => setHistoryAvailable(false));
    return () => stream.close();
  }, []);
  useEffect(() => {
    const pop = () => setView(location.pathname.slice(1) || "overview");
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, []);
  useEffect(() => {
    setExpanded(null);
    setAuthNumber("AUTH-DEMO-2026");
    setValidFrom("2026-10-01");
    setValidTo("2026-10-31");
  }, [state?.scenario.id]);
  useEffect(() => {
    if (!guide && evidence === null) return;
    const previous = modalTrigger.current;
    const overflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setGuide(false);
        setEvidence(null);
      }
      if (event.key === "Tab") {
        const controls = document.querySelectorAll<HTMLElement>(
          ".modal button:not(:disabled), .modal a[href], .modal input",
        );
        const first = controls[0];
        const last = controls[controls.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last?.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first?.focus();
        }
      }
    };
    window.addEventListener("keydown", close);
    return () => {
      window.removeEventListener("keydown", close);
      document.body.style.overflow = overflow;
      previous?.focus();
    };
  }, [guide, evidence]);

  const navigate = (next: string) => {
    history.pushState(null, "", next === "overview" ? "/" : `/${next}`);
    setView(next);
    window.scrollTo(0, 0);
  };
  const act = async (key: string, path: string, body: unknown = {}) => {
    setBusy(key);
    setError("");
    try {
      await request(path, body);
      const next = await request<Snapshot>("/api/state");
      setState((current) =>
        !current || current.instance_id !== next.instance_id || next.revision >= current.revision ? next : current,
      );
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy("");
    }
  };
  const send = (status: AuthStatus) => {
    if (!state) return;
    return act(status, `/api/scenarios/${state.scenario.id}/events`, {
      status,
      authorization_number: authNumber,
      valid_from: validFrom,
      valid_to: validTo,
    });
  };
  const showEvidence = async () => {
    modalTrigger.current = document.activeElement as HTMLElement | null;
    try {
      setEvidence(
        (await request<{ content: string }>("/api/evidence")).content,
      );
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const scenario = state?.scenario;
  const currentEvent = state?.publications[0]?.event;
  const pulledEvents = state?.pull?.events || [];
  const pulled = pulledEvents.at(-1)?.payload;
  const newEvents = state
    ? state.events.filter(
        (row) => !pulledEvents.some((old) => old.event_id === row.event_id),
      ).length
    : 0;
  const online = live && historyAvailable;
  const locked = !!busy || !online || !state;

  const activity = (publication: Publication) => {
    const stored = state?.events.find(
      (row) => row.event_id === publication.event_id,
    );
    const pending = publication.publication_state === "pending";
    const failed =
      !stored &&
      (publication.publication_state === "unconfirmed" ||
        (pending &&
          Date.now() - new Date(publication.created_at).getTime() > 30000));
    const expandedHere = expanded === publication.event_id;
    return (
      <div
        className={`activity-item ${failed ? "activity-error" : ""}`}
        key={publication.event_id}
      >
        <div
          className={`timeline-dot ${stored ? "complete" : failed ? "failed" : ""}`}
        >
          {stored ? <Check size={12} /> : failed ? <X size={12} /> : <span />}
        </div>
        <button
          className="event-heading"
          onClick={() =>
            setExpanded(expandedHere ? null : publication.event_id)
          }
          aria-expanded={expandedHere}
        >
          <div>
            <strong>{eventTitles[publication.event.status]}</strong>
            <div className="event-meta">
              <span>Update {publication.event.source_sequence}</span>
              <span>·</span>
              <time>{time(publication.created_at)}</time>
            </div>
          </div>
          <span
            className={`delivery-label ${stored ? "saved" : failed ? "failed" : ""}`}
          >
            {stored
              ? "Saved to history"
              : failed
                ? "Delivery unconfirmed"
                : pending
                  ? "Sending…"
                  : "Broker acknowledged"}
            <ChevronDown size={13} className={expandedHere ? "rotated" : ""} />
          </span>
        </button>
        {expandedHere && (
          <div className="event-details">
            <div className="delivery-steps">
              <span>
                <CheckCircle2 size={13} /> Event created
              </span>
              <span>
                {publication.acknowledgment ? (
                  <CheckCircle2 size={13} />
                ) : (
                  <Clock3 size={13} />
                )}{" "}
                Broker acknowledged
              </span>
              <span>
                {stored ? <CheckCircle2 size={13} /> : <Clock3 size={13} />}{" "}
                Saved to history
              </span>
            </div>
            <dl>
              <div>
                <dt>Event ID</dt>
                <dd>{publication.event_id}</dd>
              </div>
              {stored && (
                <div>
                  <dt>Broker location</dt>
                  <dd>
                    Partition {stored.partition_id} · Offset{" "}
                    {stored.kafka_offset}
                  </dd>
                </div>
              )}
            </dl>
            <p className="receipt-note">
              {state?.receipts.filter(
                (row) => row.event_id === publication.event_id,
              ).length || 0}{" "}
              receiver receipts · replays keep one history event
            </p>
            <details>
              <summary>View event payload</summary>
              <pre>{JSON.stringify(publication.event, null, 2)}</pre>
            </details>
            {(failed || (!pending && stored)) && (
              <button
                className="text-button"
                disabled={locked}
                onClick={() =>
                  act(
                    "retry",
                    `/api/scenarios/${scenario!.id}/events/${publication.event_id}/retry`,
                  )
                }
              >
                <RefreshCw size={13} />
                {failed ? "Retry this event" : "Replay this event"}
              </button>
            )}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a
          className="brand"
          href="/"
          onClick={(event) => {
            event.preventDefault();
            navigate("overview");
          }}
        >
          <span className="brand-icon">
            <GitBranch size={23} />
          </span>
          <span>
            fiber<span className="brand-period">.</span>
          </span>
          <span className="lab-label">LAB</span>
        </a>
        <div className="workspace-label">INTEGRATION WORKSPACE</div>
        <nav aria-label="Demo applications">
          <button
            className={view === "overview" ? "nav-link active" : "nav-link"}
            onClick={() => navigate("overview")}
          >
            <LayoutDashboard size={17} />
            Overview
            <span className="nav-dot" />
          </button>
          <button
            className={view === "humata" ? "nav-link active" : "nav-link"}
            onClick={() => navigate("humata")}
          >
            <HeartPulse size={17} />
            Humata
            <ArrowUpRight size={13} className="nav-end" />
          </button>
          <button
            className={view === "flow" ? "nav-link active" : "nav-link"}
            onClick={() => navigate("flow")}
          >
            <GitBranch size={17} />
            Flow
            <ArrowUpRight size={13} className="nav-end" />
          </button>
        </nav>
        <div className="sidebar-note">
          <span className="eyebrow">THE FIRST CONNECTION</span>
          <p>
            One decision.
            <br />
            Shared context.
            <br />
            <strong>A better next step.</strong>
          </p>
          <div className="mini-path">
            <span>Humata</span>
            <ArrowRight size={13} />
            <span>Flow</span>
          </div>
        </div>
        <div className="sidebar-bottom">
          <button
            className="guide-button"
            onClick={() => {
              modalTrigger.current =
                document.activeElement as HTMLElement | null;
              setGuide(true);
            }}
          >
            <CircleHelp size={17} />
            How to run the demo
            <ArrowUpRight size={13} />
          </button>
          <div className="local-label">
            <span />
            Local environment <span className="local-code">v1.0</span>
          </div>
        </div>
      </aside>

      <main>
        <header className="topbar">
          <div className="breadcrumb">
            Workspace
            <ChevronRight size={13} />
            <strong>Prior authorization</strong>
          </div>
          <div className="topbar-right">
            <span className="synthetic-label">
              <ShieldCheck size={13} />
              Synthetic data only
            </span>
            <span className={`live-label ${online ? "" : "offline"}`}>
              <span />
              {online ? "Live updates" : "Reconnecting"}
            </span>
          </div>
        </header>
        <div className="page-content">
          <div className="page-heading">
            <div>
              <div className="eyebrow">HUMATA → FLOW</div>
              <h1>
                {view === "humata"
                  ? "Where a decision begins."
                  : view === "flow"
                    ? "Turn shared context into action."
                    : "From decision to action."}
              </h1>
              <p>
                {view === "humata"
                  ? "Publish an authorization update and follow its journey."
                  : view === "flow"
                    ? "Bring authorization history into a better-informed denial review."
                    : "Follow an authorization decision across two applications, one event at a time."}
              </p>
            </div>
            <button
              className="button secondary new-scenario"
              disabled={locked}
              onClick={() => act("new", "/api/scenarios")}
            >
              <RefreshCw size={15} className={busy === "new" ? "spin" : ""} />
              Start new scenario
            </button>
          </div>
          <div className="scenario-strip">
            <div>
              <Fingerprint size={15} />
              <strong>
                Scenario{" "}
                {scenario ? String(scenario.number).padStart(3, "0") : "—"}
              </strong>
              <span className="separator">/</span>
              <span>Synthetic outpatient encounter</span>
            </div>
            <span className="mono">
              {scenario?.identity.source_encounter_id ||
                "Connecting to the demo…"}
            </span>
          </div>
          {!online && (
            <div className="notice warning" role="status">
              <Unplug size={17} />
              {historyAvailable
                ? "Live updates are reconnecting. Actions will be available when the connection returns."
                : "The event history is unavailable. Check PostgreSQL; the last known data is shown."}
            </div>
          )}
          {error && (
            <div className="notice error" role="alert">
              <XCircle size={17} />
              <span>{error}</span>
              <button onClick={() => setError("")} aria-label="Dismiss error">
                <X size={16} />
              </button>
            </div>
          )}
          <div className="pipeline" aria-label="Integration progress">
            {[
              {
                title: "Humata",
                text: `${state?.publications.length || 0} updates created`,
                icon: HeartPulse,
                done: !!state?.publications.length,
              },
              {
                title: "Event Hubs",
                text: state?.publications.some((p) => p.acknowledgment)
                  ? "Delivery acknowledged"
                  : "Awaiting first event",
                icon: Radio,
                done: !!state?.publications.some((p) => p.acknowledgment),
              },
              {
                title: "Event history",
                text: `${state?.events.length || 0} events saved`,
                icon: Database,
                done: !!state?.events.length,
              },
              {
                title: "Flow pull",
                text: state?.pull
                  ? `${pulledEvents.length} events retrieved`
                  : "Waiting for your pull",
                icon: ArrowDownToLine,
                done: !!state?.pull,
              },
              {
                title: "Denial review",
                text: state?.review ? "Review complete" : "Ready when you are",
                icon: ClipboardCheck,
                done: !!state?.review,
              },
            ].map((stage, index) => (
              <div
                className={`pipeline-stage ${stage.done ? "done" : ""}`}
                key={stage.title}
              >
                <div className="pipeline-icon">
                  <stage.icon size={17} />
                </div>
                <div>
                  <strong>{stage.title}</strong>
                  <span>{stage.text}</span>
                </div>
                {index < 4 && (
                  <ChevronRight size={15} className="pipeline-arrow" />
                )}
              </div>
            ))}
          </div>

          <div
            className={`app-grid ${view === "overview" ? "" : "single-app"}`}
          >
            {view !== "flow" && (
              <section
                className="app-column humata"
                aria-label="Humata producer"
              >
                <div className="app-heading">
                  <div className="app-logo humata-logo">
                    <HeartPulse size={22} />
                  </div>
                  <div>
                    <h2>Humata</h2>
                    <p>Authorization workspace</p>
                  </div>
                  <span className="role-label">EVENT PRODUCER</span>
                </div>
                <div className="card encounter-card">
                  <div className="card-title">
                    <h3>Prior authorization</h3>
                    <Status status={currentEvent?.status} />
                  </div>
                  <p className="card-description">
                    Record a decision for this encounter. Each action publishes
                    a new event.
                  </p>
                  <div className="encounter-info">
                    <div>
                      <span>ENCOUNTER</span>
                      <strong>
                        {scenario?.identity.source_encounter_id || "—"}
                      </strong>
                    </div>
                    <div>
                      <span>SERVICE DATE</span>
                      <strong>
                        {scenario ? date(scenario.claim.service_date) : "—"}
                      </strong>
                    </div>
                  </div>
                  <div className="section-caption">
                    UPDATE AUTHORIZATION STATE
                  </div>
                  <div className="action-grid">
                    <button
                      disabled={locked}
                      onClick={() => send("required")}
                      className="action-button"
                    >
                      <ClipboardCheck size={17} />
                      <span>Authorization required</span>
                      {busy === "required" && (
                        <LoaderCircle size={14} className="spin" />
                      )}
                    </button>
                    <button
                      disabled={locked}
                      onClick={() => send("attempted")}
                      className="action-button"
                    >
                      <Search size={17} />
                      <span>Attempt authorization</span>
                      {busy === "attempted" && (
                        <LoaderCircle size={14} className="spin" />
                      )}
                    </button>
                    <button
                      disabled={locked}
                      onClick={() => send("submitted")}
                      className="action-button"
                    >
                      <Send size={17} />
                      <span>Submit request</span>
                      {busy === "submitted" && (
                        <LoaderCircle size={14} className="spin" />
                      )}
                    </button>
                    <button
                      disabled={locked}
                      onClick={() => send("not_required")}
                      className="action-button"
                    >
                      <ShieldCheck size={17} />
                      <span>Authorization not required</span>
                      {busy === "not_required" && (
                        <LoaderCircle size={14} className="spin" />
                      )}
                    </button>
                  </div>
                  <div className="approval-form">
                    <div className="form-caption">
                      <FileCheck2 size={15} />
                      Approval details<span>Used when approving</span>
                    </div>
                    <label className="full-field">
                      Authorization number
                      <input
                        value={authNumber}
                        onChange={(event) => setAuthNumber(event.target.value)}
                        maxLength={80}
                        disabled={!!busy}
                      />
                    </label>
                    <div className="date-fields">
                      <label>
                        Valid from
                        <input
                          type="date"
                          value={validFrom}
                          onChange={(event) => setValidFrom(event.target.value)}
                          disabled={!!busy}
                        />
                      </label>
                      <label>
                        Valid through
                        <input
                          type="date"
                          value={validTo}
                          onChange={(event) => setValidTo(event.target.value)}
                          disabled={!!busy}
                        />
                      </label>
                    </div>
                    <div className="decision-buttons">
                      <button
                        className="button approve"
                        disabled={
                          locked ||
                          !authNumber.trim() ||
                          !validFrom ||
                          !validTo ||
                          validFrom > validTo
                        }
                        onClick={() => send("approved")}
                      >
                        {busy === "approved" ? (
                          <LoaderCircle size={16} className="spin" />
                        ) : (
                          <CheckCircle2 size={16} />
                        )}
                        Approve authorization
                      </button>
                      <button
                        className="button deny"
                        disabled={locked}
                        onClick={() => send("denied")}
                      >
                        {busy === "denied" ? (
                          <LoaderCircle size={16} className="spin" />
                        ) : (
                          <XCircle size={16} />
                        )}
                        Deny
                      </button>
                    </div>
                  </div>
                  <div className="card-footnote">
                    <GitBranch size={13} />
                    Publishes to Event Hubs using the Kafka protocol.
                  </div>
                </div>
                <div className="card activity-card">
                  <div className="card-title">
                    <h3>
                      <Activity size={16} />
                      Publication activity
                    </h3>
                    <span className="count-label">
                      {state?.publications.length || 0} updates
                    </span>
                  </div>
                  {state?.publications.length ? (
                    <div className="timeline" aria-live="polite">
                      {state.publications.map(activity)}
                    </div>
                  ) : (
                    <Empty title="Your first event starts here">
                      Choose an authorization action above to follow its
                      delivery.
                    </Empty>
                  )}
                </div>
              </section>
            )}

            {view !== "humata" && (
              <section className="app-column flow" aria-label="Flow consumer">
                <div className="app-heading">
                  <div className="app-logo flow-logo">
                    <GitBranch size={22} />
                  </div>
                  <div>
                    <h2>Flow</h2>
                    <p>Account decision workspace</p>
                  </div>
                  <span className="role-label">HISTORY CONSUMER</span>
                </div>
                <div className="card claim-card">
                  <div className="card-title">
                    <h3>Claim denial</h3>
                    <span className="status denied">
                      <span />
                      Needs review
                    </span>
                  </div>
                  <div className="denial-reason">
                    <div className="denial-icon">
                      <FileClock size={22} />
                    </div>
                    <div>
                      <strong>Missing authorization</strong>
                      <p>
                        This claim was denied because authorization was not on
                        file.
                      </p>
                    </div>
                  </div>
                  <div className="claim-info">
                    <div>
                      <span>CLAIM</span>
                      <strong>{scenario?.claim.claim_id || "—"}</strong>
                    </div>
                    <div>
                      <span>SERVICE DATE</span>
                      <strong>
                        {scenario ? date(scenario.claim.service_date) : "—"}
                      </strong>
                    </div>
                  </div>
                </div>
                <div className="card flow-data-card">
                  <div className="card-title">
                    <h3>Authorization information</h3>
                    <span
                      className={`sync-label ${newEvents && state?.pull ? "outdated" : ""}`}
                    >
                      {!state?.pull
                        ? "Not pulled"
                        : newEvents
                          ? "New history available"
                          : "Up to date"}
                    </span>
                  </div>
                  <p className="card-description">
                    Pull saved history into Flow, then use it to review the
                    denial.
                  </p>
                  <button
                    className="button pull"
                    disabled={locked}
                    onClick={() =>
                      act("pull", `/api/scenarios/${scenario!.id}/pull`)
                    }
                  >
                    {busy === "pull" ? (
                      <LoaderCircle size={16} className="spin" />
                    ) : (
                      <ArrowDownToLine size={16} />
                    )}
                    Pull latest data
                    {newEvents > 0 && (
                      <span className="button-counter">{newEvents}</span>
                    )}
                    <ArrowRight size={16} className="button-end" />
                  </button>
                  {pulled ? (
                    <div className="pulled-authorization">
                      <div className="pulled-heading">
                        <span>Latest authorization</span>
                        <Status status={pulled.status} />
                      </div>
                      {pulled.authorization_number && (
                        <div className="auth-number">
                          {pulled.authorization_number}
                        </div>
                      )}
                      {pulled.valid_from && pulled.valid_to && (
                        <p className="coverage">
                          {date(pulled.valid_from)} – {date(pulled.valid_to)}
                        </p>
                      )}
                      <div className="mapping-note">
                        <CheckCheck size={14} />
                        Matched to this account and encounter
                      </div>
                      <button className="text-button" onClick={showEvidence}>
                        <FileText size={14} />
                        View demo evidence
                        <ArrowUpRight size={13} />
                      </button>
                    </div>
                  ) : (
                    <Empty
                      icon={FileClock}
                      title={
                        state?.pull
                          ? "No authorization history in this pull"
                          : "No authorization data pulled yet"
                      }
                    >
                      {state?.pull
                        ? "Publish an update in Humata, wait for it to arrive, then pull again."
                        : "Events can arrive in history while Flow waits for your next pull."}
                    </Empty>
                  )}
                  <div className="pull-footer">
                    <Clock3 size={13} />
                    {state?.pull
                      ? `Last pulled ${time(state.pull.pulled_at)} · ${pulledEvents.length} events`
                      : "Daily-style batch · triggered manually for this demo"}
                  </div>
                  {newEvents > 0 && state?.pull && (
                    <div className="new-history">
                      <BellRing size={14} />
                      {newEvents} new{" "}
                      {newEvents === 1 ? "event is" : "events are"} in history.
                      Pull again to update this account.
                    </div>
                  )}
                  <div className="review-section">
                    <button
                      className="button review"
                      disabled={locked || !state?.pull}
                      onClick={() =>
                        act("review", `/api/scenarios/${scenario!.id}/review`)
                      }
                    >
                      {busy === "review" ? (
                        <LoaderCircle size={16} className="spin" />
                      ) : (
                        <ClipboardCheck size={16} />
                      )}
                      Review denial
                    </button>
                    {!state?.pull && <span>Pull data to enable review</span>}
                  </div>
                  {state?.review && (
                    <div
                      className={`review-result ${state.review.decision.recommendation === "dispute_candidate" ? "candidate" : "manual"}`}
                      aria-live="polite"
                    >
                      <div className="result-icon">
                        {state.review.decision.recommendation ===
                        "dispute_candidate" ? (
                          <Sparkles size={19} />
                        ) : (
                          <Search size={19} />
                        )}
                      </div>
                      <div>
                        <span className="eyebrow">REVIEW RECOMMENDATION</span>
                        <h4>
                          {state.review.decision.recommendation ===
                          "dispute_candidate"
                            ? "Candidate for dispute review"
                            : "Manual review needed"}
                        </h4>
                        <p>{state.review.decision.reason}</p>
                        <small>
                          Based on the last pull ·{" "}
                          {time(state.review.reviewed_at)}
                          <br />A person reviews the evidence before taking
                          action.
                        </small>
                      </div>
                    </div>
                  )}
                </div>
                <div className="card incoming-card">
                  <div className="card-title">
                    <h3>
                      <Radio size={16} />
                      Incoming event activity
                    </h3>
                    <span className="small-live">
                      <span />
                      {online ? "Live" : "Paused"}
                    </span>
                  </div>
                  <p className="card-description">
                    Platform history updates here. Flow's account changes when
                    you pull.
                  </p>
                  {state?.events.length ? (
                    <div className="incoming-list" aria-live="polite">
                      {[...state.events].reverse().map((row) => (
                        <div className="incoming-item" key={row.event_id}>
                          <div className="incoming-check">
                            <Check size={13} />
                          </div>
                          <div>
                            <strong>{eventTitles[row.payload.status]}</strong>
                            <span>
                              Saved to history · {time(row.received_at)}
                            </span>
                          </div>
                          <span className="incoming-sequence">
                            #{row.payload.source_sequence}
                          </span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <Empty
                      icon={Database}
                      title="Listening for authorization events"
                    >
                      Publish a decision in Humata. Received events will appear
                      here automatically.
                    </Empty>
                  )}
                </div>
              </section>
            )}
          </div>
          <footer className="page-footer">
            <span>
              <ShieldCheck size={13} />A local learning environment. All
              encounters and decisions are synthetic.
            </span>
            <div>
              <span
                className={`service-dot ${state?.broker.status === "connected" && online ? "connected" : ""}`}
              />
              Event Hubs{state?.broker.status !== "connected" ? " offline" : ""}{" "}
              <span
                className={`service-dot ${state?.receiver.status === "connected" && online ? "connected" : ""}`}
              />
              Receiver{state?.receiver.status !== "connected" ? " offline" : ""}{" "}
              <span className={`service-dot ${online ? "connected" : ""}`} />
              PostgreSQL
            </div>
          </footer>
        </div>
      </main>
      {(guide || evidence !== null) && (
        <div
          className="modal-backdrop"
          onClick={() => {
            setGuide(false);
            setEvidence(null);
          }}
        >
          <section
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="modal-title"
            onClick={(event) => event.stopPropagation()}
          >
            <button
              className="modal-close"
              aria-label="Close dialog"
              onClick={() => {
                setGuide(false);
                setEvidence(null);
              }}
              autoFocus
            >
              <X size={20} />
            </button>
            {guide ? (
              <>
                <div className="eyebrow">A FIVE-MINUTE WALKTHROUGH</div>
                <h2 id="modal-title">Make the connection visible.</h2>
                <p>
                  Use the overview to watch both applications, or open Humata
                  and Flow in separate tabs.
                </p>
                <ol className="guide-steps">
                  <li>
                    <strong>Publish a decision in Humata.</strong>
                    <span>
                      Try Required, Submitted, then Approved. Every action
                      creates a new event.
                    </span>
                  </li>
                  <li>
                    <strong>Watch the delivery.</strong>
                    <span>
                      The broker acknowledges the event. The receiver saves it
                      into PostgreSQL history.
                    </span>
                  </li>
                  <li>
                    <strong>Pull the history into Flow.</strong>
                    <span>
                      Incoming activity is live; this pull represents Flow's
                      daily batch.
                    </span>
                  </li>
                  <li>
                    <strong>Review the claim denial.</strong>
                    <span>
                      Flow uses only the last pulled data to look for an
                      approval covering the service date.
                    </span>
                  </li>
                </ol>
                <div className="guide-tip">
                  Try denying authorization after a pull. The new event appears
                  immediately, but Flow keeps its old snapshot until you pull
                  again.
                </div>
                <button
                  className="button approve"
                  onClick={() => setGuide(false)}
                >
                  Explore the demo
                  <ArrowRight size={16} />
                </button>
              </>
            ) : (
              <>
                <div className="eyebrow">SOURCE-OWNED EVIDENCE · SYNTHETIC</div>
                <h2 id="modal-title">Supporting approval document</h2>
                <p>
                  This local fixture represents the evidence reference supplied
                  by Humata.
                </p>
                <pre className="evidence-content">{evidence}</pre>
                <p className="evidence-note">
                  The production integration would retrieve the document from
                  Humata using the event's evidence reference and verify its
                  content hash.
                </p>
              </>
            )}
          </section>
        </div>
      )}
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
