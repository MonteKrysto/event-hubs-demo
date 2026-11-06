import { StrictMode, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import {
  ArrowDownToLine,
  ArrowRight,
  ArrowUpRight,
  Check,
  CheckCheck,
  CheckCircle2,
  ChevronDown,
  CircleHelp,
  ClipboardCheck,
  Clock3,
  Database,
  FileCheck2,
  FileClock,
  FileText,
  GitBranch,
  HeartPulse,
  LayoutDashboard,
  LoaderCircle,
  Radio,
  RefreshCw,
  Search,
  Send,
  ShieldCheck,
  Unplug,
  X,
  XCircle,
} from "lucide-react";
import type { AuthStatus, Publication, Snapshot } from "./types";
import { latestEvent, learningState } from "./learning";
import { DeliveryLab } from "./DeliveryLab";
import {
  FlowFreshness,
  JourneyProgress,
  ActionFeedback,
  ReviewExplanation,
  AuthorizationComparison,
} from "./LearningUI";
import "./styles.css";
import "./clarity.css";

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
  const [dataNotice, setDataNotice] = useState("");
  const [guide, setGuide] = useState(false);
  const [evidence, setEvidence] = useState<{
    content: string;
    kind: string;
    status: string;
    sha256: string;
  } | null>(null);
  const [deliveryDetails, setDeliveryDetails] = useState(false);
  const [approvalDetails, setApprovalDetails] = useState(false);
  const [authNumber, setAuthNumber] = useState("AUTH-DEMO-2026");
  const [validFrom, setValidFrom] = useState("2026-10-01");
  const [validTo, setValidTo] = useState("2026-10-31");
  const [expanded, setExpanded] = useState<string | null>(null);
  const modalTrigger = useRef<HTMLElement | null>(null);

  useEffect(() => {
    const receive = (next: Snapshot) => {
      setState((current) =>
        !current ||
        current.instance_id !== next.instance_id ||
        next.revision >= current.revision
          ? next
          : current,
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
    setDeliveryDetails(false);
    setApprovalDetails(false);
    const suggested = state?.scenario.details?.suggested_authorization;
    setAuthNumber(suggested?.authorization_number || "AUTH-DEMO-2026");
    setValidFrom(suggested?.valid_from || "2026-10-01");
    setValidTo(suggested?.valid_to || "2026-10-31");
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
        !current ||
        current.instance_id !== next.instance_id ||
        next.revision >= current.revision
          ? next
          : current,
      );
      return true;
    } catch (e) {
      setError((e as Error).message);
      return false;
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
      const event = latestEvent(state?.pull?.events || []);
      if (!state || !event) return;
      setEvidence(
        await request<{
          content: string;
          kind: string;
          status: string;
          sha256: string;
        }>(
          `/api/scenarios/${state.scenario.id}/events/${event.event_id}/evidence`,
        ),
      );
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const scenario = state?.scenario;
  const claimStatus = scenario?.claim.status || "denied";
  const hasDenial = claimStatus === "denied";
  const denialReason =
    (
      {
        missing_authorization: "Missing authorization",
        service_not_covered: "Service not covered",
        eligibility: "Coverage eligibility issue",
      } as Record<string, string>
    )[scenario?.claim.denial_reason || ""] || "Other denial reason";
  const currentEvent = state?.publications[0]?.event;
  const pulledEvents = state?.pull?.events || [];
  const pulled = latestEvent(pulledEvents);
  const journey = state ? learningState(state) : null;
  const newEvents = state
    ? state.events.filter(
        (row) => !pulledEvents.some((old) => old.event_id === row.event_id),
      ).length
    : 0;
  const online = live && historyAvailable;
  const locked = !!busy || !online || !state;
  const approvalInvalid =
    !authNumber.trim() || !validFrom || !validTo || validFrom > validTo;
  const showArea = (id: string, app: string) => {
    if (view !== "overview" && view !== app) navigate(app);
    window.setTimeout(
      () =>
        document.getElementById(id)?.scrollIntoView({
          block: "start",
          behavior: window.matchMedia("(prefers-reduced-motion: reduce)")
            .matches
            ? "auto"
            : "smooth",
        }),
      0,
    );
  };

  const activity = (publication: Publication) => {
    const stored = state?.events.find(
      (row) => row.event_id === publication.event_id,
    );
    const pending = publication.publication_state === "pending";
    const sourceWebhook = state?.integration?.webhooks.find((row) => row.event_id === publication.event_id);
    const receivedByIntegration = state?.integration?.inbox.some((row) => row.event_id === publication.event_id);
    const webhookMissing = !receivedByIntegration && (sourceWebhook?.state === "failed" || sourceWebhook?.state === "skipped");
    const failed =
      !stored &&
      (webhookMissing || publication.publication_state === "unconfirmed" ||
        (pending &&
          !state?.integration?.controls.publisher_paused &&
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
              ? "Saved in shared history"
              : webhookMissing
                ? sourceWebhook?.state === "skipped" ? "Webhook skipped; recover via API" : "Webhook delivery failed"
              : receivedByIntegration && state?.integration?.controls.publisher_paused
                ? "Queued; publishing paused"
              : failed
                ? "Delivery unconfirmed"
                : pending
                  ? "Sending…"
                  : "Accepted by Event Hubs"}
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
                Accepted by Event Hubs
              </span>
              <span>
                {stored ? <CheckCircle2 size={13} /> : <Clock3 size={13} />}{" "}
                Saved in shared history
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
                {state?.integration ? "Resend this webhook" : failed ? "Retry this event" : "Replay this event"}
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
            <strong>Authorization demo</strong>
          </div>
          <div className="topbar-right">
            <button
              className="topbar-guide"
              onClick={() => {
                modalTrigger.current =
                  document.activeElement as HTMLElement | null;
                setGuide(true);
              }}
            >
              <CircleHelp size={16} />
              How it works
            </button>
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
                  ? "Humata authorization"
                  : view === "flow"
                    ? "Flow claim review"
                    : "Prior authorization demo"}
              </h1>
              <p>
                {view === "humata"
                  ? "Choose what happens with this encounter’s authorization."
                  : view === "flow"
                    ? "Load authorization history and compare it with this claim’s original status."
                    : "Change the authorization in Humata. Load it in Flow to see how it relates to the claim."}
              </p>
            </div>
            <div className="new-scenario-control">
              {" "}
              <button
                className="button secondary new-scenario"
                disabled={locked}
                onClick={() => act("new", "/api/scenarios")}
              >
                <RefreshCw size={15} className={busy === "new" ? "spin" : ""} />
                New encounter
              </button>
              <p>New encounter in both apps. Previous history is kept.</p>
            </div>
          </div>
          {state && (
            <section
              className="encounter-context"
              aria-label="Selected encounter"
            >
              <div className="encounter-picker">
                <label>
                  Encounter <span>({state.encounters.length})</span>
                  <select
                    value={state.scenario.id}
                    disabled={locked}
                    onChange={(event) =>
                      act(
                        "select",
                        `/api/scenarios/${event.target.value}/select`,
                      )
                    }
                  >
                    {state.encounters.map((encounter) => (
                      <option key={encounter.id} value={encounter.id}>
                        {encounter.details?.patient_name ||
                          `Previous encounter ${encounter.number}`}{" "}
                        · {encounter.details?.procedure || "Outpatient visit"} ·{" "}
                        {date(encounter.claim.service_date)}
                        {" · "}
                        {encounter.claim.status || "denied"}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  className="button secondary"
                  disabled={locked}
                  onClick={async () => {
                    setDataNotice("");
                    if (await act("samples", "/api/samples"))
                      setDataNotice(
                        "Sample encounters are available. Choose an encounter from the list.",
                      );
                  }}
                >
                  <Database
                    size={16}
                    className={busy === "samples" ? "spin" : ""}
                  />
                  {busy === "samples"
                    ? "Preparing sample encounters…"
                    : "Add sample encounters"}
                </button>
              </div>
              <div className="encounter-context-line">
                <strong>
                  {scenario?.details?.patient_name || "Previous demo encounter"}
                </strong>
                <span>
                  {scenario?.details?.procedure ||
                    "No additional encounter details saved"}
                  {scenario?.details?.payer
                    ? ` · ${scenario.details.payer}`
                    : ""}
                </span>
              </div>
              {scenario?.details?.patient_id && (
                <details className="encounter-more">
                  <summary>Encounter details</summary>
                  <dl>
                    <div>
                      <dt>Patient ID (synthetic)</dt>
                      <dd>{scenario.details.patient_id}</dd>
                    </div>
                    <div>
                      <dt>Provider</dt>
                      <dd>{scenario.details.provider}</dd>
                    </div>
                    <div>
                      <dt>Procedure ID (demo)</dt>
                      <dd>{scenario.details.procedure_code}</dd>
                    </div>
                    <div>
                      <dt>Claim amount</dt>
                      <dd>
                        {new Intl.NumberFormat("en-US", {
                          style: "currency",
                          currency: "USD",
                        }).format(scenario.details.billed_amount || 0)}
                      </dd>
                    </div>
                    <div>
                      <dt>
                        {hasDenial
                          ? "Original denial date"
                          : "Original claim date"}
                      </dt>
                      <dd>
                        {scenario.details.denial_date ||
                        scenario.details.claim_date
                          ? date(
                              (scenario.details.denial_date ||
                                scenario.details.claim_date)!,
                            )
                          : "Not recorded"}
                      </dd>
                    </div>
                    {claimStatus === "paid" && (
                      <div>
                        <dt>Paid amount (synthetic)</dt>
                        <dd>
                          {new Intl.NumberFormat("en-US", {
                            style: "currency",
                            currency: "USD",
                          }).format(scenario.details.paid_amount || 0)}
                        </dd>
                      </div>
                    )}
                    <div>
                      <dt>Service date</dt>
                      <dd>{date(scenario.claim.service_date)}</dd>
                    </div>
                  </dl>
                  <p>
                    All people, payers, providers, and amounts in this demo are
                    fictional.
                  </p>
                </details>
              )}
              {dataNotice && (
                <p className="data-notice" role="status">
                  {dataNotice}
                </p>
              )}
            </section>
          )}
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
                </div>
                <div className="card encounter-card" id="humata-decision">
                  <div className="card-title">
                    <h3>Humata authorization</h3>
                    <Status status={currentEvent?.status} />
                  </div>
                  <p className="card-description">
                    {currentEvent?.status === "required"
                      ? "This encounter needs authorization. An approval has not been granted."
                      : "Choose an action to update this encounter’s authorization."}
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
                  <div
                    className="authorization-actions"
                    role="group"
                    aria-label="Authorization actions"
                  >
                    <button
                      className="authorization-action required"
                      disabled={locked}
                      onClick={() => send("required")}
                    >
                      <span className="authorization-action-title">
                        {busy === "required" ? (
                          <LoaderCircle size={18} className="spin" />
                        ) : (
                          <ClipboardCheck size={18} />
                        )}
                        <strong>Require authorization</strong>
                      </span>
                      <span className="authorization-action-description">
                        This visit needs prior authorization.
                      </span>
                    </button>
                    <button
                      className="authorization-action not_required"
                      disabled={locked}
                      onClick={() => send("not_required")}
                    >
                      <span className="authorization-action-title">
                        {busy === "not_required" ? (
                          <LoaderCircle size={18} className="spin" />
                        ) : (
                          <ShieldCheck size={18} />
                        )}
                        <strong>No authorization needed</strong>
                      </span>
                      <span className="authorization-action-description">
                        This visit does not need prior authorization.
                      </span>
                    </button>
                    <button
                      className="authorization-action attempted"
                      disabled={locked}
                      onClick={() => send("attempted")}
                    >
                      <span className="authorization-action-title">
                        {busy === "attempted" ? (
                          <LoaderCircle size={18} className="spin" />
                        ) : (
                          <Search size={18} />
                        )}
                        <strong>Start authorization</strong>
                      </span>
                      <span className="authorization-action-description">
                        Begin an attempt to obtain authorization.
                      </span>
                    </button>
                    <button
                      className="authorization-action submitted"
                      disabled={locked}
                      onClick={() => send("submitted")}
                    >
                      <span className="authorization-action-title">
                        {busy === "submitted" ? (
                          <LoaderCircle size={18} className="spin" />
                        ) : (
                          <Send size={18} />
                        )}
                        <strong>Submit authorization request</strong>
                      </span>
                      <span className="authorization-action-description">
                        The request has been sent for a decision.
                      </span>
                    </button>
                    <button
                      className="authorization-action approved"
                      disabled={locked || approvalInvalid}
                      onClick={() => send("approved")}
                    >
                      <span className="authorization-action-title">
                        {busy === "approved" ? (
                          <LoaderCircle size={18} className="spin" />
                        ) : (
                          <CheckCircle2 size={18} />
                        )}
                        <strong>Approve authorization</strong>
                      </span>
                      <span className="authorization-action-description">
                        Authorization has been granted.
                      </span>
                    </button>
                    <button
                      className="authorization-action denied"
                      disabled={locked}
                      onClick={() => send("denied")}
                    >
                      <span className="authorization-action-title">
                        {busy === "denied" ? (
                          <LoaderCircle size={18} className="spin" />
                        ) : (
                          <XCircle size={18} />
                        )}
                        <strong>Deny authorization</strong>
                      </span>
                      <span className="authorization-action-description">
                        Authorization has not been granted.
                      </span>
                    </button>
                  </div>
                  <p className="simulation-note">
                    Each button saves a decision in Humata. Delivery progress appears below.
                  </p>
                  <ActionFeedback
                    state={state}
                    busy={busy}
                    onDetails={() => {
                      const lab = document.querySelector<HTMLDetailsElement>("#delivery-lab");
                      if (lab) { lab.open = true; lab.scrollIntoView({ block: "nearest" }); }
                      else setDeliveryDetails(true);
                    }}
                  />{" "}
                  <details
                    className="approval-form"
                    id="approval-details"
                    open={approvalDetails}
                    onToggle={(event) =>
                      setApprovalDetails(event.currentTarget.open)
                    }
                  >
                    <summary className="form-caption">
                      <FileCheck2 size={16} />
                      Approval number & dates
                      {approvalInvalid && <span>Needs attention</span>}
                      <ChevronDown size={14} />
                    </summary>
                    <p className="form-helper">
                      Approval dates are compared with the service date:{" "}
                      {scenario ? date(scenario.claim.service_date) : "—"}. They
                      do not need to cover today.
                    </p>
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
                    {approvalInvalid && (
                      <p className="field-error">
                        Enter an authorization number and valid coverage dates.
                        The end date must be on or after the start date.
                      </p>
                    )}
                  </details>
                  {journey?.saved && !journey.inFlow && (
                    <button
                      className="button continue-flow"
                      onClick={() => showArea("flow-data", "flow")}
                    >
                      Continue in Flow
                      <ArrowRight size={16} />
                    </button>
                  )}
                </div>
                {state && <DeliveryLab key={state.scenario.id} state={state} locked={locked} act={act} />}
                <details
                  className="card activity-card technical-details"
                  id="publication-activity"
                  open={deliveryDetails}
                  onToggle={(event) =>
                    setDeliveryDetails(event.currentTarget.open)
                  }
                >
                  <summary>
                    Event history & delivery details
                    <span>{state?.publications.length || 0} updates</span>
                    <ChevronDown size={15} />
                  </summary>{" "}
                  {state?.publications.length ? (
                    <div className="timeline" aria-live="polite">
                      {state.publications.map(activity)}
                    </div>
                  ) : (
                    <Empty title="Your first event starts here">
                      Choose any authorization action. Its event will appear
                      here.
                    </Empty>
                  )}
                </details>
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
                </div>
                <div className="card flow-workspace" id="flow-data">
                  <div className="card-title">
                    <h3>
                      {hasDenial
                        ? "Original claim denial"
                        : "Original claim status"}
                    </h3>
                    <span className={`status ${claimStatus}`}>
                      <span />
                      Original status: {claimStatus}
                    </span>
                  </div>
                  <div className="denial-reason">
                    <div className={`denial-icon ${claimStatus}`}>
                      {claimStatus === "paid" ? (
                        <CheckCircle2 size={22} />
                      ) : (
                        <FileClock size={22} />
                      )}
                    </div>
                    <div>
                      <strong>
                        {hasDenial
                          ? denialReason
                          : claimStatus === "paid"
                            ? "Claim paid"
                            : "Awaiting payer decision"}
                      </strong>
                      <p>
                        {hasDenial
                          ? "This is the claim’s original denial reason."
                          : claimStatus === "paid"
                            ? "This example claim has already been paid."
                            : "This example claim is still awaiting a payment decision."}{" "}
                        Updating an authorization does not change the claim
                        status.
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
                  <div className="flow-information">
                    <div className="card-title">
                      <h3>Authorization status</h3>
                      <span
                        className={`sync-label ${newEvents && state?.pull ? "outdated" : ""}`}
                      >
                        {!state?.pull
                          ? "Not loaded"
                          : journey?.inFlow
                            ? "Latest update loaded"
                            : journey?.latest
                              ? "Earlier data loaded"
                              : "No authorization loaded"}
                      </span>
                    </div>
                    <p className="card-description">
                      Flow uses the information you load here. New Humata
                      decisions do not change it automatically.
                    </p>
                    {state && <AuthorizationComparison state={state} />}
                    {state && <FlowFreshness state={state} />}
                    <div className="flow-actions">
                      {" "}
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
                        Load latest update
                        {newEvents > 0 && (
                          <span className="button-counter">{newEvents}</span>
                        )}
                        <ArrowRight size={16} className="button-end" />
                      </button>
                      {hasDenial && (
                        <button
                          className="button review"
                          disabled={locked || !state?.pull}
                          onClick={() =>
                            act(
                              "review",
                              `/api/scenarios/${scenario!.id}/review`,
                            )
                          }
                        >
                          {busy === "review" ? (
                            <LoaderCircle size={16} className="spin" />
                          ) : (
                            <ClipboardCheck size={16} />
                          )}
                          Evaluate denial
                        </button>
                      )}
                    </div>{" "}
                    {!hasDenial && (
                      <div className="claim-outcome-note" role="status">
                        <CheckCircle2 size={17} />
                        <span>
                          {claimStatus === "paid"
                            ? "This claim is paid. There is no denial to evaluate."
                            : "This claim is pending. No denial has been issued."}{" "}
                          You can still load its authorization history.
                        </span>
                      </div>
                    )}
                    {pulled && (
                      <div className="pulled-authorization">
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
                          Matched to this encounter
                        </div>
                        <button className="text-button" onClick={showEvidence}>
                          <FileText size={14} />
                          View evidence for this decision
                          <ArrowUpRight size={13} />
                        </button>
                      </div>
                    )}
                    <div className="pull-footer">
                      <Clock3 size={13} />
                      {state?.pull
                        ? `Loaded ${new Date(state.pull.pulled_at).toLocaleString()} · ${pulledEvents.length} saved events`
                        : hasDenial
                          ? "Load an update to enable evaluation"
                          : "Load an update to inspect authorization history"}
                    </div>
                    {state && <ReviewExplanation state={state} />}
                  </div>
                </div>
                <details className="card incoming-card shared-history technical-details">
                  <summary>
                    Received event history
                    <span>{state?.events.length || 0} events</span>
                    <ChevronDown size={15} />
                  </summary>{" "}
                  <p className="card-description">
                    The receiver saves events here automatically. Seeing an
                    event here does not mean Flow has loaded it.
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
                              Saved in shared history · {time(row.received_at)}
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
                </details>
              </section>
            )}
          </div>
          <details className="connection-details technical-details">
            <summary>
              How the connection works
              <ChevronDown size={15} />
            </summary>
            <p>
              Humata sends a webhook to our receiver. The receiver saves an inbox
              entry and an outgoing event together. A background publisher sends
              that event to the local Event Hubs emulator using the
              Kafka protocol. A separate receiver saves it in PostgreSQL. Flow
              reads that saved history when you load an update.
            </p>
            <JourneyProgress state={state} />
          </details>
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
                      All six authorization actions are together on the
                      encounter. Choose any action; every click shares a new
                      update.
                    </span>
                  </li>
                  <li>
                    <strong>Watch the delivery.</strong>
                    <span>
                      Event Hubs accepts the event. A separate receiver saves it
                      in shared history. Acceptance and saving are different
                      steps.
                    </span>
                  </li>
                  <li>
                    <strong>Load the history into Flow.</strong>
                    <span>
                      Shared history updates automatically. Flow’s load is
                      explicit and represents the daily batch.
                    </span>
                  </li>
                  <li>
                    <strong>Evaluate the claim denial.</strong>
                    <span>
                      Flow uses only its last loaded snapshot. The result
                      explains each check and leaves the next action to a
                      person.
                    </span>
                  </li>
                </ol>
                <div className="guide-tip">
                  Try denying authorization after loading an approval. Shared
                  history updates automatically, but Flow keeps its earlier
                  information until you load again.
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
                <h2 id="modal-title">
                  {evidence?.kind === "legacy-fixture"
                    ? "Reused sample document"
                    : evidence?.status === "approved"
                      ? "Approval source record"
                      : "Authorization decision record"}
                </h2>
                <p>
                  {evidence?.kind === "legacy-fixture"
                    ? "This older event references the original shared approval fixture. Its text may not describe this event’s decision. It is shown unchanged to preserve the original reference."
                    : "This synthetic document matches the selected decision and its evidence reference. It has no clinical or payer authority."}
                </p>
                <pre className="evidence-content">{evidence?.content}</pre>
                <p className="evidence-note">
                  The local demo resolved the event’s original evidence
                  reference and checked the document’s content hash. A person
                  still reviews the content before taking action.
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
