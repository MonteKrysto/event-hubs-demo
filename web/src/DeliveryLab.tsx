import { ChevronDown, RefreshCw, Unplug } from "lucide-react";
import type { Snapshot } from "./types";
import "./delivery-lab.css";

type Action = (key: string, path: string, body?: unknown) => Promise<boolean>;
export function DeliveryLab({ state, locked, act }: { state: Snapshot; locked: boolean; act: Action }) {
  const demo = state.integration;
  if (!demo) return null;
  const { controls, stats } = demo;
  const latest = state.publications[0];
  const latestReceived = !!latest && demo.inbox.some((item) => item.event_id === latest.event_id);
  const settings = (body: Record<string, boolean>) => act("delivery-controls", `/api/scenarios/${state.scenario.id}/delivery-controls`, body);
  return (
    <details className="card delivery-lab technical-details" id="delivery-lab">
      <summary>
        <Unplug size={16} /> Delivery lab
        <span>{stats.missing ? `${stats.missing} not received` : stats.pending ? `${stats.pending} waiting to publish` : "Try failures & recovery"}</span>
        <ChevronDown size={15} />
      </summary>
      <div className="delivery-lab-body">
        <p className="lab-intro">Try these experiments on this encounter. Humata keeps every decision in its simulated history API, even when its webhook does not arrive.</p>
        <div className="lab-experiment">
          <h4>1. A webhook cannot be delivered</h4>
          <p>Stop the webhook receiver, then choose an authorization action above. Humata saves the decision and gets an HTTP failure. Restore the receiver to let Humata retry the same event.</p>
          <button className="button" disabled={locked} onClick={() => settings({ receiver_available: !controls.receiver_available })}>
            {controls.receiver_available ? "Stop webhook receiver" : "Restore receiver & retry"}
          </button>
          {!controls.receiver_available && <p className="lab-warning" role="status">Webhook endpoint returns HTTP 503 for this encounter. API recovery can still run.</p>}
        </div>
        <div className="lab-experiment">
          <h4>2. The same webhook arrives twice</h4>
          <p>Resend the latest decision with its original event ID. The receiver acknowledges the repeat and keeps one outgoing event.</p>
          <button className="button" disabled={locked || !latestReceived || !controls.receiver_available} onClick={() => act("duplicate-webhook", `/api/scenarios/${state.scenario.id}/events/${latest!.event_id}/retry`)}>
            Send duplicate webhook
          </button>
          {!latest && <small>Choose an authorization action first.</small>}
          {latest && !latestReceived && <small>Let the latest update reach our receiver before resending it.</small>}
        </div>
        <div className="lab-experiment">
          <h4>3. A webhook never arrives</h4>
          <p>Skip the next webhook, then choose an authorization action above. The update stays in Humata only. Check its API to find and queue the missing update.</p>
          <div className="lab-actions">
            <button className="button" disabled={locked} aria-pressed={controls.skip_next_webhook} onClick={() => settings({ skip_next_webhook: !controls.skip_next_webhook })}>
              {controls.skip_next_webhook ? "Cancel skipped delivery" : "Skip next webhook"}
            </button>
            <button className="button primary" disabled={locked} onClick={() => act("api-recovery", `/api/scenarios/${state.scenario.id}/recover`)}>
              <RefreshCw size={14} /> Check Humata API
            </button>
          </div>
          {controls.skip_next_webhook && <p className="lab-warning" role="status">Armed: the next authorization action will save its decision without sending a webhook.</p>}
          <label className="lab-checkbox"><input type="checkbox" checked={controls.automatic_recovery} disabled={locked} onChange={(event) => settings({ automatic_recovery: event.target.checked })} /> Also check the API automatically every 30 seconds</label>
          <p className="lab-bookmark">API bookmark: update {controls.recovery_cursor} · Last completed check: {controls.last_recovery_at ? new Date(controls.last_recovery_at).toLocaleTimeString() : "Not checked yet"}</p>
          {controls.recovery_error && <p className="lab-warning" role="status">{controls.recovery_error}</p>}
        </div>
        <div className="lab-counts" aria-live="polite">
          <div><strong>{stats.received}</strong><span>Unique updates received</span></div>
          <div><strong>{stats.duplicates}</strong><span>Repeat deliveries ignored</span></div>
          <div><strong>{stats.recovered}</strong><span>Recovered through API</span></div>
        </div>
        <p className="lab-flow-note">Received updates still travel through Event Hubs and the Kafka receiver. Once saved in shared history, choose <strong>Load latest update</strong> in Flow. Recovery does not change Flow’s loaded snapshot automatically.</p>
        <details className="lab-extra">
          <summary>More failure controls</summary>
          <label className="lab-checkbox"><input type="checkbox" checked={controls.publisher_paused} disabled={locked} onChange={(event) => settings({ publisher_paused: event.target.checked })} /> Pause publishing to Event Hubs</label>
          <p>Webhooks are accepted and saved while outgoing events wait in the outbox. Resume to drain it. This pauses our publisher; the broker itself stays running.</p>
          <label className="lab-checkbox"><input type="checkbox" checked={!controls.api_available} disabled={locked} onChange={(event) => settings({ api_available: !event.target.checked })} /> Make the Humata API unavailable</label>
          <p>Run an API check to see a failure. Its bookmark stays at the last page saved successfully.</p>
        </details>
        {demo.activity.length > 0 && <details className="lab-extra" open>
          <summary>What happened</summary>
          <ol className="lab-activity" aria-label="Delivery activity">
            {demo.activity.map((item) => <li key={item.id}><time>{new Date(item.created_at).toLocaleTimeString()}</time><span>{item.message}</span></li>)}
          </ol>
        </details>}
      </div>
    </details>
  );
}
