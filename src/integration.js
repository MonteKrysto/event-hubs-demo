import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { validate } from "./domain.js";
import { DemoError } from "./demo-error.js";

const defaults = {
  receiver_available: true, publisher_paused: false, api_available: true,
  skip_next_webhook: false, automatic_recovery: false, recovery_cursor: "0",
  last_recovery_at: null, last_recovery_attempt_at: null, recovery_error: null,
};
function cursor(value) {
  const text = String(value);
  if (!/^\d{1,19}$/.test(text) || BigInt(text) > 9223372036854775807n)
    throw new DemoError("Invalid change-history position.");
  return text;
}
function simulated(message) {
  const error = new DemoError(message, 503);
  error.simulated = true;
  return error;
}

// A local vendor simulator and integration receiver. Source delivery, receiver
// acceptance, broker publication, and Kafka ingestion are separate commits.
export class IntegrationDemo {
  constructor(service, { pageSize = 20 } = {}) {
    this.service = service;
    this.history = service.history;
    this.pageSize = pageSize;
    this.webhookSecret = process.env.DEMO_WEBHOOK_SECRET || randomUUID();
    this.apiKey = process.env.DEMO_HUMATA_API_KEY || randomUUID();
    this.fetch = (...args) => fetch(...args);
  }

  signature(raw) {
    return createHmac("sha256", this.webhookSecret).update(raw).digest("hex");
  }

  verify(raw, supplied) {
    return Buffer.isBuffer(raw) && typeof supplied === "string" && /^[a-f0-9]{64}$/.test(supplied) &&
      timingSafeEqual(Buffer.from(this.signature(raw), "hex"), Buffer.from(supplied, "hex"));
  }

  async state(client, id, lock = false) {
    if (lock) await client.query(
      "INSERT INTO integration_state (scenario_id) VALUES ($1) ON CONFLICT DO NOTHING", [id]);
    return (await client.query(
      `SELECT * FROM integration_state WHERE scenario_id=$1${lock ? " FOR UPDATE" : ""}`, [id]
    )).rows[0] || { ...defaults, scenario_id: id };
  }

  activity(client, id, eventId, kind, message) {
    return client.query(
      "INSERT INTO integration_activity (scenario_id,event_id,kind,message) VALUES ($1,$2,$3,$4)",
      [id, eventId, kind, message]);
  }

  async controls(id, input) {
    const keys = Object.keys(input);
    const allowed = ["receiver_available", "publisher_paused", "api_available", "skip_next_webhook", "automatic_recovery"];
    if (!keys.length || keys.some((key) => !allowed.includes(key) || typeof input[key] !== "boolean"))
      throw new DemoError("Choose a supported delivery control.");
    return this.history.transaction(async (client) => {
      await this.service.getScenario(client, id, true);
      await this.state(client, id, true);
      // Column names come exclusively from the fixed allowlist above.
      await client.query(`UPDATE integration_state SET ${keys.map((key, i) => `${key}=$${i + 2}`).join(",")} WHERE scenario_id=$1`,
        [id, ...keys.map((key) => input[key])]);
      if (input.receiver_available === true) await client.query(
        "UPDATE integration_webhooks w SET next_attempt_at=clock_timestamp() FROM ui_publications p WHERE w.event_id=p.event_id AND p.scenario_id=$1 AND w.state='failed'", [id]);
      await this.activity(client, id, null, "controls", "Delivery settings changed for this encounter.");
      return this.state(client, id);
    });
  }

  async queueWebhook(client, id, event) {
    const state = await this.state(client, id, true);
    const skipped = state.skip_next_webhook;
    await client.query("INSERT INTO integration_webhooks (event_id,state) VALUES ($1,$2)",
      [event.event_id, skipped ? "skipped" : "queued"]);
    if (skipped) await client.query(
      "UPDATE integration_state SET skip_next_webhook=FALSE WHERE scenario_id=$1", [id]);
    await this.activity(client, id, event.event_id, skipped ? "webhook_skipped" : "source_saved",
      skipped ? `Update ${event.source_sequence} saved in Humata. Its webhook was deliberately skipped; recover it through the API.`
        : `Update ${event.source_sequence} saved in Humata and queued for webhook delivery.`);
  }

  async replay(id, eventId) {
    await this.service.getScenario(this.history.pool, id);
    const result = await this.history.pool.query(
      "SELECT event_id FROM ui_publications WHERE scenario_id=$1 AND event_id=$2", [id, eventId]);
    if (!result.rowCount) throw new DemoError("This event could not be found.", 404);
    await this.history.pool.query(
      "INSERT INTO integration_webhooks (event_id,state) VALUES ($1,'queued') ON CONFLICT DO NOTHING", [eventId]);
    return this.deliverWebhook(eventId, true);
  }

  async deliverWebhook(eventId, replay = false) {
    const token = randomUUID();
    const job = (await this.history.pool.query(`
      UPDATE integration_webhooks SET lease_token=$2,lease_until=clock_timestamp()+interval '30 seconds',attempts=attempts+1
      WHERE event_id=$1 AND (lease_until IS NULL OR lease_until<clock_timestamp())
        AND ($3 OR (state IN ('queued','failed') AND next_attempt_at<=clock_timestamp())) RETURNING *`,
      [eventId, token, replay])).rows[0];
    if (!job) return { result: "not_due" };
    const source = (await this.history.pool.query(
      "SELECT * FROM ui_publications WHERE event_id=$1", [eventId])).rows[0];
    let response, result, error;
    try {
      const raw = JSON.stringify(source.event);
      response = await this.fetch(`${this.baseUrl}/api/integration/webhook`, {
        method: "POST", headers: { "Content-Type": "application/json", "x-demo-signature": this.signature(raw) },
        body: raw, signal: AbortSignal.timeout(5000),
      });
      if (!response.ok) throw new Error(`Webhook receiver returned HTTP ${response.status}.`);
      result = await response.json();
    } catch (failure) {
      error = failure;
    }
    await this.history.transaction(async (client) => {
      const updated = await client.query(`UPDATE integration_webhooks SET
        state=CASE WHEN state='delivered' THEN state ELSE $3 END,last_error=$4,
        next_attempt_at=clock_timestamp()+($5 * interval '1 second'),lease_token=NULL,lease_until=NULL
        WHERE event_id=$1 AND lease_token=$2 RETURNING event_id`,
      [eventId, token, error ? "failed" : "delivered", error?.message || null, Math.min(60, 15 * 2 ** Math.min(job.attempts - 1, 3))]);
      if (updated.rowCount && error) await this.activity(client, source.scenario_id, eventId, "webhook_failed",
        `Update ${source.source_sequence}: ${error.message} Humata will retry the same event.`);
    });
    return error ? { result: "failed", error: error.message } : result;
  }

  // Receiver logic uses the local identity crosswalk, not Humata's source tables.
  async accept(client, event, via) {
    try { validate(event); } catch (error) { throw new DemoError(`Invalid authorization event: ${error.message}`); }
    const scenario = (await client.query(`SELECT id FROM ui_scenarios WHERE
      identity->>'tenant_id'=$1 AND identity->>'source_account_id'=$2 AND identity->>'source_encounter_id'=$3`,
    [event.tenant_id, event.source_account_id, event.source_encounter_id])).rows[0];
    if (!scenario) throw new DemoError("The event does not match a known encounter.", 404);
    if (via === "webhook" && !(await this.state(client, scenario.id)).receiver_available)
      throw simulated("The webhook receiver is stopped for this encounter.");
    const inserted = await client.query(`INSERT INTO integration_inbox (event_id,scenario_id,event,received_via)
      VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING event_id`, [event.event_id, scenario.id, event, via]);
    if (!inserted.rowCount) {
      const old = (await client.query("SELECT event FROM integration_inbox WHERE event_id=$1", [event.event_id])).rows[0];
      if (!old || !isDeepStrictEqual(old.event, event))
        throw new DemoError("An event ID or revision was reused with conflicting data. Recovery has not advanced past it.", 409);
      await this.activity(client, scenario.id, event.event_id, "duplicate",
        `Update ${event.source_sequence} arrived again through ${via === "api" ? "the API" : "a webhook"}. Duplicate ignored; one outgoing event kept.`);
      return { result: "duplicate", event_id: event.event_id };
    }
    await client.query("INSERT INTO integration_outbox (event_id) VALUES ($1)", [event.event_id]);
    await this.activity(client, scenario.id, event.event_id, via === "api" ? "api_recovered" : "webhook_received",
      `Update ${event.source_sequence} ${via === "api" ? "recovered through the Humata API" : "received by webhook"}. Saved with a pending outgoing event in one transaction.`);
    return { result: "accepted", event_id: event.event_id };
  }

  receive(event) {
    return this.history.transaction((client) => this.accept(client, event, "webhook"));
  }

  // Simulated vendor endpoint: a stable, per-encounter revision cursor. Source
  // revisions are allocated under the encounter row lock, so commit order cannot
  // jump over an earlier uncommitted revision as a global BIGSERIAL cursor could.
  async sourcePage(id, after = "0", through) {
    after = cursor(after);
    if (through !== undefined) through = cursor(through);
    return this.history.transaction(async (client) => {
      await this.service.getScenario(client, id);
      if (!(await this.state(client, id)).api_available) throw simulated("The simulated Humata API is unavailable.");
      const maximum = (await client.query(
        "SELECT COALESCE(MAX(source_sequence),0)::text AS cursor FROM ui_publications WHERE scenario_id=$1", [id])).rows[0].cursor;
      through ??= maximum;
      if (BigInt(after) > BigInt(through) || BigInt(through) > BigInt(maximum))
        throw new DemoError("The requested history position is outside this encounter’s history.");
      const rows = (await client.query(`SELECT event,source_sequence FROM ui_publications
        WHERE scenario_id=$1 AND source_sequence>$2 AND source_sequence<=$3 ORDER BY source_sequence LIMIT $4`,
      [id, after, through, this.pageSize])).rows;
      const next = rows.at(-1)?.source_sequence || after;
      return { events: rows.map((row) => row.event), next_cursor: String(next), through, has_more: BigInt(next) < BigInt(through) };
    }, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  }

  async recover(id) {
    const current = await this.service.getScenario(this.history.pool, id);
    const token = randomUUID();
    await this.history.pool.query("INSERT INTO integration_state (scenario_id) VALUES ($1) ON CONFLICT DO NOTHING", [id]);
    const owned = (await this.history.pool.query(`UPDATE integration_state SET recovery_token=$2,
      recovery_until=clock_timestamp()+interval '30 seconds',last_recovery_attempt_at=clock_timestamp()
      WHERE scenario_id=$1 AND (recovery_until IS NULL OR recovery_until<clock_timestamp()) RETURNING recovery_cursor`, [id, token])).rows[0];
    if (!owned) throw new DemoError("An API check is already running for this encounter.", 409);
    let position = owned.recovery_cursor, through;
    let recovered = 0, duplicates = 0, pages = 0;
    try {
      do {
        const url = new URL(`${this.baseUrl}/api/humata/scenarios/${id}/changes`);
        url.searchParams.set("after", position);
        if (through !== undefined) url.searchParams.set("through", through);
        const response = await this.fetch(url, { headers: { "x-demo-api-key": this.apiKey }, signal: AbortSignal.timeout(5000) });
        if (!response.ok) throw new Error(`Humata API returned HTTP ${response.status}. The saved bookmark has been kept.`);
        const page = await response.json();
        const next = cursor(page.next_cursor), bound = cursor(page.through);
        if (!Array.isArray(page.events) || page.events.length > this.pageSize || typeof page.has_more !== "boolean" ||
            (through !== undefined && bound !== through) || BigInt(next) < BigInt(position) || BigInt(next) > BigInt(bound) ||
            page.has_more !== (BigInt(next) < BigInt(bound)) || (!page.events.length && next !== position))
          throw new Error("Humata API returned an invalid page. The saved bookmark has been kept.");
        let previous = BigInt(position);
        for (const event of page.events) {
          if (BigInt(event.source_sequence) <= previous || BigInt(event.source_sequence) > BigInt(next) ||
              event.tenant_id !== current.identity.tenant_id || event.source_account_id !== current.identity.source_account_id ||
              event.source_encounter_id !== current.identity.source_encounter_id)
            throw new Error("Humata API returned an inconsistent page. The saved bookmark has been kept.");
          previous = BigInt(event.source_sequence);
        }
        if (previous !== BigInt(next) || (page.has_more && next === position)) throw new Error("Humata API did not advance safely.");
        await this.history.transaction(async (client) => {
          const state = await this.state(client, id, true);
          if (state.recovery_token !== token || state.recovery_cursor !== position)
            throw new DemoError("Another API check owns this bookmark. Retry this check.", 409);
          for (const event of page.events) {
            const result = await this.accept(client, event, "api");
            if (result.result === "accepted") recovered++; else duplicates++;
          }
          // Inbox, outbox, and bookmark commit together. Broker publication is later.
          await client.query(`UPDATE integration_state SET recovery_cursor=$2,recovery_error=NULL,
            recovery_until=clock_timestamp()+interval '30 seconds' WHERE scenario_id=$1`, [id, next]);
        });
        pages++;
        position = next;
        through = bound;
        if (!page.has_more) break;
      } while (pages < 100);
      await this.history.transaction(async (client) => {
        const updated = await client.query(`UPDATE integration_state SET last_recovery_at=CASE WHEN recovery_cursor=$3::bigint THEN clock_timestamp() ELSE last_recovery_at END,
          recovery_error=NULL,recovery_token=NULL,recovery_until=NULL WHERE scenario_id=$1 AND recovery_token=$2 RETURNING scenario_id`, [id, token, through]);
        if (!updated.rowCount) throw new DemoError("Another API check now owns this bookmark.", 409);
        await this.activity(client, id, null, "api_checked", `API check saved ${recovered} missing update${recovered === 1 ? "" : "s"}; ${duplicates} already received. Bookmark: ${position}${position !== through ? " (more pages remain)" : ""}.`);
      });
      return { recovered, duplicates, pages, cursor: position, caught_up: position === through };
    } catch (error) {
      await this.history.transaction(async (client) => {
        const updated = await client.query(`UPDATE integration_state SET recovery_error=$3,recovery_token=NULL,recovery_until=NULL
          WHERE scenario_id=$1 AND recovery_token=$2 RETURNING scenario_id`, [id, token, error.message]);
        if (updated.rowCount) await this.activity(client, id, null, "api_failed", error.message);
      });
      if (error instanceof DemoError) throw error;
      const failure = new DemoError(error.message, 502);
      failure.simulated = !(await this.state(this.history.pool, id)).api_available;
      throw failure;
    }
  }

  async publishOne() {
    const token = randomUUID();
    const job = await this.history.transaction(async (client) => {
      const row = (await client.query(`SELECT o.event_id,i.event,i.scenario_id,o.attempts FROM integration_outbox o
        JOIN integration_inbox i USING (event_id) LEFT JOIN integration_state s USING (scenario_id)
        WHERE o.state='pending' AND o.next_attempt_at<=clock_timestamp()
          AND (o.lease_until IS NULL OR o.lease_until<clock_timestamp()) AND NOT COALESCE(s.publisher_paused,FALSE)
          AND NOT EXISTS (SELECT 1 FROM integration_outbox older JOIN integration_inbox oi USING (event_id)
            WHERE older.state='pending' AND oi.scenario_id=i.scenario_id
              AND (oi.event->>'source_sequence')::bigint<(i.event->>'source_sequence')::bigint)
        ORDER BY i.received_at FOR UPDATE OF o SKIP LOCKED LIMIT 1`)).rows[0];
      if (row) await client.query(`UPDATE integration_outbox SET attempts=attempts+1,lease_token=$2,
        lease_until=clock_timestamp()+interval '2 minutes' WHERE event_id=$1`, [row.event_id, token]);
      return row;
    });
    if (!job) return false;
    let acknowledgment, failure;
    try {
      const acks = await this.service.publisher([job.event]);
      if (!Array.isArray(acks) || acks.length !== 1 || acks[0].offset == null) throw new Error("Event Hubs did not return an acknowledgment.");
      acknowledgment = acks[0];
    } catch (error) { failure = error; }
    await this.history.transaction(async (client) => {
      const updated = await client.query(`UPDATE integration_outbox SET state=$3,last_error=$4,
        next_attempt_at=clock_timestamp()+($5 * interval '1 second'),sent_at=CASE WHEN $3='sent' THEN clock_timestamp() ELSE NULL END,
        lease_token=NULL,lease_until=NULL WHERE event_id=$1 AND lease_token=$2 RETURNING event_id`,
      [job.event_id, token, failure ? "pending" : "sent", failure?.message || null, Math.min(60, 2 ** Math.min(job.attempts, 6))]);
      if (!updated.rowCount) return;
      await client.query("UPDATE ui_publications SET publication_state=$2,acknowledgment=$3,error=$4,updated_at=clock_timestamp() WHERE event_id=$1",
        [job.event_id, failure ? "unconfirmed" : "published", acknowledgment || null, failure?.message || null]);
      await this.activity(client, job.scenario_id, job.event_id, failure ? "publish_failed" : "published",
        failure ? `Update ${job.event.source_sequence} is safely queued. Event Hubs publication failed; it will be retried.`
          : `Event Hubs accepted update ${job.event.source_sequence}. The Kafka receiver can now save it to shared history.`);
    });
    if (failure) console.error(`Outbox publication failed: ${failure.message}`);
    return true;
  }

  async pump(limit = 10) {
    for (let i = 0; i < limit && await this.publishOne(); i++) { /* bounded work per tick */ }
  }

  async tick() {
    if (this.running) return this.running;
    this.running = (async () => {
      const jobs = (await this.history.pool.query(`SELECT event_id FROM integration_webhooks
        WHERE state IN ('queued','failed') AND next_attempt_at<=clock_timestamp()
          AND (lease_until IS NULL OR lease_until<clock_timestamp()) ORDER BY next_attempt_at LIMIT 5`)).rows;
      for (const job of jobs) await this.deliverWebhook(job.event_id);
      const checks = (await this.history.pool.query(`SELECT scenario_id FROM integration_state WHERE automatic_recovery
        AND (last_recovery_attempt_at IS NULL OR last_recovery_attempt_at<clock_timestamp()-interval '30 seconds')
        AND (recovery_until IS NULL OR recovery_until<clock_timestamp()) LIMIT 5`)).rows;
      for (const { scenario_id } of checks) {
        try { await this.recover(scenario_id); } catch (error) {
          if (!error.simulated && error.status !== 409) console.error(`API recovery failed: ${error.message}`);
        }
      }
      await this.pump();
    })().finally(() => { this.running = null; });
    return this.running;
  }

  start(baseUrl, { automatic = true } = {}) {
    this.baseUrl = baseUrl;
    this.service.integration = this;
    if (automatic) {
      this.timer = setInterval(() => this.tick().catch((error) => console.error(`Integration worker failed: ${error.message}`)), 1000);
      this.timer.unref();
    }
  }

  async stop() {
    clearInterval(this.timer);
    await this.running;
  }

  async snapshot(client, id) {
    const settings = await this.state(client, id);
    const stats = (await client.query(`SELECT
      (SELECT COUNT(*)::int FROM integration_inbox WHERE scenario_id=$1) AS received,
      (SELECT COUNT(*)::int FROM integration_inbox WHERE scenario_id=$1 AND received_via='api') AS recovered,
      (SELECT COUNT(*)::int FROM integration_activity WHERE scenario_id=$1 AND kind='duplicate') AS duplicates,
      (SELECT COUNT(*)::int FROM integration_outbox o JOIN integration_inbox i USING(event_id) WHERE i.scenario_id=$1 AND o.state='pending') AS pending,
      (SELECT COUNT(*)::int FROM ui_publications p WHERE scenario_id=$1 AND NOT EXISTS(SELECT 1 FROM integration_inbox i WHERE i.event_id=p.event_id)
        AND NOT EXISTS(SELECT 1 FROM events e WHERE e.event_id=p.event_id)) AS missing`, [id])).rows[0];
    return {
      controls: Object.fromEntries(Object.keys(defaults).map((key) => [key, settings[key]])),
      stats,
      inbox: (await client.query("SELECT event_id,received_via,received_at FROM integration_inbox WHERE scenario_id=$1 ORDER BY received_at DESC", [id])).rows,
      webhooks: (await client.query("SELECT w.*,p.source_sequence FROM integration_webhooks w JOIN ui_publications p USING(event_id) WHERE p.scenario_id=$1 ORDER BY p.source_sequence DESC", [id])).rows.map(({ lease_token, ...row }) => row),
      activity: (await client.query("SELECT * FROM integration_activity WHERE scenario_id=$1 ORDER BY id DESC LIMIT 20", [id])).rows,
    };
  }
}
