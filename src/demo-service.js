import { randomUUID } from "node:crypto";
import { interpret, scenario, validate } from "./domain.js";
import { publish } from "./transport.js";

export class DemoError extends Error {
  constructor(message, status = 400) {
    super(message);
    this.status = status;
  }
}

export class DemoService {
  constructor(history, publisher = publish) {
    this.history = history;
    this.publisher = publisher;
  }

  async createScenario(client) {
    const id = randomUUID();
    const suffix = id.slice(0, 8).toUpperCase();
    const identity = {
      tenant_id: "demo-hospital",
      source_account_id: `H-ACCOUNT-${suffix}`,
      source_encounter_id: `H-ENC-${suffix}`,
      account_id: `FLOW-ACCOUNT-${suffix}`,
      encounter_id: `FLOW-ENC-${suffix}`,
    };
    const claim = {
      claim_id: `CLAIM-${suffix}`,
      tenant_id: identity.tenant_id,
      account_id: identity.account_id,
      encounter_id: identity.encounter_id,
      denial_reason: "missing_authorization",
      service_date: "2026-10-15",
    };
    const result = await client.query(
      "INSERT INTO ui_scenarios (id,identity,claim) VALUES ($1,$2,$3) RETURNING *",
      [id, identity, claim],
    );
    await client.query(
      "INSERT INTO ui_state (name,scenario_id) VALUES ('active',$1) ON CONFLICT (name) DO UPDATE SET scenario_id=EXCLUDED.scenario_id",
      [id],
    );
    return result.rows[0];
  }

  async activeScenario() {
    return this.history.transaction(async (client) => {
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtext('fiber-ui-active'))",
      );
      const result = await client.query(
        "SELECT s.* FROM ui_scenarios s JOIN ui_state a ON a.scenario_id=s.id WHERE a.name='active'",
      );
      return result.rows[0] || this.createScenario(client);
    });
  }

  async startScenario() {
    return this.history.transaction(async (client) => {
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtext('fiber-ui-active'))",
      );
      return this.createScenario(client);
    });
  }

  async getScenario(client, id, lock = false) {
    if (
      !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(
        id,
      )
    )
      throw new DemoError("Invalid scenario identifier.");
    const result = await client.query(
      `SELECT * FROM ui_scenarios WHERE id=$1${lock ? " FOR UPDATE" : ""}`,
      [id],
    );
    if (!result.rowCount)
      throw new DemoError("This scenario could not be found.", 404);
    return result.rows[0];
  }

  async send(id, input) {
    if (
      ![
        "not_required",
        "required",
        "attempted",
        "submitted",
        "approved",
        "denied",
      ].includes(input.status)
    )
      throw new DemoError("Choose a supported authorization status.");
    const event = await this.history.transaction(async (client) => {
      const current = await this.getScenario(client, id, true);
      const revision = await client.query(
        "SELECT COALESCE(MAX(source_sequence),0)+1 AS next FROM ui_publications WHERE scenario_id=$1",
        [id],
      );
      const template = scenario()[0];
      const event = {
        ...template,
        event_id: randomUUID(),
        tenant_id: current.identity.tenant_id,
        source_account_id: current.identity.source_account_id,
        source_encounter_id: current.identity.source_encounter_id,
        source_sequence: Number(revision.rows[0].next),
        occurred_at: new Date().toISOString(),
        published_at: new Date().toISOString(),
        status: input.status,
      };
      if (input.status === "approved") {
        const number =
          typeof input.authorization_number === "string"
            ? input.authorization_number.trim()
            : "";
        if (!number || number.length > 80)
          throw new DemoError(
            "Enter an authorization number between 1 and 80 characters.",
          );
        Object.assign(event, {
          authorization_number: number,
          valid_from: input.valid_from,
          valid_to: input.valid_to,
        });
      }
      try {
        validate(event);
      } catch (error) {
        throw new DemoError(
          `Check the authorization details: ${error.message}`,
        );
      }
      await client.query(
        `INSERT INTO ui_publications (event_id,scenario_id,source_sequence,event,publication_state)
        VALUES ($1,$2,$3,$4,'pending')`,
        [event.event_id, id, event.source_sequence, event],
      );
      return event;
    });
    return this.deliver(event);
  }

  async retry(id, eventId) {
    await this.getScenario(this.history.pool, id);
    if (!/^[a-f0-9-]{36}$/i.test(eventId))
      throw new DemoError("Invalid event identifier.");
    const result = await this.history.pool.query(
      "SELECT event FROM ui_publications WHERE scenario_id=$1 AND event_id=$2",
      [id, eventId],
    );
    if (!result.rowCount)
      throw new DemoError("This event could not be found.", 404);
    await this.history.pool.query(
      "UPDATE ui_publications SET publication_state='pending',error=NULL,updated_at=clock_timestamp() WHERE event_id=$1",
      [eventId],
    );
    return this.deliver(result.rows[0].event);
  }

  async deliver(event) {
    let acknowledgments;
    try {
      // Only the Kafka receiver writes to events. The web API records publication metadata.
      acknowledgments = await this.publisher([event]);
      if (
        !Array.isArray(acknowledgments) ||
        acknowledgments.length !== 1 ||
        acknowledgments[0].offset == null
      ) {
        throw new Error("No delivery acknowledgment was returned");
      }
    } catch (error) {
      await this.history.pool.query(
        "UPDATE ui_publications SET publication_state='unconfirmed',error=$2,updated_at=clock_timestamp() WHERE event_id=$1",
        [event.event_id, error.message],
      );
      throw new DemoError(
        "Event Hubs did not confirm this delivery. You can retry the same event safely from its activity card.",
        502,
      );
    }
    await this.history.pool.query(
      "UPDATE ui_publications SET publication_state='published',acknowledgment=$2,error=NULL,updated_at=clock_timestamp() WHERE event_id=$1",
      [event.event_id, acknowledgments[0]],
    );
    return { event, acknowledgment: acknowledgments[0] };
  }

  async pull(id) {
    return this.history.transaction(async (client) => {
      const current = await this.getScenario(client, id);
      const rows = await this.historyRows(client, current);
      return (
        await client.query(
          "INSERT INTO ui_pulls (scenario_id,events) VALUES ($1,$2) RETURNING *",
          [id, JSON.stringify(rows)],
        )
      ).rows[0];
    }, "BEGIN ISOLATION LEVEL REPEATABLE READ");
  }

  async review(id) {
    return this.history.transaction(async (client) => {
      const current = await this.getScenario(client, id);
      const pull = (
        await client.query(
          "SELECT * FROM ui_pulls WHERE scenario_id=$1 ORDER BY id DESC LIMIT 1",
          [id],
        )
      ).rows[0];
      if (!pull)
        throw new DemoError(
          "Pull the latest authorization data before reviewing the denial.",
          409,
        );
      const decision = interpret(
        pull.events.map((row) => row.payload),
        [current.identity],
        [current.claim],
      ).decisions[0];
      return (
        await client.query(
          "INSERT INTO ui_reviews (scenario_id,pull_id,decision) VALUES ($1,$2,$3) RETURNING *",
          [id, pull.id, decision],
        )
      ).rows[0];
    }, "BEGIN ISOLATION LEVEL REPEATABLE READ");
  }

  async historyRows(client, current) {
    const i = current.identity;
    return (
      await client.query(
        "SELECT * FROM events WHERE tenant_id=$1 AND source_account_id=$2 AND source_encounter_id=$3 ORDER BY source_sequence",
        [i.tenant_id, i.source_account_id, i.source_encounter_id],
      )
    ).rows;
  }

  async snapshot() {
    await this.activeScenario();
    return this.history.transaction(async (client) => {
      const current = (
        await client.query(
          "SELECT s.* FROM ui_scenarios s JOIN ui_state a ON a.scenario_id=s.id WHERE a.name='active'",
        )
      ).rows[0];
      const publications = (
        await client.query(
          "SELECT * FROM ui_publications WHERE scenario_id=$1 ORDER BY source_sequence DESC",
          [current.id],
        )
      ).rows;
      const events = await this.historyRows(client, current);
      const receipts = (
        await client.query(
          "SELECT r.* FROM receipts r JOIN ui_publications p ON p.event_id=r.event_id WHERE p.scenario_id=$1 ORDER BY r.id DESC",
          [current.id],
        )
      ).rows;
      const pull =
        (
          await client.query(
            "SELECT * FROM ui_pulls WHERE scenario_id=$1 ORDER BY id DESC LIMIT 1",
            [current.id],
          )
        ).rows[0] || null;
      const review = pull
        ? (
            await client.query(
              "SELECT * FROM ui_reviews WHERE pull_id=$1 ORDER BY id DESC LIMIT 1",
              [pull.id],
            )
          ).rows[0] || null
        : null;
      const heartbeat = (
        await client.query("SELECT * FROM service_heartbeats WHERE name=$1", [
          `sink:${process.env.EVENTHUB_NAME || "prior-authorization"}:${process.env.KAFKA_GROUP_ID || "r1dp-sink"}`,
        ])
      ).rows[0];
      return {
        scenario: current,
        publications,
        events,
        receipts,
        pull,
        review,
        receiver: {
          status:
            heartbeat?.status === "connected" &&
            Date.now() - new Date(heartbeat.seen_at).getTime() < 15000
              ? "connected"
              : "unavailable",
          last_seen: heartbeat?.seen_at || null,
        },
      };
    }, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  }
}
