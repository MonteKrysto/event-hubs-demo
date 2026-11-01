# Browser demo walkthrough

The browser demo adds two React/TypeScript interfaces to the existing integration. Buttons publish real Kafka events to the Event Hubs emulator. The existing Node.js receiver saves them into PostgreSQL. Flow then reads that saved history when you explicitly request a pull.

## Start the demo

For a fresh checkout, run `make setup`, review the emulator license linked in `.env.example`, and set `ACCEPT_EULA=Y` in `.env`.

```sh
make web
```

Open the overview at [http://localhost:58080](http://localhost:58080). It shows Humata and Flow together. You can also open [Humata](http://localhost:58080/humata) and [Flow](http://localhost:58080/flow) in separate windows. They share the active scenario and update automatically.

The port can be changed with `WEB_HOST_PORT` in `.env`. `make web` prints the configured address.

## 1. Start a fresh scenario

Click **Start new scenario**. Both apps get a new synthetic encounter and claim with fresh identifiers. Existing history is retained; the new scenario is isolated from earlier runs.

Starting a scenario does not publish an event. Flow initially has no pulled authorization data, and **Review denial** is disabled until a pull has been made.

## 2. Publish decisions in Humata

Try these buttons in order:

1. **Authorization required**
2. **Attempt authorization**
3. **Submit request**
4. **Approve authorization**

The approval fields are prefilled with an authorization number and October 2026 coverage. The synthetic claim's service date is October 15, 2026, so the default approval covers it regardless of today's date.

Each click creates a new event ID and an increasing source revision. The server validates the event, records the publication attempt, and sends it through the existing KafkaJS producer.

The publication feed distinguishes event creation, broker acknowledgment, and history persistence. Expand an event to see its delivery steps, receiver receipt count, broker coordinates, and optional JSON payload.

**Broker acknowledged** means Event Hubs accepted the message. **Saved to history** means the separate Kafka receiver actually committed it to PostgreSQL. Delivery can take a few seconds; the browser does not manufacture an arrival just because a button was clicked.

If delivery cannot be confirmed, the activity card offers **Retry this event**. It reuses the same event ID and payload, so a prior successful delivery can be recognized safely. Successfully received events also offer **Replay this event**, which creates another delivery receipt without creating another unique fact.

## 3. Watch Flow's incoming activity

The incoming feed updates automatically when the receiver saves an event. This feed represents platform history, not Flow's account snapshot.

Even after an approval appears in the incoming feed, the account still says **No authorization data pulled yet** until you pull it.

Connection indicators show the browser's live connection, Event Hubs availability, the receiver's recent broker heartbeat, and database availability. A stopped receiver cannot be mistaken for successful ingestion: an acknowledged event remains unsaved until the receiver processes it.

## 4. Pull saved history into Flow

Click **Pull latest data**.

Flow queries PostgreSQL for this scenario's encounter and stores a snapshot of those events. It uses the generated crosswalk to match Humata's identifiers to Flow's account and encounter.

The authorization section now shows the latest pulled state. For an approval, it includes the authorization number, coverage dates, and a **View demo evidence** button. The evidence is the same synthetic fixture referenced by the event; no real source document is fetched from Humata.

This explicit pull represents the daily R1DP batch in the proposed architecture. The demo triggers it manually so you can see the boundary without waiting a day.

## 5. Review the missing-authorization denial

Click **Review denial**.

The review reads only Flow's last pulled snapshot. With the default approval, it finds a matching encounter and coverage for the service date, and displays **Candidate for dispute review**.

This is a recommendation for a person to review the evidence. It does not submit an appeal or perform an external workflow action.

## 6. Make the batch boundary visible

After pulling an approval, click **Deny** in Humata.

The denial appears in incoming history. Flow shows **New history available**, but its account snapshot and earlier recommendation remain unchanged. Reviewing again still uses the earlier pull.

Click **Pull latest data**, then **Review denial** again. Flow now sees the newer denial and recommends **Manual review needed**. The higher source revision wins even if an older event is replayed later.

You can also test coverage by approving with dates outside October 15, then pulling and reviewing again.

## What runs underneath

| Piece | Code |
| --- | --- |
| React/TypeScript screens | [web/src/main.tsx](../web/src/main.tsx) |
| Responsive styles | [web/src/styles.css](../web/src/styles.css) |
| HTTP API and live updates | [src/web-server.js](../src/web-server.js) |
| Scenario, publication, pull, and review operations | [src/demo-service.js](../src/demo-service.js) |
| Kafka producer and receiver | [src/transport.js](../src/transport.js) |
| Database tables | [db/schema.sql](../db/schema.sql) |

The Node API serves the built React files and sends browser updates through Server-Sent Events. It checks PostgreSQL about once a second and broadcasts current state. Event Hubs availability is checked periodically; receiver availability comes from broker heartbeat instrumentation.

The browser API records publication metadata in `ui_publications`. It never inserts a received event into `events`; only the Kafka receiver does that. `ui_pulls` stores Flow snapshots and `ui_reviews` stores their review results. These browser scenarios are separate from the CLI's `demo_scenarios` and `flow_batches` fixtures.

## Tests and development

```sh
make test             # Node, PostgreSQL, HTTP, and snapshot behavior
make test-acceptance  # Real Kafka acceptance exercise
```

The suite checks that acknowledgment alone does not create received history, new events cannot silently change a pulled snapshot, retries preserve event IDs, concurrent actions use distinct revisions, and new scenarios remain isolated.

For frontend development with Node.js 24+ installed:

```sh
make web
npm ci
npm run dev:web
```

Vite serves the interface on port 5173 and proxies API requests to the running Docker web service. Set `DEMO_API_URL` when using a different API address. `npm run build:web` checks TypeScript and builds the frontend. Changes to server code or the Docker-served interface require `make web` to rebuild and restart the affected services.

## Stop or reset

`make down` stops the demo and preserves PostgreSQL data, including the browser scenario and snapshots. `make web` restores it later.

Use **Start new scenario** for an ordinary fresh presentation. `make reset` deletes the demo's PostgreSQL volume and stops its containers; use that only when you intend to clear all saved demo data.
