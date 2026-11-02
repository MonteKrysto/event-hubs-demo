# Fiber: local Event Hubs + Kafka demo

A synthetic **Humata → Azure Event Hubs over Kafka → PostgreSQL event history → Flow** example. Producers, consumers, the Flow simulator, and tests all use **Node.js**. Docker Compose runs the applications, Microsoft's Event Hubs emulator, Azurite, and PostgreSQL.

The story is `required → attempted → submitted → approved`. Later, Flow sees a missing-authorization claim denial, finds the approval for the same encounter and service date, and labels it `dispute_candidate` for human review. It never submits an appeal or changes another application's workflow.

## Walkthroughs

- [Browser demo walkthrough](docs/browser-demo-walkthrough.md): operate the Humata and Flow screens and follow a decision through the pipeline.
- [Application walkthrough](docs/application-walkthrough.md): a simple explanation of what we built, the business scenario, and how the pieces work together.
- [Producer code walkthrough](docs/producer-walkthrough.md): follow an event from creation through validation, publishing, and acknowledgment.

## What runs locally

```mermaid
flowchart LR
    H[Humata simulator: Node.js] -->|Kafka producer| EH[Azure Event Hubs emulator]
    AZ[Azurite] -. emulator dependency .-> EH
    EH -->|Kafka consumer: Node.js| S[History sink]
    S --> DB[(PostgreSQL event history)]
    DB -->|Explicit daily-style batch pull| F[Flow simulator: Node.js]
    CW[Identity crosswalk + synthetic claim] --> F
    F --> R[Saved review recommendation]
```

| Production concept | Local implementation |
| --- | --- |
| Humata | Node.js producer using KafkaJS and synthetic authorization events |
| Azure Event Hubs | Microsoft's official emulator, Kafka endpoint |
| Storage needed by the emulator | Azurite; internal to Compose |
| R1DP / Databricks sink and history | Node.js Kafka consumer writing PostgreSQL through `pg` |
| Agreed schema + schema registry | Proposed v1 JSON Schema in `schemas/`, validated with Ajv by producer and sink |
| CADM/data-platform crosswalk | Explicit mapping in `fixtures/crosswalk.json` |
| Humata-owned immutable evidence | Content-addressed `humata://` reference and SHA-256 of a synthetic fixture |
| Flow daily pull | On-demand `flow-pull` command reading a database snapshot and saving a batch result |

R1DP, Databricks, Humata, Flow, and a registry service are represented by local substitutes. This proves transport, persistence, identity matching, and interpretation for one synthetic integration. It does not prove real platform interoperability, clinical correctness, an agreed production contract, or customer value. PostgreSQL stands in for R1DP; it is not a Databricks emulator.

No cloud account, Azure resources, or real patient data are needed. Initial image/package downloads require internet access; the running scenario is local. KafkaJS is a client library; there is no separate Apache Kafka or Confluent broker.

## Start and run

Prerequisites: Docker Desktop running with Docker Compose v2. The images support native Apple Silicon and x86-64. All commands below run in containers, so no host Node.js installation is required.

### Browser demo

After setting `ACCEPT_EULA=Y` in `.env`, start the React/TypeScript interfaces and local Node API:

```sh
make web
```

Open [both apps side by side](http://localhost:58080), [Humata](http://localhost:58080/humata), or [Flow](http://localhost:58080/flow). Use **New encounter** to create fresh synthetic identities without deleting prior history. Both routes share the active scenario, so they can be opened in separate browser windows.

Choose any of the six authorization actions in Humata; all are visible on the encounter. For a first run, choose **Approve authorization**, wait for **Shared successfully**, then use **Load latest update** and **Evaluate denial** in Flow. Flow uses only the information you explicitly load. Event history, delivery details, and the connection diagram are available in expandable sections. `make down` stops the web service along with the rest of the demo. Set `WEB_HOST_PORT` in `.env` to change the browser port.

Use **Add sample encounters** to add 48 fictional encounters and 126 authorization events through the Kafka pipeline. Select a case from the **Encounter** list; expand **Encounter details** for payer, procedure, provider, claim amount, and dates. The **Original claim denial** stays separate from the side-by-side **Humata’s latest status** and **Loaded in Flow** values. Each encounter retains its own loaded data and evaluation. Existing data is kept, and adding the sample set again does not duplicate it.


The existing command-line workflow below remains available and uses its own saved scenario.

The Makefile provides shortcuts for the full workflow:

```sh
make setup       # Create .env if missing and build the Node.js image.
# Review the emulator license and set ACCEPT_EULA=Y in .env.
make demo        # Build, start the services, publish, receive, and run Flow.
make help        # List every target.
```

`make setup` preserves an existing `.env`. `make demo` (also available as `make run`), `make produce`, and `make flow-pull` start the services automatically. Make checks the configured license acceptance before starting the emulator.

| Command | Purpose |
| --- | --- |
| `make up` / `make ready` | Build/start the services; optionally wait for Kafka readiness |
| `make web` | Start the React interfaces and browser API |
| `make produce` / `make flow-pull` | Publish the saved scenario / run Flow's batch |
| `make inspect` / `make db` | Inspect saved data / open PostgreSQL's interactive client |
| `make sample > scenario.json` | Generate fresh event JSON; build output goes to stderr |
| `make test-unit` | Run domain tests without starting services |
| `make test` | Run the Node.js suite against PostgreSQL |
| `make test-acceptance` / `make test-all` | Run Kafka acceptance / both test suites |
| `make status` / `make logs SERVICES=sink` | Show containers / follow receiver logs |
| `make stop` / `make down` | Stop / remove containers while preserving database data |
| `make restart SERVICES=sink` | Restart only the receiver |
| `make reset` | **Delete PostgreSQL data** and remove this demo's containers |

Pass application options through `ARGS`, using `/workspace` paths for files in this project:

```sh
make produce ARGS="--file /workspace/scenario.json --duplicate"
make flow-pull ARGS="--claims /workspace/my-claims.json"
```

`make reset` leaves services stopped; run `make demo` afterward for a fresh scenario. Restarting the emulator discards its transport state, while PostgreSQL retains history unless explicitly reset. The Docker Compose equivalents and the receiver-outage exercise follow below.

For a fresh checkout, create the local settings file:

```sh
cp .env.example .env
```

Read the [Microsoft emulator license](https://github.com/Azure/azure-event-hubs-emulator-installer/blob/main/LICENSE), then set `ACCEPT_EULA=Y` in `.env` to accept it. The example defaults to `N`.

```sh
docker compose up -d --build sink
docker compose run --rm --no-deps app demo
```

The build installs dependencies from `package-lock.json`. Compose waits for PostgreSQL's health check before starting the sink, and the app waits for authenticated Kafka topic metadata before publishing or consuming.

The demo sends **five messages**: four different events and an exact replay of the approval. Look for:

```text
READY: Kafka endpoint exposes prior-authorization with 2 partitions
1. HUMATA: publishing four states plus a duplicate approval
BROKER ACK: {"topic":"prior-authorization","partition":...,"offset":"..."}
2. HISTORY: all five deliveries received, four unique events persisted; triggering Flow's daily pull
...
"recommendation": "dispute_candidate"
"authorization_number": "AUTH-DEMO-2026"
...
PASS: Kafka send + receive, PostgreSQL history, identity crosswalk, and Flow batch interpretation
```

PostgreSQL contains four unique scenario events, five delivery receipts, and a Flow batch result. The approval's evidence reference accompanies the recommendation. The CLI’s four states share one synthetic evidence fixture for simplicity. Browser decisions use individual synthetic records matching each decision; older browser events keep their original references and clearly label the reused fixture. A real producer would reference source-owned evidence for each state.

Re-running `demo` reuses event IDs saved in PostgreSQL. Broker offsets advance and new delivery receipts appear, while the unique history stays at four events. Each successful run saves another Flow batch. A new receipt is required for every message sent in that run: old history alone cannot make the check pass if the receiver is stopped.

## See each step separately

Start the services as above, then watch reception in one terminal:

```sh
docker compose logs -f sink
```

In another terminal:

```sh
# Publish four states without triggering Flow.
docker compose run --rm --no-deps app produce

# Inspect events, delivery receipts, rejections, and earlier Flow batches.
docker compose run --rm --no-deps app inspect

# Trigger the daily-style query now instead of waiting a day.
docker compose run --rm --no-deps app flow-pull
```

Wait for the sink's `stored` or `duplicate` messages before pulling. `demo` waits automatically. Flow never subscribes directly to Event Hubs. It sees the history persisted by the time of its pull; a pull before any events returns `manual_review`.

The sink receives continuously. The daily boundary is represented by an explicit command; no scheduler is installed. Each pull uses a PostgreSQL **REPEATABLE READ full-history snapshot**, not an incremental export. It saves the result and a `through_ingest_id` in one transaction. IDs and Kafka offsets are represented as strings where needed to preserve 64-bit precision. Sequence gaps are normal after duplicate inserts.

## Inspect PostgreSQL directly

Use the bundled database client:

```sh
docker compose exec postgres psql -U fiber -d fiber
```

Then explore:

```sql
-- Event payloads and their original broker coordinates.
SELECT ingest_id, payload->>'status' AS status, source_sequence,
       topic, partition_id, kafka_offset
FROM events ORDER BY ingest_id;

-- Every delivery, including replays.
SELECT id, result, event_id, partition_id, kafka_offset
FROM receipts ORDER BY id;

-- Invalid messages remain inspectable.
SELECT id, reason, raw_base64 FROM rejections ORDER BY id;

-- Flow changes only when a batch is pulled.
SELECT id, pulled_at, report->'decisions' AS decisions
FROM flow_batches ORDER BY id;

-- Quit psql with: \q
```

For a desktop database client, use host `localhost`, port `55432`, database `fiber`, user `fiber`, and password `fiber-local-only`. These are disposable local demo credentials. Only loopback is published. Change the host port using `POSTGRES_HOST_PORT` in `.env` if needed.

The tables are defined in `db/schema.sql` and initialized by the application. Both sink and CLI serialize initialization safely. `demo_scenarios` stores the default event array so replay IDs survive application and database restarts.

## Understand the code and event

| File | Start here to learn |
| --- | --- |
| `src/transport.js` | KafkaJS configuration, producer acknowledgments, consumer, and manual offset commits |
| `src/history.js` | Parameterized PostgreSQL queries, transactions, deduplication, quarantine, and Flow snapshots |
| `src/domain.js` | Synthetic events, schema validation, identity mapping, and interpretation |
| `src/cli.js` | Commands and the end-to-end verification sequence |
| `db/schema.sql` | Event history, receipts, rejections, Flow batches, and replay fixtures |
| `schemas/prior-authorization.v1.json` | Proposed event contract |

Generate a fresh JSON example to inspect or edit:

```sh
docker compose run --rm --no-deps app sample > scenario.json
```

| Field | Purpose |
| --- | --- |
| `event_id` | Stable fact identifier; unchanged on retry/replay |
| `event_type`, `schema_version` | Contract and version |
| `tenant_id` | Scopes identity to a synthetic customer |
| `source_account_id`, `source_encounter_id` | Humata identifiers, mapped explicitly to Flow identifiers |
| `source_sequence` | Increasing revision for the source account/encounter; resolves late arrival |
| `occurred_at`, `published_at` | Source business time versus publication time |
| `status` | `not_required`, `required`, `attempted`, `submitted`, `approved`, or `denied` |
| `authorization_number`, `valid_from`, `valid_to` | Required on approval; service-date coverage is inclusive |
| `evidence.uri`, `evidence.sha256` | Reference to source-owned evidence and its expected content hash |

The timeline is fixed in October 2026 to make the demo repeatable. Flow uses the claim's fixed service date, not today's date. Publication time reflects creation of the saved scenario; replay preserves the original payload. Revision and identity conventions would need agreement with Humata.

The Kafka key is `[tenant_id, source_encounter_id]`. With the configured partition count, the same encounter goes to the same partition. Kafka has no global ordering across partitions. Flow chooses the highest source sequence rather than the last-arriving event.

The schema file is a draft contract, not a registry. The emulator does not support Azure schema registry integration. Clinical state-transition rules are not enforced. The `humata://` URI is not a live document endpoint; Flow preserves its reference and hash without fetching a real source document. No evidence files are uploaded into shared storage.

## Learning exercises

**Replay a delivery.** Run `demo` twice, then query `events` and `receipts`. The unique event count stays fixed while receipts grow. JSON property order does not affect equality, but changing content while retaining an event ID is quarantined.

**Try a later denial.** Generate `scenario.json`, keep only its approval event, assign a new UUID to `event_id`, set `source_sequence` to `5`, and change `status` to `denied`. Keep the file as a JSON array with the same source identity.

```sh
docker compose run --rm --no-deps app produce --file /workspace/scenario.json
# Wait for the sink's stored message.
docker compose run --rm --no-deps app flow-pull
```

Flow now returns `manual_review`, even though the old approval remains in history. Replaying sequence 4 cannot override sequence 5. The default `demo` approval check will fail until you reset the database or publish a suitable later approval.

A newly generated four-event sample has fresh IDs. Publishing it over an existing sequence 1–4 timeline correctly creates revision conflicts. Use the saved default scenario for replay, or use a new encounter with its own crosswalk and claim.

**Break the crosswalk.** Copy `fixtures/crosswalk.json`, change the source encounter, and run `flow-pull --crosswalk /workspace/your-crosswalk.json`. Flow returns `manual_review` with an unresolved identity. Duplicate and many-to-one ambiguous mappings fail the batch instead of guessing.

**Change the service date.** Copy `fixtures/claims.json`, set `service_date` to `2026-11-01`, and run `flow-pull --claims /workspace/your-claims.json`. The October approval no longer covers the claim. Real matching would also require payer, procedure, authorization scope, and other agreed rules.

**Stop the receiver.** Run `docker compose stop sink`, then `docker compose run --rm --no-deps app demo --timeout 3`. Broker acknowledgments can succeed, but the demo must fail because there are no new receiver receipts. Restore it with `docker compose start sink`, then rerun the demo. Pending deliveries are handled safely.

## Delivery and database behavior

The producer validates the entire input before sending and awaits Kafka acknowledgments. An acknowledgment proves broker receipt, not PostgreSQL persistence or Flow processing. The demo separately waits for the corresponding receiver receipts by topic, partition, and offset after its starting receipt cursor.

The Node.js consumer disables automatic offset commits. For each delivery it commits an event or a rejection **and its receipt in one PostgreSQL transaction**, then explicitly commits the next Kafka offset. A crash after the database commit but before the Kafka commit causes redelivery; uniqueness on `event_id` and source revision prevents duplicate facts. Concurrent copies are handled with PostgreSQL's unique constraints and `ON CONFLICT`. This is at-least-once delivery with application deduplication, not end-to-end exactly-once delivery.

Malformed JSON, invalid schema, wrong keys, tombstones, and conflicting events go into `rejections` with the original bytes encoded as base64. The consumer then commits past them so a bad message does not block its partition. This quarantine is a PostgreSQL table, not a Kafka dead-letter topic. Replaying rejected content after correction is a manual exercise.

Database failures roll back both the event and receipt and propagate to the consumer. The failed delivery's Kafka offset is not committed. The sink exits on a consumer crash and Compose restarts it to retry. The application inserts history and never updates existing event payloads, but this is not a hardened immutable/WORM database.

Kafka coordinates are diagnostic, not globally unique: offsets can restart after the emulator restarts. A receipt cursor identifies new observations independently of broker offset reuse.

## Tests

Start the services and build the image first, then run the Node test suite:

```sh
docker compose run --rm --no-deps --entrypoint npm app test
```

The suite includes domain tests, tests against **real PostgreSQL** in disposable isolated schemas, and checks that failed storage prevents Kafka commits. It covers duplicate/concurrent delivery, conflicting revisions, malformed events, tenant isolation, ambiguous mappings, date coverage, atomic rollback, saved replay IDs, and 64-bit offsets.

Run the actual Kafka acceptance exercise:

```sh
docker compose run --rm --no-deps --entrypoint npm app run test:acceptance
```

It starts a Node consumer, publishes a separate encounter in reverse sequence order, sends a duplicate and invalid JSON, verifies four stored facts and quarantine, then confirms Flow selects the approval. It uses the dedicated `prior-authorization-tests` event hub and a disposable PostgreSQL schema, leaving the default scenario untouched.

For host development, install Node.js 24+, run `npm ci`, and use `npm run test:unit` for domain-only tests. PostgreSQL-backed tests also need `DATABASE_URL`, for example `postgresql://fiber:fiber-local-only@localhost:55432/fiber`. The tested Kafka path keeps clients inside Compose to avoid broker-address differences between host and container networking.

## Stop and reset

Stop this project's services while preserving PostgreSQL data:

```sh
docker compose down
```

Start again using the original `up` command. The `postgres-data` volume retains history, receipts, saved scenario IDs, and Flow batches. The emulator's own events and broker state are disposable. Replaying `demo` repopulates the transport and deduplicates against PostgreSQL.

To delete this demo's PostgreSQL data and start fresh:

```sh
docker compose down -v
docker compose up -d --build sink
docker compose run --rm --no-deps app demo
```

If upgrading from the earlier Python/SQLite prototype, that old `history` Docker volume is left untouched and unused. The active implementation creates a fresh PostgreSQL scenario; it does not migrate old synthetic SQLite batches.

## Configuration and troubleshooting

| Setting | Default | Meaning |
| --- | --- | --- |
| `ACCEPT_EULA` in `.env` | `N` in the example | Set `Y` after accepting emulator terms |
| `KAFKA_HOST_PORT` | `59092` | Host mapping of Kafka port 9092 |
| `EMULATOR_HEALTH_PORT` | `55300` | Host mapping of emulator HTTP port 5300 |
| `POSTGRES_HOST_PORT` | `55432` | Host mapping of PostgreSQL port 5432 |
| `WEB_HOST_PORT` | `58080` | Host mapping of the local React interface and API |
| `DATABASE_URL` in app environment | Local `fiber` PostgreSQL database | Node PostgreSQL connection string |
| `DB_SCHEMA` in app environment | `public` | PostgreSQL schema; tests use isolated schemas |
| `KAFKA_BOOTSTRAP_SERVERS` in app environment | `eventhubs:9092` | Kafka address inside Compose |
| `EVENTHUB_CONNECTION_STRING` in app environment | Local development connection string | SASL password; not an Azure credential |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` | Emulator protocol; use `SASL_SSL` in Azure |
| `EVENTHUB_NAME` | `prior-authorization` | Event hub / Kafka topic |
| `KAFKA_GROUP_ID` | `r1dp-sink` | Sink consumer group |
| `KAFKAJS_LOG_LEVEL` in `.env` or shell environment | `error` | Kafka client verbosity; `debug` includes expected shutdown resets |

Host port settings are read from `.env`; application settings are in the shared Compose environment block. PostgreSQL, Kafka, and emulator HTTP host ports bind only to loopback. Azurite stays internal.

```sh
docker compose ps -a
docker compose logs --tail 100 eventhubs sink postgres
docker compose run --rm --no-deps app ready
docker compose run --rm --no-deps app inspect
```

- **Startup timeout:** check the license setting, Docker resources, and emulator logs. Connection refusals can appear briefly before startup finishes.
- **Port already allocated:** select an unused host port in `.env` and run `up` again. The defaults avoid the other projects running on this machine.
- **PostgreSQL unavailable:** check its health and logs. `--no-deps` commands assume the services were already started.
- **Expected disconnects:** a narrowly scoped KafkaJS logging patch records `ECONNRESET` at DEBUG only after a local socket shutdown with no pending or in-flight requests. Active resets, connection refusals, timeouts, and other errors retain ERROR severity. Request rejection and retry behavior are unchanged. Use `KAFKAJS_LOG_LEVEL=debug make produce` to include the expected shutdown diagnostics. Raw emulator logs can still contain tenant and AMQP teardown warnings; use `make logs SERVICES=sink` to focus on application processing.
- **Node 24 timer warning:** the pinned KafkaJS version has an upstream idle-timer bug. `scripts/patch-kafkajs.js` applies a narrow install-time fix for [issue #1751](https://github.com/tulios/kafkajs/issues/1751), skipping scheduling when no request or throttle is pending. Installation fails if the dependency source no longer matches. Do not bypass lifecycle scripts when installing this project.
- **Processor mismatch on Apple Silicon:** remove any forced Intel platform, run `docker compose pull eventhubs`, then run `up` again for the native image.
- **Event acknowledged but missing from history:** inspect sink logs and `rejections`. Broker receipt is only the first checkpoint.
- **Code, schema, or bundled fixture changes:** rebuild with `docker compose up -d --build sink`. SQL edits are not an automatic migration system for existing tables. Custom input files can be read directly from the read-only `/workspace` mount.
- **Entity configuration changes:** recreate the emulator after editing `config/eventhubs.json`; the demo does not create topics through a client management API.

## Moving toward the real POC

1. Agree with Humata and the data-platform team on event identity, revisions, corrections, schema ownership, and crosswalk ownership.
2. Connect to an Azure Event Hubs namespace using Kafka with `SASL_SSL`, real credentials or the agreed identity mechanism, and sender/receiver access.
3. Register and govern the agreed schema through the selected registry; checked-in validation does not test registry compatibility policies.
4. Replace PostgreSQL ingestion with the actual R1DP/Databricks pipeline, preserving raw events, metadata, rejections, replay behavior, and durable checkpoints.
5. Replace the fixture crosswalk and manual batch with approved identity data and Flow's actual daily R1DP interface.
6. Validate source evidence access, integrity, authorization scope, and customer-specific interpretation with the owning systems.

This excludes Flink, cross-system aggregation, a shared operational read model, orchestration, and evidence relocation. It does not establish business value or resolve whether an overlapping customer should gate the MVP.

## References

- [Microsoft: Event Hubs emulator overview and limitations](https://learn.microsoft.com/en-us/azure/event-hubs/overview-emulator)
- [Microsoft: Local Compose setup and Kafka authentication](https://learn.microsoft.com/en-us/azure/event-hubs/test-locally-with-event-hub-emulator)
- [KafkaJS: Producing messages](https://kafka.js.org/docs/producing)
- [KafkaJS: Consuming messages and manual offset commits](https://kafka.js.org/docs/consuming)
- [node-postgres: Transactions](https://node-postgres.com/features/transactions)
