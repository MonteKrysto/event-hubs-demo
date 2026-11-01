# Application walkthrough

We built a small, working demonstration of **one application sharing information that another application can use later**.

The information is a prior-authorization decision. The first application represents Humata. The second represents Flow. Everything runs locally on your computer, using made-up data.

You interact with it through terminal commands. Those commands start programs, send events, inspect saved information, and run the example decision. The [README](../README.md#start-and-run) explains setup and the emulator license setting.

## The business story

Imagine a hospital encounter going through this sequence:

1. Humata determines that authorization is required.
2. Humata attempts to obtain authorization.
3. Humata submits the request.
4. The payer approves it and provides an authorization number.
5. Later, Flow sees a claim denial saying authorization was missing.
6. Flow checks the information Humata previously shared and finds the approval.

Flow can then say:

> There is an approval covering this encounter's service date. Someone should review the evidence to see whether this denial can be disputed.

That is what our example demonstrates. It does not automatically submit a dispute.

## How the information travels

There are five main pieces:

```text
Humata simulator
      ↓
Event Hubs
      ↓
History receiver
      ↓
PostgreSQL
      ↓
Flow simulator
```

Each piece has a separate job.

### 1. The Humata simulator announces what happened

This is a small Node.js program pretending to be Humata.

When you run:

```sh
make produce
```

it sends four announcements:

```text
Authorization required
Authorization attempted
Authorization submitted
Authorization approved
```

We call each announcement an **event**.

An event describes something that happened. For example:

> Authorization for encounter H-ENC-001 was approved, with authorization number AUTH-DEMO-2026.

The event includes identifying information, dates, and a reference to supporting evidence.

Before sending it, our program checks that the event contains the expected information.

### 2. Event Hubs receives the announcements

Event Hubs provides the shared place where applications publish events and other programs receive them.

Humata sends its announcement there. It does not have to find Flow, call Flow directly, or wait for Flow to make a decision.

Our computer runs Microsoft's **Event Hubs emulator**: a local version intended for development and learning.

The Kafka protocol is the agreed way our Node.js programs communicate with that service. KafkaJS is the library handling that communication.

| Piece | Role |
| --- | --- |
| Event Hubs | The service receiving and holding events |
| Kafka protocol | How our programs communicate with it |
| KafkaJS | The Node.js library doing that communication for us |

When you see `BROKER ACK`, Event Hubs has confirmed that it accepted an announcement. The receiver still needs to process that delivery.

### 3. The history receiver saves what arrived

We also built a second Node.js program that stays running and receives events from Event Hubs.

Its job is straightforward:

> Whenever an event arrives, check it and save it.

It writes events into PostgreSQL.

It also records a receipt showing that a particular delivery was processed. That lets us distinguish between "Humata sent this" and "the receiver actually handled this."

If the same event arrives twice, it recognizes the duplicate and keeps one copy of the event. If a message is invalid, it puts it aside for inspection.

### 4. PostgreSQL becomes the lasting record

PostgreSQL stores the authorization history, delivery receipts, invalid messages, and Flow's saved results.

In the eventual architecture, R1DP would provide the historical information. **Our PostgreSQL database is playing that role for this local demonstration.**

This separation matters: Event Hubs transports the events; PostgreSQL keeps the history our example needs later.

You can see that information with:

```sh
make inspect
```

### 5. The Flow simulator uses the saved information

Flow is another small Node.js program representing the future consuming application.

When you run:

```sh
make flow-pull
```

it reads PostgreSQL and asks:

> What do we know about authorization for this denied claim?

Humata and Flow use different encounter identifiers, so we supplied a small [mapping file](../fixtures/crosswalk.json) that tells the demo which identifiers refer to the same encounter.

Flow finds the latest authorization state, checks the service date against the approval dates, and produces:

```text
dispute_candidate
```

That means "a person should review this as a possible dispute."

The receiver saves events continuously. Flow reads them when you run the pull command. That manual command represents the proposed daily pull without making you wait a day.

## What the supporting tools do

Docker runs the programs and services in separate containers. Docker Compose starts them together and connects them.

Azurite supplies storage services needed behind the scenes by the Event Hubs emulator.

The Makefile gives you short commands for operating everything:

| Command | What you are doing |
| --- | --- |
| `make setup` | Prepare the local settings and application image |
| `make up` | Start the services and history receiver |
| `make produce` | Have simulated Humata announce its events |
| `make inspect` | Look at what has been saved |
| `make flow-pull` | Have simulated Flow evaluate the saved information |
| `make demo` | Run and verify the whole sequence automatically |
| `make down` | Stop the services while preserving database history |

## What this proves

We have demonstrated that meaningful authorization information can be sent, received, saved, matched to an encounter, and interpreted by another application. We also checked duplicate delivery, messages arriving out of order, invalid messages, and recovery behavior.

The real Humata integration, actual R1DP/Databricks storage, production identity mappings, and real Flow integration remain future work. The example uses synthetic claims and evidence, so it establishes a working technical path rather than measured customer value.

Continue with the [producer code walkthrough](producer-walkthrough.md).
