# Producer code walkthrough

The producer has two main parts:

- **Create the events:** `scenario()` in [src/domain.js](../src/domain.js).
- **Send them to Event Hubs:** `publish()` in [src/transport.js](../src/transport.js).

This walkthrough follows what happens when you run `make produce`. For the wider business story, see the [application walkthrough](application-walkthrough.md).

## 1. The command starts the publishing process

The command reaches the `produce` branch in [src/cli.js](../src/cli.js):

```js
await waitReady();
await publish(await history.savedScenario(), {
  duplicate: values.duplicate
});
```

In plain English:

> Wait until Event Hubs is reachable, get our example events, and publish them.

The default scenario is saved in PostgreSQL by `savedScenario()` in [src/history.js](../src/history.js). The first use saves the example events. Subsequent runs reuse those saved events, including their original IDs and timestamps, which lets us practice replaying the same information.

If you supply a custom event file with `--file`, the command reads those events instead.

## 2. The scenario creates the example events

In `domain.js`, this list supplies one authorization state for each event:

```js
['required', 'attempted', 'submitted', 'approved']
```

The function uses `.map()` to go through the list and build an event for each item.

Inside that loop, we build an ordinary JavaScript object. Here is a shortened version; the complete object includes other required fields:

```js
const event = {
  event_id: randomUUID(),
  tenant_id: 'demo-hospital',
  source_encounter_id: 'H-ENC-001',
  source_sequence: index + 1,
  status,
};
```

| Field | Meaning |
| --- | --- |
| `event_id` | A unique identifier for this particular announcement |
| `tenant_id` | Which customer the information belongs to |
| `source_encounter_id` | Which encounter it concerns |
| `source_sequence` | Whether this is the first, second, third, or fourth update |
| `status` | What happened: required, attempted, submitted, or approved |

The complete object also includes the event type, account identifier, timestamps, and an evidence reference. For the approval, the code adds the authorization number and coverage dates:

```js
if (status === 'approved') {
  Object.assign(event, {
    authorization_number: 'AUTH-DEMO-2026',
    valid_from: '2026-10-01',
    valid_to: '2026-10-31'
  });
}
```

In a real Humata integration, these events would be created as authorization decisions happen. Our demo generates the four example states.

## 3. Publishing checks the events

Back in `transport.js`:

```js
events.forEach(validate);
```

This checks every event against our agreed message template: the [JSON Schema](../schemas/prior-authorization.v1.json).

For example, an approved event must include an authorization number and valid dates. If validation fails, publishing stops before any of the events are sent.

## 4. The producer connects and sends

Next:

```js
const client = producer();
await client.connect();
```

`producer()` creates a KafkaJS sender. Its settings tell it where Event Hubs is and how to authenticate. Inside Docker, the address is `eventhubs:9092`, supplied by [compose.yaml](../compose.yaml).

Then, for each event, we call:

```js
const acknowledgments = await client.send({
  topic,
  acks: -1,
  messages: [{
    key: eventKey(event),
    value: JSON.stringify(event)
  }]
});
```

The important pieces are:

- **`topic`** selects the event hub, `prior-authorization`.
- **`key`** identifies the tenant and encounter, helping keep that encounter's messages together on one partition with the current configuration.
- **`value`** contains the event. `JSON.stringify()` turns the JavaScript object into JSON text.
- **`await`** makes our code wait for the send operation to finish.
- **`acks: -1`** requests confirmation from the service.

KafkaJS handles the Kafka communication for us.

If duplicate mode is enabled, the sending loop sends the final event one extra time. The normal `make produce` command sends four messages. `make produce ARGS="--duplicate"` sends five messages, including a repeat of the approval.

## 5. Event Hubs confirms receipt

The response contains the partition and offset where the message was accepted:

```js
console.log(`BROKER ACK: ${JSON.stringify(receipt)}`);
```

That produces output such as:

```text
BROKER ACK: {"topic":"prior-authorization","partition":0,"offset":"20"}
```

The offset is the message's position within that partition. Your offset values will change as you publish more messages.

Finally, the producer closes its connection:

```js
await client.disconnect();
```

That cleanup is inside a `finally` block, so it runs even when an earlier operation fails.

At that point, the producer's job is complete. Event Hubs has accepted the events; the separate receiver is responsible for reading them and saving the history. A broker acknowledgment alone does not prove that the receiver or Flow has processed the event.
