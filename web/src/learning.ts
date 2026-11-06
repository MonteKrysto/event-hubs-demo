import type { Event, HistoryEvent, Snapshot } from "./types.ts";

export function latestEvent(rows: HistoryEvent[]): Event | undefined {
  return rows.reduce<Event | undefined>(
    (latest, row) =>
      !latest || row.payload.source_sequence > latest.source_sequence
        ? row.payload
        : latest,
    undefined,
  );
}

export function learningState(state: Snapshot) {
  const hasDenial =
    !state.scenario.claim.status || state.scenario.claim.status === "denied";
  const publication = [...state.publications].sort(
    (a, b) => b.event.source_sequence - a.event.source_sequence,
  )[0];
  const received = latestEvent(state.events);
  const loaded = latestEvent(state.pull?.events || []);
  const latest =
    publication &&
    (!received || publication.event.source_sequence >= received.source_sequence)
      ? publication.event
      : received;
  const saved =
    !!latest && state.events.some((row) => row.event_id === latest.event_id);
  const webhook = state.integration?.webhooks.find((row) => row.event_id === latest?.event_id);
  const receivedByWebhookService = state.integration?.inbox.some((row) => row.event_id === latest?.event_id);
  const webhookMissing = !saved && !receivedByWebhookService && (webhook?.state === "skipped" || webhook?.state === "failed");
  const deliveryMessage = !saved && webhookMissing
    ? webhook?.state === "skipped"
      ? "Saved in Humata. Its webhook was skipped. Open Delivery lab and choose Check Humata API to recover it."
      : "Saved in Humata. Its webhook could not be delivered. Restore the webhook receiver in Delivery lab, or check the Humata API."
    : !saved && receivedByWebhookService && state.integration?.controls.publisher_paused
      ? "Safely received. Publishing to Event Hubs is paused in Delivery lab."
      : null;
  const accepted =
    saved ||
    (!!publication?.acknowledgment &&
      publication.event_id === latest?.event_id);
  const inFlow =
    !!latest &&
    (state.pull?.events || []).some((row) => row.event_id === latest.event_id);
  const newHistory = state.events.filter(
    (row) =>
      !(state.pull?.events || []).some((old) => old.event_id === row.event_id),
  ).length;
  const currentReview =
    hasDenial &&
    !!latest &&
    inFlow &&
    !!state.review &&
    state.review.pull_id === state.pull?.id;
  const reviewStale = hasDenial && !!state.review && !!latest && !inFlow;
  const deliveryUnconfirmed =
    !saved &&
    !!publication &&
    (webhookMissing || publication.publication_state === "unconfirmed" ||
      (publication.publication_state === "pending" &&
        !state.integration?.controls.publisher_paused &&
        Date.now() - new Date(publication.created_at).getTime() > 30000));
  const stages = [
    {
      title: "Humata creates",
      text: latest
        ? `Update ${latest.source_sequence} recorded`
        : "Record an approval",
      done: !!latest,
      attention: false,
    },
    {
      title: "Event Hubs accepts",
      text: accepted
        ? `Update ${latest!.source_sequence} accepted`
        : deliveryUnconfirmed
          ? "Delivery unconfirmed"
          : latest
            ? "Waiting for acceptance"
            : "Waiting for a decision",
      done: accepted,
      attention: deliveryUnconfirmed,
    },
    {
      title: "Receiver saves",
      text: saved
        ? `Update ${latest!.source_sequence} saved`
        : accepted
          ? "Waiting for receiver"
          : "Waiting for delivery",
      done: saved,
      attention: accepted && !saved,
    },
    {
      title: "Flow loads",
      text: inFlow
        ? `Using update ${loaded!.source_sequence}`
        : loaded
          ? `Still using update ${loaded.source_sequence}`
          : "No data loaded",
      done: inFlow,
      attention: !!latest && !inFlow && !!state.pull,
    },
    {
      title: "Flow evaluates",
      text: !hasDenial
        ? "Not applicable — no denial"
        : currentReview
          ? "Latest data evaluated"
          : reviewStale
            ? "Earlier data evaluated"
            : latest
              ? "Evaluation still needed"
              : "Waiting for data",
      done: currentReview,
      attention: reviewStale,
    },
  ];
  if (webhook || receivedByWebhookService) stages.splice(1, 0, {
    title: "Webhook / API receiver",
    text: receivedByWebhookService ? `Update ${latest!.source_sequence} safely received`
      : webhook?.state === "skipped" ? "Webhook skipped; API recovery needed"
      : webhook?.state === "failed" ? "Webhook delivery failed" : "Waiting for webhook",
    done: !!receivedByWebhookService,
    attention: !!webhookMissing,
  });
  let next: {
    step: number;
    title: string;
    description: string;
    label: string;
    action:
      | "approve"
      | "wait"
      | "load"
      | "review"
      | "explore"
      | "retry"
      | "details";
  };
  if (!hasDenial && inFlow)
    next = {
      step: 5,
      title: "Authorization history loaded",
      description: `The claim is ${state.scenario.claim.status}; there is no denial to evaluate.`,
      label: "No denial to evaluate",
      action: "wait",
    };
  else if (!latest)
    next = {
      step: 1,
      title: "Start with an approval in Humata",
      description:
        "This synthetic claim was denied for missing authorization. Record an approval so Flow can discover it later.",
      label: "Publish sample approval",
      action: "approve",
    };
  else if (deliveryUnconfirmed)
    next = {
      step: 2,
      title: "Check this delivery before continuing",
      description:
        deliveryMessage || "Acceptance has not been confirmed. The outbox publisher will retry; inspect the delivery details if needed.",
      label: "Show delivery details",
      action: "retry",
    };
  else if (!saved)
    next = {
      step: 2,
      title: accepted
        ? "Accepted by Event Hubs; waiting for the receiver"
        : "Sending the decision to Event Hubs",
      description: accepted
        ? "The event has been accepted, but it is not saved in shared history yet. Flow cannot load it until the receiver saves it."
        : "A click records a decision. We wait for a real acknowledgment before marking it accepted.",
      label: "Waiting for delivery…",
      action: "wait",
    };
  else if (!inFlow)
    next = {
      step: 3,
      title: "The event is saved. Now load it into Flow.",
      description: loaded
        ? `Shared history includes update ${latest.source_sequence}. Flow is still using update ${loaded.source_sequence} from its earlier load.`
        : "The platform has received the event. Flow has not loaded this authorization information yet.",
      label: "Load into Flow",
      action: "load",
    };
  else if (!currentReview)
    next = {
      step: 4,
      title: "Use the loaded information to evaluate the denial",
      description: `Flow has loaded update ${loaded!.source_sequence}. Evaluate whether it contains an approval that covers the claim’s service date.`,
      label: "Evaluate the denial",
      action: "review",
    };
  else if (state.review!.decision.recommendation === "dispute_candidate")
    next = {
      step: 5,
      title: "You connected a decision to a review",
      description:
        "Flow found a possible dispute using its loaded data. Try a denial next, then load and evaluate again to see what changes.",
      label: "Try another outcome",
      action: "explore",
    };
  else if (loaded?.status === "approved")
    next = {
      step: 5,
      title: "The evaluation explains why manual review is needed",
      description:
        "An approval alone is not enough. Check its dates against the service date, then publish a correction if appropriate.",
      label: "Check approval details",
      action: "details",
    };
  else
    next = {
      step: 5,
      title: "This loaded decision needs manual review",
      description:
        "Flow evaluated the information it loaded. Publish an approval to compare how a different decision changes the result.",
      label: "Publish sample approval",
      action: "approve",
    };
  return {
    latest,
    received,
    loaded,
    saved,
    accepted,
    inFlow,
    newHistory,
    currentReview,
    reviewStale,
    deliveryUnconfirmed,
    deliveryMessage,
    stages,
    next,
  };
}

export function reviewChecks(state: Snapshot) {
  const event = latestEvent(state.pull?.events || []);
  const identity = state.scenario.identity;
  const claim = state.scenario.claim;
  const matched =
    !!event &&
    event.tenant_id === identity.tenant_id &&
    event.source_account_id === identity.source_account_id &&
    event.source_encounter_id === identity.source_encounter_id &&
    claim.account_id === identity.account_id &&
    claim.encounter_id === identity.encounter_id &&
    claim.tenant_id === identity.tenant_id;
  const dates = !!event?.valid_from && !!event?.valid_to;
  const covered =
    dates &&
    event!.valid_from! <= claim.service_date &&
    claim.service_date <= event!.valid_to!;
  const evidence = !!event?.evidence?.uri && !!event.evidence.sha256;
  return [
    {
      label: "Same encounter",
      result: !event ? "unavailable" : matched ? "pass" : "fail",
      detail: !event
        ? "No authorization data was loaded."
        : matched
          ? "Humata’s encounter is mapped to this Flow account and visit."
          : "The source identifiers do not match this account and visit.",
    },
    {
      label: "Authorization approved",
      result: !event
        ? "unavailable"
        : event.status === "approved"
          ? "pass"
          : "fail",
      detail: !event
        ? "No decision was available to evaluate."
        : event.status === "approved"
          ? `Approval ${event.authorization_number} was loaded.`
          : `The loaded status is ${event.status.replace("_", " ")}.`,
    },
    {
      label: "Service date covered",
      result: !dates ? "unavailable" : covered ? "pass" : "fail",
      detail: !dates
        ? "No approval dates were available to check."
        : covered
          ? `${claim.service_date} falls within ${event!.valid_from} through ${event!.valid_to}.`
          : `${claim.service_date} falls outside ${event!.valid_from} through ${event!.valid_to}.`,
    },
    {
      label: "Evidence reference provided",
      result: !event ? "unavailable" : evidence ? "pass" : "fail",
      detail: evidence
        ? "A source document is referenced. A person still needs to review it."
        : "No supporting document reference was loaded.",
    },
    {
      label: "Denial concerns authorization",
      result: !event
        ? "unavailable"
        : claim.denial_reason === "missing_authorization"
          ? "pass"
          : "fail",
      detail:
        claim.denial_reason === "missing_authorization"
          ? "The original denial cites missing authorization."
          : "The original denial concerns another issue. An approval alone does not address it.",
    },
  ];
}
