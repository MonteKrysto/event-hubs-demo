import { createHash } from "node:crypto";

export function demoEvidence(event) {
  const lines = [
    "SYNTHETIC HUMATA DEMO DOCUMENT — no patient data.",
    `Encounter: ${event.source_encounter_id}`,
    `Authorization decision: ${event.status.replaceAll("_", " ")}`,
    `Update: ${event.source_sequence}`,
    `Event: ${event.event_id}`,
    `Recorded: ${event.occurred_at}`,
  ];
  if (event.status === "approved")
    lines.push(
      `Authorization number: ${event.authorization_number}`,
      `Coverage: ${event.valid_from} through ${event.valid_to}`,
    );
  else if (event.status === "denied")
    lines.push(
      "This synthetic decision records a denial. It does not grant an authorization or approval coverage.",
    );
  else if (event.status === "not_required")
    lines.push(
      "This synthetic decision records that authorization was not required. It is not an approval.",
    );
  else
    lines.push(
      "This is a progress record. No approval has been granted in this update.",
    );
  lines.push(
    "Teaching fixture only. This document has no clinical or payer authority.",
  );
  const content = `${lines.join("\n")}\n`;
  const sha256 = createHash("sha256").update(content).digest("hex");
  return { content, reference: { uri: `humata://evidence/${sha256}`, sha256 } };
}
