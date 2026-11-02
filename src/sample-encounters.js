// Entirely fictional teaching data. Codes are demo identifiers, not billing codes.
const procedures = [
  "Knee MRI",
  "Physical therapy",
  "Sleep study",
  "Outpatient surgery",
  "Cardiac imaging",
  "Infusion visit",
];
const payers = [
  "DemoCare PPO",
  "Sample Health HMO",
  "Example Benefits",
  "Training Health Plan",
];
const histories = [
  {
    label: "Approval covers the visit",
    statuses: ["required", "attempted", "submitted", "approved"],
  },
  { label: "Authorization is still required", statuses: ["required"] },
  {
    label: "Request awaiting a decision",
    statuses: ["required", "attempted", "submitted"],
  },
  {
    label: "Payer denied authorization",
    statuses: ["required", "submitted", "denied"],
  },
  {
    label: "Approval dates do not cover the visit",
    statuses: ["required", "submitted", "approved"],
    outsideCoverage: true,
  },
  { label: "Authorization was not required", statuses: ["not_required"] },
  { label: "An attempt has started", statuses: ["required", "attempted"] },
  {
    label: "A later denial supersedes approval",
    statuses: ["required", "submitted", "approved", "denied"],
  },
];

export function sampleEncounters() {
  const originals = Array.from({ length: 24 }, (_, index) => {
    const number = String(index + 1).padStart(3, "0");
    const history = histories[index % histories.length];
    const service_date = `2026-10-${String(6 + (index % 22)).padStart(2, "0")}`;
    const denialDate = new Date(`${service_date}T12:00:00Z`);
    denialDate.setUTCDate(denialDate.getUTCDate() + 10);
    return {
      sample_key: `practice-v1-${number}`,
      patient_name: `Demo patient ${number}`,
      patient_id: `DEMO-PATIENT-${number}`,
      procedure: procedures[index % procedures.length],
      procedure_code: `DEMO-PROC-${String((index % procedures.length) + 1).padStart(3, "0")}`,
      payer: payers[index % payers.length],
      provider: [
        "Demo Imaging Center",
        "Training Outpatient Clinic",
        "Example Medical Center",
      ][index % 3],
      billed_amount: 1100 + index * 135,
      service_date,
      denial_date: denialDate.toISOString().slice(0, 10),
      denial_reason: "missing_authorization",
      practice_case: history.label,
      suggested_authorization: {
        authorization_number: `AUTH-SAMPLE-${number}`,
        valid_from: history.outsideCoverage ? "2026-11-01" : "2026-10-01",
        valid_to: history.outsideCoverage ? "2026-11-30" : "2026-10-31",
      },
      statuses: history.statuses,
    };
  });
  // Append new cases. Existing sample keys and histories remain unchanged.
  const mixedClaims = originals.map((profile, index) => {
    const number = String(index + 25).padStart(3, "0");
    const kind = index % 6;
    const claim_status =
      kind === 1 ? "paid" : kind === 2 ? "pending" : "denied";
    const denial_reason =
      claim_status !== "denied"
        ? null
        : kind === 3
          ? "service_not_covered"
          : kind === 4
            ? "eligibility"
            : "missing_authorization";
    let statuses = profile.statuses;
    if (claim_status === "paid")
      statuses =
        index % 12 === 1
          ? ["required", "submitted", "approved"]
          : ["not_required"];
    else if (claim_status === "pending")
      statuses =
        index % 12 === 2
          ? ["required", "submitted", "approved"]
          : ["required", "submitted"];
    else if (denial_reason !== "missing_authorization")
      statuses = ["required", "submitted", "approved"];
    return {
      ...profile,
      sample_key: `practice-v2-${number}`,
      patient_name: `Demo patient ${number}`,
      patient_id: `DEMO-PATIENT-${number}`,
      billed_amount: profile.billed_amount + 450,
      claim_status,
      denial_reason,
      claim_date: profile.denial_date,
      denial_date: claim_status === "denied" ? profile.denial_date : null,
      paid_amount:
        claim_status === "paid"
          ? Math.round((profile.billed_amount + 450) * 0.7)
          : null,
      practice_case:
        claim_status === "paid"
          ? "Paid claim — no denial to evaluate"
          : claim_status === "pending"
            ? "Pending claim — no denial has been issued"
            : denial_reason === "service_not_covered"
              ? "Approved authorization, but the service was not covered"
              : denial_reason === "eligibility"
                ? "Approved authorization, but coverage was inactive"
                : profile.practice_case,
      suggested_authorization: {
        ...profile.suggested_authorization,
        authorization_number: `AUTH-SAMPLE-${number}`,
        ...(denial_reason !== "missing_authorization"
          ? { valid_from: "2026-10-01", valid_to: "2026-10-31" }
          : {}),
      },
      statuses,
    };
  });
  return [...originals, ...mixedClaims];
}

export function newEncounterDetails(suffix) {
  return {
    patient_name: `Demo patient ${suffix}`,
    patient_id: `DEMO-PATIENT-${suffix}`,
    procedure: "Knee MRI",
    procedure_code: "DEMO-PROC-001",
    payer: "DemoCare PPO",
    provider: "Demo Imaging Center",
    billed_amount: 1850,
    denial_date: "2026-10-25",
  };
}
