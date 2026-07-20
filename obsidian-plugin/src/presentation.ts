import { ZodError } from "zod";

import { LedgerClientError } from "./backend-client";
import type { DraftProposal, ProposalEdits } from "./contracts";

export interface ProposalPresentation {
  summary: string;
  advisory: string;
  warning: string | null;
  edits: ProposalEdits;
}

export function createProposalPresentation(proposal: DraftProposal): ProposalPresentation {
  const familyId = proposal.lineage.family_id;
  if (typeof familyId !== "string") {
    throw new TypeError("proposal lineage family_id must be a string");
  }
  return {
    summary: `${proposal.registration_status} · ${proposal.schema_id} · ${shortHash(
      proposal.schema_hash,
    )}`,
    advisory: proposal.fresh_window
      ? "Advisory check: the extracted out-of-sample window is untouched."
      : "Advisory check: the extracted out-of-sample window is already touched.",
    warning: proposal.fresh_window
      ? null
      : "The extracted out-of-sample window has already been touched and cannot be preregistered unchanged. Edit the dates or cancel. The backend always rechecks under the ledger lock when you confirm.",
    edits: {
      forecast: {
        strategy_id: proposal.forecast.strategy_id,
        expected_metrics: { ...proposal.forecast.expected_metrics },
        in_sample_window: { ...proposal.forecast.in_sample_window },
        out_of_sample_window: { ...proposal.forecast.out_of_sample_window },
        invalidation: proposal.forecast.invalidation,
        edge_source: proposal.forecast.edge_source,
      },
      decision: proposal.decision,
      family_id: familyId,
    },
  };
}

export function formatCaptureError(error: unknown): string {
  if (error instanceof LedgerClientError) {
    const details = error.details.length > 0 ? `\n${error.details.join("\n")}` : "";
    return `${error.message}${details}`;
  }
  if (error instanceof ZodError) {
    return error.issues
      .map((issue) => `${issue.path.join(".") || "proposal"}: ${issue.message}`)
      .join("\n");
  }
  return "The hypothesis could not be captured. No ledger write was confirmed.";
}

function shortHash(value: string): string {
  return `${value.slice(0, 15)}…`;
}
