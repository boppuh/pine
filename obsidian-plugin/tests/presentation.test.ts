import { describe, expect, it } from "vitest";

import { LedgerClientError } from "../src/backend-client";
import { proposalEditsSchema } from "../src/contracts";
import {
  createProposalPresentation,
  formatCaptureError,
} from "../src/presentation";
import { proposal } from "./fixtures";

describe("proposal presentation", () => {
  it("renders an untouched advisory and independent editable values", () => {
    const presentation = createProposalPresentation(proposal);

    expect(presentation.summary).toContain("preregistered");
    expect(presentation.summary).toContain("finance/strategy-edge:1");
    expect(presentation.advisory).toContain("untouched");
    expect(presentation.warning).toBeNull();
    expect(proposalEditsSchema.parse(presentation.edits)).toEqual(presentation.edits);

    presentation.edits.forecast.strategy_id = "edited-strategy";
    expect(proposal.forecast.strategy_id).toBe("vwap_mr_v3.1");
  });

  it("renders a blocking-quality warning for a touched draft window", () => {
    const presentation = createProposalPresentation({
      ...proposal,
      fresh_window: false,
    });

    expect(presentation.advisory).toContain("already touched");
    expect(presentation.warning).toContain("cannot be preregistered unchanged");
    expect(presentation.warning).toContain("rechecks under the ledger lock");
  });

  it("shows stable API details without exposing unknown errors", () => {
    const known = new LedgerClientError(
      "forecast validation failed",
      "invalid_forecast",
      422,
      ["$.forecast.expected_metrics.win_rate: must be <= 1"],
    );

    expect(formatCaptureError(known)).toContain("win_rate");
    expect(formatCaptureError(new Error("private transport internals"))).toBe(
      "The hypothesis could not be captured. No ledger write was confirmed.",
    );
  });
});
