import { describe, expect, it, vi } from "vitest";

import { CaptureWorkflow } from "../src/capture-session";
import type {
  CaptureResponse,
  PreregisteredCaptureRequest,
  ProposalEdits,
} from "../src/contracts";
import {
  captureResponse,
  forecast,
  proposal,
  readyExtraction,
} from "./fixtures";

function edits(): ProposalEdits {
  return {
    forecast: {
      ...forecast,
      expected_metrics: { ...forecast.expected_metrics, sharpe: 1.75 },
      in_sample_window: { ...forecast.in_sample_window },
      out_of_sample_window: { start: "2026-04-01", end: "2026-06-30" },
    },
    decision: "Run the edited frozen strategy.",
    family_id: "edited-family",
  };
}

describe("CaptureWorkflow", () => {
  it("returns an explicit unable result without allocating an idempotency key", async () => {
    const createDraft = vi.fn(async () => ({
      status: "unable" as const,
      proposal: null,
      errors: ["missing OOS window"],
    }));
    const capture = vi.fn();
    const keyFactory = vi.fn(() => "must-not-be-used");
    const workflow = new CaptureWorkflow({ createDraft, capture }, keyFactory);

    const result = await workflow.prepare("incomplete note");

    expect(result).toEqual({ status: "unable", errors: ["missing OOS window"] });
    expect(keyFactory).not.toHaveBeenCalled();
    expect(capture).not.toHaveBeenCalled();
  });

  it("preserves one idempotency key and authoritative lineage across retries", async () => {
    const createDraft = vi.fn(async () => readyExtraction);
    const capture = vi
      .fn<(request: PreregisteredCaptureRequest) => Promise<CaptureResponse>>()
      .mockRejectedValueOnce(new Error("connection reset after send"))
      .mockResolvedValueOnce(captureResponse);
    const keyFactory = vi.fn(() => "obsidian-stable-key");
    const workflow = new CaptureWorkflow({ createDraft, capture }, keyFactory);
    const prepared = await workflow.prepare("Original active note body");
    expect(prepared.status).toBe("ready");
    if (prepared.status !== "ready") {
      throw new Error("expected a ready capture session");
    }

    const firstEdits = edits();
    await expect(prepared.session.confirm(firstEdits)).rejects.toThrow("connection reset");
    expect(prepared.session.hasSubmittedRequest).toBe(true);
    const changedRetryEdits = edits();
    changedRetryEdits.forecast.strategy_id = "must-not-replace-frozen-request";
    await expect(prepared.session.confirm(changedRetryEdits)).resolves.toEqual(
      captureResponse,
    );

    expect(keyFactory).toHaveBeenCalledOnce();
    expect(capture).toHaveBeenCalledTimes(2);
    expect(capture.mock.calls[0]?.[0]).toEqual(capture.mock.calls[1]?.[0]);
    const request = capture.mock.calls[1]?.[0];
    if (request === undefined) {
      throw new Error("expected a second capture request");
    }
    expect(request).toEqual({
      idempotency_key: "obsidian-stable-key",
      schema_id: proposal.schema_id,
      forecast: firstEdits.forecast,
      decision: firstEdits.decision,
      lineage: {
        ...proposal.lineage,
        family_id: "edited-family",
      },
      body: "Original active note body",
    });
    expect(request).not.toHaveProperty("registration_status");
    expect(request).not.toHaveProperty("schema_hash");
    expect(request).not.toHaveProperty("fresh_window");
    expect(request.body).not.toBe(proposal.body);
  });

  it("rejects invalid modal edits before calling capture", async () => {
    const createDraft = vi.fn(async () => readyExtraction);
    const capture = vi.fn(async () => captureResponse);
    const workflow = new CaptureWorkflow(
      { createDraft, capture },
      () => "obsidian-validation-key",
    );
    const prepared = await workflow.prepare("note");
    if (prepared.status !== "ready") {
      throw new Error("expected a ready capture session");
    }
    const invalid = edits();
    invalid.forecast.expected_metrics.win_rate = 1.5;

    await expect(prepared.session.confirm(invalid)).rejects.toThrow();

    const blankNumericField = edits();
    blankNumericField.forecast.expected_metrics.sharpe = Number.NaN;
    await expect(prepared.session.confirm(blankNumericField)).rejects.toThrow();

    expect(capture).not.toHaveBeenCalled();
  });
});
