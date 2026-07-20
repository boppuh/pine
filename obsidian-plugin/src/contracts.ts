import { z } from "zod";

export const DEFAULT_SCHEMA_ID = "finance/strategy-edge:1";

const dateSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "expected an ISO date");

export const expectedMetricsSchema = z
  .object({
    sharpe: z.number(),
    win_rate: z.number().min(0).max(1),
    max_drawdown: z.number(),
    expectancy: z.number(),
  })
  .strict();

export const dateWindowSchema = z
  .object({
    start: dateSchema,
    end: dateSchema,
  })
  .strict()
  .refine((window) => window.end >= window.start, {
    message: "window end must be on or after its start",
  });

export const strategyEdgeForecastSchema = z
  .object({
    strategy_id: z.string().trim().min(1),
    expected_metrics: expectedMetricsSchema,
    in_sample_window: dateWindowSchema,
    out_of_sample_window: dateWindowSchema,
    invalidation: z.string().trim().min(1),
    edge_source: z.string().trim().min(1),
  })
  .strict();

const lineageSchema = z
  .record(z.string(), z.json())
  .refine(
    (lineage) =>
      typeof lineage.family_id === "string" && lineage.family_id.trim().length > 0,
    { message: "lineage.family_id must be a non-empty string" },
  );

export const draftProposalSchema = z
  .object({
    schema_id: z.string().trim().min(1),
    schema_hash: z.string().regex(/^sha256:[0-9a-f]{64}$/),
    registration_status: z.literal("preregistered"),
    forecast: strategyEdgeForecastSchema,
    decision: z.string().trim().min(1),
    lineage: lineageSchema,
    body: z.string(),
    fresh_window: z.boolean(),
  })
  .strict();

const readyExtractionSchema = z
  .object({
    status: z.literal("ready"),
    proposal: draftProposalSchema,
    errors: z.array(z.never()).max(0),
  })
  .strict();

const unableExtractionSchema = z
  .object({
    status: z.literal("unable"),
    proposal: z.null(),
    errors: z.array(z.string()).min(1),
  })
  .strict();

export const extractionResultSchema = z.discriminatedUnion("status", [
  readyExtractionSchema,
  unableExtractionSchema,
]);

export const proposalEditsSchema = z
  .object({
    forecast: strategyEdgeForecastSchema,
    decision: z.string().trim().min(1),
    family_id: z.string().trim().min(1).max(256),
  })
  .strict();

export const preregisteredCaptureRequestSchema = z
  .object({
    idempotency_key: z.string().min(1).max(256),
    schema_id: z.string().trim().min(1),
    forecast: strategyEdgeForecastSchema,
    decision: z.string().trim().min(1),
    lineage: lineageSchema,
    body: z.string(),
  })
  .strict();

export const captureResponseSchema = z
  .object({
    prediction_id: z.string().min(1),
    run_id: z.string().min(1),
    record_ref: z.string().min(1),
    snapshot_ref: z.string().min(1),
    schema_id: z.string().min(1),
    schema_hash: z.string().regex(/^sha256:[0-9a-f]{64}$/),
    immutable_hash: z.string().regex(/^sha256:[0-9a-f]{64}$/),
    created: z.boolean(),
  })
  .strict();

export const backendDescriptorSchema = z
  .object({
    api_version: z.literal("v1"),
    host: z.literal("127.0.0.1"),
    port: z.number().int().min(1).max(65_535),
    pid: z.number().int().positive(),
    instance_id: z.string().min(1),
    token_ref: z.literal(".ledger/backend.token"),
    started_at: z.iso.datetime({ offset: true }),
  })
  .strict();

export const healthResponseSchema = z
  .object({
    status: z.literal("ok"),
    api_version: z.literal("v1"),
  })
  .strict();

export const apiErrorResponseSchema = z
  .object({
    error: z
      .object({
        code: z.string().min(1),
        message: z.string().min(1),
        details: z.array(z.string()),
      })
      .strict(),
  })
  .strict();

export type APIErrorResponse = z.infer<typeof apiErrorResponseSchema>;
export type BackendDescriptor = z.infer<typeof backendDescriptorSchema>;
export type CaptureResponse = z.infer<typeof captureResponseSchema>;
export type DraftProposal = z.infer<typeof draftProposalSchema>;
export type ExtractionResult = z.infer<typeof extractionResultSchema>;
export type HealthResponse = z.infer<typeof healthResponseSchema>;
export type PreregisteredCaptureRequest = z.infer<
  typeof preregisteredCaptureRequestSchema
>;
export type ProposalEdits = z.infer<typeof proposalEditsSchema>;
export type StrategyEdgeForecast = z.infer<typeof strategyEdgeForecastSchema>;
