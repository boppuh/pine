import type {
  CaptureResponse,
  DraftProposal,
  ExtractionResult,
  StrategyEdgeForecast,
} from "../src/contracts";

export const TOKEN = "test-token-abcdefghijklmnopqrstuvwxyz-0123456789";

export const descriptor = {
  api_version: "v1",
  host: "127.0.0.1",
  port: 41_275,
  pid: 1_234,
  instance_id: "instance-test",
  token_ref: ".ledger/backend.token",
  started_at: "2026-07-20T12:00:00-04:00",
} as const;

export const forecast: StrategyEdgeForecast = {
  strategy_id: "vwap_mr_v3.1",
  expected_metrics: {
    sharpe: 1.5,
    win_rate: 0.56,
    max_drawdown: 0.12,
    expectancy: 0.002,
  },
  in_sample_window: { start: "2025-01-01", end: "2025-12-31" },
  out_of_sample_window: { start: "2026-01-01", end: "2026-03-31" },
  invalidation: "OOS Sharpe below 1.0",
  edge_source: "VWAP dislocation and intraday mean reversion",
};

export const proposal: DraftProposal = {
  schema_id: "finance/strategy-edge:1",
  schema_hash: `sha256:${"a".repeat(64)}`,
  registration_status: "preregistered",
  forecast,
  decision: "Run the frozen strategy against the untouched OOS window.",
  lineage: {
    family_id: "vwap-mean-reversion-us-equities",
    extraction: {
      provider: "openai",
      configured_model: "gpt-5.6",
      prompt_version: "finance-strategy-edge-extraction:v1",
    },
  },
  body: "Provider-returned body",
  fresh_window: true,
};

export const readyExtraction: ExtractionResult = {
  status: "ready",
  proposal,
  errors: [],
};

export const captureResponse: CaptureResponse = {
  prediction_id: "pred_test",
  run_id: "run_test",
  record_ref: "predictions/pred_test.md",
  snapshot_ref: ".ledger/snapshots/pred_test.json",
  schema_id: "finance/strategy-edge:1",
  schema_hash: `sha256:${"a".repeat(64)}`,
  immutable_hash: `sha256:${"b".repeat(64)}`,
  created: true,
};
