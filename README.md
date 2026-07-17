# Decision Edge Engine

This repository contains the local-first integrity substrate for the Decision Edge
Engine: strict forecast validation, a versioned schema registry, a WAL-mode SQLite
authority, frozen committed records, and atomic/idempotent snapshot + Markdown writes.
The structured capture service serializes the full preregistration critical section:
schema revalidation, fresh-window enforcement, ID creation, snapshot capture, artifact
publication, and registry commit.

It intentionally contains no network, LLM, or Obsidian integration. MSM state enters
through the local `SnapshotProvider` protocol; `MSMSnapshotProvider` is the strict
adapter for MSM's point-in-time ClickHouse snapshot source. The `msm-ledger run`
wrapper is the local execution boundary: it persists an immutable pre-run envelope
before it hands control to an MSM process.

## Development

```bash
uv sync --extra dev
uv run pytest
```

The first forecast shape is `finance/strategy-edge:1`, stored at
`.ledger/schemas/finance/strategy-edge.1.json`. Runtime database, lock, and snapshot
files are ignored by git.

## Structured capture

Construct a `PreregisteredCaptureRequest` after the user confirms a hypothesis, then
pass it to `CaptureService.capture`. The request type deliberately has no
`registration_status` input: this path can only create a new preregistered record and
cannot promote an exploratory record. The provider is called under the shared ledger
lock and must return the frozen, point-in-time state without executing a backtest.

Retries must reuse the same `idempotency_key`. An identical retry returns the original
prediction without recapturing; reusing a key for different content fails closed. The
advisory `check_fresh_window` method supports pre-confirmation UI feedback, while
`capture` always repeats the inclusive overlap check under the lock.

### Local MSM composition

When both projects are installed in the same Python environment, compose the local
source and ledger adapter explicitly:

```python
from ledger import CaptureService, MSMSnapshotProvider
from msm.ch.client import get_client
from msm.utils.ledger_snapshot import LedgerSnapshotSource

provider = MSMSnapshotProvider(LedgerSnapshotSource(get_client()))
service = CaptureService(vault_root="/path/to/vault", snapshot_provider=provider)
result = service.capture(confirmed_request)
```

The MSM source never runs a backtest. It requires a clean MSM commit, a recent
timezone-aware decision timestamp, complete active-part coverage for every configured
table, and two identical reads of the ClickHouse part manifest. Pine independently
validates the full response and binds its strategy, date windows, and data-as-of time
to the pending prediction. Any mismatch aborts before artifacts or registry rows are
published.

## MSM run wrapper

Install Pine and MSM in the same Python environment, then invoke the MSM command after
`--`. Arguments are passed as an argv vector with `shell=False`; they are never parsed
as a shell command.

An exploratory run requires the strategy and both research windows. The wrapper asks
MSM for a strict point-in-time snapshot and commits a permanent `exploratory` envelope
to `.ledger/registry.db` before the subprocess can start:

```bash
msm-ledger run \
  --vault-root /path/to/pine-vault \
  --idempotency-key vwap-smoke-20260422 \
  --strategy-id vwap_mr_v3.1 \
  --in-sample-start 2026-04-22 \
  --in-sample-end 2026-04-22 \
  --out-of-sample-start 2026-04-22 \
  --out-of-sample-end 2026-04-22 \
  --working-directory /path/to/msm \
  -- uv run python scripts/frozen/frozen_vwap_mr_v3_1.py \
       --start-date 2026-04-22 --end-date 2026-04-22
```

A preregistered run instead names an existing committed prediction. Its frozen
snapshot and Markdown integrity hash are revalidated before the allocated `run_id` is
bound to the command:

```bash
msm-ledger run \
  --vault-root /path/to/pine-vault \
  --idempotency-key pred-abc-first-execution \
  --prediction-id pred_abc \
  --working-directory /path/to/msm \
  -- uv run python scripts/frozen/frozen_vwap_mr_v3_1.py \
       --start-date 2026-04-22 --end-date 2026-04-22
```

Retries must reuse the same idempotency key and exact request. A completed or failed
retry is a no-op and returns the stored process exit code; changing the command under
an existing key fails closed. If the executor cannot launch the command, the first call
and every retry return the same failed JSON result and exit code. The child receives
`LEDGER_RUN_ID`, `LEDGER_REGISTRATION_STATUS`, `LEDGER_DATASET_VERSION`, and
`LEDGER_ENVELOPE_HASH`.
Preregistered children also receive `LEDGER_PREDICTION_ID`. Immediately before process
start, the wrapper verifies that `--working-directory` is a clean Git checkout at the
exact commit frozen in the snapshot. A per-run process lock distinguishes a live
execution from a wrapper that disappeared mid-run; the next retry permanently marks an
orphaned `running` entry as failed and never launches a duplicate process.

## External-run audit recovery

A completed MSM run that bypassed the wrapper can be recovered from one explicit JSON
evidence document:

```bash
msm-ledger ingest-external \
  --vault-root /path/to/pine-vault \
  --evidence /path/to/direct-run-evidence.json
```

The document is validated by `ExternalRunEvidence`. It must identify the source system
and source run, actual start/completion times and exit state, the original argv and
absolute working directory, a complete `StrategySnapshot`, and a non-empty artifact
manifest. Artifact entries contain a normalized output-relative path, byte size, and
`sha256:...` content digest, sorted by path. Optional JSON metadata may preserve
additional source-specific context.

This path never executes MSM and never creates a prediction. It atomically creates an
already-terminal run binding with permanent registration status
`unregistered_external`, low-integrity reason `wrapper_bypass`, and the complete
evidence embedded in its hashed envelope. An exact retry of the same
`(source_system, source_run_id)` is a no-op (`created: false`); changed evidence for that
identity fails closed. Registry triggers prohibit rewriting or deleting the evidence
and status, and there is no promotion API. Because the evidence is retrospective, its
content hashes are preserved as claims rather than treated as preregistration-grade
attestation.

## Vault observation

`VaultWatcher` provides the read-only boundary for later registry revalidation and
indexing. Callers supply a record callback and a managed-path violation reporter; the
watcher never changes either records or managed state:

```python
from ledger import VaultWatcher

watcher = VaultWatcher(
    "/path/to/pine-vault",
    on_record=lambda event: reindex_queue.put(event),
    on_violation=lambda violation: integrity_queue.put(violation),
)
watcher.start()
try:
    run_local_backend()
finally:
    watcher.stop()
```

Markdown events are debounced per path. A note is treated as a ledger record when its
frontmatter identifies a prediction or its filename resolves to a committed registry
row; the latter ensures damaged frontmatter still reaches the future mismatch checker.
Native watchdog events are backed by a narrow reconciliation pass over `.ledger/` and
configured record roots so atomic hard-link publication is observed consistently
across platforms. The default record root is `predictions/`; custom `LedgerWriter`
record directories should also be supplied through `record_roots`.

The managed-directory policy ignores expected SQLite/WAL/lock churn, accepts a new
snapshot only when it belongs to a committed registry row, and reports snapshot
rewrites/deletes, unregistered snapshots, schema changes, registry removal, and unknown
managed files. The re-index and violation callbacks are intentionally integration
stubs: embedding and note-to-registry quarantine remain later milestones.

## Atomicity boundary

The SQLite row with `transaction_state = committed` is the visibility boundary for a
prediction. Readers must not consume a Markdown or snapshot path unless that row is
committed. The two artifacts live in different directories, so no portable filesystem
primitive can publish both physical paths in one operation; a durable manifest lets the
next writer remove any uncommitted debris after a process or machine crash. Ordinary
exceptions clean both paths before returning. Artifact publication uses atomic
no-replace links so concurrent editor activity can never be overwritten.
