# Decision Edge Engine

This repository contains the local-first integrity substrate for the Decision Edge
Engine: strict forecast validation, a versioned schema registry, a WAL-mode SQLite
authority, frozen committed records, and atomic/idempotent snapshot + Markdown writes.
The structured capture service serializes the full preregistration critical section:
schema revalidation, fresh-window enforcement, ID creation, snapshot capture, artifact
publication, and registry commit.

It intentionally contains no network, LLM, or Obsidian integration. MSM state enters
through the local `SnapshotProvider` protocol; `MSMSnapshotProvider` is the strict
adapter for MSM's point-in-time ClickHouse snapshot source.

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

## Atomicity boundary

The SQLite row with `transaction_state = committed` is the visibility boundary for a
prediction. Readers must not consume a Markdown or snapshot path unless that row is
committed. The two artifacts live in different directories, so no portable filesystem
primitive can publish both physical paths in one operation; a durable manifest lets the
next writer remove any uncommitted debris after a process or machine crash. Ordinary
exceptions clean both paths before returning. Artifact publication uses atomic
no-replace links so concurrent editor activity can never be overwritten.
