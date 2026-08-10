# Research Console reliability gate

This gate covers the browser, failure-boundary, accessibility, and secret-handling
requirements that must pass before the console is packaged for production. Deployment,
systemd, Tailscale Serve, backup, and rollback checks remain part of the following release
PR.

## Local validation

Run the normal deterministic suite and static checks:

```console
uv sync --locked --extra dev
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run pyright ledger
```

After installing the Playwright Chromium and WebKit runtimes, run the opt-in browser gate:

```console
PINE_RUN_BROWSER_TESTS=1 \
PINE_BROWSER_EVIDENCE_DIR=test-results/browser \
uv run pytest -m browser tests/console/test_capture_browser.py
```

The browser fixtures use only synthetic strategy and credential values. Successful journeys
discard their traces and temporary HAR files. A failed instrumented journey retains a trace
under `PINE_BROWSER_EVIDENCE_DIR`; CI uploads those files only when the browser job fails and
keeps them for seven days.

## Covered release risks

- failure before request freezing leaves the workflow reviewable and sends no backend call;
- interruption after freezing recovers to an exact-replay state;
- interruption after receipt persistence reuses the committed receipt without another call;
- a failed console-state migration rolls back and remains migratable;
- concurrent confirmations from separate store/session views issue one backend call;
- Chromium and WebKit complete keyboard capture and verified inspection journeys;
- page landmarks, names, focus behavior, scrollable evidence regions, and text contrast are
  audited in both light and dark modes;
- browser requests stay same-origin and produce no CSP violations;
- rendered pages, local/session storage, static assets, HAR traffic, traces, and captured logs
  exclude the backend bearer credential and its runtime path.

This gate does not create a production ledger record and does not exercise a real backend,
OpenAI extraction, MSM, ClickHouse, or public network service.
