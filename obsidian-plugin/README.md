# Decision Edge Ledger for Obsidian

This desktop-only plugin is the UI surface for Pine's local Decision Edge backend. It
does not write ledger records or run backtests itself.

## Development

```bash
npm ci
npm run check
```

The production build emits `main.js`. Copy `main.js`, `manifest.json`, and `styles.css`
to `<vault>/.obsidian/plugins/decision-edge-ledger/`, reload Obsidian, and enable the
plugin. Start the Pine backend against the same vault, then invoke **Decision Edge
Ledger: Log strategy hypothesis** from the command palette while a Markdown note is
active.

The plugin reads `.ledger/backend.json` and the descriptor's fixed token reference on
each request so backend restarts are discovered automatically. All HTTP requests are
restricted to the descriptor's validated loopback port.
