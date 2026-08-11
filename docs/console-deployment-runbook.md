# Pine Research Console deployment and rollback

This runbook deploys the private Research Console through Pine's immutable Pine/MSM
release process. The EC2 ledger vault remains the only authority. Console SQLite is
operational workflow state and is backed up separately; it is never added to a ledger
backup or restored over the ledger vault.

## Production paths and identities

- selected release: `/opt/decision-edge/current`;
- authoritative vault: `/var/lib/pine/vault`, owned by the existing backend identity;
- console state: `/var/lib/pine/console/console.db`, mode `0600` below a mode-`0700`
  directory owned by `pine-console`;
- console backups: `/var/backups/pine-console/console-*`;
- console socket: `/run/pine/console.sock`, `pine-console:pine-ingress`, mode `0660`;
- console configuration: `/etc/pine/console.env`, root-owned mode `0600`;
- backend credential inside the service only:
  `/run/credentials/pine-console.service/backend-token`.

The console process has no TCP listener. It can write only its state and runtime
directories. The ledger vault is read-only at the service boundary and its source
token path is inaccessible. `tailscaled` is the sole approved HTTPS/identity ingress;
Tailscale Funnel is not approved.

For a Unix-socket proxy, Tailscale Serve sends the internal `Host` as `localhost` and
places the original HTTPS destination in `X-Forwarded-Host`. The console trusts that
forwarded destination only after authenticating the approved Tailscale identity on the
configured Unix socket, requires `X-Forwarded-Proto: https`, and accepts only the
configured host with no port or the default `:443` port.

## Deploy an exact release

1. Record the exact reviewed Pine and MSM commits and require clean source checkouts.
2. Confirm no MSM ledger run is active.
3. Create and verify the normal ledger backup.
4. If console state already exists, create and verify an explicit state backup:

   ```bash
   sudo -u pine-console /opt/decision-edge/current/venv/bin/pine-console-state backup \
     --state-path /var/lib/pine/console/console.db \
     --backup-root /var/backups/pine-console
   ```

5. Configure `/etc/pine/console.env` as root. Set one normalized Tailscale DNS name in
   `PINE_CONSOLE_ALLOWED_HOST` and the exact approved login or comma-separated logins
   in `PINE_CONSOLE_ALLOWED_IDENTITIES`. Keep secrets out of this file.
6. Run the installer with exact commits:

   ```bash
   sudo /home/ubuntu/dev/pine/deploy/install-release.sh \
     /home/ubuntu/dev/pine <pine-commit> \
     /home/ubuntu/msm <msm-commit>
   ```

Before changing `current`, the installer verifies the shared Pine/MSM runtime, console
assets, configuration, a temporary restricted Unix socket, the backend health contract
when an existing backend is discoverable, and the console schema. An older console
schema receives an online verified backup before its transactional migration. Any
failure before selection leaves `current` unchanged and removes the incomplete release.
At service start, a credential-free state preflight runs before the console server. The
server then verifies its loaded backend credential and backend readiness before opening
the Unix socket; the periodic readiness unit repeats the full credentialed check.

## Configure Tailscale Serve

First confirm the installed Tailscale CLI accepts a Unix proxy target. Then publish
only the private HTTPS Serve mapping, never Funnel:

```bash
sudo tailscale serve --bg --https=443 unix:/run/pine/console.sock
sudo tailscale serve status --json
```

Tailnet grants must restrict the Serve URL to the approved operator. Confirm the
reported mapping is Serve, not Funnel, and targets exactly `/run/pine/console.sock`.
The current Tailscale Serve command and identity-header behavior should be checked
against the official Tailscale documentation during each host rollout.

## Non-writing validation

Run these checks before using any capture screen:

```bash
sudo systemctl start pine-backend-readiness.service
sudo systemctl start pine-console-readiness.service
sudo systemctl --no-pager --full status pine-backend.service pine-console.service
sudo -u pine-console /opt/decision-edge/current/venv/bin/pine-console-state preflight \
  --state-path /var/lib/pine/console/console.db
sudo stat -c '%a %U %G %n' /run/pine/console.sock \
  /var/lib/pine/console /etc/pine/console.env
sudo ss -ltnp
```

Expected results:

- backend only on `127.0.0.1:8765`;
- console socket mode/owner/group exactly `660 pine-console pine-ingress`;
- no console TCP listener;
- both readiness commands report API `v1`, current console schema, and `ready: true`;
- the readiness commands do not change workflow rows or ledger authority counts;
- the token appears in no process arguments, environment output, or journal entry.

Open the Serve URL and validate authentication denial, dashboard, predictions, one
prediction detail, and status. Record ledger prediction/run/snapshot/record counts
before and after; they must remain unchanged.

## One deliberate writing smoke

After the non-writing gate passes, use the actual console UI to create one clearly
labeled, non-investment plumbing hypothesis. Confirm preregistration exactly once.
Verify exactly one committed prediction, one allocated run, one Markdown record, one
snapshot, matching immutable hashes, and no automatic MSM execution. Replay only the
frozen request and verify an idempotent no-op. Never delete the immutable smoke record.

## Reconciliation

Before an upgrade or rollback, inspect console workflows. Any `frozen`, `submitting`,
`uncertain`, or `retryable_failure` workflow is protected operational state. Assign an
owner and reconcile it through the exact-retry/status UI. Do not edit SQLite directly,
discard the frozen request, or recreate it under a new idempotency key.

## Rollback

Rollback selects code; it never downgrades, resets, deletes, or restores console state
in place.

1. Stop new console use and record protected workflow states.
2. Identify the previous immutable release and its supported schema range from:

   ```bash
   /opt/decision-edge/releases/<previous>/venv/bin/pine-research-console \
     release-check --release-root /opt/decision-edge/releases/<previous>
   ```

3. While still running the current tool, preflight state against that exact range:

   ```bash
   sudo -u pine-console /opt/decision-edge/current/venv/bin/pine-console-state preflight \
     --state-path /var/lib/pine/console/console.db \
     --minimum-schema-version <previous-minimum> \
     --maximum-schema-version <previous-maximum>
   ```

4. If compatibility fails, stop. Keep the current release and state unchanged. Do not
   restore or downgrade the database to force the rollback.
5. If compatible, atomically repoint `current` to the retained release, reload systemd,
   restart backend then console, and repeat every non-writing validation check.
6. Confirm protected workflows and ledger counts are unchanged.

For the first console-enabled release, the safe application rollback is to stop and
disable `pine-console.service` and continue using the retained desktop bridge. Do not
select a pre-console service definition merely to make the web console appear healthy.

## Shadow window and desktop retirement

Keep the Obsidian plugin and Mac bridge unchanged throughout shadow deployment and ten
consecutive console decisions. Retire the desktop path only after explicit product and
operations approval with zero duplicates, partial records, unexplained integrity
warnings, or desktop fallbacks. Retirement disables the LaunchAgent/plugin, removes
desktop discovery/token copies, rotates the backend token in a maintenance window,
and repeats non-writing validation. Retain the last verified desktop bundle for one
release window.
