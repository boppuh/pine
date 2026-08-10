# Durable Pine deployment

## Authority model

The EC2 host owns the only writable Pine vault. The Pine backend, MSM snapshot source,
ClickHouse, MSM execution, result evidence, and SQLite registry are co-located. The
backend binds only to `127.0.0.1:8765`; it must not be exposed through nginx,
Cloudflare, a security group, or a public load balancer.

Obsidian remains a desktop application. `pine-obsidian-bridge` forwards one desktop
loopback port through SSH to the EC2 loopback backend and publishes the descriptor and
private token expected by the unmodified plugin. Never synchronize a live
`.ledger/registry.db`, its WAL/SHM files, locks, backend descriptor, or backend token.

## EC2 installation

Install a release only from reviewed commits:

```bash
sudo /path/to/pine/deploy/install-release.sh \
  /home/ubuntu/pine <pine-commit> \
  /home/ubuntu/msm <msm-commit>
```

The installer creates:

- `/opt/decision-edge/releases/<pine-sha>-<msm-sha>/` — immutable source and runtime;
- `/opt/decision-edge/current` — atomically selected release;
- `/var/lib/pine/vault` — authoritative ledger vault;
- `/var/lib/pine/console` — non-authoritative console workflow state;
- `/var/backups/pine` — verified local backups;
- `/var/backups/pine-console` — separately verified console-state backups;
- `/etc/pine/backend.env` — root-readable provider configuration;
- `/etc/pine/console.env` — root-readable, non-secret console configuration;
- hardened backend, console, readiness, and backup systemd units.

The shared virtual environment installs Pine and MSM editably from the immutable
release checkouts. The installer rejects a runtime whose module paths resolve through
copied `site-packages` trees instead of the pinned release sources, because MSM result
evidence must inspect the exact Git checkout before emission.

The installer also verifies the console's packaged templates/static assets,
configuration, restricted Unix-socket bind, and state compatibility before selecting
the release. Older console state is backed up online and verified before transactional
migration. Configure `/etc/pine/console.env` before retrying a first install; an invalid
or placeholder configuration leaves the prior `current` selection unchanged. See the
[console deployment and rollback runbook](console-deployment-runbook.md) for Tailscale
Serve, non-writing validation, smoke, and rollback gates.

Use the host secret-management workflow to set `OPENAI_API_KEY` in
`/etc/pine/backend.env`; do not put it in Git or shell history. Then start and verify:

```bash
sudo systemctl enable pine-backend.service pine-backend-readiness.timer pine-backup.timer
sudo systemctl restart pine-backend.service
sudo systemctl start pine-backend-readiness.timer pine-backup.timer
sudo systemctl start pine-backend-readiness.service
sudo systemctl start pine-backup.service
systemctl --no-pager --full status pine-backend.service
sudo -u ubuntu /opt/decision-edge/current/venv/bin/pine-ledger-backend \
  health --vault-root /var/lib/pine/vault
```

The backend readiness timer runs every five minutes. The backup timer runs nightly and
refuses to snapshot while an MSM run is active. Backups use SQLite's online backup API,
exclude runtime credentials and locks, include immutable schemas/snapshots/predictions
and `vault/runs`, and publish only after every hash and `PRAGMA quick_check` passes.

## Desktop Obsidian installation

Copy these three release artifacts into
`<desktop-vault>/.obsidian/plugins/decision-edge-ledger/`:

- `/opt/decision-edge/current/pine/obsidian-plugin/main.js`
- `/opt/decision-edge/current/pine/obsidian-plugin/manifest.json`
- `/opt/decision-edge/current/pine/obsidian-plugin/styles.css`

Install the Pine command package on the desktop, ensure key-based SSH access to EC2,
then run the bridge while Obsidian is open:

```bash
pine-obsidian-bridge \
  --vault-root /path/to/desktop-vault \
  --ssh-destination ubuntu@your-ec2-host \
  --remote-vault-root /var/lib/pine/vault
```

The bridge fetches the backend descriptor and token through `scp`, validates them,
starts SSH with `ExitOnForwardFailure`, verifies the tunneled API, and only then
publishes the desktop discovery document. Run it under the desktop's user-level
service manager for automatic restart. The token remains mode `0600`; the descriptor
is removed when the bridge exits.

## Operator-triggered MSM execution

Research trials remain explicit operator actions. Use the pinned runtime and MSM
release; keep all evidence and outputs below the backed-up `vault/runs` tree:

```bash
/opt/decision-edge/current/venv/bin/msm-ledger run \
  --vault-root /var/lib/pine/vault \
  --working-directory /opt/decision-edge/current/msm \
  ...
```

Do not schedule research trials merely because the backend is durable. Preregistration,
fresh-window, and family-trial authorization remain separate decisions.

## Upgrade and rollback

Install every upgrade as a new release directory. The installer changes `current` only
after clean-checkout, runtime, plugin, lint, and test checks pass. Restart the backend
after selecting a release. Rollback selects a previously retained release and restarts
the service; never downgrade or overwrite the authoritative vault.

Before restoring, stop the backend and all ledger jobs. Verify a backup with
`pine-ledger-backup verify --backup <directory>` and restore into a new empty vault
path first. Do not overwrite the authoritative vault until the restored registry,
prediction records, snapshots, and run evidence have been independently checked.
