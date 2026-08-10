#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: sudo install-release.sh <pine-repo> <pine-commit> <msm-repo> <msm-commit>" >&2
    exit 2
}

if [[ $# -ne 4 ]]; then
    usage
fi
if [[ $(id -u) -ne 0 ]]; then
    echo "install-release.sh must run as root" >&2
    exit 2
fi

service_user=ubuntu
service_group=ubuntu
console_user=pine-console
console_group=pine-ingress
pine_repo=$(realpath "$1")
pine_commit=$(runuser -u "$service_user" -- git -C "$pine_repo" rev-parse --verify "$2^{commit}")
msm_repo=$(realpath "$3")
msm_commit=$(runuser -u "$service_user" -- git -C "$msm_repo" rev-parse --verify "$4^{commit}")
release_id="${pine_commit:0:12}-${msm_commit:0:12}"
release_root="/opt/decision-edge/releases/$release_id"
current_link=/opt/decision-edge/current
stage_root="/opt/decision-edge/releases/.${release_id}.$$.tmp"
cleanup_release=""
console_was_active=false
restart_console_on_failure=false

if [[ -e "$release_root" ]]; then
    echo "release already exists: $release_root" >&2
    exit 2
fi

install -d -m 0755 /opt/decision-edge /opt/decision-edge/releases
install -d -m 0750 -o "$service_user" -g "$service_group" "$stage_root"

cleanup() {
    if [[ -n ${stage_root:-} && -d $stage_root ]]; then
        rm -rf -- "$stage_root"
    fi
    if [[ -n ${cleanup_release:-} && -d $cleanup_release ]]; then
        rm -rf -- "$cleanup_release"
    fi
    if [[ ${restart_console_on_failure:-false} == true ]]; then
        systemctl start pine-console.service >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

runuser -u "$service_user" -- git clone --quiet --no-local --no-checkout "$pine_repo" "$stage_root/pine"
runuser -u "$service_user" -- git -C "$stage_root/pine" checkout --quiet --detach "$pine_commit"
runuser -u "$service_user" -- git clone --quiet --no-local --no-checkout "$msm_repo" "$stage_root/msm"
runuser -u "$service_user" -- git -C "$stage_root/msm" checkout --quiet --detach "$msm_commit"

if [[ -n $(runuser -u "$service_user" -- git -C "$stage_root/pine" status --porcelain) ]]; then
    echo "Pine release checkout is not clean" >&2
    exit 2
fi
if [[ -n $(runuser -u "$service_user" -- git -C "$stage_root/msm" status --porcelain) ]]; then
    echo "MSM release checkout is not clean" >&2
    exit 2
fi

runuser -u "$service_user" -- npm --prefix "$stage_root/pine/obsidian-plugin" ci --ignore-scripts
runuser -u "$service_user" -- npm --prefix "$stage_root/pine/obsidian-plugin" run check

mv "$stage_root" "$release_root"
stage_root=""
cleanup_release="$release_root"

runuser -u "$service_user" -- /home/ubuntu/.local/bin/uv venv --python 3.11 "$release_root/venv"
runuser -u "$service_user" -- /home/ubuntu/.local/bin/uv pip install \
    --python "$release_root/venv/bin/python" \
    --editable "$release_root/pine" \
    --editable "$release_root/msm"
# Wheel archives can contain owner-only file modes. The immutable shared runtime
# must remain readable and traversable by each dedicated service identity.
chmod -R a+rX "$release_root/venv"

runtime_check=$(
    runuser -u "$service_user" -- \
        "$release_root/venv/bin/msm-ledger-result" runtime-check
)
if ! runuser -u "$service_user" -- \
    "$release_root/venv/bin/python" -m ledger.release_runtime \
    --release-root "$release_root" <<<"$runtime_check"; then
    echo "shared runtime check failed" >&2
    exit 2
fi
chmod 0755 "$release_root"

if ! runuser -u "$service_user" -- \
    "$release_root/venv/bin/pine-research-console" release-check \
    --release-root "$release_root"; then
    echo "console release asset check failed" >&2
    exit 2
fi

if ! getent group "$console_group" >/dev/null; then
    groupadd --system "$console_group"
fi
if ! id -u "$console_user" >/dev/null 2>&1; then
    useradd --system --user-group --home-dir /nonexistent \
        --shell /usr/sbin/nologin "$console_user"
fi
usermod --append --groups "$console_group" "$console_user"
console_state_group=$(id -gn "$console_user")

install -d -m 0711 -o root -g root /var/lib/pine
install -d -m 0700 -o "$service_user" -g "$service_group" /var/lib/pine/vault
install -d -m 0700 -o "$console_user" -g "$console_state_group" /var/lib/pine/console
install -d -m 0700 -o "$service_user" -g "$service_group" /var/backups/pine
install -d -m 0700 -o "$console_user" -g "$console_state_group" /var/backups/pine-console
install -d -m 0750 -o "$console_user" -g "$console_group" /run/pine
install -d -m 0750 -o root -g "$service_group" /etc/pine
if [[ ! -e /etc/pine/backend.env ]]; then
    install -m 0600 -o root -g root "$release_root/pine/deploy/backend.env.example" /etc/pine/backend.env
fi
if [[ ! -e /etc/pine/console.env ]]; then
    install -m 0600 -o root -g root "$release_root/pine/deploy/console.env.example" /etc/pine/console.env
fi
if [[ $(stat -c '%a:%u:%g' /etc/pine/console.env) != "600:0:0" ]]; then
    echo "/etc/pine/console.env must be root-owned with mode 0600" >&2
    exit 2
fi

probe_socket="/run/pine/.console-install-${release_id}.sock"
if ! systemd-run --quiet --wait --pipe --collect \
    --unit="pine-console-install-${release_id}" \
    --property=Type=oneshot \
    --property="User=$console_user" \
    --property="Group=$console_group" \
    --property=EnvironmentFile=/etc/pine/console.env \
    --property=NoNewPrivileges=true \
    --property=ProtectSystem=strict \
    --property=ReadWritePaths=/run/pine \
    "$release_root/venv/bin/pine-research-console" socket-check \
    --socket-path "$probe_socket"; then
    echo "console configuration or Unix-socket preflight failed" >&2
    exit 2
fi

if [[ -f /var/lib/pine/vault/.ledger/backend.json ]]; then
    runuser -u "$service_user" -- \
        "$release_root/venv/bin/pine-ledger-backend" health \
        --vault-root /var/lib/pine/vault --timeout-seconds 3 >/dev/null
fi

if systemctl is-active --quiet pine-console.service; then
    console_was_active=true
    systemctl stop pine-console.service
    restart_console_on_failure=true
fi

console_migration=$(
    runuser -u "$console_user" -- \
        "$release_root/venv/bin/pine-console-state" migrate \
        --state-path /var/lib/pine/console/console.db \
        --backup-root /var/backups/pine-console
)
# A successful migration may no longer be readable by the previous release. Do
# not restart that release on a later preselection failure.
restart_console_on_failure=false
runuser -u "$console_user" -- \
    "$release_root/venv/bin/pine-console-state" preflight \
    --state-path /var/lib/pine/console/console.db >/dev/null

temporary_link="/opt/decision-edge/.current-$release_id"
ln -s "$release_root" "$temporary_link"
mv -Tf "$temporary_link" "$current_link"
cleanup_release=""
# From this point the recovery trap starts the selected release, never the
# potentially schema-incompatible previous release.
restart_console_on_failure=$console_was_active

install -m 0644 "$release_root/pine/deploy/pine-backend.service" /etc/systemd/system/pine-backend.service
install -m 0644 "$release_root/pine/deploy/pine-backend-readiness.service" /etc/systemd/system/pine-backend-readiness.service
install -m 0644 "$release_root/pine/deploy/pine-backend-readiness.timer" /etc/systemd/system/pine-backend-readiness.timer
install -m 0644 "$release_root/pine/deploy/pine-backup.service" /etc/systemd/system/pine-backup.service
install -m 0644 "$release_root/pine/deploy/pine-backup.timer" /etc/systemd/system/pine-backup.timer
install -m 0644 "$release_root/pine/deploy/pine-console.service" /etc/systemd/system/pine-console.service
install -m 0644 "$release_root/pine/deploy/pine-console-readiness.service" /etc/systemd/system/pine-console-readiness.service
install -m 0644 "$release_root/pine/deploy/pine-console-readiness.timer" /etc/systemd/system/pine-console-readiness.timer
systemctl daemon-reload

echo "release=$release_root"
echo "pine_commit=$pine_commit"
echo "msm_commit=$msm_commit"
echo "runtime_ready=true"
echo "console_preflight=true"
echo "console_migration=$console_migration"
if grep -Eq '^OPENAI_API_KEY=[^[:space:]]+$' /etc/pine/backend.env; then
    systemctl enable pine-backend.service pine-backend-readiness.timer pine-backup.timer \
        pine-console.service pine-console-readiness.timer
    systemctl restart pine-backend.service
    systemctl start pine-backend-readiness.timer pine-backup.timer
    systemctl start pine-backup.service
    systemctl start pine-backend-readiness.service
    systemctl restart pine-console.service
    socket_deadline=$((SECONDS + 30))
    while [[ ! -S /run/pine/console.sock ]]; do
        if ! systemctl is-active --quiet pine-console.service; then
            echo "console service exited before publishing its Unix socket" >&2
            exit 2
        fi
        if ((SECONDS >= socket_deadline)); then
            echo "console service did not publish its Unix socket before the deadline" >&2
            exit 2
        fi
        sleep 0.1
    done
    if ! systemctl is-active --quiet pine-console.service; then
        echo "console service exited after publishing its Unix socket" >&2
        exit 2
    fi
    if [[ $(stat -c '%a:%U:%G' /run/pine/console.sock) != "660:$console_user:$console_group" ]]; then
        echo "console Unix socket identity or mode is unsafe" >&2
        exit 2
    fi
    restart_console_on_failure=false
    systemctl start pine-console-readiness.timer
    systemctl start pine-console-readiness.service
    echo "backend_started=true"
    echo "console_started=true"
else
    restart_console_on_failure=false
    echo "backend_started=false"
    echo "console_started=false"
    echo "configure OPENAI_API_KEY in /etc/pine/backend.env, then enable and start the Pine units"
fi
