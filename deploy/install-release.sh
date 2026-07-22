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
pine_repo=$(realpath "$1")
pine_commit=$(runuser -u "$service_user" -- git -C "$pine_repo" rev-parse --verify "$2^{commit}")
msm_repo=$(realpath "$3")
msm_commit=$(runuser -u "$service_user" -- git -C "$msm_repo" rev-parse --verify "$4^{commit}")
release_id="${pine_commit:0:12}-${msm_commit:0:12}"
release_root="/opt/decision-edge/releases/$release_id"
current_link=/opt/decision-edge/current
stage_root="/opt/decision-edge/releases/.${release_id}.$$.tmp"
cleanup_release=""

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
    "$release_root/pine" \
    "$release_root/msm"

runtime_check=$(cd "$release_root/pine" && "$release_root/venv/bin/msm-ledger-result" runtime-check)
if ! grep -q '"ready": true' <<<"$runtime_check"; then
    echo "shared runtime check failed" >&2
    exit 2
fi
cleanup_release=""

install -d -m 0700 -o "$service_user" -g "$service_group" /var/lib/pine/vault
install -d -m 0700 -o "$service_user" -g "$service_group" /var/backups/pine
install -d -m 0750 -o root -g "$service_group" /etc/pine
if [[ ! -e /etc/pine/backend.env ]]; then
    install -m 0600 -o root -g root "$release_root/pine/deploy/backend.env.example" /etc/pine/backend.env
fi

install -m 0644 "$release_root/pine/deploy/pine-backend.service" /etc/systemd/system/pine-backend.service
install -m 0644 "$release_root/pine/deploy/pine-backend-readiness.service" /etc/systemd/system/pine-backend-readiness.service
install -m 0644 "$release_root/pine/deploy/pine-backend-readiness.timer" /etc/systemd/system/pine-backend-readiness.timer
install -m 0644 "$release_root/pine/deploy/pine-backup.service" /etc/systemd/system/pine-backup.service
install -m 0644 "$release_root/pine/deploy/pine-backup.timer" /etc/systemd/system/pine-backup.timer

temporary_link="/opt/decision-edge/.current-$release_id"
ln -s "$release_root" "$temporary_link"
mv -Tf "$temporary_link" "$current_link"

systemctl daemon-reload

echo "release=$release_root"
echo "pine_commit=$pine_commit"
echo "msm_commit=$msm_commit"
echo "runtime_ready=true"
if grep -Eq '^OPENAI_API_KEY=.+$' /etc/pine/backend.env; then
    systemctl enable pine-backend.service pine-backend-readiness.timer pine-backup.timer
    systemctl restart pine-backend.service
    systemctl start pine-backend-readiness.timer pine-backup.timer
    systemctl start pine-backup.service
    systemctl start pine-backend-readiness.service
    echo "backend_started=true"
else
    echo "backend_started=false"
    echo "configure OPENAI_API_KEY in /etc/pine/backend.env, then enable and start the Pine units"
fi
