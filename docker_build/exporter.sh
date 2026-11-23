#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 2 ]]; then
    printf 'Usage: %s {mq_prometheus_ent|mq_prometheus} {start|stop|restart|deploy|sync}\n' "$0" >&2
    exit 2
fi

case "$1" in
    mq_prometheus_ent|mq_prometheus) service=$1 ;;
    *) printf 'Unknown exporter: %s\n' "$1" >&2; exit 2 ;;
esac

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
repo_dir=$(cd .. && pwd -P)

if [[ "$service" == mq_prometheus_ent ]]; then
    mkdir -p -- "$repo_dir/bin"
    exec {lock_fd}<"$repo_dir/bin"
    flock "$lock_fd"
fi

sync_enterprise_binary() {
    local container staging artifact expected actual
    container=$(docker compose ps -q "$service")
    if [[ -z "$container" || "$container" == *$'\n'* ]]; then
        printf 'Expected one running %s container.\n' "$service" >&2
        return 1
    fi

    staging=$(mktemp -d -- "$repo_dir/bin/.mq_prometheus_ent.XXXXXX")
    artifact="$staging/mq_prometheus_ent"
    (
        trap 'if [[ -f "$artifact" ]]; then unlink "$artifact"; fi; rmdir "$staging"' EXIT
        docker cp "$container:/usr/local/bin/mq_prometheus_ent" "$artifact"
        expected=$(docker exec "$container" sha256sum /proc/1/exe)
        actual=$(sha256sum "$artifact")
        if [[ "${actual%% *}" != "${expected%% *}" || "$(docker compose ps -q "$service")" != "$container" ]]; then
            printf 'Unable to verify the deployed binary. Retry the sync.\n' >&2
            exit 1
        fi
        chmod 755 "$artifact"
        mv -T -- "$artifact" "$repo_dir/bin/mq_prometheus_ent"
        printf 'Synced %s (SHA-256 %s)\n' "$repo_dir/bin/mq_prometheus_ent" "${actual%% *}"
    )
}

case "$2" in
    start) docker compose up -d "$service" ;;
    stop) docker compose stop "$service"; exit ;;
    restart) docker compose restart "$service" ;;
    deploy) docker compose up -d --build "$service" ;;
    sync)
        if [[ "$service" != mq_prometheus_ent ]]; then
            printf 'The sync action is for mq_prometheus_ent.\n' >&2
            exit 2
        fi
        ;;
    *) printf 'Unknown action: %s\n' "$2" >&2; exit 2 ;;
esac

if [[ "$service" == mq_prometheus_ent ]]; then
    sync_enterprise_binary
fi
