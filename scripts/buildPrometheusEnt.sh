#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
mq_dir=${MQ_INSTALLATION_PATH:-"$repo_dir/10.0.0.0-IBM-MQC-Redist-LinuxX64"}
output_dir=${OUTDIR:-"$repo_dir/bin/local"}

if ! command -v go >/dev/null; then
    printf 'Go 1.25 or newer is required.\n' >&2
    exit 1
fi

export GOWORK=off GOTOOLCHAIN=local CGO_ENABLED=1
if [[ "$(go env GOOS)/$(go env GOARCH)" != linux/amd64 ]]; then
    printf 'This build requires a Linux amd64 Go target for the IBM MQ Linux x64 client.\n' >&2
    exit 1
fi

if [[ ! -r "$mq_dir/inc/cmqc.h" || ! -r "$mq_dir/lib64/libmqm_r.so" ]]; then
    printf 'IBM MQ client headers or libraries are missing under %s. Set MQ_INSTALLATION_PATH to the client root.\n' "$mq_dir" >&2
    exit 1
fi
mq_dir=$(cd -- "$mq_dir" && pwd -P)
mkdir -p -- "$output_dir"
output_dir=$(cd -- "$output_dir" && pwd -P)
if [[ "$output_dir" == "$repo_dir/bin" ]]; then
    printf '%s is reserved for the deployed binary. Use docker_build/exporter.sh mq_prometheus_ent deploy or choose another OUTDIR.\n' "$output_dir" >&2
    exit 1
fi

export CGO_CFLAGS="\"-I$mq_dir/inc\"${CGO_CFLAGS:+ $CGO_CFLAGS}"
export CGO_LDFLAGS="\"-L$mq_dir/lib64\" \"-Wl,-rpath,$mq_dir/lib64\" -Wl,--enable-new-dtags${CGO_LDFLAGS:+ $CGO_LDFLAGS}"
export CGO_LDFLAGS_ALLOW="${CGO_LDFLAGS_ALLOW:+$CGO_LDFLAGS_ALLOW|}-Wl,-rpath,(/opt/mqm/lib64|/usr/lib64)"

cd -- "$repo_dir"
go build -mod=vendor -trimpath -o "$output_dir/mq_prometheus_ent" ./cmd_ent/mq_prometheus
printf 'Built %s/mq_prometheus_ent\n' "$output_dir"
