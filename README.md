# mq_prometheus

This repository contains the IBM MQ Prometheus exporter in `cmd/mq_prometheus`
and the enterprise variant in `cmd_ent/mq_prometheus`. The
[enterprise framework brief](mq_prometheus_enterprise_framework_prompts.md)
defines the target architecture and acceptance criteria.

## Deploy the enterprise exporter

```bash
./docker_build/exporter.sh mq_prometheus_ent deploy
```

The deployment refreshes `bin/mq_prometheus_ent` with a verified copy of the
running container's executable. From `bin/` it finds the bundled MQ client
relative to its location, so `./bin/mq_prometheus_ent --version` works without
setting `LD_LIBRARY_PATH`. See the [Docker workflow](docker_build/README.md).

## Build the enterprise variant locally

On Linux x86-64 with Go 1.25 or newer, a C compiler (such as GCC), and the
local IBM MQ redistributable client, run:

```bash
./scripts/buildPrometheusEnt.sh
./bin/local/mq_prometheus_ent -h
```

The script uses the checked-in Go vendor tree and builds
`bin/local/mq_prometheus_ent`. The container build sets its own output directory.
Set `MQ_INSTALLATION_PATH` to use a different IBM MQ client root (containing
`inc` and `lib64`), or set `OUTDIR` to choose another output directory.
Relative override paths are resolved from the invoking directory. The binary
records the selected MQ library directory as an absolute runtime search path.
If you move it to another machine or relocate the client, rebuild against its
new location or set `LD_LIBRARY_PATH` to the client’s `lib64` directory.
