# IBM MQ and Go exporter stack

Run the following commands from this directory.

```sh
docker compose build builder mq_prometheus_ent mq_prometheus
docker compose up -d builder qm1 qm2 prometheus grafana mq_prometheus
./exporter.sh mq_prometheus_ent start
docker compose ps
```

| Service | Purpose | Host endpoint |
| --- | --- | --- |
| `builder` | Go 1.25, C/C++ toolchain, IBM MQ SDK, and source. Builds both binaries in `/out`. | |
| `qm1` | QM1 with its migrated IBM MQ data | MQ 1415, console HTTPS 9444 |
| `qm2` | QM2 with its migrated IBM MQ data | MQ 1416, console HTTPS 9445 |
| `prometheus` | Migrated history and scrape jobs for both Go exporters | http://localhost:9091 |
| `grafana` | Migrated Grafana database and an IBM MQ Go Exporters dashboard folder | http://localhost:3000 |
| `mq_prometheus_ent` | Enterprise exporter for QM1 | http://localhost:9457/metrics |
| `mq_prometheus` | Standard exporter for QM2 | http://localhost:9458/metrics |

The build uses the IBM MQ redistributable already present in the repository root.
The two exporter images contain their binaries and runtime client libraries.
The four migrated services use the original Docker images. To restore those
images on another Docker host, run:

```sh
docker image load -i images/migrated-images.tar.gz
```

Their copied data lives in `data/`, `prometheus-data/`, and `grafana-data/`.
The original Prometheus configuration and Grafana dashboard JSON files are saved in
`prometheus/original.yml` and `grafana/dashboards/`.

Use the script to operate either exporter independently.

```sh
./exporter.sh mq_prometheus_ent start
./exporter.sh mq_prometheus_ent stop
./exporter.sh mq_prometheus_ent restart
./exporter.sh mq_prometheus_ent deploy
./exporter.sh mq_prometheus_ent sync
./exporter.sh mq_prometheus start
./exporter.sh mq_prometheus stop
./exporter.sh mq_prometheus restart
```

From the repo root, use `./docker_build/exporter.sh mq_prometheus_ent deploy`
to rebuild and deploy the enterprise exporter. After enterprise `start`,
`restart`, or `deploy`, the script checks the copied executable against the
running process and updates `../bin/mq_prometheus_ent` atomically. Run `sync`
to refresh the copy from an already-running container after a direct Compose
operation. A restart uses the current image.
