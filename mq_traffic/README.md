# MQ Traffic Console

A local C++20 web application for generating and observing test traffic on
`APP.TEST.LOCAL.001` through `APP.TEST.LOCAL.500` on QM1 and QM2. Build it with
G++ and the IBM MQ Linux x64 redistributable client from this checkout.

```bash
./mq_traffic/build.sh
./mq_traffic/run.sh
```

Open <http://127.0.0.1:8088/>. Pass a port to `run.sh` to use a different local
HTTP port. Set `MQ_INSTALLATION_PATH` before building to use another MQ client
installation.

The UI defaults to the Docker Compose stack's `DEV.APP.SVRCONN` channel and
`app` user, with host ports 1415 and 1416.

Set `MQ_TRAFFIC_PASSWORD` in the shell that starts the app. The server uses
that shared password for both queue managers. In Bash, this prompts once
without echoing the password or putting it in shell history:

```bash
read -rsp 'MQ password: ' MQ_TRAFFIC_PASSWORD
printf '\n'
MQ_TRAFFIC_PASSWORD="$MQ_TRAFFIC_PASSWORD" ./mq_traffic/run.sh
unset MQ_TRAFFIC_PASSWORD
```

Set **Elapsed time** from 1 to 3,600 seconds and select a load profile.
The default is 60 seconds at Light. Each manager cycles through its 500 test
queues in order until the timer expires or you click Stop. The time limit
takes precedence if a run is too short to visit all 500 queues. Each worker
opens a queue on first use and keeps its handle until the run disconnects.

| Load | MQ connections per manager | Maximum visits/s per manager | Messages per visit | Payload |
| --- | ---: | ---: | ---: | ---: |
| Light | 1 | 10 | 1 | 256 B |
| Medium | 1 | 50 | 2 | 4 KiB |
| Heavy | 4 | 400 | 8 | 64 KiB |
| Extreme | 8 to 64 in Put and get | Unthrottled | 32 | 128 KiB to 1 MiB |

Heavy and Extreme divide the queues among concurrent workers. Each worker has
its own MQ connection. The dashboard shows connected workers and the slowest
queue visit, which includes all MQ operations in that visit.

Round-trip mode puts and retrieves each message before sending the next.
**Extreme** with **Put and get** starts at eight connections per manager and
128 KiB nonpersistent messages. Every eight seconds it checks host CPU usage,
completed gets, and new MQ errors. While the workload is making progress below
95% host CPU, it alternates adding eight workers and doubling the fast payload,
up to 64 workers per manager and 1 MiB. It holds near CPU saturation and backs
off on MQ errors or an interval with no completed gets. Workers above a reduced
target park until the target increases or the run ends. CPU is sampled from
Linux `/proc/stat` and represents host CPU use across the app, MQ, and other
processes. The dashboard shows the target, payload, CPU, and latest decision.

**Include persistent disk load** adds a persistent stream to adaptive Extreme.
Up to four of the workers use 1 MiB persistent messages with four messages
per visit. Both streams run outside a unit of work. The dashboard shows
persistent counts separately and updates totals after each message pair.
**Extreme** with **Put only** uses 32 workers, 256 KiB messages, and the queue's
default persistence.
**Put only** leaves messages on the queues and can fill them, especially at
Extreme load. **Get by run ID** retrieves only messages tagged with the
provided ID. Choose the same managers and allow enough time to drain the run.
Stop allows the current message operation or round-trip pair to finish.

The dashboard shows application workload counters, payload volumes, elapsed
time, manager status, MQ reason codes, and per-queue activity. Rates represent successful operations from this
application. The browser charts samples collected while the dashboard is open.
The service holds the current or most recent run's counters in memory.
