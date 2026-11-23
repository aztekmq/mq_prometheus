FROM golang:1.25-bookworm AS builder
WORKDIR /src
ENV MQ_INSTALLATION_PATH=/opt/mqm GOWORK=off GOTOOLCHAIN=local CGO_ENABLED=1 \
    CGO_CFLAGS="-I/opt/mqm/inc" CGO_LDFLAGS="-L/opt/mqm/lib64" \
    LD_LIBRARY_PATH=/opt/mqm/lib64
COPY 10.0.0.0-IBM-MQC-Redist-LinuxX64/ /opt/mqm/
COPY go.mod ./
COPY vendor/ vendor/
COPY ibmmq/ ibmmq/
COPY mqmetric/ mqmetric/
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY cmd_ent/ cmd_ent/
COPY scripts/buildPrometheusEnt.sh scripts/buildPrometheusEnt.sh
RUN mkdir /out \
    && go build -mod=vendor -trimpath -o /out/mq_prometheus ./cmd/mq_prometheus \
    && CGO_LDFLAGS="$CGO_LDFLAGS -Wl,-rpath,\$ORIGIN/../10.0.0.0-IBM-MQC-Redist-LinuxX64/lib64" \
       OUTDIR=/out ./scripts/buildPrometheusEnt.sh

FROM debian:bookworm-slim AS runtime
ENV LD_LIBRARY_PATH=/opt/mqm/lib64 MQ_INSTALLATION_PATH=/opt/mqm
COPY --from=builder /opt/mqm/lib/ /opt/mqm/lib/
COPY --from=builder /opt/mqm/gskit9/ /opt/mqm/gskit9/
COPY --from=builder /opt/mqm/licenses/ /opt/mqm/licenses/
COPY --from=builder /opt/mqm/lib64/ /opt/mqm/lib64/
COPY --from=builder /opt/mqm/msg/ /opt/mqm/msg/
RUN useradd --create-home --uid 10001 exporter
USER exporter

FROM runtime AS standard
COPY --from=builder /out/mq_prometheus /usr/local/bin/mq_prometheus
ENTRYPOINT ["/usr/local/bin/mq_prometheus"]

FROM runtime AS enterprise
COPY --from=builder /out/mq_prometheus_ent /usr/local/bin/mq_prometheus_ent
ENTRYPOINT ["/usr/local/bin/mq_prometheus_ent"]
