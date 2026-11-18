# docker build -t dreamgate .
# docker run -d --name dreamgate -p 127.0.0.1:8787:8787 -v dreamgate-data:/data --stop-timeout 60 dreamgate
#
# The port is published on loopback for a reverse proxy on the same host that terminates TLS: published on
# every interface, it is plain HTTP that Docker's own rules open past a host firewall.
#
# The build stage uses the toolchain CI pins. The image that runs is a slim Debian with the CA bundle,
# because the HTTP client checks an upstream's certificate against the system's store, so an https
# connection fails without it.

FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin dreamgate \
    && mkdir -p /data \
    && chown dreamgate /data
# The dashboard is served from web/ under the working directory.
WORKDIR /app
COPY web ./web
COPY --from=build /src/target/release/dreamgate /usr/local/bin/dreamgate
USER dreamgate
ENV DREAMGATE_ADDR=0.0.0.0:8787 \
    DREAMGATE_DB=/data/dreamgate.db
VOLUME /data
EXPOSE 8787
# The process handles SIGTERM itself, which is what `docker stop` sends, and it runs as PID 1 because the
# command is in exec form. It drains for up to DREAMGATE_SHUTDOWN_GRACE_SECS (30 by default), then waits up
# to ten seconds for open connections and five for the last rows, so the stop timeout has to be longer than
# the grace window plus fifteen seconds, or Docker kills it while those rows are written: `--stop-timeout 60`,
# or `stop_grace_period` in Compose.
STOPSIGNAL SIGTERM
# Asks the port above; a DREAMGATE_ADDR set to another port needs a check of its own.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
    CMD curl -fsS http://127.0.0.1:8787/health || exit 1
CMD ["dreamgate"]
