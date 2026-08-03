# Multi-stage build shared by all three binaries. `BIN` selects which one:
#   docker build --build-arg BIN=chainscope-indexer .
# The workspace compiles once per image, but the runtime layer carries only the
# one static-ish binary plus certs — no toolchain, no source.
FROM rust:1-slim-bookworm AS builder
WORKDIR /app

# librdkafka (indexer/alerter) is built from source by rdkafka-sys, so the
# builder needs a C/C++ toolchain, cmake, and the ssl headers reqwest wants.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev libsasl2-dev libcurl4-openssl-dev zlib1g-dev \
        cmake make clang g++ ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY . .
ARG BIN
RUN test -n "$BIN" || (echo "BIN build-arg is required" && false)
RUN cargo build --release --bin "$BIN" \
    && cp "target/release/$BIN" /app/service

# Runtime: a bare debian with certs (TLS to the RPC endpoint) and curl (the
# api healthcheck hits /healthz). The binary is renamed to a fixed path so the
# entrypoint does not depend on BIN.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
# Run from /app and ship the default config beside the binary: the indexer looks
# for `chainscope.toml` at a path relative to its working directory, so without
# both of these it starts in / with no config and crash-loops on a missing
# chain_id. Secrets still come from the environment (.env / compose); this file
# holds only the shareable defaults (chain id, pool list, tuning knobs).
WORKDIR /app
COPY --from=builder /app/service /usr/local/bin/service
COPY --from=builder /app/chainscope.toml ./chainscope.toml
ENTRYPOINT ["/usr/local/bin/service"]
