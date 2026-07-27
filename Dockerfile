# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------
# Stage 1 — builder
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS builder

WORKDIR /build

# Step 1: cache dependencies by building a dummy crate with the real manifest.
# Both a dummy main.rs and a dummy lib.rs are required because this crate has
# an implicit library target (src/lib.rs) alongside its binary target.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && echo '// empty' > src/lib.rs \
    && cargo build --release \
    && rm -rf src \
        target/release/deps/messagebox_server* \
        target/release/deps/libmessagebox_server* \
        target/release/messagebox-server* \
        target/release/.fingerprint/messagebox-server-* \
        target/release/.fingerprint/messagebox_server-*

# Step 2: copy the real sources, embedded migrations, and the OpenAPI spec
# (src/docs.rs embeds it via include_str!("../openapi.json") — the build fails
# without it), then build for real. `--locked` honors Cargo.lock byte-for-byte.
# `touch` on all .rs files guards against Cargo's mtime-based staleness check
# preserving stale dummy-build artifacts when COPY leaves older mtimes.
COPY src ./src
COPY migrations ./migrations
COPY openapi.json ./
RUN find src -name '*.rs' -exec touch {} + \
    && cargo build --release --locked

# ---------------------------------------------------------------------------
# Stage 2 — runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Only ca-certificates is needed — TLS is handled in-process by rustls
# (reqwest + sqlx), so libssl / OpenSSL are not required at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Non-root system user for the service.
RUN useradd --system --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin messagebox

COPY --from=builder /build/target/release/messagebox-server /usr/local/bin/messagebox-server

# Writable state directory, and the process's cwd. The persist worker captures
# messages it could not store to MySQL in an append-only dead-letter file at a
# path that is relative by default (persist.rs / DEAD_LETTER_PATH); with the
# default cwd of `/` and a non-root user that open() fails EACCES, silently
# turning "captured for recovery" into a lost message. Note the layer is still
# part of the container's ephemeral filesystem: this survives a process restart,
# NOT a container replacement. Point DEAD_LETTER_PATH at a mounted volume where
# the messages must outlive the container.
RUN mkdir -p /var/lib/messagebox && chown messagebox:messagebox /var/lib/messagebox
WORKDIR /var/lib/messagebox

USER messagebox

# Railway injects $PORT at runtime; 8080 is the documented dev default and
# serves as the declared container port.
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/messagebox-server"]
