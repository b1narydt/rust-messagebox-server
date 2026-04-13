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

# Step 2: copy the real sources and embedded migrations, then build for real.
# `--locked` honors Cargo.lock byte-for-byte. `touch` on all .rs files
# guards against Cargo's mtime-based staleness check preserving stale
# dummy-build artifacts when COPY leaves older mtimes.
COPY src ./src
COPY migrations ./migrations
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

USER messagebox

# Railway injects $PORT at runtime; 8080 is the documented dev default and
# serves as the declared container port.
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/messagebox-server"]
