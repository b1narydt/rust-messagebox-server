# rust-messagebox-server

Rust implementation of the MessageBox server protocol — a **drop-in-compatible reimplementation of the TS `@bsv/messagebox-server`** (Parity++: same public API and intended behavior, with the TS bugs deliberately fixed — see [TS parity](#ts-parity--deliberate-deviations-parity)). Stores and routes messages between authenticated parties. All API requests are authenticated via BRC-104 (BRC-103's HTTP binding — the `x-bsv-auth-*` headers + `/.well-known/auth`) using `bsv-auth-axum-middleware`. WebSocket connections use raw BRC-103 mutual authentication via the shared `authsocket` crate (`bsv-sdk` Peer).

Built with axum + socketioxide.

## What it does

The MessageBox server is the central communication hub for the MPC system. Parties never connect directly to each other -- all protocol messages flow through this server:

- **Store messages** -- parties send messages addressed to other parties' identity keys, tagged with a message box name.
- **Route messages** -- recipients poll for messages or receive them in real time via WebSocket.
- **Authenticate** -- every HTTP request is verified via BRC-104 mutual auth. WebSocket connections perform the BRC-103 handshake on connect.

## API routes

All routes require BRC-104 authentication (via `AuthLayer`). The authenticated caller's identity key is extracted from the `x-bsv-auth-*` headers.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sendMessage` | Send a message to a recipient's message box |
| `POST` | `/listMessages` | List messages in a message box (supports `messageBox` filter) |
| `POST` | `/acknowledgeMessage` | Acknowledge (delete) messages by ID |
| `POST` | `/registerDevice` | Register an FCM device token for push notifications (upsert on token) |
| `GET` | `/devices` | List the caller's registered devices (token masked to last 10 chars) |
| `POST` | `/permissions/set` | Set message permissions for a sender |
| `GET` | `/permissions/get` | Get permission for a specific sender |
| `GET` | `/permissions/list` | List all permissions (accepts both `message_box` and `messageBox` filter params; rows serialize snake_case — pinned to client 2.1.0) |
| `GET` | `/permissions/quote` | Get payment quote for sending messages |

The handshake endpoint `POST /.well-known/auth` is handled automatically by the `AuthLayer`.

Pre-auth (like TS): `GET /docs` (Swagger UI) and `GET /openapi.json`.

### Push notifications (FCM)

Messages stored to the magic box name **`notifications`** additionally trigger a Firebase Cloud Messaging (v1) push to every **active** device the recipient registered via `/registerDevice` — best-effort, after the send, never failing it. The visible notification is `title: "New Message"`, `body: <messageId>` (content stays E2E-encrypted; the ID lets the app fetch it). Delivery success bumps the device's `lastUsed`; an FCM invalid-token response (`NOT_FOUND`/`UNREGISTERED`) deactivates the device. Enablement is explicit: `ENABLE_FIREBASE=true` + `FIREBASE_PROJECT_ID` + one credential source (see env vars). The service-account key material is **never logged** (upstream TS logs its first 100 chars at init — deliberately not reproduced).

Delivery is **free out of the box** for every box, `notifications` included (delivery fee 0, smart-default recipient fee 0). This is a deliberate deviation from the TS server, which seeds `notifications` at 10 sats. An operator who wants a fee arms it per box via `MESSAGEBOX_FEES` (e.g. `MESSAGEBOX_FEES=notifications=10`).

## WebSocket (Socket.IO)

The server runs a Socket.IO layer (via `socketioxide`) for real-time message delivery:

- Clients connect and perform BRC-103 authentication using `bsv-sdk` Peer.
- On successful auth, the server associates the socket with the client's identity key.
- When a message is sent via HTTP, the server pushes it to the recipient's WebSocket if connected.
- The `ws` module handles connection, authentication, and broadcast.

## Running

```bash
SERVER_PRIVATE_KEY="<64-hex-private-key>" PORT=3322 cargo run --release --bin messagebox-server
```

### Database: fresh deploys only

The migration chain was squashed to a single fresh-deploy baseline
(`20260717000000_baseline_schema.sql`, design decision D2). **Point this server at
an empty database.**

Against a database that ran the older 4-migration chain, `sqlx` finds
`_sqlx_migrations` rows whose versions no longer exist in the source and returns
`VersionMissing`; `main()` treats that as fatal, so the process panics on every
start (crash loop). There is no in-place upgrade path — drop and recreate the
schema, or provision a new database.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PRIVATE_KEY` | *required* | 64-character hex private key for the server's identity |
| `PORT` / `HTTP_PORT` | `8080` (dev), `3000` (prod) | TCP listen port |
| `NODE_ENV` | `development` | `production` or `development` |
| `DATABASE_URL` | *(none)* | MySQL connection URL (Railway convention, takes precedence) |
| `KNEX_DB_CONNECTION` | *(none)* | MySQL connection as Knex-style JSON (team/TS convention) |
| `DB_SOURCE` | `mysql://root:root@127.0.0.1:3306/messagebox` | MySQL connection URL (backwards compat) |
| `DB_MAX_CONNECTIONS` | `50` | sqlx MySQL pool size |
| `ROUTING_PREFIX` | *(empty)* | Optional URL prefix for all API routes |
| `BSV_NETWORK` | `mainnet` | BSV network |
| `REDIS_URL` | *(none)* | Unset → **Model A** (single instance, in-process routing — the default). Set → **Model B**: Redis pub/sub backplane for cross-instance live push; run N replicas behind a **sticky** LB. See below. |
| `MAX_CONNECTIONS` | `0` (unlimited) | Per-instance WebSocket connection ceiling (admission control). Past it, NEW connections get `503` + `Retry-After` (Model B: the LB sheds to another instance; Model A: the client retries). In-flight sessions are never affected. |
| `DRAIN_TIMEOUT_SECS` | `30` | Per-phase bound on the SIGTERM graceful drain (in-flight send quiesce, persist-queue flush). |
| `MESSAGEBOX_FEES` | *(none)* | Operator per-box delivery-fee overrides, `box=sats` comma-separated (e.g. `notifications=10,priority=100`). Upserted at boot **before** the fee cache is primed. Out-of-box seed is free delivery for every box: `notifications=0`, `inbox=0`, `payment_inbox=0` (deviates from TS, which seeds `notifications=10`). |
| `ENABLE_FIREBASE` | `false` | Explicit opt-in for FCM push notifications (must be exactly `true`, TS parity). |
| `FIREBASE_PROJECT_ID` | *(none)* | Firebase project id — required when Firebase is enabled. |
| `FIREBASE_SERVICE_ACCOUNT_JSON` | *(none)* | Service-account key JSON (inline). SECRET — never logged or Debug-printed. |
| `FIREBASE_SERVICE_ACCOUNT_PATH` | *(none)* | Path to the service-account key file (alternative to the inline JSON). |

## Topology: Model A / Model B

One binary, topology chosen by config (`REDIS_URL`). Durable state always
lives in MySQL — an instance holds only ephemeral WebSocket/room state,
rebuilt when a client reconnects — so any instance can serve any mailbox
over HTTP. Redis's one job is bridging **live push** between instances.

```
MODEL A — single self-contained instance (default)
   [ MBS ] ── [ MySQL ]           no Redis, no LB

MODEL B — horizontal fleet (REDIS_URL set)
   [ LB, sticky WS ] → [ MBS × N ] ── shared [ MySQL ] + shared [ Redis pub/sub ]
```

Model B delivery is **carry-unsigned / sign-on-owner**: `broadcast_to_room`
publishes the *unsigned* message to Redis; every instance subscribes and
signs **only for its own local room members** — BRC-103 signing is pinned to
the instance holding that socket's authsocket `Peer` session, so no other
instance can (or does) sign for it. The local delivery path is byte-identical
in both models; Model A merely skips the publish.

Redis is live-push only, **never durability**: if Redis is down, local
delivery keeps working, cross-instance recipients fall back to the durable
mailbox (`/listMessages` from any instance), and the degradation is logged +
counted — the server never fails or blocks a send on Redis.

## Operations (unauthenticated, at the root — never under `ROUTING_PREFIX`)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Plain-text banner (legacy uptime check) |
| `GET` | `/health/live` | Liveness: the event loop answered — always `200` |
| `GET` | `/health/ready` | Readiness: `200` when routable, `503` + JSON `{ready, db, redis, draining}` on DB loss, on Redis-subscription loss in Model B (Model A skips the check), or while draining |
| `GET` | `/metrics` | Prometheus text: connections/rooms, fan-out + sign-latency histograms, persist queue depth / inline-fallback / dead-letter counters, Model B publish/drop/lag, admission + drain gauges. Operational counts only — no identities, no message data, no key material. Bind to a scrape network in production. |

**Admission control** (`MAX_CONNECTIONS`): gates only *new* WS handshakes
(engine.io requests without a `sid`). Established sessions, all API routes,
and the ops endpoints are never gated — an in-flight ceremony's sends are
never nacked by capacity pressure. Rejections are `503` + `Retry-After: 5`
with `ERR_SERVER_AT_CAPACITY` / `ERR_SERVER_DRAINING`.

**Graceful drain** (SIGTERM, each phase bounded by `DRAIN_TIMEOUT_SECS`):
stop admission and flip readiness (the LB deregisters) → wait for in-flight
sends → disconnect sockets (clients re-handshake against another instance;
the MySQL mailbox covers the gap) → flush the persist queue → exit. Zero
message loss: every accepted message ends durably stored, a confirmed
duplicate, or dead-lettered to disk.

## TS parity & deliberate deviations (Parity++)

This server matches the TS `@bsv/messagebox-server` (1.1.13) public API and
intended behavior — routes, request/response shapes, error codes and
statuses, the notification/device system, fee semantics, and the WS event
surface (including the failure events `authenticationFailed` / `joinFailed`
/ `leaveFailed` / `messageFailed`). The full row-by-row audit lives in the
PARAGON wiki (`mbs-ts-parity-audit-2026-07-17`).

**Seven deviations are deliberate and will not be "fixed back" to TS** —
they correct TS bugs or add enterprise behavior the client is agnostic to
(client 2.1.0 works identically against both servers):

1. **Room-scoped signed live delivery.** TS live delivery is a global
   `io.emit` — every WS message is broadcast to *every* connected
   authenticated socket (a privacy/scale leak). Here delivery is signed and
   scoped to the recipient's room members only.
2. **Cryptographically verified WS sender.** TS stores the client-*claimed*
   `identityKey` as the message sender (spoofable within an authed socket).
   Here the sender is always the BRC-103-verified key; claimed keys are
   ignored.
3. **Own-room join enforcement.** A client may only join
   `{itsIdentityKey}-{box}` rooms; TS performs no ownership check. Rejected
   joins get a signed `joinFailed` (not silence), but stay rejected.
4. **No Firebase-key logging.** Upstream TS logs the first 100 characters of
   the service-account JSON at init. This server never logs or Debug-prints
   key material (test-pinned).
5. **Push-live-first + async persistence.** TS inserts synchronously on the
   request path (and never live-pushes HTTP sends). Here live push happens
   before the DB write on both paths, the durable INSERT runs in a bounded
   background worker (retry → inline fallback → dead-letter, never a silent
   drop) — consequence: a duplicate `messageId` is idempotent success
   instead of TS's 400 `ERR_DUPLICATE_MESSAGE`.
6. **Normalized stored-body wrap.** TS wraps HTTP-stored bodies in
   `{"message": …}` but stores WS bodies raw (internally inconsistent). Here
   both paths store the wrap; the client's parser handles it natively.
7. **Enterprise config & ops.** Delivery-fee in-memory cache (fees read at
   boot; restart or `MESSAGEBOX_FEES` to change), 10 MB body cap (vs TS's
   1 GB), saner port/env handling, plus the whole ops surface below
   (health/readiness, metrics, admission control, graceful drain, Model B).

One **contract pin** where TS disagrees with itself: `/permissions/list`
rows are snake_case and the `message_box` filter param is accepted, because
that is what `@bsv/message-box-client` 2.1.0 actually sends/reads — the TS
*server* uses camelCase rows and only reads `messageBox` (so the client's
filter is silently dead against it). Parity is pinned to the client;
`messageBox` is also accepted.

Also carried as a **compat surface, not a control**: the fee/permission
plane (`/permissions/*`, payments) matches TS wire-for-wire, but the WS
`sendMessage` path bypasses it on **both** implementations — do not build
authorization or monetization on it without new work (see
`handlers/permissions.rs` module docs).

## Auth stack

```
Incoming HTTP request
  |
  +-- /.well-known/auth --> AuthLayer handles the BRC-104 handshake via bsv-sdk Peer
  |
  +-- x-bsv-auth-* headers present --> AuthLayer verifies request signature,
  |   calls handler, signs response --> mutual auth complete
  |
  +-- no auth headers --> 401 Unauthorized (allow_unauthenticated = false)

Incoming WebSocket (Socket.IO)
  |
  +-- on connect --> BRC-103 auth via bsv-sdk Peer
  +-- on auth success --> socket bound to identity key, receives live message pushes
```

## Key modules

| Module | Purpose |
|--------|---------|
| `main` | Server setup: database, auth middleware, Socket.IO, route mounting |
| `config` | `Config` struct loaded from environment |
| `db` | MySQL (InnoDB) database: connection pool, migrations, queries |
| `handlers` | HTTP handlers: send, list, acknowledge, devices, permissions |
| `firebase` | FCM v1 push delivery + service-account OAuth2 (explicit `ENABLE_FIREBASE` opt-in; key material never logged) |
| `docs` | `GET /docs` (Swagger UI) + `GET /openapi.json` (static OpenAPI 3.0 spec) |
| `ws` | MessageBox app layer over the shared `authsocket` crate (BRC-103 sessions, rooms, signed broadcast, TS-parity failure events) |
| `backplane` | Model B Redis pub/sub backplane: unsigned cross-instance envelopes, sign-on-owner delivery, degrade-don't-fail |
| `ops` | Operational floor: admission control (connection ceiling), structured liveness/readiness, graceful drain |
| `metrics` | Prometheus text exposition for `GET /metrics` (histograms + scrape-time counter sampling) |
| `cloneable_wallet` | `CloneableProtoWallet` wrapper for `bsv-sdk` `ProtoWallet` (needed for `Peer<W: Clone>`) |
| `logger` | Tracing/logging initialization |
