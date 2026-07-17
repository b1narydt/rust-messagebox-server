# rust-messagebox-server

Rust implementation of the MessageBox server protocol. Stores and routes messages between authenticated parties. All API requests are authenticated via BRC-104 (BRC-103's HTTP binding — the `x-bsv-auth-*` headers + `/.well-known/auth`) using `bsv-auth-axum-middleware`. WebSocket connections use raw BRC-103 mutual authentication via the shared `authsocket` crate (`bsv-sdk` Peer).

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
| `POST` | `/permissions/set` | Set message permissions for a sender |
| `GET` | `/permissions/get` | Get permission for a specific sender |
| `GET` | `/permissions/list` | List all permissions |
| `GET` | `/permissions/quote` | Get payment quote for sending messages |

The handshake endpoint `POST /.well-known/auth` is handled automatically by the `AuthLayer`.

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
| `handlers` | HTTP handlers: send, list, acknowledge, permissions |
| `ws` | MessageBox app layer over the shared `authsocket` crate (BRC-103 sessions, rooms, signed broadcast) |
| `backplane` | Model B Redis pub/sub backplane: unsigned cross-instance envelopes, sign-on-owner delivery, degrade-don't-fail |
| `cloneable_wallet` | `CloneableProtoWallet` wrapper for `bsv-sdk` `ProtoWallet` (needed for `Peer<W: Clone>`) |
| `logger` | Tracing/logging initialization |
