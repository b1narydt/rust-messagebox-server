# rust-messagebox-server

Rust implementation of the MessageBox server protocol. Stores and routes messages between authenticated parties. All API requests are authenticated via BRC-31 (HTTP) using `bsv-auth-axum-middleware`. WebSocket connections use BRC-103 mutual authentication via `bsv-sdk` Peer.

Built with axum + socketioxide.

## What it does

The MessageBox server is the central communication hub for the MPC system. Parties never connect directly to each other -- all protocol messages flow through this server:

- **Store messages** -- parties send messages addressed to other parties' identity keys, tagged with a message box name.
- **Route messages** -- recipients poll for messages or receive them in real time via WebSocket.
- **Authenticate** -- every HTTP request is verified via BRC-31 mutual auth. WebSocket connections perform BRC-103 handshake on connect.
- **Push notifications** -- optional Firebase Cloud Messaging for mobile clients.

## API routes

All routes require BRC-31 authentication (via `AuthLayer`). The authenticated caller's identity key is extracted from the `x-bsv-auth-*` headers.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sendMessage` | Send a message to a recipient's message box |
| `POST` | `/listMessages` | List messages in a message box (supports `messageBox` filter) |
| `POST` | `/acknowledgeMessage` | Acknowledge (delete) messages by ID |
| `POST` | `/registerDevice` | Register a device for push notifications |
| `GET` | `/devices` | List registered devices |
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
| `DB_DRIVER` | `sqlite3` | Database driver |
| `DB_SOURCE` | `messagebox.db` | SQLite database path |
| `ROUTING_PREFIX` | *(empty)* | Optional URL prefix for all API routes |
| `BSV_NETWORK` | `mainnet` | BSV network |
| `ENABLE_WEBSOCKETS` | `false` | Enable Socket.IO WebSocket layer |
| `FIREBASE_PROJECT_ID` | *(none)* | Firebase project ID for push notifications |
| `FIREBASE_SERVICE_ACCOUNT_JSON` | *(none)* | Firebase service account JSON (inline) |
| `FIREBASE_SERVICE_ACCOUNT_PATH` | *(none)* | Path to Firebase service account JSON file |

## Auth stack

```
Incoming HTTP request
  |
  +-- /.well-known/auth --> AuthLayer handles BRC-31 handshake via bsv-sdk Peer
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
| `db` | SQLite database: connection pool, migrations, queries |
| `handlers` | HTTP handlers: send, list, acknowledge, devices, permissions |
| `ws` | WebSocket (Socket.IO) handlers: connection, BRC-103 auth, message broadcast |
| `cloneable_wallet` | `CloneableProtoWallet` wrapper for `bsv-sdk` `ProtoWallet` (needed for `Peer<W: Clone>`) |
| `firebase` | Optional Firebase Cloud Messaging for push notifications |
| `logger` | Tracing/logging initialization |
