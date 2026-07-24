use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::timeout::TimeoutLayer;

use bsv::auth::peer::Peer;
use bsv::primitives::private_key::PrivateKey;
use bsv::wallet::cached_key_deriver::CachedKeyDeriver;
use bsv::wallet::proto_wallet::ProtoWallet as SdkProtoWallet;
use bsv_auth_axum_middleware::ActixTransport;
use bsv_wallet_toolbox::services::Services;
use bsv_wallet_toolbox::storage::manager::WalletStorageManager;
use bsv_wallet_toolbox::storage::remoting::{StorageClient, WalletArc};
use bsv_wallet_toolbox::types::Chain;
use bsv_wallet_toolbox::wallet::types::WalletArgs;
use bsv_wallet_toolbox::wallet::wallet::Wallet;

use messagebox_server::{cloneable_wallet, config, db, handlers, logger, ws};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let config = config::Config::load().expect("Failed to load config");

    logger::init(config.is_development());

    // Emit any MESSAGEBOX_FEES parse warnings now that the logger is up.
    for warning in &config.message_box_fees_warnings {
        tracing::warn!("{warning}");
    }

    let pool = db::new(&config.db_source, config.db_max_connections)
        .await
        .expect("Failed to open database");
    db::migrate(&pool).await.expect("Failed to run migrations");

    // Fee policy: free delivery by default; MESSAGEBOX_PARITY_FEES=true restores
    // the reference (TS/Go/CF) pay-to-deliver economics for `notifications`.
    // The flag drives both the recipient smart-default (in db::queries) and the
    // server delivery-fee seed below. Set it before any fee lookup or cache prime.
    db::queries::set_parity_fees(config.parity_fees);
    if config.parity_fees {
        // Seed the reference notifications delivery fee unless the operator set
        // it explicitly via MESSAGEBOX_FEES (that override wins and is applied
        // just below).
        let overridden = config
            .message_box_fees
            .iter()
            .any(|(b, _)| b == "notifications");
        if !overridden {
            if let Err(e) = db::queries::upsert_server_fee(&pool, "notifications", 10).await {
                tracing::error!("failed to seed parity notifications delivery fee: {e}");
            } else {
                tracing::info!(
                    "MESSAGEBOX_PARITY_FEES=true — seeded notifications delivery fee = 10 (reference parity)"
                );
            }
        }
    }

    // Apply operator fee overrides from MESSAGEBOX_FEES before the in-memory
    // cache is primed so the cache always reflects the latest values.
    if config.message_box_fees.is_empty() {
        tracing::info!("no env fee overrides configured (MESSAGEBOX_FEES not set)");
    } else {
        let mut applied: Vec<String> = Vec::new();
        for (box_name, fee) in &config.message_box_fees {
            match db::queries::upsert_server_fee(&pool, box_name, *fee).await {
                Ok(()) => applied.push(format!("{box_name}={fee}")),
                Err(e) => tracing::error!("failed to upsert server fee for {box_name}={fee}: {e}"),
            }
        }
        if !applied.is_empty() {
            tracing::info!("applied env fee overrides: {}", applied.join(", "));
        }
    }

    db::queries::init_delivery_fee_cache(&pool)
        .await
        .expect("Failed to prime delivery-fee cache");

    // Firebase (§4.3): EXPLICIT opt-in — ENABLE_FIREBASE=true, then
    // FIREBASE_PROJECT_ID + one credential source. Init failure is non-fatal
    // (the server runs without push). The service-account key material is
    // never logged (E2).
    if config.enable_firebase {
        messagebox_server::firebase::initialize(
            config.firebase_project_id.as_deref(),
            config.firebase_service_account_json.as_deref(),
            config.firebase_service_account_path.as_deref(),
        )
        .await;
    } else {
        tracing::info!("Firebase disabled (ENABLE_FIREBASE not 'true') — no push notifications");
    }

    // Create bsv-sdk wallet from server private key
    let sdk_private_key = match PrivateKey::from_hex(&config.server_private_key) {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Failed to parse SERVER_PRIVATE_KEY: {e}");
            std::process::exit(1);
        }
    };
    let sdk_wallet = Arc::new(SdkProtoWallet::new(sdk_private_key.clone()));

    // Build the funded wallet that can internalize incoming delivery-fee payments.
    // Uses StorageClient<WalletArc<SdkProtoWallet>> to forward all storage calls
    // (including internalizeAction) to the remote wallet-storage backend via BRC-103/104
    // authenticated JSON-RPC. The WalletArc wrapper makes SdkProtoWallet Clone-able
    // so it satisfies StorageClient<W: Clone>'s bound.
    let chain = if config.bsv_network == "mainnet" {
        Chain::Main
    } else {
        Chain::Test
    };
    let key_deriver = Arc::new(CachedKeyDeriver::new(sdk_private_key.clone(), None));
    let identity_key_hex = key_deriver.identity_key().to_der_hex();
    let wallet_arc = WalletArc::new(SdkProtoWallet::new(sdk_private_key));
    let storage_client = Arc::new(StorageClient::new(wallet_arc, &config.wallet_storage_url));
    let storage_manager = WalletStorageManager::new(
        identity_key_hex.clone(),
        Some(storage_client as Arc<dyn bsv_wallet_toolbox::storage::traits::WalletStorageProvider>),
        vec![],
    );
    let services = Arc::new(Services::from_chain(chain.clone()));
    let wallet_args = WalletArgs {
        chain,
        key_deriver: key_deriver.clone(),
        storage: Arc::new(storage_manager),
        services: Some(services),
        monitor: None,
        privileged_key_manager: None,
        settings_manager: None,
        lookup_resolver: None,
    };
    let funded_wallet: Arc<dyn bsv::wallet::interfaces::WalletInterface> =
        match Wallet::new(wallet_args) {
            Ok(w) => Arc::new(w),
            Err(e) => {
                tracing::error!("Failed to construct funded wallet: {e}");
                std::process::exit(1);
            }
        };
    tracing::info!("Wallet storage backend: {}", config.wallet_storage_url);

    // Log server identity key via bsv-sdk
    let identity_key = {
        use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
        match sdk_wallet
            .get_public_key(
                GetPublicKeyArgs {
                    identity_key: true,
                    protocol_id: None,
                    key_id: None,
                    counterparty: None,
                    privileged: false,
                    privileged_reason: None,
                    for_self: None,
                    seek_permission: None,
                },
                None,
            )
            .await
        {
            Ok(r) => r.public_key.to_der_hex(),
            Err(e) => {
                tracing::error!("Failed to get identity key: {e}");
                std::process::exit(1);
            }
        }
    };
    tracing::info!("Server identity key: {identity_key}");

    let port = config.port;
    let prefix = config.routing_prefix.clone();
    let drain_timeout = std::time::Duration::from_secs(config.drain_timeout_secs);

    // Admission/drain state (Phase 3, D3): per-instance connection ceiling +
    // the draining flag + in-flight send tracking. Shared by the admission
    // middleware, both send paths, health, /metrics, and shutdown.
    let ops = messagebox_server::ops::OpsState::new(config.max_connections);
    if config.max_connections > 0 {
        tracing::info!(
            max_connections = config.max_connections,
            "admission control: per-instance connection ceiling enabled"
        );
    }

    // Topology (transport-architecture WS2-3): no REDIS_URL → Model A
    // (single instance, in-process routing). REDIS_URL set → Model B
    // (Redis pub/sub backplane; N replicas behind a sticky LB). Same binary,
    // config decides. Redis is live-push only — durability stays in MySQL.
    let backplane = config
        .redis_url
        .as_deref()
        .map(messagebox_server::backplane::Backplane::new);
    match &backplane {
        Some(bp) => tracing::info!(
            instance = %bp.instance_id(),
            "topology: Model B — Redis backplane enabled (cross-instance live push)"
        ),
        None => tracing::info!("topology: Model A — single instance, in-process routing"),
    }

    // Set up Socket.IO for WebSocket live message push
    let (sio_layer, io) = socketioxide::SocketIo::new_layer();
    let ws_broadcast = ws::WsBroadcast::new(
        io.clone(),
        config.server_private_key.clone(),
        pool.clone(),
        backplane.clone(),
        ops.clone(),
    );
    ws::setup_handlers(&io, ws_broadcast.clone());
    tracing::info!("Socket.IO WebSocket server ready");

    // Set up BRC-103/104 auth middleware (bsv-sdk Peer — concurrency-safe API).
    //
    // BRC-103 fix: Peer is now fully interior-mutable (Arc<Peer>, every method
    // &self). No outer Mutex — concurrent requests on one session no longer
    // serialize on a per-Peer lock. AuthLayer::from_config takes Arc<Peer>.
    let transport = Arc::new(ActixTransport::new());
    let auth_wallet = {
        let pk = PrivateKey::from_hex(&config.server_private_key).unwrap();
        cloneable_wallet::CloneableProtoWallet(Arc::new(SdkProtoWallet::new(pk)))
    };
    let peer = Arc::new(Peer::new(auth_wallet.clone(), transport.clone()));

    let auth_config = bsv_auth_axum_middleware::AuthMiddlewareConfigBuilder::new()
        .wallet(auth_wallet.clone())
        .allow_unauthenticated(false)
        .build()
        .expect("auth middleware config");

    let auth_layer = bsv_auth_axum_middleware::AuthLayer::from_config(auth_config, peer, transport)
        .await
        .expect("auth middleware layer");

    let app_state = handlers::helpers::AppState {
        db: pool.clone(),
        config: Arc::new(config),
        funded_wallet,
        ws: ws_broadcast.clone(),
    };

    // CORS. Auth is BRC-103 request-signature based (no cookies, no
    // credentials), so a permissive default is not a CSRF vector, but an
    // internet-facing operator can lock the browser origin down:
    // CORS_ALLOWED_ORIGINS=https://app.example.com,https://admin.example.com
    // restricts to an explicit allowlist; unset keeps the permissive default.
    let cors = match std::env::var("CORS_ALLOWED_ORIGINS")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
        Some(list) => {
            let origins: Vec<axum::http::HeaderValue> = list
                .split(',')
                .filter_map(|o| o.trim().parse().ok())
                .collect();
            tracing::info!(count = origins.len(), "CORS restricted to an explicit origin allowlist");
            CorsLayer::new()
                .allow_origin(origins)
                .allow_headers(Any)
                .allow_methods(Any)
                .expose_headers(Any)
        }
        None => CorsLayer::new()
            .allow_origin(Any)
            .allow_headers(Any)
            .allow_methods(Any)
            .expose_headers(Any),
    };

    // Request timeout + body size limits — applied only to API routes,
    // NOT to the health endpoint (uptime checks stay fast, GET / avoids
    // body-limit parsing entirely).
    let timeout_secs: u64 = std::env::var("REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30);
    let max_body_bytes: usize = std::env::var("MAX_BODY_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10 * 1024 * 1024);

    // Unauthenticated ops endpoints — always at the root, never under the
    // routing prefix, never behind BRC-103/104 auth. `GET /` returns plain
    // text (the pre-existing extension over TS, which has no health route);
    // `GET /metrics` is the Prometheus scrape target (operational counts
    // only — no identities, no message data, no key material);
    // `/health/live` + `/health/ready` are the structured probes (readiness
    // fails on DB loss, Redis loss in Model B, and while draining).
    // Public pre-auth surface. `/` + `/health/live` are harmless liveness
    // probes; `/docs` + `/openapi.json` are pre-auth in the TS and Go references
    // too (their swagger mounts sit before the auth middleware), so they stay
    // public here. The operational surface that leaks live counts — `/metrics`
    // and `/health/ready` — is NOT here; it moves to a private ops listener
    // below (no reference exposes those on the public port).
    let openapi_prefix = prefix.clone();
    let public_routes = Router::new()
        .route(
            "/",
            get(|| async {
                (
                    [(axum::http::header::CONTENT_TYPE, "text/plain")],
                    "BSV MessageBox Server",
                )
            }),
        )
        .route("/docs", get(messagebox_server::docs::docs_page))
        .route(
            "/openapi.json",
            get(move || {
                let prefix = openapi_prefix.clone();
                async move { messagebox_server::docs::openapi_json(&prefix) }
            }),
        )
        .route(
            "/health/live",
            get(|| async { axum::Json(serde_json::json!({"status": "alive"})) }),
        );

    // Private ops surface (Prometheus scrape + readiness). Bound to OPS_BIND
    // (default 127.0.0.1:9091) on its own listener so an internet-facing
    // deployment never exposes connection/identity/queue/admission counts or the
    // dependency-health readiness probe. Scrapers and orchestrators reach it over
    // the private/internal network. Use OPS_BIND=0.0.0.0:<port> to deliberately
    // publish it. `/health/live` stays public for platform health checks.
    let metrics_ws = ws_broadcast.clone();
    let metrics_backplane = backplane.clone();
    let metrics_ops = ops.clone();
    let ready_pool = pool.clone();
    let ready_backplane = backplane.clone();
    let ready_ops = ops.clone();
    let ops_routes = Router::new()
        .route(
            "/health/ready",
            get(move || {
                let pool = ready_pool.clone();
                let backplane = ready_backplane.clone();
                let ops = ready_ops.clone();
                async move {
                    messagebox_server::ops::readiness(&pool, backplane.as_deref(), &ops).await
                }
            }),
        )
        .route(
            "/metrics",
            get(move || {
                let ws = metrics_ws.clone();
                let backplane = metrics_backplane.clone();
                let ops = metrics_ops.clone();
                async move {
                    let (connections, identities) = ws.live_counts();
                    let (depth, capacity) = ws.persist_queue();
                    let persist = ws.persist_stats();
                    let page = messagebox_server::metrics::render(
                        &messagebox_server::metrics::Snapshot {
                            connections,
                            authenticated_identities: identities,
                            persist_queue_depth: depth,
                            persist_queue_capacity: capacity,
                            persist: &persist,
                            backplane: backplane.as_ref().map(|bp| {
                                messagebox_server::metrics::BackplaneSnapshot {
                                    published: bp.published(),
                                    dropped: bp.dropped(),
                                    subscribed: bp.is_subscribed(),
                                    subscriptions: bp.active_subscription_count() as u64,
                                }
                            }),
                            ops: Some(messagebox_server::metrics::OpsSnapshot {
                                draining: ops.is_draining(),
                                in_flight_sends: ops.in_flight_sends(),
                                admission_rejected: ops.admission_rejected(),
                                max_connections: ops.max_connections() as u64,
                            }),
                        },
                    );
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4",
                        )],
                        page,
                    )
                }
            }),
        );

    // Protected API routes — BRC-103/104 auth via bsv-sdk Peer middleware
    let api_routes = Router::new()
        .route("/sendMessage", post(handlers::send_message::send_message))
        .route(
            "/listMessages",
            post(handlers::list_messages::list_messages),
        )
        .route(
            "/acknowledgeMessage",
            post(handlers::acknowledge_message::acknowledge_message),
        )
        .route("/registerDevice", post(handlers::devices::register_device))
        .route("/devices", get(handlers::devices::list_devices))
        .route(
            "/permissions/set",
            post(handlers::permissions::set_permission),
        )
        .route(
            "/permissions/get",
            get(handlers::permissions::get_permission),
        )
        .route(
            "/permissions/list",
            get(handlers::permissions::list_permissions),
        )
        .route("/permissions/quote", get(handlers::permissions::get_quote))
        .layer(auth_layer)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(timeout_secs),
        ))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(app_state);

    // Admission control (D3): gates ONLY new WS handshakes (engine.io
    // requests without a `sid`). In-flight sessions, all API routes, and the
    // ops endpoints pass untouched. Sits inside CORS so preflights are still
    // answered, outside socketioxide so a rejected handshake never reaches it.
    let admission_ops = ops.clone();
    let admission_io = io.clone();
    let admission = axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let ops = admission_ops.clone();
            let io = admission_io.clone();
            async move {
                messagebox_server::ops::gate_admission(
                    &ops,
                    move || io.sockets().map(|s| s.len()).unwrap_or(0),
                    req,
                    next,
                )
                .await
            }
        },
    );

    // Per-IP rate limiting (outermost application concern, inside CORS so
    // preflights are still answered). Protects the unauthenticated BRC-103
    // handshake and the public routes. Disabled with RATE_LIMIT_RPS=0.
    let rate_limiter = messagebox_server::ratelimit::Limiter::from_env();
    if let Some(rl) = &rate_limiter {
        rl.spawn_gc();
    }

    let app = if prefix.is_empty() {
        Router::new().merge(public_routes).merge(api_routes)
    } else {
        Router::new().merge(public_routes).nest(&prefix, api_routes)
    }
    .layer(sio_layer)
    .layer(admission);

    let app = if let Some(rl) = rate_limiter {
        app.layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let rl = rl.clone();
                async move {
                    let peer = req
                        .extensions()
                        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                        .map(|ci| ci.0.ip());
                    match rl.check(req.headers(), peer) {
                        Ok(()) => next.run(req).await,
                        Err(resp) => resp,
                    }
                }
            },
        ))
    } else {
        app
    };

    let app = app.layer(cors);

    // Private ops listener (Prometheus + readiness). Bind failure is non-fatal:
    // the main server still serves; only /metrics + /health/ready are then
    // unavailable. Bound to loopback by default so it is never internet-exposed.
    let ops_bind = std::env::var("OPS_BIND")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1:9091".to_string());
    match tokio::net::TcpListener::bind(&ops_bind).await {
        Ok(ops_listener) => {
            tracing::info!(
                "ops endpoints (/metrics, /health/ready) on private {ops_bind} (set OPS_BIND to change; 0.0.0.0:<port> to publish)"
            );
            tokio::spawn(async move {
                if let Err(e) = axum::serve(ops_listener, ops_routes).await {
                    tracing::error!("ops server error: {e}");
                }
            });
        }
        Err(e) => {
            tracing::error!(
                "failed to bind OPS_BIND={ops_bind}: {e} — /metrics and /health/ready are unavailable"
            );
        }
    }

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("MessageBox server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind TCP listener");

    // Graceful drain (Phase-3 HA): on SIGTERM/ctrl-c, run the drain sequence
    // (stop admission → quiesce in-flight sends → disconnect sockets → flush
    // the persist queue, each phase bounded by DRAIN_TIMEOUT_SECS) and only
    // then let axum stop accepting — zero message loss across a rolling
    // deploy.
    let drain_ops = ops.clone();
    let drain_ws = ws_broadcast.clone();
    let drain_io = io.clone();
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        messagebox_server::ops::drain(&drain_ops, &drain_ws, &drain_io, drain_timeout).await;
    })
    .await
    .expect("Server error");

    tracing::info!("Server shut down gracefully");
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    tracing::info!("Shutdown signal received");
}
