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
use bsv::wallet::proto_wallet::ProtoWallet as SdkProtoWallet;
use bsv_auth_axum_middleware::ActixTransport;

use messagebox_server::{cloneable_wallet, config, db, firebase, handlers, logger, ws};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let config = config::Config::load().expect("Failed to load config");

    logger::init(config.is_development());

    let pool = db::new(&config.db_source, config.db_max_connections)
        .await
        .expect("Failed to open database");
    db::migrate(&pool).await.expect("Failed to run migrations");
    db::queries::init_delivery_fee_cache(&pool)
        .await
        .expect("Failed to prime delivery-fee cache");

    firebase::initialize(
        config.firebase_project_id.as_deref(),
        config.firebase_service_account_json.as_deref(),
        config.firebase_service_account_path.as_deref(),
    )
    .await;

    // Create bsv-sdk wallet from server private key
    let sdk_private_key = match PrivateKey::from_hex(&config.server_private_key) {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Failed to parse SERVER_PRIVATE_KEY: {e}");
            std::process::exit(1);
        }
    };
    let sdk_wallet = Arc::new(SdkProtoWallet::new(sdk_private_key));

    // Log server identity key via bsv-sdk
    let identity_key = {
        use bsv::wallet::interfaces::{GetPublicKeyArgs, WalletInterface};
        match sdk_wallet.get_public_key(
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
        ).await {
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

    // Set up Socket.IO for WebSocket live message push
    let (sio_layer, io) = socketioxide::SocketIo::new_layer();
    let ws_broadcast = ws::WsBroadcast::new(io.clone(), config.server_private_key.clone(), pool.clone());
    ws::setup_handlers(&io, ws_broadcast.clone());
    tracing::info!("Socket.IO WebSocket server ready");

    // Set up BRC-31 auth middleware (bsv-sdk Peer — compatible signatures)
    //
    // The bsv-sdk ProtoWallet doesn't implement Clone, but Peer<W> requires
    // W: Clone. Wrap in Arc for shared ownership (same pattern as cosigner).
    let transport = Arc::new(ActixTransport::new());
    let auth_wallet = {
        let pk = PrivateKey::from_hex(&config.server_private_key).unwrap();
        cloneable_wallet::CloneableProtoWallet(Arc::new(SdkProtoWallet::new(pk)))
    };
    let peer = Peer::new(auth_wallet.clone(), transport.clone());
    let peer = Arc::new(tokio::sync::Mutex::new(peer));

    let auth_config = bsv_auth_axum_middleware::AuthMiddlewareConfigBuilder::new()
        .wallet(auth_wallet.clone())
        .allow_unauthenticated(false)
        .build()
        .expect("auth middleware config");

    let auth_layer = bsv_auth_axum_middleware::AuthLayer::from_config(
        auth_config,
        peer,
        transport,
    )
    .await;

    let app_state = handlers::helpers::AppState {
        db: pool,
        config: Arc::new(config),
        wallet: sdk_wallet,
        ws: ws_broadcast,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers(Any)
        .allow_methods(Any)
        .expose_headers(Any);

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

    // Unauthenticated health endpoint — always at `/`, never under the
    // routing prefix, never behind BRC-31 auth. Returns plain text.
    let health_routes = Router::new().route(
        "/",
        get(|| async {
            (
                [(axum::http::header::CONTENT_TYPE, "text/plain")],
                "BSV MessageBox Server",
            )
        }),
    );

    // Protected API routes — BRC-31 auth via bsv-sdk Peer middleware
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
        .route(
            "/registerDevice",
            post(handlers::devices::register_device),
        )
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
        .route(
            "/permissions/quote",
            get(handlers::permissions::get_quote),
        )
        .layer(auth_layer)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(timeout_secs),
        ))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(app_state);

    let app = if prefix.is_empty() {
        Router::new().merge(health_routes).merge(api_routes)
    } else {
        Router::new().merge(health_routes).nest(&prefix, api_routes)
    }
    .layer(sio_layer)
    .layer(cors);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("MessageBox server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind TCP listener");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
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
