//! Operational floor — admission control, graceful drain, structured health
//! (`mbs-enterprise-production-design` Phase 3, decision D3).
//!
//! ## Custody-aware backpressure (D3)
//!
//! The one rule: **never nack an in-flight connected session's sends.** A
//! ceremony in progress must be able to finish on the connection it started
//! on. So admission control gates exactly one thing — **NEW** WebSocket
//! connections (the engine.io handshake, the only request without a session
//! id). Everything else is always admitted:
//!
//! * engine.io requests carrying `sid=` (polling frames / websocket upgrade
//!   of an already-admitted session),
//! * every HTTP API route (an authenticated client's `/sendMessage` is a
//!   send from an existing session — capacity pressure never rejects it),
//! * the ops endpoints (`/`, `/metrics`, `/health/*`).
//!
//! Past the per-instance ceiling ([`crate::config::Config::max_connections`])
//! a new handshake gets **503 + `Retry-After`** — a *clear, retryable* signal.
//! In Model B the LB interprets it and sheds the client to another instance;
//! in Model A the client backs off and retries (or falls back to the durable
//! HTTP mailbox, which is never gated). Nothing is silently dropped.
//!
//! ## Graceful drain (SIGTERM → zero message loss)
//!
//! [`drain`] runs the Phase-3 HA sequence: stop admission (the draining flag
//! also flips readiness → the LB deregisters), let in-flight sends finish
//! (bounded), disconnect the remaining sockets (clients re-handshake against
//! another instance — the mailbox covers the gap), flush the persist queue
//! (bounded), then return so `axum::serve`'s graceful shutdown completes.
//! Durability is MySQL-only, so "queue flushed" ⇒ every accepted message is
//! either durably stored or dead-lettered to disk — zero silent loss.
//!
//! ## Structured health
//!
//! * **Liveness** (`/health/live`): the event loop answered — always 200.
//! * **Readiness** (`/health/ready`): 503 unless the DB answers a probe
//!   within [`DB_PROBE_TIMEOUT`], the Model B backplane holds a live Redis
//!   subscription (Model A skips the check), and the instance is not
//!   draining. Readiness gates *traffic routing*, not process restarts.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use socketioxide::SocketIo;
use tracing::{info, warn};

use crate::backplane::Backplane;
use crate::db::DbPool;
use crate::ws::WsBroadcast;

/// `Retry-After` value (seconds) sent with an admission rejection.
pub const RETRY_AFTER_SECS: u64 = 5;

/// Upper bound on the readiness DB probe (`SELECT 1`). A DB that cannot
/// answer within this is unreachable for readiness purposes.
const DB_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Poll interval for [`OpsState::wait_send_idle`].
const IDLE_POLL: Duration = Duration::from_millis(25);

// ---------------------------------------------------------------------------
// Ops state
// ---------------------------------------------------------------------------

/// Per-instance admission/drain state. One instance per process, shared by
/// the admission middleware, both send paths, the health routes, `/metrics`,
/// and the shutdown path.
pub struct OpsState {
    /// Connection ceiling (`MAX_CONNECTIONS`); `0` = unlimited.
    max_connections: usize,
    draining: AtomicBool,
    in_flight_sends: AtomicU64,
    admission_rejected: AtomicU64,
}

impl OpsState {
    pub fn new(max_connections: usize) -> Arc<Self> {
        Arc::new(Self {
            max_connections,
            draining: AtomicBool::new(false),
            in_flight_sends: AtomicU64::new(0),
            admission_rejected: AtomicU64::new(0),
        })
    }

    /// Mark a send (HTTP or WS) as in flight for the guard's lifetime. Held
    /// across the full handler so [`drain`] can wait for real work — not just
    /// open sockets — to finish.
    pub fn begin_send(self: &Arc<Self>) -> SendGuard {
        self.in_flight_sends.fetch_add(1, Ordering::AcqRel);
        SendGuard {
            ops: Arc::clone(self),
        }
    }

    /// Send operations currently executing on this instance.
    pub fn in_flight_sends(&self) -> u64 {
        self.in_flight_sends.load(Ordering::Acquire)
    }

    /// Flip to draining: admission stops and readiness reports unready. One
    /// way — an instance never un-drains (it exits).
    pub fn start_drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// NEW connections rejected by admission control (ceiling or draining).
    pub fn admission_rejected(&self) -> u64 {
        self.admission_rejected.load(Ordering::Relaxed)
    }

    /// Configured ceiling (`0` = unlimited).
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Should a NEW connection be admitted at `current_connections`?
    fn admission(&self, current_connections: usize) -> AdmissionDecision {
        if self.is_draining() {
            return AdmissionDecision::RejectDraining;
        }
        if self.max_connections > 0 && current_connections >= self.max_connections {
            return AdmissionDecision::RejectAtCapacity {
                current: current_connections,
                max: self.max_connections,
            };
        }
        AdmissionDecision::Admit
    }

    /// Bounded wait until zero sends are in flight. `true` = quiesced;
    /// `false` = `timeout` elapsed first (the caller proceeds anyway — the
    /// drain is bounded, and the persist flush still covers durability).
    pub async fn wait_send_idle(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.in_flight_sends() == 0 {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(IDLE_POLL).await;
        }
    }
}

/// RAII marker for one in-flight send. Dropping it (on any exit path,
/// including panics unwinding through the handler) decrements the counter.
pub struct SendGuard {
    ops: Arc<OpsState>,
}

impl Drop for SendGuard {
    fn drop(&mut self) {
        self.ops.in_flight_sends.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------
// Admission control (D3)
// ---------------------------------------------------------------------------

/// Outcome of the admission check for a NEW connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    Admit,
    /// At/over the per-instance ceiling.
    RejectAtCapacity {
        current: usize,
        max: usize,
    },
    /// Instance is draining (SIGTERM received).
    RejectDraining,
}

/// Is this request a **new** WebSocket handshake — the only thing admission
/// control gates? engine.io routes every request of an *established* session
/// with a `sid` query parameter (long-polling frames and the websocket
/// upgrade both carry it); only the initial handshake has none.
fn is_new_ws_handshake(path: &str, query: Option<&str>) -> bool {
    if !path.starts_with("/socket.io") {
        return false;
    }
    !query.is_some_and(|q| q.split('&').any(|p| p.starts_with("sid=")))
}

/// Admission middleware body (wrap via `axum::middleware::from_fn`).
///
/// `current_connections` is sampled lazily — only new handshakes pay for the
/// socket enumeration. Everything that is not a new WS handshake passes
/// through untouched (custody rule: in-flight sessions are never gated).
pub async fn gate_admission(
    ops: &Arc<OpsState>,
    current_connections: impl FnOnce() -> usize,
    req: Request<Body>,
    next: Next,
) -> Response {
    if !is_new_ws_handshake(req.uri().path(), req.uri().query()) {
        return next.run(req).await;
    }
    match ops.admission(current_connections()) {
        AdmissionDecision::Admit => next.run(req).await,
        AdmissionDecision::RejectAtCapacity { current, max } => {
            ops.admission_rejected.fetch_add(1, Ordering::Relaxed);
            warn!(
                current,
                max, "admission: connection ceiling reached — NEW connection rejected (retryable)"
            );
            reject_response(
                "ERR_SERVER_AT_CAPACITY",
                "Server is at its connection ceiling. Retry shortly or use another instance.",
            )
        }
        AdmissionDecision::RejectDraining => {
            ops.admission_rejected.fetch_add(1, Ordering::Relaxed);
            info!("admission: draining — NEW connection rejected (retryable)");
            reject_response(
                "ERR_SERVER_DRAINING",
                "Server is draining for shutdown. Reconnect to another instance.",
            )
        }
    }
}

/// 503 + `Retry-After` + a structured error body: the *clear, retryable*
/// rejection D3 requires. In Model B the LB sheds to another instance; in
/// Model A the client backs off and retries.
fn reject_response(code: &str, description: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECS.to_string())],
        axum::Json(serde_json::json!({
            "status": "error",
            "code": code,
            "description": description,
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Structured health
// ---------------------------------------------------------------------------

/// Readiness probe result — serialized verbatim as the `/health/ready` body.
#[derive(Debug, serde::Serialize)]
pub struct ReadinessReport {
    /// `true` ⇔ 200: route traffic here.
    pub ready: bool,
    /// `"ok"` | `"unreachable"` — MySQL answered `SELECT 1` within the probe
    /// timeout. DB loss ⇒ unready in both models (durability is MySQL-only).
    pub db: &'static str,
    /// Model B: `"ok"` | `"down"` (live Redis subscription held?). Model A:
    /// `null` — no Redis in the topology, check skipped.
    pub redis: Option<&'static str>,
    /// Draining instances report unready so the LB deregisters them.
    pub draining: bool,
}

/// Run the readiness checks: DB reachable (both models), Redis subscription
/// live (Model B only), not draining.
pub async fn readiness(
    db: &DbPool,
    backplane: Option<&Backplane>,
    ops: &OpsState,
) -> ReadinessReport {
    let db_ok = matches!(
        tokio::time::timeout(DB_PROBE_TIMEOUT, sqlx::query("SELECT 1").execute(db)).await,
        Ok(Ok(_))
    );
    let redis = backplane.map(|bp| if bp.is_subscribed() { "ok" } else { "down" });
    let draining = ops.is_draining();
    ReadinessReport {
        ready: db_ok && redis != Some("down") && !draining,
        db: if db_ok { "ok" } else { "unreachable" },
        redis,
        draining,
    }
}

impl IntoResponse for ReadinessReport {
    fn into_response(self) -> Response {
        let status = if self.ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        (status, axum::Json(self)).into_response()
    }
}

// ---------------------------------------------------------------------------
// Graceful drain
// ---------------------------------------------------------------------------

/// The SIGTERM sequence (Phase-3 HA): runs *inside* `axum::serve`'s
/// graceful-shutdown future, after the signal and before axum stops
/// accepting. Bounded by `timeout` per phase — a drain never wedges an
/// instance, and nothing is dropped silently (unflushed jobs stay on the
/// worker; permanent failures are already dead-lettered to disk).
pub async fn drain(ops: &Arc<OpsState>, ws: &WsBroadcast, io: &SocketIo, timeout: Duration) {
    // 1. Stop admission. The same flag flips readiness → the LB deregisters
    //    this instance while existing sessions keep working.
    ops.start_drain();
    info!("drain: admission stopped, readiness unready (LB deregistration)");

    // 2. Let in-flight sends finish (bounded).
    let in_flight = ops.in_flight_sends();
    if ops.wait_send_idle(timeout).await {
        info!(
            in_flight_at_signal = in_flight,
            "drain: in-flight sends quiesced"
        );
    } else {
        warn!(
            still_in_flight = ops.in_flight_sends(),
            timeout_secs = timeout.as_secs(),
            "drain: in-flight sends did not quiesce in time — proceeding (persist flush still runs)"
        );
    }

    // 3. Disconnect remaining sockets: no further sends originate here, and
    //    axum's connection-drain can complete. Clients re-handshake against
    //    another instance (Model B) or retry (Model A); the MySQL mailbox
    //    covers anything sent meanwhile.
    let sockets = io.sockets().unwrap_or_default();
    let disconnected = sockets.len();
    for socket in sockets {
        if let Err(e) = socket.disconnect() {
            warn!(error = %e, "drain: socket disconnect failed (already gone?)");
        }
    }
    info!(disconnected, "drain: live sockets disconnected");

    // 4. Flush the persist queue (bounded): every accepted message durable in
    //    MySQL, confirmed duplicate, or dead-lettered to disk — zero silent
    //    loss even across the restart.
    if ws.flush_persist(timeout).await {
        info!("drain: persist queue flushed — all accepted messages durable");
    } else {
        // flush_persist already logged the un-durable count at WARN.
        warn!("drain: persist queue flush timed out — worker keeps draining until exit");
    }

    info!("drain: complete — exiting");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::any;
    use axum::Router;
    use tower::ServiceExt;

    // ---- admission decision matrix (pure) ----

    #[test]
    fn admission_matrix() {
        let ops = OpsState::new(5);
        assert_eq!(ops.admission(0), AdmissionDecision::Admit);
        assert_eq!(ops.admission(4), AdmissionDecision::Admit);
        assert_eq!(
            ops.admission(5),
            AdmissionDecision::RejectAtCapacity { current: 5, max: 5 }
        );
        assert_eq!(
            ops.admission(9),
            AdmissionDecision::RejectAtCapacity { current: 9, max: 5 }
        );

        // 0 = unlimited: never capacity-rejected.
        let unlimited = OpsState::new(0);
        assert_eq!(unlimited.admission(1_000_000), AdmissionDecision::Admit);

        // Draining rejects regardless of load — and wins over capacity.
        unlimited.start_drain();
        assert_eq!(unlimited.admission(0), AdmissionDecision::RejectDraining);
        let full = OpsState::new(1);
        full.start_drain();
        assert_eq!(full.admission(5), AdmissionDecision::RejectDraining);
    }

    #[test]
    fn handshake_detection() {
        // New handshakes (no sid): gated.
        assert!(is_new_ws_handshake("/socket.io/", None));
        assert!(is_new_ws_handshake(
            "/socket.io/",
            Some("EIO=4&transport=polling")
        ));
        assert!(is_new_ws_handshake(
            "/socket.io/",
            Some("EIO=4&transport=websocket")
        ));
        // Established sessions (sid present): never gated.
        assert!(!is_new_ws_handshake(
            "/socket.io/",
            Some("EIO=4&transport=polling&sid=abc123")
        ));
        assert!(!is_new_ws_handshake(
            "/socket.io/",
            Some("sid=abc123&EIO=4&transport=websocket")
        ));
        // `sid=` must be a parameter, not a substring of another value.
        assert!(is_new_ws_handshake(
            "/socket.io/",
            Some("EIO=4&transport=polling&x=sid=notasid")
        ));
        // Non-socket.io paths: never gated.
        assert!(!is_new_ws_handshake("/sendMessage", None));
        assert!(!is_new_ws_handshake("/health/ready", None));
        assert!(!is_new_ws_handshake("/", None));
    }

    // ---- send guards + idle wait ----

    #[tokio::test]
    async fn send_guard_counts_and_idle_wait_is_immediate_when_idle() {
        let ops = OpsState::new(0);
        assert_eq!(ops.in_flight_sends(), 0);
        {
            let _a = ops.begin_send();
            let _b = ops.begin_send();
            assert_eq!(ops.in_flight_sends(), 2);
        }
        assert_eq!(ops.in_flight_sends(), 0);
        assert!(ops.wait_send_idle(Duration::from_millis(1)).await);
    }

    #[tokio::test]
    async fn wait_send_idle_waits_for_guard_release() {
        let ops = OpsState::new(0);
        let guard = ops.begin_send();
        let held_for = Duration::from_millis(150);
        tokio::spawn(async move {
            tokio::time::sleep(held_for).await;
            drop(guard);
        });
        let start = tokio::time::Instant::now();
        assert!(
            ops.wait_send_idle(Duration::from_secs(10)).await,
            "must quiesce once the guard drops"
        );
        assert!(
            start.elapsed() >= held_for,
            "must actually wait for the in-flight send"
        );
    }

    #[tokio::test]
    async fn wait_send_idle_times_out_bounded() {
        let ops = OpsState::new(0);
        let _held = ops.begin_send();
        let start = tokio::time::Instant::now();
        assert!(!ops.wait_send_idle(Duration::from_millis(100)).await);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "bounded, not stuck"
        );
    }

    // ---- middleware behavior over a real router ----

    fn gated_app(ops: Arc<OpsState>, current: usize) -> Router {
        let app_ops = ops;
        Router::new()
            .fallback(any(|| async { "ok" }))
            .layer(axum::middleware::from_fn(
                move |req: Request<Body>, next: Next| {
                    let ops = Arc::clone(&app_ops);
                    async move { gate_admission(&ops, move || current, req, next).await }
                },
            ))
    }

    async fn get(app: &Router, uri: &str) -> (StatusCode, Option<String>, String) {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = res.status();
        let retry_after = res
            .headers()
            .get(header::RETRY_AFTER)
            .map(|v| v.to_str().expect("ascii").to_string());
        let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
            .await
            .expect("body");
        (status, retry_after, String::from_utf8_lossy(&body).into())
    }

    #[tokio::test]
    async fn at_ceiling_new_handshake_rejected_clearly_and_counted() {
        let ops = OpsState::new(2);
        let app = gated_app(Arc::clone(&ops), 2); // at the ceiling

        let (status, retry_after, body) = get(&app, "/socket.io/?EIO=4&transport=polling").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(retry_after.as_deref(), Some("5"), "retryable signal");
        assert!(
            body.contains("ERR_SERVER_AT_CAPACITY"),
            "clear code: {body}"
        );
        assert_eq!(ops.admission_rejected(), 1);
    }

    #[tokio::test]
    async fn at_ceiling_established_session_and_api_are_never_gated() {
        let ops = OpsState::new(2);
        let app = gated_app(Arc::clone(&ops), 99); // way past the ceiling

        // Established engine.io session (sid): the in-flight connection's own
        // transport — always admitted.
        let (status, _, _) = get(&app, "/socket.io/?EIO=4&transport=websocket&sid=abc123").await;
        assert_eq!(status, StatusCode::OK);

        // HTTP API send from a connected client: never nacked by capacity.
        let (status, _, _) = get(&app, "/sendMessage").await;
        assert_eq!(status, StatusCode::OK);

        // Ops surface stays reachable.
        let (status, _, _) = get(&app, "/health/ready").await;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(ops.admission_rejected(), 0);
    }

    #[tokio::test]
    async fn below_ceiling_new_handshake_admitted() {
        let ops = OpsState::new(2);
        let app = gated_app(Arc::clone(&ops), 1);
        let (status, _, _) = get(&app, "/socket.io/?EIO=4&transport=polling").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ops.admission_rejected(), 0);
    }

    #[tokio::test]
    async fn draining_rejects_new_handshake_only() {
        let ops = OpsState::new(0); // unlimited — draining alone must reject
        ops.start_drain();
        let app = gated_app(Arc::clone(&ops), 0);

        let (status, retry_after, body) = get(&app, "/socket.io/?EIO=4&transport=polling").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(retry_after.as_deref(), Some("5"));
        assert!(body.contains("ERR_SERVER_DRAINING"), "clear code: {body}");

        // Existing sessions keep flowing during drain.
        let (status, _, _) = get(&app, "/socket.io/?EIO=4&transport=polling&sid=abc").await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, _) = get(&app, "/listMessages").await;
        assert_eq!(status, StatusCode::OK);
    }

    // ---- readiness (DB-backed cases live in ops_db tests below) ----

    #[tokio::test]
    async fn readiness_unready_on_unreachable_db() {
        // Lazy pool against a dead port: the probe fails fast (refused).
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy("mysql://nobody@127.0.0.1:1/none")
            .expect("lazy pool");
        let ops = OpsState::new(0);
        let report = readiness(&pool, None, &ops).await;
        assert!(!report.ready);
        assert_eq!(report.db, "unreachable");
        assert_eq!(report.redis, None, "Model A: redis check skipped");
        assert!(!report.draining);
    }

    #[test]
    fn readiness_report_serializes_structured() {
        let report = ReadinessReport {
            ready: false,
            db: "ok",
            redis: Some("down"),
            draining: false,
        };
        let v = serde_json::to_value(&report).expect("serialize");
        assert_eq!(v["ready"], false);
        assert_eq!(v["db"], "ok");
        assert_eq!(v["redis"], "down");
        assert_eq!(v["draining"], false);
    }
}

/// DB-backed ops tests (Docker MySQL via the shared testcontainer harness,
/// like the db/handler suites): readiness flips on real dependency loss, and
/// the full drain sequence preserves every accepted message.
#[cfg(test)]
mod ops_db_tests {
    use super::*;
    use crate::test_support;

    /// secp256k1 scalar `1` — valid server key for WsBroadcast.
    const TEST_SERVER_KEY: &str =
        "0000000000000000000000000000000000000000000000000000000000000001";

    /// Readiness flips ready→unready when the DB goes away (the "flips on
    /// dependency loss" Phase-3 test).
    #[tokio::test]
    async fn readiness_flips_on_db_loss() {
        let pool = test_support::fresh_pool().await;
        let ops = OpsState::new(0);

        let report = readiness(&pool, None, &ops).await;
        assert!(report.ready, "live DB ⇒ ready: {report:?}");
        assert_eq!(report.db, "ok");

        // Dependency loss: the pool closes (DB unreachable from this process).
        pool.close().await;
        let report = readiness(&pool, None, &ops).await;
        assert!(!report.ready, "DB loss must flip readiness");
        assert_eq!(report.db, "unreachable");
    }

    /// Model B: a dead Redis makes the instance unready even with a healthy
    /// DB (spec: readiness = DB reachable AND Redis reachable in Model B).
    #[tokio::test]
    async fn readiness_unready_on_redis_loss_in_model_b() {
        let pool = test_support::fresh_pool().await;
        let ops = OpsState::new(0);

        // Port 1: nothing listens; the backplane never subscribes.
        let bp = Backplane::new("redis://127.0.0.1:1/");
        let report = readiness(&pool, Some(&bp), &ops).await;
        assert!(!report.ready, "Model B + Redis down ⇒ unready");
        assert_eq!(report.db, "ok", "the DB is fine — Redis is the failure");
        assert_eq!(report.redis, Some("down"));

        // Same DB in Model A (no backplane): ready — Redis is not part of
        // the Model A topology.
        let report = readiness(&pool, None, &ops).await;
        assert!(report.ready);
        assert_eq!(report.redis, None);
    }

    /// Draining flips readiness (the LB-deregistration signal).
    #[tokio::test]
    async fn readiness_unready_while_draining() {
        let pool = test_support::fresh_pool().await;
        let ops = OpsState::new(0);
        assert!(readiness(&pool, None, &ops).await.ready);

        ops.start_drain();
        let report = readiness(&pool, None, &ops).await;
        assert!(!report.ready);
        assert!(report.draining);
        assert_eq!(report.db, "ok");
    }

    /// The full drain sequence: waits for the in-flight send to finish
    /// (bounded), flushes the persist queue, and every accepted message is
    /// durable in MySQL afterwards — zero message loss.
    #[tokio::test]
    async fn drain_waits_for_in_flight_and_flushes_queue_with_zero_loss() {
        let pool = test_support::fresh_pool().await;
        let (_layer, io) = socketioxide::SocketIo::new_layer();
        let ops = OpsState::new(0);
        let ws = WsBroadcast::new(
            io.clone(),
            TEST_SERVER_KEY.to_string(),
            pool.clone(),
            None,
            Arc::clone(&ops),
        );
        // Register the default namespace (as main.rs always does before
        // serving) — `SocketIo::sockets()` panics without it.
        crate::ws::setup_handlers(&io, ws.clone());

        // Accepted-but-not-yet-durable work: five messages on the persist queue.
        for i in 0..5 {
            let outcome = ws
                .persist_async(crate::persist::PersistJob::new(
                    format!("drain-msg-{i}"),
                    test_support::RECIPIENT_KEY,
                    "mpc_inbox",
                    test_support::TEST_KEY,
                    format!("payload-{i}"),
                ))
                .await;
            assert_eq!(outcome, crate::persist::Enqueued::Queued);
        }

        // One send still executing when SIGTERM lands.
        let guard = ops.begin_send();
        let held_for = Duration::from_millis(150);
        tokio::spawn(async move {
            tokio::time::sleep(held_for).await;
            drop(guard);
        });

        let start = tokio::time::Instant::now();
        drain(&ops, &ws, &io, Duration::from_secs(20)).await;

        assert!(
            start.elapsed() >= held_for,
            "drain must wait for the in-flight send"
        );
        assert!(ops.is_draining(), "drain leaves the instance draining");
        assert_eq!(ops.in_flight_sends(), 0, "in-flight completed, not nacked");

        // Queue flushed…
        let stats = ws.persist_stats();
        let enqueued = stats.enqueued.load(std::sync::atomic::Ordering::Acquire);
        let completed = stats.completed.load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(enqueued, 5);
        assert!(completed >= enqueued, "flush means completed >= enqueued");
        assert_eq!(
            stats
                .dead_lettered
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // …and every accepted message is durable in MySQL: zero loss.
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM messages WHERE messageId LIKE 'drain-msg-%'")
                .fetch_one(&pool)
                .await
                .expect("count persisted rows");
        assert_eq!(count, 5, "zero message loss across drain");
    }
}
