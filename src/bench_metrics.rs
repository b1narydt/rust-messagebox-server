//! ============================================================================
//! BENCH-ONLY per-message timing instrumentation (MEASUREMENT BUILD).
//! ============================================================================
//!
//! Added for the colocated throughput/attribution benchmark. This module is a
//! lightweight, lock-free accumulator that splits the WS send path into three
//! stages so we can attribute per-message wall time:
//!
//!   (a) `verify`  — BRC-103 `peer.process_pending()` (signature/nonce/session)
//!   (b) `route`   — `broadcast_to_room` (signed live fan-out to room members)
//!   (c) `persist` — `persist_async` enqueue (hand-off to the background worker)
//!
//! Each stage keeps an atomic count + summed-nanoseconds + max-ns. Mean is
//! sum/count. It is exposed via the unauthenticated `GET /__bench_metrics`
//! endpoint (registered in `main.rs`) and only does work when the
//! `MB_BENCH_METRICS=1` env var is set, so it is a strict no-op in production
//! unless explicitly turned on.
//!
//! THIS IS NOT A PRODUCT CHANGE — it is a measurement scaffold. Search for
//! `BENCH-ONLY` to find every line touched. Safe to delete the module, the
//! `pub mod bench_metrics;` line, the `/__bench_metrics` route, and the four
//! `bench_metrics::*` call sites to fully remove it.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);

/// Call once at startup. Reads `MB_BENCH_METRICS` — when "1"/"true", timing is
/// recorded; otherwise every `record_*` is a single relaxed atomic load + return.
pub fn init() {
    let on = std::env::var("MB_BENCH_METRICS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    ENABLED.store(on, Ordering::Relaxed);
    if on {
        tracing::warn!("BENCH-ONLY: MB_BENCH_METRICS=1 — per-message send-path timing is ON (/__bench_metrics)");
    }
}

#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

struct Stage {
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl Stage {
    const fn new() -> Self {
        Stage {
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }
    #[inline]
    fn record(&self, ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        // Best-effort max (a benign race here only undercounts the tail slightly).
        let mut cur = self.max_ns.load(Ordering::Relaxed);
        while ns > cur {
            match self.max_ns.compare_exchange_weak(cur, ns, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
    fn snapshot(&self, name: &str) -> serde_json::Value {
        let count = self.count.load(Ordering::Relaxed);
        let sum_ns = self.sum_ns.load(Ordering::Relaxed);
        let max_ns = self.max_ns.load(Ordering::Relaxed);
        let mean_ms = if count > 0 {
            (sum_ns as f64 / count as f64) / 1_000_000.0
        } else {
            0.0
        };
        serde_json::json!({
            "stage": name,
            "count": count,
            "mean_ms": (mean_ms * 1000.0).round() / 1000.0,
            "max_ms": (max_ns as f64 / 1_000_000.0 * 1000.0).round() / 1000.0,
            "total_ms": (sum_ns as f64 / 1_000_000.0 * 100.0).round() / 100.0,
        })
    }
}

static VERIFY: Stage = Stage::new();
static ROUTE: Stage = Stage::new();
static PERSIST: Stage = Stage::new();

/// (d) messageBox existence-cache hit/miss counters. A MISS means
/// `ensure_message_box` fell through to the DB (2 round-trips: INSERT IGNORE +
/// SELECT id); a HIT is a pure in-memory resolve (0 DB round-trips). The win is
/// HITs dominating after warmup → per-message DB checkouts on the send path drop
/// toward ~1 (`insert_message` only).
static MBOX_CACHE_HIT: AtomicU64 = AtomicU64::new(0);
static MBOX_CACHE_MISS: AtomicU64 = AtomicU64::new(0);

/// Record a messageBox existence-cache lookup. Always counts when bench is on,
/// regardless of which path (HTTP or persist worker) called `ensure_message_box`.
#[inline]
pub fn record_messagebox_cache(hit: bool) {
    if enabled() {
        if hit {
            MBOX_CACHE_HIT.fetch_add(1, Ordering::Relaxed);
        } else {
            MBOX_CACHE_MISS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// (a) BRC-103 verify — `peer.process_pending()`.
#[inline]
pub fn record_verify(ns: u64) {
    if enabled() {
        VERIFY.record(ns);
    }
}

/// (b) route/broadcast — `broadcast_to_room`.
#[inline]
pub fn record_route(ns: u64) {
    if enabled() {
        ROUTE.record(ns);
    }
}

/// (c) persist enqueue — `persist_async`.
#[inline]
pub fn record_persist(ns: u64) {
    if enabled() {
        PERSIST.record(ns);
    }
}

/// JSON snapshot for the `/__bench_metrics` endpoint.
pub fn snapshot() -> serde_json::Value {
    let hits = MBOX_CACHE_HIT.load(Ordering::Relaxed);
    let misses = MBOX_CACHE_MISS.load(Ordering::Relaxed);
    let total = hits + misses;
    // DB round-trips charged by ensure_message_box: 2 per miss, 0 per hit. This
    // is the per-message-attributable saving from the existence cache.
    let ensure_db_roundtrips = misses * 2;
    serde_json::json!({
        "enabled": enabled(),
        "note": "BENCH-ONLY per-message send-path attribution. verify=BRC-103 process_pending, route=broadcast_to_room, persist=persist_async enqueue.",
        "stages": [
            VERIFY.snapshot("verify"),
            ROUTE.snapshot("route"),
            PERSIST.snapshot("persist"),
        ],
        "messagebox_cache": {
            "hits": hits,
            "misses": misses,
            "lookups": total,
            "hit_rate": if total > 0 { (hits as f64 / total as f64 * 10000.0).round() / 10000.0 } else { 0.0 },
            "ensure_message_box_db_roundtrips": ensure_db_roundtrips,
            "note": "miss = 2 DB round-trips (INSERT IGNORE + SELECT id); hit = 0. After warmup hits should dominate.",
        },
    })
}
