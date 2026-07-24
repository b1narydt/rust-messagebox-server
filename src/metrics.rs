//! Prometheus metrics — the Phase-3 observability floor
//! (`mbs-enterprise-production-design` §Phase 3, Layer 1).
//!
//! Hand-rolled text exposition (format 0.0.4) over atomics — no new
//! dependency, no global registry magic. Two kinds of sources:
//!
//! * **Histograms** (`static` [`LazyLock`]s below) are observed inline on the
//!   hot paths: broadcast fan-out + sign latency in `ws::WsBroadcast`, Redis
//!   pub/sub lag in the Model B delivery task.
//! * **Counters/gauges** are *sampled at scrape time* from the live atomics
//!   that already exist for tests/ops ([`crate::persist::PersistStats`], the
//!   [`crate::backplane::Backplane`] counters, socket counts) — so there is
//!   exactly one source of truth per number and no double bookkeeping.
//!
//! The `GET /metrics` endpoint (wired in `main.rs`) is **unauthenticated**,
//! like `GET /`: it exposes operational counts only (no identities, no
//! message data, no key material). Bind it to a scrape network in production.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

/// Fixed-bucket histogram: lock-free `observe`, cumulative buckets computed at
/// render time. Sum is accumulated in micro-units (value × 1e6) so it stays an
/// atomic integer; rendered back as a float.
pub struct Histogram {
    /// Upper bounds (ascending). The implicit `+Inf` bucket is `count`.
    buckets: &'static [f64],
    /// Non-cumulative per-bucket hit counts (index-aligned with `buckets`).
    counts: Vec<AtomicU64>,
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn new(buckets: &'static [f64]) -> Self {
        Self {
            buckets,
            counts: buckets.iter().map(|_| AtomicU64::new(0)).collect(),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one observation.
    pub fn observe(&self, v: f64) {
        if let Some(i) = self.buckets.iter().position(|b| v <= *b) {
            self.counts[i].fetch_add(1, Ordering::Relaxed);
        }
        // Negative values can't occur for durations/counts; clamp defensively.
        let micros = (v.max(0.0) * 1e6) as u64;
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Total number of observations.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn render_into(&self, name: &str, help: &str, out: &mut String) {
        use std::fmt::Write;
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0u64;
        for (i, bound) in self.buckets.iter().enumerate() {
            cumulative += self.counts[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(out, "{name}_sum {sum}");
        let _ = writeln!(out, "{name}_count {count}");
    }
}

/// Members a signed broadcast was delivered to (local leg).
pub static BROADCAST_FANOUT: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(&[0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0]));

/// Duration of the signed local-delivery leg (per-member BRC-103 signs run
/// concurrently inside it) — the "fan-out + sign latency" SLO input.
pub static BROADCAST_SIGN_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(&[
        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ])
});

/// Model B: Redis pub/sub round-trip lag, measured on **own-origin** envelopes
/// (publish → Redis → subscribe on the same instance — one clock, no skew).
/// Only publishing instances report it; that is every instance under load.
pub static BACKPLANE_LAG_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(&[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ])
});

// ---------------------------------------------------------------------------
// Scrape-time snapshot + render
// ---------------------------------------------------------------------------

/// Model B counters sampled from the [`crate::backplane::Backplane`].
pub struct BackplaneSnapshot {
    pub published: u64,
    pub dropped: u64,
    pub subscribed: bool,
}

/// Admission/drain state sampled from [`crate::ops::OpsState`] (Phase-3 D3).
pub struct OpsSnapshot {
    pub draining: bool,
    pub in_flight_sends: u64,
    pub admission_rejected: u64,
    /// `0` = unlimited.
    pub max_connections: u64,
}

/// Everything the scrape handler samples at request time. One source of truth
/// per number: the live atomics the server already maintains.
pub struct Snapshot<'a> {
    /// Connected Socket.IO sockets on this instance.
    pub connections: usize,
    /// Distinct verified BRC-103 identities among those sockets. Every room is
    /// `{identityKey}-{messageBox}` and only its owner may join it (own-room
    /// enforcement), so this is the room-owner count — a lower bound on joined
    /// rooms (authsocket 0.1.0 exposes no room enumeration; a client with N
    /// boxes counts once).
    pub authenticated_identities: usize,
    pub persist_queue_depth: usize,
    pub persist_queue_capacity: usize,
    pub persist: &'a crate::persist::PersistStats,
    /// `None` in Model A — the backplane families are omitted entirely.
    pub backplane: Option<BackplaneSnapshot>,
    pub ops: Option<OpsSnapshot>,
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

/// Render the full Prometheus text page.
pub fn render(s: &Snapshot) -> String {
    let mut out = String::with_capacity(4096);

    // Connections / rooms.
    gauge(
        &mut out,
        "mbs_ws_connections",
        "Connected Socket.IO sockets on this instance.",
        s.connections as u64,
    );
    gauge(
        &mut out,
        "mbs_rooms",
        "Distinct authenticated identities connected (room owners; lower bound on joined rooms).",
        s.authenticated_identities as u64,
    );

    // Broadcast path histograms.
    BROADCAST_FANOUT.render_into(
        "mbs_broadcast_fanout",
        "Local room members a signed broadcast was delivered to.",
        &mut out,
    );
    BROADCAST_SIGN_SECONDS.render_into(
        "mbs_broadcast_sign_seconds",
        "Duration of the signed local-delivery leg (concurrent per-member signs + emits).",
        &mut out,
    );

    // Persist pipeline.
    gauge(
        &mut out,
        "mbs_persist_queue_depth",
        "Jobs currently waiting in the bounded persist queue.",
        s.persist_queue_depth as u64,
    );
    gauge(
        &mut out,
        "mbs_persist_queue_capacity",
        "Configured persist queue capacity.",
        s.persist_queue_capacity as u64,
    );
    let p = s.persist;
    counter(
        &mut out,
        "mbs_persist_enqueued_total",
        "Jobs handed to the background persist worker (fast path).",
        p.enqueued.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "mbs_persist_completed_total",
        "Queued jobs the worker finished (stored, duplicate, or dead-lettered).",
        p.completed.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "mbs_persist_inline_fallback_total",
        "Times the fast path was bypassed for an inline write (queue full/closed).",
        p.inline_persists.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "mbs_persist_dead_letter_total",
        "Jobs dead-lettered to disk (permanent error or transient exhaustion).",
        p.dead_lettered.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "mbs_persist_worker_panics_total",
        "Supervised persist-worker restarts after a panic.",
        p.worker_panics.load(Ordering::Relaxed),
    );

    // Model B backplane (omitted entirely in Model A).
    if let Some(bp) = &s.backplane {
        counter(
            &mut out,
            "mbs_backplane_published_total",
            "Envelopes successfully PUBLISHed to Redis.",
            bp.published,
        );
        counter(
            &mut out,
            "mbs_backplane_dropped_total",
            "Envelopes dropped instead of published (Redis down / queue full).",
            bp.dropped,
        );
        gauge(
            &mut out,
            "mbs_backplane_subscribed",
            "1 while this instance holds a live subscription to the backplane channel.",
            u64::from(bp.subscribed),
        );
        BACKPLANE_LAG_SECONDS.render_into(
            "mbs_backplane_lag_seconds",
            "Redis pub/sub round-trip lag observed on own-origin envelopes (same clock).",
            &mut out,
        );
    }

    // Admission / drain (Phase-3 D3).
    if let Some(ops) = &s.ops {
        gauge(
            &mut out,
            "mbs_draining",
            "1 while the instance is draining (SIGTERM received; admission stopped).",
            u64::from(ops.draining),
        );
        gauge(
            &mut out,
            "mbs_in_flight_sends",
            "Send operations (HTTP + WS) currently executing on this instance.",
            ops.in_flight_sends,
        );
        counter(
            &mut out,
            "mbs_admission_rejected_total",
            "NEW WS connections rejected by admission control (ceiling or draining).",
            ops.admission_rejected,
        );
        gauge(
            &mut out,
            "mbs_max_connections",
            "Configured per-instance connection ceiling (0 = unlimited).",
            ops.max_connections,
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative_with_inf_sum_count() {
        static BUCKETS: [f64; 3] = [1.0, 5.0, 10.0];
        let h = Histogram::new(&BUCKETS);
        h.observe(0.5); // le=1
        h.observe(3.0); // le=5
        h.observe(7.0); // le=10
        h.observe(100.0); // +Inf only

        let mut out = String::new();
        h.render_into("t", "help", &mut out);
        assert!(out.contains("# TYPE t histogram"));
        assert!(out.contains("t_bucket{le=\"1\"} 1"));
        assert!(out.contains("t_bucket{le=\"5\"} 2"));
        assert!(out.contains("t_bucket{le=\"10\"} 3"));
        assert!(out.contains("t_bucket{le=\"+Inf\"} 4"));
        assert!(out.contains("t_count 4"));
        // Sum = 0.5 + 3 + 7 + 100 = 110.5
        assert!(out.contains("t_sum 110.5"), "sum line missing: {out}");
    }

    #[test]
    fn render_exposes_all_core_families() {
        let persist = crate::persist::PersistStats::default();
        persist.dead_lettered.store(3, Ordering::Relaxed);
        persist.inline_persists.store(2, Ordering::Relaxed);
        persist.enqueued.store(10, Ordering::Relaxed);
        persist.completed.store(9, Ordering::Relaxed);

        let page = render(&Snapshot {
            connections: 7,
            authenticated_identities: 4,
            persist_queue_depth: 1,
            persist_queue_capacity: 4096,
            persist: &persist,
            backplane: Some(BackplaneSnapshot {
                published: 100,
                dropped: 5,
                subscribed: true,
            }),
            ops: Some(OpsSnapshot {
                draining: false,
                in_flight_sends: 2,
                admission_rejected: 1,
                max_connections: 500,
            }),
        });

        for family in [
            "mbs_ws_connections 7",
            "mbs_rooms 4",
            "mbs_broadcast_fanout_count",
            "mbs_broadcast_sign_seconds_count",
            "mbs_persist_queue_depth 1",
            "mbs_persist_queue_capacity 4096",
            "mbs_persist_enqueued_total 10",
            "mbs_persist_completed_total 9",
            "mbs_persist_inline_fallback_total 2",
            "mbs_persist_dead_letter_total 3",
            "mbs_persist_worker_panics_total 0",
            "mbs_backplane_published_total 100",
            "mbs_backplane_dropped_total 5",
            "mbs_backplane_subscribed 1",
            "mbs_backplane_lag_seconds_count",
            "mbs_draining 0",
            "mbs_in_flight_sends 2",
            "mbs_admission_rejected_total 1",
            "mbs_max_connections 500",
        ] {
            assert!(page.contains(family), "missing {family} in:\n{page}");
        }
    }

    #[test]
    fn model_a_omits_backplane_families() {
        let persist = crate::persist::PersistStats::default();
        let page = render(&Snapshot {
            connections: 0,
            authenticated_identities: 0,
            persist_queue_depth: 0,
            persist_queue_capacity: 4096,
            persist: &persist,
            backplane: None,
            ops: None,
        });
        assert!(!page.contains("mbs_backplane_"));
        assert!(!page.contains("mbs_draining"));
    }
}
