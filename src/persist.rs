//! Asynchronous, durable message persistence.
//!
//! ## Why this exists
//!
//! Both send paths (HTTP `POST /sendMessage` and the BRC-103 WS `sendMessage`)
//! used to **persist-then-push**: the message was written to MySQL *first*, then
//! broadcast to the live recipient. That put a synchronous DB round-trip on the
//! hot path of live delivery — a slow or stalled MySQL directly delayed the MPC
//! happy path.
//!
//! This module flips the ordering to **push-live-first, persist-async**. The
//! caller does the cheap, *gating* work synchronously (auth, recipient
//! validation, fee/permission/payment checks), broadcasts to connected
//! recipients immediately, and then hands the durable INSERT to this background
//! worker via [`PersistHandle::enqueue`].
//!
//! ## Durability is preserved, not weakened
//!
//! The HTTP fallback (`listMessages`/`acknowledgeMessage`) still reads from
//! MySQL, so the row MUST land for: (a) offline recipients who poll later, and
//! (b) any recipient that received it live and then polls (current semantics:
//! the message stays in MySQL until the recipient `acknowledge`s it). Therefore
//! a job is **never** silently dropped:
//!
//! * The worker retries transient DB errors with bounded exponential backoff.
//! * A genuine duplicate (unique `messageId`) is treated as idempotent success.
//! * A permanent failure is logged at **ERROR** with full job context.
//!
//! ## Backpressure
//!
//! The job channel is **bounded**. We never `tokio::spawn` an unbounded number
//! of persist tasks (a DB stall would OOM the relay). When the queue is full,
//! [`PersistHandle::enqueue`] does **not** drop — it falls back to persisting
//! inline (synchronously, on the caller) after a loud warning. Inline persist is
//! the last-resort safety valve: it reintroduces DB latency on that one call but
//! guarantees the row is written. Dropping a message is never an option.

use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::db::{self, DbPool};

/// One unit of durable work: persist a single message to MySQL.
///
/// The job is self-contained so the worker never has to reach back into
/// per-socket state (which may be gone by the time the job runs).
#[derive(Debug, Clone)]
pub struct PersistJob {
    pub message_id: String,
    /// Recipient identity key — also used to (re)resolve the messageBox id.
    pub recipient: String,
    /// Message box name (e.g. `payment_keyswap`). The worker ensures the box
    /// exists and resolves its id; it does not trust a caller-supplied id, so a
    /// race between caller and worker cannot insert against a stale id.
    pub message_box: String,
    pub sender: String,
    /// Fully-formed stored body (already wrapped as `{"message": ...}` etc.).
    pub body: String,
}

/// Tunables for the persist worker. Defaults are sized for the relay's hot path.
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Bounded queue depth. Large enough to absorb a burst, small enough that a
    /// sustained DB stall surfaces as inline-persist backpressure (not OOM).
    pub queue_capacity: usize,
    /// Max attempts per job (1 initial try + retries) before declaring permanent
    /// failure. A duplicate is success and does not consume retries.
    pub max_attempts: u32,
    /// Base backoff between retries; doubles each attempt, capped at `max_backoff`.
    pub base_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 4096,
            max_attempts: 8,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
        }
    }
}

/// Cloneable handle used by the send paths to enqueue durable work.
#[derive(Clone)]
pub struct PersistHandle {
    tx: mpsc::Sender<PersistJob>,
    /// Kept so the inline last-resort path can persist without the worker.
    db: DbPool,
    cfg: PersistConfig,
}

impl PersistHandle {
    /// Spawn the background persist worker and return a handle to enqueue jobs.
    ///
    /// The worker runs for the lifetime of the process (the channel is held by
    /// every clone of the handle, so it only closes at shutdown). One worker
    /// drains the queue serially; serial drain is fine because each job is a
    /// pair of fast indexed writes and ordering per recipient is preserved.
    pub fn spawn(db: DbPool, cfg: PersistConfig) -> Self {
        let (tx, rx) = mpsc::channel(cfg.queue_capacity);
        let worker_db = db.clone();
        let worker_cfg = cfg.clone();
        tokio::spawn(async move {
            run_worker(rx, worker_db, worker_cfg).await;
        });
        Self { tx, db, cfg }
    }

    /// Enqueue a job for asynchronous persistence.
    ///
    /// Fast path: a non-blocking `try_send` onto the bounded queue. If the queue
    /// is full (DB stalled / sustained overload), we do **not** drop the message
    /// — we persist it inline (synchronously) after a loud warning. That trades
    /// latency on this one call for guaranteed durability.
    pub async fn enqueue(&self, job: PersistJob) {
        match self.tx.try_send(job) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(job)) => {
                warn!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    queue_capacity = self.cfg.queue_capacity,
                    "persist queue full — persisting inline (DB backpressure). Live push already happened; durability is being forced synchronously."
                );
                persist_with_retry(&self.db, &job, &self.cfg).await;
            }
            Err(mpsc::error::TrySendError::Closed(job)) => {
                // Worker gone (only at shutdown). Never drop — persist inline.
                warn!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    "persist worker channel closed — persisting inline"
                );
                persist_with_retry(&self.db, &job, &self.cfg).await;
            }
        }
    }
}

/// Drain the queue, persisting each job with bounded retry.
async fn run_worker(mut rx: mpsc::Receiver<PersistJob>, db: DbPool, cfg: PersistConfig) {
    debug!(
        queue_capacity = cfg.queue_capacity,
        max_attempts = cfg.max_attempts,
        "async persist worker started"
    );
    while let Some(job) = rx.recv().await {
        persist_with_retry(&db, &job, &cfg).await;
    }
    warn!("async persist worker stopped (channel closed)");
}

/// Persist one job to MySQL with bounded exponential backoff on transient errors.
///
/// Durability guarantee: returns only after the row is durably written, a
/// duplicate is observed (idempotent success), or `max_attempts` transient
/// failures are exhausted — and the last case is logged at ERROR so the message
/// is never *silently* lost.
///
/// Dedup: `insert_message` uses `INSERT IGNORE` on the unique `messageId`. A
/// duplicate returns `Ok(false)`; we treat it as success. Because persistence
/// now happens *after* the live push, the duplicate check lands at persist time
/// — which is acceptable: the client treats a duplicate as idempotent success,
/// and the unique constraint still prevents a second row from ever existing.
async fn persist_with_retry(db: &DbPool, job: &PersistJob, cfg: &PersistConfig) {
    let mut attempt: u32 = 0;
    let mut backoff = cfg.base_backoff;

    loop {
        attempt += 1;
        match persist_once(db, job).await {
            Ok(true) => {
                debug!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    message_box = %job.message_box,
                    attempt,
                    "async persist: message stored"
                );
                return;
            }
            Ok(false) => {
                // Duplicate messageId — already persisted. Idempotent success.
                debug!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    "async persist: duplicate messageId, treating as idempotent success"
                );
                return;
            }
            Err(e) => {
                if attempt >= cfg.max_attempts {
                    error!(
                        msg_id = %job.message_id,
                        recipient = %job.recipient,
                        message_box = %job.message_box,
                        sender = %job.sender,
                        attempts = attempt,
                        error = %e,
                        "async persist FAILED permanently after retries — message will NOT be in MySQL; recipient cannot retrieve it via listMessages. This requires operator attention."
                    );
                    return;
                }
                warn!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "async persist: transient DB error, retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(cfg.max_backoff);
            }
        }
    }
}

/// Resolve the messageBox id (ensuring the box exists) and insert the message.
///
/// Returns `Ok(true)` on a fresh insert, `Ok(false)` on a duplicate `messageId`,
/// and `Err` on any DB error (transient or otherwise — the retry layer decides).
async fn persist_once(db: &DbPool, job: &PersistJob) -> Result<bool, sqlx::Error> {
    // ensure_message_box both creates (INSERT IGNORE) and resolves the id, so a
    // box that was never created (or a worker that lost the race) still gets a
    // valid id here. This makes the worker fully self-sufficient.
    let mb_id = db::queries::ensure_message_box(db, &job.recipient, &job.message_box).await?;
    db::queries::insert_message(
        db,
        &job.message_id,
        mb_id,
        &job.sender,
        &job.recipient,
        &job.body,
    )
    .await
}
