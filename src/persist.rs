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
//! a job is **never *silently* dropped**. The exact guarantee is:
//!
//! * The worker retries **transient** DB errors (pool timeouts, I/O,
//!   connection loss, MySQL lock-wait-timeout `1205` / deadlock `1213`) with
//!   bounded exponential backoff ([`PersistConfig::max_attempts`] attempts,
//!   doubling from `base_backoff` up to `max_backoff`).
//! * A **permanent** DB error (schema mismatch, data-too-long, any non-dup
//!   constraint violation, etc.) **fails fast** — it is *not* retried, because
//!   retrying it would only stall the single-consumer queue for ~25s before
//!   giving up anyway.
//! * A genuine duplicate (unique `messageId`, via `INSERT IGNORE`) is treated as
//!   idempotent success — it does not consume retries.
//! * On a permanent error **or** retry exhaustion on a sustained transient
//!   outage, the failed job is **dead-lettered**: appended as one JSON line to a
//!   file on disk ([`PersistConfig::dead_letter_path`], `DEAD_LETTER_PATH` env;
//!   default `dead_letter.jsonl` in the process working directory), logged at
//!   **ERROR** with full context, and counted (see [`PersistHandle::stats`]).
//!   A file (not a DB table) because the DB may be the thing that is down.
//!   [`PersistHandle::spawn`] probes the path for writability at boot and logs
//!   at ERROR if capture cannot work (e.g. a read-only working directory in a
//!   container image) so the operator learns at startup, not at the moment of
//!   data loss. `dead_lettered` counts only records actually on disk; a failed
//!   capture increments `dead_letter_failures` instead — that job is LOST.
//!   The dead-letter file is recoverable by an operator / a future startup
//!   replay (see the `replay` TODO below).
//!
//! ## Backpressure
//!
//! The job channel is **bounded** ([`PersistConfig::queue_capacity`], default
//! 4096). We never `tokio::spawn` an unbounded number of persist tasks (a DB
//! stall would OOM the relay). When the queue is full,
//! [`PersistHandle::enqueue`] does **not** drop — it falls back to persisting
//! inline (synchronously, on the caller) after a loud warning. Inline persist is
//! the last-resort safety valve: it reintroduces DB latency on that one call but
//! guarantees the row is written (or dead-lettered). [`PersistHandle::enqueue`]
//! returns an [`Enqueued`] so callers can meter how often the fast path is
//! bypassed (useful under load test). A message is never *silently* dropped:
//! backpressure forces an inline write, and permanent/exhausted failures are
//! dead-lettered to disk and logged at ERROR.
//!
//! ## Worker supervision
//!
//! The drain loop runs under a supervisor ([`run_supervised`]). If the drain
//! task panics, the supervisor logs `ERR_PERSIST_WORKER_PANIC` at ERROR and
//! **restarts** the drain over the same receiver (held in an `Arc<Mutex<_>>`),
//! so a single poisoned job cannot permanently kill persistence and silently
//! degrade every subsequent send onto the inline hot path. Graceful close (the
//! last `PersistHandle` dropped → channel closed) is distinguished from a panic
//! and ends the supervisor cleanly.
//!
//! ## TODO (follow-up): startup replay + durable sink
//!
//! Capturing failed jobs to the dead-letter file is implemented here. A clean
//! follow-up is a startup pass that reads the file, re-enqueues each line, and
//! truncates on success. Not implemented now to keep this change focused on the
//! capture path (the durability-critical half). The capture is also only as
//! durable as the disk under it: on ephemeral-filesystem deploys (Railway, most
//! containers) the file survives process crashes and restarts but NOT container
//! replacement — it narrows the loss window rather than closing it. A durable
//! sink (object storage / a second store) belongs with the replay work.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, warn, Instrument};

use crate::db::queries::InsertOutcome;
use crate::db::{self, DbPool};

/// One unit of durable work: persist a single message to MySQL.
///
/// The job is self-contained so the worker never has to reach back into
/// per-socket state (which may be gone by the time the job runs). Construct it
/// via [`PersistJob::new`] (which performs the `{"message": body}` wrap) or, for
/// a caller that has already assembled a richer stored body (e.g. the HTTP path
/// that also embeds a per-recipient `payment`), via
/// [`PersistJob::with_stored_body`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistJob {
    pub(crate) message_id: String,
    /// Recipient identity key — also used to (re)resolve the messageBox id.
    pub(crate) recipient: String,
    /// Message box name (e.g. `payment_keyswap`). The worker ensures the box
    /// exists and resolves its id; it does not trust a caller-supplied id, so a
    /// race between caller and worker cannot insert against a stale id.
    pub(crate) message_box: String,
    pub(crate) sender: String,
    /// Fully-formed stored body (already wrapped as `{"message": ...}` etc.).
    pub(crate) body: String,
}

impl PersistJob {
    /// Blessed constructor. Wraps the raw message `body` as the canonical stored
    /// shape `{"message": <body>}` (matching both send paths' on-disk format).
    ///
    /// `body` is the raw message string as supplied by the sender; this method
    /// owns the wrap so call sites no longer duplicate it.
    pub fn new(
        message_id: impl Into<String>,
        recipient: impl Into<String>,
        message_box: impl Into<String>,
        sender: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        let stored_body = serde_json::json!({ "message": body.into() }).to_string();
        Self {
            message_id: message_id.into(),
            recipient: recipient.into(),
            message_box: message_box.into(),
            sender: sender.into(),
            body: stored_body,
        }
    }

    /// Constructor for callers that have already assembled the full stored body
    /// (e.g. the HTTP path embeds a per-recipient `payment` alongside `message`).
    /// `stored_body` is used verbatim — no wrapping is applied.
    pub fn with_stored_body(
        message_id: impl Into<String>,
        recipient: impl Into<String>,
        message_box: impl Into<String>,
        sender: impl Into<String>,
        stored_body: impl Into<String>,
    ) -> Self {
        Self {
            message_id: message_id.into(),
            recipient: recipient.into(),
            message_box: message_box.into(),
            sender: sender.into(),
            body: stored_body.into(),
        }
    }
}

/// Outcome of a [`PersistHandle::enqueue`] call. `#[must_use]` so callers can
/// meter when the fast (queued) path is bypassed — every non-`Queued` variant
/// means the caller paid DB latency inline (or the job was dead-lettered).
#[must_use = "the persist outcome reports whether the fast path was bypassed — meter it"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enqueued {
    /// Handed to the background worker (the fast path). Durability is async.
    Queued,
    /// Queue was full or the worker was gone; persisted inline and the row is
    /// now durably in MySQL (or was a confirmed duplicate).
    InlineOk,
    /// Persisted inline but the DB write hit retry exhaustion on a transient
    /// outage; the job was dead-lettered to disk. Never silently dropped.
    InlineDeadLettered,
    /// Persisted inline but the DB write hit a permanent error; the job was
    /// dead-lettered to disk. Never silently dropped.
    DeadLettered,
    /// Not stored and deliberately NOT dead-lettered: the `messageId` collides
    /// with an existing id under the column's collation, so it can never be
    /// inserted and there is nothing to replay. The send is rejected, not lost.
    Rejected,
}

/// Tunables for the persist worker. Defaults are sized for the relay's hot path.
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Bounded queue depth. Large enough to absorb a burst, small enough that a
    /// sustained DB stall surfaces as inline-persist backpressure (not OOM).
    /// Must be `>= 1`; `spawn` fails loudly on `0`.
    pub queue_capacity: usize,
    /// Max attempts per job (1 initial try + retries) before declaring a
    /// transient outage exhausted. A duplicate is success and does not consume
    /// retries; a permanent error fails on the first attempt regardless.
    pub max_attempts: u32,
    /// Base backoff between retries; doubles each attempt, capped at `max_backoff`.
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    /// Append-only file that captures jobs that could not be persisted
    /// (permanent error, or transient exhaustion). Overridable via the
    /// `DEAD_LETTER_PATH` env ([`PersistConfig::from_env`]); defaults to
    /// `dead_letter.jsonl` in the process working directory.
    pub dead_letter_path: PathBuf,
}

/// Default dead-letter file, relative to the process working directory.
const DEFAULT_DEAD_LETTER_PATH: &str = "dead_letter.jsonl";

/// Ceiling on the dead-letter file. Past this, capture fails loudly (counted in
/// `dead_letter_failures`) rather than continuing to consume the disk the
/// service itself needs to run.
const DEAD_LETTER_MAX_BYTES: u64 = 256 * 1024 * 1024;

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 4096,
            max_attempts: 8,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            dead_letter_path: PathBuf::from(DEFAULT_DEAD_LETTER_PATH),
        }
    }
}

impl PersistConfig {
    /// Defaults with the operator env overrides applied: `DEAD_LETTER_PATH`
    /// sets [`Self::dead_letter_path`]. The default is relative, so any
    /// deployment whose working directory is not writable MUST point this at a
    /// writable location or every capture fails at the moment it is needed —
    /// [`PersistHandle::spawn`] probes it at boot and logs at ERROR. (The
    /// shipped Docker image sets a writable `WORKDIR` owned by its non-root
    /// user, so the default works there; that directory is still part of the
    /// container's ephemeral layer.) Env is read here, not in [`Default`], so
    /// `default()` stays hermetic for tests.
    pub fn from_env() -> Self {
        Self {
            dead_letter_path: dead_letter_path_from(std::env::var("DEAD_LETTER_PATH").ok()),
            ..Self::default()
        }
    }
}

/// `DEAD_LETTER_PATH` value → path; unset/blank → the default. Separate from
/// [`PersistConfig::from_env`] so resolution is testable without mutating
/// process-global env.
fn dead_letter_path_from(var: Option<String>) -> PathBuf {
    match var {
        Some(v) if !v.trim().is_empty() => PathBuf::from(v),
        _ => PathBuf::from(DEFAULT_DEAD_LETTER_PATH),
    }
}

/// Counters for observability. Cloneable handles share one set via `Arc`.
#[derive(Debug, Default)]
pub struct PersistStats {
    /// Jobs captured to the dead-letter file (permanent error or transient
    /// exhaustion). Counted only once the record is actually on disk.
    pub dead_lettered: AtomicU64,
    /// Dead-letter captures that FAILED (serialize/open/write/flush): the job
    /// is gone — not in MySQL, not on disk. Nonzero requires operator action
    /// (fix `DEAD_LETTER_PATH` / the disk under it).
    pub dead_letter_failures: AtomicU64,
    /// Sends rejected because the `messageId` collides with a stored id that
    /// differs only by case/padding/collation weight. Client-caused and not
    /// dead-lettered; a sustained rate means a buggy or probing client.
    pub id_conflicts: AtomicU64,
    /// Times the fast path was bypassed for an inline write (queue full / closed).
    pub inline_persists: AtomicU64,
    /// Times the supervised worker restarted after a panic.
    pub worker_panics: AtomicU64,
    /// Jobs successfully handed to the background worker (the fast path).
    /// Inline persists are NOT counted here — they are durable (or
    /// dead-lettered) before `enqueue` returns, so they never occupy the queue.
    pub enqueued: AtomicU64,
    /// Queued jobs the worker has finished (stored, duplicate, or
    /// dead-lettered). `completed >= enqueued` ⇔ the queue is flushed.
    pub completed: AtomicU64,
}

/// Classification of a `sqlx::Error` for retry purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorClass {
    /// Worth retrying: pool timeouts, I/O, connection loss, and MySQL
    /// lock-wait-timeout / deadlock.
    Transient,
    /// Not worth retrying: schema mismatch, data-too-long, non-dup constraint,
    /// configuration, decode, etc. Retrying only stalls the single-consumer
    /// queue.
    Permanent,
}

/// MySQL transient error codes worth retrying.
const MYSQL_LOCK_WAIT_TIMEOUT: &str = "1205";
const MYSQL_DEADLOCK: &str = "1213";

/// Classify a `sqlx::Error` as transient (retry) or permanent (fail fast).
fn classify(err: &sqlx::Error) -> ErrorClass {
    match err {
        // Pool/connection lifecycle — the DB is momentarily unreachable.
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::WorkerCrashed => ErrorClass::Transient,
        // A live DB error: retry only the known-transient MySQL codes.
        sqlx::Error::Database(dberr) => match dberr.code().as_deref() {
            Some(MYSQL_LOCK_WAIT_TIMEOUT) | Some(MYSQL_DEADLOCK) => ErrorClass::Transient,
            // Everything else (schema, data-too-long, constraint, etc.) is
            // permanent — no amount of retrying fixes it.
            _ => ErrorClass::Permanent,
        },
        // Configuration, Decode, Protocol, RowNotFound, ColumnNotFound, TLS, …
        // — none are fixed by retrying.
        _ => ErrorClass::Permanent,
    }
}

/// Cloneable handle used by the send paths to enqueue durable work.
#[derive(Clone)]
pub struct PersistHandle {
    tx: mpsc::Sender<PersistJob>,
    /// Kept so the inline last-resort path can persist without the worker.
    db: DbPool,
    cfg: PersistConfig,
    stats: Arc<PersistStats>,
}

impl PersistHandle {
    /// Spawn the supervised background persist worker and return a handle to
    /// enqueue jobs.
    ///
    /// The worker runs for the lifetime of the process (the channel is held by
    /// every clone of the handle, so it only closes at shutdown). One worker
    /// drains the queue serially; serial drain is fine because each job is a
    /// pair of fast indexed writes and ordering per recipient is preserved.
    ///
    /// # Panics
    /// Panics if `cfg.queue_capacity == 0` (an unusable channel). This is a
    /// programming/config error, surfaced loudly at startup rather than as a
    /// confusing `mpsc::channel` panic deeper in.
    pub fn spawn(db: DbPool, cfg: PersistConfig) -> Self {
        assert!(
            cfg.queue_capacity >= 1,
            "PersistConfig.queue_capacity must be >= 1 (got 0); a zero-capacity persist queue cannot accept jobs"
        );
        // Fail loud at boot, not at the moment of data loss: an unwritable
        // dead-letter path means every future capture fails and those messages
        // are lost. Deliberately not fatal — a server that cannot dead-letter
        // should still serve (failures are counted and logged as they happen).
        probe_dead_letter_path(&cfg.dead_letter_path);
        let (tx, rx) = mpsc::channel(cfg.queue_capacity);
        let stats = Arc::new(PersistStats::default());
        let worker_db = db.clone();
        let worker_cfg = cfg.clone();
        let worker_stats = Arc::clone(&stats);
        tokio::spawn(async move {
            run_supervised(rx, worker_db, worker_cfg, worker_stats).await;
        });
        Self { tx, db, cfg, stats }
    }

    /// Observability counters (dead-letters, inline persists, worker panics).
    pub fn stats(&self) -> &Arc<PersistStats> {
        &self.stats
    }

    /// Jobs currently waiting in the bounded queue (scrape-time sample).
    pub fn queue_depth(&self) -> usize {
        self.cfg.queue_capacity.saturating_sub(self.tx.capacity())
    }

    /// Configured queue capacity.
    pub fn queue_capacity(&self) -> usize {
        self.cfg.queue_capacity
    }

    /// Wait (bounded) until every job handed to the background worker has been
    /// handled — stored in MySQL, confirmed duplicate, or dead-lettered to
    /// disk. Used by graceful drain: after callers stop enqueuing (in-flight
    /// sends drained), a `true` return means zero queued messages remain
    /// un-durable. Returns `false` if `timeout` elapsed first (the worker keeps
    /// running; nothing is dropped — the drain is bounded, not lossy).
    pub async fn flush(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let enqueued = self.stats.enqueued.load(Ordering::Acquire);
            let completed = self.stats.completed.load(Ordering::Acquire);
            if completed >= enqueued {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    enqueued,
                    completed,
                    "persist flush timed out — {} queued job(s) not yet durable (worker still draining)",
                    enqueued - completed
                );
                return false;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Enqueue a job for asynchronous persistence.
    ///
    /// Fast path: a non-blocking `try_send` onto the bounded queue, returning
    /// [`Enqueued::Queued`]. If the queue is full (DB stalled / sustained
    /// overload) or the worker is gone (shutdown), we do **not** drop the
    /// message — we persist it inline (synchronously) after a loud warning and
    /// return the corresponding inline [`Enqueued`] variant so the caller can
    /// meter the bypass.
    pub async fn enqueue(&self, job: PersistJob) -> Enqueued {
        match self.tx.try_send(job) {
            Ok(()) => {
                self.stats.enqueued.fetch_add(1, Ordering::Release);
                Enqueued::Queued
            }
            Err(mpsc::error::TrySendError::Full(job)) => {
                self.stats.inline_persists.fetch_add(1, Ordering::Relaxed);
                warn!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    queue_capacity = self.cfg.queue_capacity,
                    "persist queue full — persisting inline (DB backpressure). Live push already happened; durability is being forced synchronously."
                );
                self.persist_inline(&job).await
            }
            Err(mpsc::error::TrySendError::Closed(job)) => {
                // Worker gone (only at shutdown). Never drop — persist inline.
                self.stats.inline_persists.fetch_add(1, Ordering::Relaxed);
                warn!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    "persist worker channel closed — persisting inline"
                );
                self.persist_inline(&job).await
            }
        }
    }

    /// Persist a job synchronously on the caller, mapping the worker outcome to
    /// the inline [`Enqueued`] variants.
    async fn persist_inline(&self, job: &PersistJob) -> Enqueued {
        match persist_with_retry(&self.db, job, &self.cfg, &self.stats, persist_once_db).await {
            PersistOutcome::Stored | PersistOutcome::Duplicate => Enqueued::InlineOk,
            PersistOutcome::Rejected => Enqueued::Rejected,
            PersistOutcome::DeadLetteredTransient => Enqueued::InlineDeadLettered,
            PersistOutcome::DeadLetteredPermanent => Enqueued::DeadLettered,
        }
    }
}

/// Supervise the drain loop: restart it (loudly) if it panics, end cleanly when
/// the channel closes gracefully.
async fn run_supervised(
    rx: mpsc::Receiver<PersistJob>,
    db: DbPool,
    cfg: PersistConfig,
    stats: Arc<PersistStats>,
) {
    debug!(
        queue_capacity = cfg.queue_capacity,
        max_attempts = cfg.max_attempts,
        dead_letter_path = %cfg.dead_letter_path.display(),
        "async persist worker started (supervised)"
    );
    // The receiver lives in an Arc<Mutex<_>> so a fresh drain task can reclaim
    // it after a panic without losing the queue contents.
    let rx = Arc::new(Mutex::new(rx));
    loop {
        let drain_rx = Arc::clone(&rx);
        let drain_db = db.clone();
        let drain_cfg = cfg.clone();
        let drain_stats = Arc::clone(&stats);
        // Run the drain in a child task so a panic is catchable via the JoinHandle.
        let handle =
            tokio::spawn(
                async move { run_drain(drain_rx, drain_db, drain_cfg, drain_stats).await },
            );
        match handle.await {
            // Graceful: channel closed (all handles dropped at shutdown).
            Ok(DrainExit::ChannelClosed) => {
                warn!("async persist worker stopped (channel closed — graceful shutdown)");
                return;
            }
            // Panic in the drain task — restart over the same receiver.
            Err(join_err) => {
                stats.worker_panics.fetch_add(1, Ordering::Relaxed);
                error!(
                    code = "ERR_PERSIST_WORKER_PANIC",
                    error = %join_err,
                    "async persist worker PANICKED — restarting drain over the same queue. Investigate: a poisoned job nearly degraded all sends to the inline hot path."
                );
                // Loop and respawn. The Arc<Mutex<rx>> still holds the queue.
            }
        }
    }
}

/// Why the drain loop returned.
enum DrainExit {
    /// `rx.recv()` yielded `None` — all senders dropped (graceful shutdown).
    ChannelClosed,
}

/// Drain the queue, persisting each job with bounded retry. Returns when the
/// channel closes; panics propagate to the supervisor as a `JoinError`.
async fn run_drain(
    rx: Arc<Mutex<mpsc::Receiver<PersistJob>>>,
    db: DbPool,
    cfg: PersistConfig,
    stats: Arc<PersistStats>,
) -> DrainExit {
    loop {
        // Hold the lock only across recv; release it during the (awaited)
        // persist so a restart can reclaim the receiver if persist panics.
        let next = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        match next {
            Some(job) => {
                let span = tracing::debug_span!(
                    "persist_job",
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    message_box = %job.message_box,
                );
                persist_with_retry(&db, &job, &cfg, &stats, persist_once_db)
                    .instrument(span)
                    .await;
                // Counted after the retry loop resolves (stored / duplicate /
                // dead-lettered): `completed` means "no longer pending", which
                // is what `flush` waits on during graceful drain.
                stats.completed.fetch_add(1, Ordering::Release);
            }
            None => return DrainExit::ChannelClosed,
        }
    }
}

/// Final disposition of a single job after the retry loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistOutcome {
    Stored,
    Duplicate,
    /// The send was rejected, not failed: the messageId can never be stored.
    /// Distinct from the dead-lettered variants because there is nothing to
    /// capture or replay — see the `IdConflict` arm in [`persist_with_retry`].
    Rejected,
    DeadLetteredTransient,
    DeadLetteredPermanent,
}

/// Persist one job to MySQL with bounded exponential backoff on **transient**
/// errors only. `persist` is injectable so the retry/classification/dead-letter
/// logic can be unit-tested without a live DB.
///
/// Durability guarantee: returns only after the row is durably written, a
/// duplicate is observed (idempotent success), or the job has been
/// **dead-lettered** to disk (on a permanent error, or after `max_attempts`
/// transient failures). A permanent error is *not* retried — that only stalls
/// the single-consumer queue. The terminal failure cases are logged at ERROR
/// and counted, so a message is never *silently* lost.
///
/// Dedup: `insert_message` uses `INSERT IGNORE` on the unique `messageId`. A
/// duplicate returns [`InsertOutcome::Duplicate`]; we treat it as success. Because persistence
/// now happens *after* the live push, the duplicate check lands at persist time
/// — which is acceptable: the client treats a duplicate as idempotent success,
/// and the unique constraint still prevents a second row from ever existing.
async fn persist_with_retry<F, Fut>(
    db: &DbPool,
    job: &PersistJob,
    cfg: &PersistConfig,
    stats: &PersistStats,
    persist: F,
) -> PersistOutcome
where
    F: Fn(DbPool, PersistJob) -> Fut,
    Fut: std::future::Future<Output = Result<InsertOutcome, sqlx::Error>>,
{
    let mut attempt: u32 = 0;
    let mut backoff = cfg.base_backoff;

    loop {
        attempt += 1;
        match persist(db.clone(), job.clone()).await {
            Ok(InsertOutcome::Inserted) => {
                debug!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    message_box = %job.message_box,
                    attempt,
                    "async persist: message stored"
                );
                return PersistOutcome::Stored;
            }
            Ok(InsertOutcome::Duplicate) => {
                // Duplicate messageId — already persisted. Idempotent success.
                debug!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    "async persist: duplicate messageId, treating as idempotent success"
                );
                return PersistOutcome::Duplicate;
            }
            Ok(InsertOutcome::IdConflict) => {
                // A REJECTED SEND, not a failed one — deliberately NOT
                // dead-lettered. The id can never be stored (the unique index
                // considers it taken by different bytes), so capturing the body
                // would burn the dead-letter budget on something no replay can
                // ever land. Capture exists for messages that *should* be in
                // MySQL and are not; this one must not be there at all.
                // Counted so a spike is visible: the sender chose the id, and a
                // client that keeps colliding is either buggy or probing.
                stats.id_conflicts.fetch_add(1, Ordering::Relaxed);
                warn!(
                    msg_id = %job.message_id,
                    recipient = %job.recipient,
                    message_box = %job.message_box,
                    "async persist: messageId conflicts with a stored id that differs only by \
                     case, trailing space, or collation weight — message NOT stored and NOT \
                     dead-lettered (it can never be inserted under this id). The sender must \
                     retry with a distinct messageId."
                );
                return PersistOutcome::Rejected;
            }
            Err(e) => match classify(&e) {
                ErrorClass::Permanent => {
                    error!(
                        msg_id = %job.message_id,
                        recipient = %job.recipient,
                        message_box = %job.message_box,
                        sender = %job.sender,
                        attempt,
                        error = %e,
                        "async persist PERMANENT error — not retrying (would only stall the queue). Dead-lettering to disk for recovery."
                    );
                    dead_letter(job, cfg, stats, &e, "permanent").await;
                    return PersistOutcome::DeadLetteredPermanent;
                }
                ErrorClass::Transient => {
                    if attempt >= cfg.max_attempts {
                        error!(
                            msg_id = %job.message_id,
                            recipient = %job.recipient,
                            message_box = %job.message_box,
                            sender = %job.sender,
                            attempts = attempt,
                            error = %e,
                            "async persist FAILED after exhausting transient retries — dead-lettering to disk. The row is NOT yet in MySQL; recovery requires replaying the dead-letter file. Operator attention required."
                        );
                        dead_letter(job, cfg, stats, &e, "transient-exhausted").await;
                        return PersistOutcome::DeadLetteredTransient;
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
            },
        }
    }
}

/// One JSON line in the dead-letter file.
#[derive(serde::Serialize)]
struct DeadLetterRecord<'a> {
    /// RFC3339 timestamp when the job was dead-lettered.
    dead_lettered_at: String,
    /// Why it was dead-lettered: `"permanent"` or `"transient-exhausted"`.
    reason: &'a str,
    /// The DB error string that caused the failure.
    error: String,
    /// The full job (so it can be replayed verbatim).
    job: &'a PersistJob,
}

/// Append the failed job to the dead-letter file as one JSON line. The
/// `dead_lettered` counter is bumped only once the line is on disk (open +
/// write + flush all succeeded) — a capture that failed must not be reported
/// as a capture. Failures bump `dead_letter_failures` and are logged at ERROR
/// (we cannot do more — the DB is also down).
async fn dead_letter(
    job: &PersistJob,
    cfg: &PersistConfig,
    stats: &PersistStats,
    err: &sqlx::Error,
    reason: &str,
) {
    let record = DeadLetterRecord {
        dead_lettered_at: chrono::Utc::now().to_rfc3339(),
        reason,
        error: err.to_string(),
        job,
    };
    let mut line = match serde_json::to_string(&record) {
        Ok(s) => s,
        Err(e) => {
            stats.dead_letter_failures.fetch_add(1, Ordering::Relaxed);
            error!(
                msg_id = %job.message_id,
                error = %e,
                "FATAL: could not serialize dead-letter record — message LOST (not in MySQL, not on disk)"
            );
            return;
        }
    };
    line.push('\n');

    match append_dead_letter_line(&cfg.dead_letter_path, line.as_bytes()).await {
        Ok(()) => {
            stats.dead_lettered.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            stats.dead_letter_failures.fetch_add(1, Ordering::Relaxed);
            error!(
                msg_id = %job.message_id,
                path = %cfg.dead_letter_path.display(),
                error = %e,
                "FATAL: could not write dead-letter record — message LOST (not in MySQL, not on disk). Fix DEAD_LETTER_PATH."
            );
        }
    }
}

/// Open-append-flush of one dead-letter record. Split out so every I/O
/// failure — the flush included, which for `tokio::fs::File` is where a
/// buffered write actually surfaces — funnels into the single failure
/// accounting in [`dead_letter`].
///
/// Refuses to grow the file past [`DEAD_LETTER_MAX_BYTES`]. Capture is a
/// last-resort safety net, not an unbounded sink: each record carries the full
/// message body, and any input that reliably produces a *permanent* DB error
/// turns a send loop into disk consumption on the host. Filling the disk takes
/// down the whole service, which is strictly worse than losing the captures
/// past the ceiling — those are counted and logged as losses.
async fn append_dead_letter_line(path: &Path, line: &[u8]) -> std::io::Result<()> {
    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let size = f.metadata().await?.len();
    if size.saturating_add(line.len() as u64) > DEAD_LETTER_MAX_BYTES {
        return Err(std::io::Error::other(format!(
            "dead-letter file is at its {DEAD_LETTER_MAX_BYTES}-byte ceiling ({size} bytes); \
             drain or rotate it (see DEAD_LETTER_PATH)"
        )));
    }
    f.write_all(line).await?;
    f.flush().await
}

/// Boot-time writability probe: attempt the exact create/append open the
/// capture path uses. Returns whether the path is writable; logs at ERROR when
/// it is not. The server keeps running either way — see [`PersistHandle::spawn`].
///
/// The probe deliberately does NOT clean up a file it created. "Remove it if it
/// did not exist a moment ago" is a time-of-check/time-of-use race, and losing
/// it destroys exactly what this module exists to protect: a peer sharing the
/// path (replicas on one volume, a rolling restart, or two tests in the same
/// binary) can append records between the check and the unlink, and they would
/// be deleted while already counted as captured. An empty file costs nothing —
/// the capture path opens with these same flags and appends to it.
fn probe_dead_letter_path(path: &Path) -> bool {
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(_) => true,
        Err(e) => {
            error!(
                path = %path.display(),
                error = %e,
                "dead-letter path is NOT writable — persist failures cannot be captured and WILL be lost. Point DEAD_LETTER_PATH at a writable location."
            );
            false
        }
    }
}

/// Resolve the messageBox id (ensuring the box exists) and insert the message.
/// This is the real DB-backed persist closure injected into [`persist_with_retry`].
///
/// Returns the [`InsertOutcome`] of the insert attempt,
/// and `Err` on any DB error (transient or otherwise — `classify` decides).
async fn persist_once_db(db: DbPool, job: PersistJob) -> Result<InsertOutcome, sqlx::Error> {
    // ensure_message_box both creates (INSERT IGNORE) and resolves the id, so a
    // box that was never created (or a worker that lost the race) still gets a
    // valid id here. This makes the worker fully self-sufficient.
    let mb_id = db::queries::ensure_message_box(&db, &job.recipient, &job.message_box).await?;
    db::queries::insert_message(
        &db,
        &job.message_id,
        mb_id,
        &job.sender,
        &job.recipient,
        &job.body,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    fn test_cfg(dir: &std::path::Path) -> PersistConfig {
        PersistConfig {
            queue_capacity: 8,
            max_attempts: 8,
            base_backoff: Duration::from_millis(0),
            max_backoff: Duration::from_millis(0),
            dead_letter_path: dir.join("dead_letter.jsonl"),
        }
    }

    fn unique_tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("persist-test-{tag}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // A fake DbPool we never actually touch; the injected closure ignores it.
    // We can't construct a real MySqlPool offline, so tests that need the
    // `db: &DbPool` argument use the public seam below instead.
    //
    // persist_with_retry takes `db: &DbPool`, but the injected closure ignores
    // it — so we pass a pool only where one is needed. To keep these tests
    // DB-free, we exercise persist_with_retry via a thin wrapper that supplies a
    // lazily-built pool that is never connected. sqlx's MySqlPool::connect_lazy
    // builds without a live server.
    fn lazy_pool() -> DbPool {
        sqlx::MySqlPool::connect_lazy("mysql://test:test@127.0.0.1:3306/test")
            .expect("connect_lazy builds without a server")
    }

    fn job() -> PersistJob {
        PersistJob::new("mid-1", "recipient-key", "chat", "sender-key", "hello")
    }

    // Build a transient and a permanent sqlx::Error for classify() tests.
    fn io_err() -> sqlx::Error {
        sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ))
    }

    #[test]
    fn new_wraps_body() {
        let j = PersistJob::new("m", "r", "box", "s", "hi");
        assert_eq!(j.body, r#"{"message":"hi"}"#);
        assert_eq!(j.message_id, "m");
    }

    #[test]
    fn with_stored_body_is_verbatim() {
        let j =
            PersistJob::with_stored_body("m", "r", "box", "s", r#"{"message":"x","payment":1}"#);
        assert_eq!(j.body, r#"{"message":"x","payment":1}"#);
    }

    #[test]
    fn classify_transient_variants() {
        assert_eq!(classify(&sqlx::Error::PoolTimedOut), ErrorClass::Transient);
        assert_eq!(classify(&sqlx::Error::PoolClosed), ErrorClass::Transient);
        assert_eq!(classify(&io_err()), ErrorClass::Transient);
        assert_eq!(classify(&sqlx::Error::WorkerCrashed), ErrorClass::Transient);
    }

    #[test]
    fn classify_permanent_variants() {
        assert_eq!(classify(&sqlx::Error::RowNotFound), ErrorClass::Permanent);
        assert_eq!(
            classify(&sqlx::Error::Configuration("bad".into())),
            ErrorClass::Permanent
        );
    }

    #[tokio::test]
    async fn transient_then_success_lands_once() {
        let dir = unique_tmp_dir("transient-success");
        let cfg = test_cfg(&dir);
        let stats = PersistStats::default();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = Arc::clone(&calls);
        let persist = move |_db: DbPool, _job: PersistJob| {
            let calls = Arc::clone(&calls2);
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(io_err()) // transient: fail first two attempts
                } else {
                    Ok(InsertOutcome::Inserted) // then succeed
                }
            }
        };
        let outcome = persist_with_retry(&lazy_pool(), &job(), &cfg, &stats, persist).await;
        assert_eq!(outcome, PersistOutcome::Stored);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "two failures then one success"
        );
        assert_eq!(stats.dead_lettered.load(Ordering::Relaxed), 0);
        assert!(
            !cfg.dead_letter_path.exists(),
            "no dead-letter on eventual success"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn permanent_error_fails_fast_and_dead_letters() {
        let dir = unique_tmp_dir("permanent");
        let cfg = test_cfg(&dir);
        let stats = PersistStats::default();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = Arc::clone(&calls);
        let persist = move |_db: DbPool, _job: PersistJob| {
            let calls = Arc::clone(&calls2);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(sqlx::Error::RowNotFound) // permanent
            }
        };
        let outcome = persist_with_retry(&lazy_pool(), &job(), &cfg, &stats, persist).await;
        assert_eq!(outcome, PersistOutcome::DeadLetteredPermanent);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "permanent error must NOT be retried 8x"
        );
        assert_eq!(stats.dead_lettered.load(Ordering::Relaxed), 1);
        let contents = std::fs::read_to_string(&cfg.dead_letter_path).unwrap();
        assert!(contents.contains(r#""reason":"permanent""#));
        assert!(contents.contains("mid-1"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn transient_exhaustion_dead_letters() {
        let dir = unique_tmp_dir("exhausted");
        let mut cfg = test_cfg(&dir);
        cfg.max_attempts = 3;
        let stats = PersistStats::default();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = Arc::clone(&calls);
        let persist = move |_db: DbPool, _job: PersistJob| {
            let calls = Arc::clone(&calls2);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(io_err()) // always transient
            }
        };
        let outcome = persist_with_retry(&lazy_pool(), &job(), &cfg, &stats, persist).await;
        assert_eq!(outcome, PersistOutcome::DeadLetteredTransient);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "exactly max_attempts tries"
        );
        assert_eq!(stats.dead_lettered.load(Ordering::Relaxed), 1);
        let contents = std::fs::read_to_string(&cfg.dead_letter_path).unwrap();
        assert!(contents.contains(r#""reason":"transient-exhausted""#));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn duplicate_is_idempotent_success() {
        let dir = unique_tmp_dir("dup");
        let cfg = test_cfg(&dir);
        let stats = PersistStats::default();
        let persist = |_db: DbPool, _job: PersistJob| async { Ok(InsertOutcome::Duplicate) };
        let outcome = persist_with_retry(&lazy_pool(), &job(), &cfg, &stats, persist).await;
        assert_eq!(outcome, PersistOutcome::Duplicate);
        assert_eq!(stats.dead_lettered.load(Ordering::Relaxed), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn queue_full_falls_back_to_inline() {
        // queue_capacity 1, never drain → second enqueue sees Full → inline.
        // The inline path uses persist_once_db against a lazy (unconnected)
        // pool, which yields a transient connect error → dead-lettered inline.
        let dir = unique_tmp_dir("queue-full");
        let mut cfg = test_cfg(&dir);
        cfg.queue_capacity = 1;
        cfg.max_attempts = 1; // don't spin on the unconnected pool
                              // Build a handle WITHOUT spawning a draining worker: we want the queue
                              // to stay full. We construct the channel directly.
        let (tx, _rx) = mpsc::channel::<PersistJob>(cfg.queue_capacity);
        let handle = PersistHandle {
            tx,
            db: lazy_pool(),
            cfg: cfg.clone(),
            stats: Arc::new(PersistStats::default()),
        };
        // First enqueue fills the (undrained) queue → Queued.
        assert_eq!(handle.enqueue(job()).await, Enqueued::Queued);
        // Second enqueue: queue full → inline persist against unconnected pool.
        // Any non-Queued outcome proves the inline path ran (no live DB here, so
        // the inline write itself dead-letters — that's expected and fine; the
        // contract under test is "full queue does NOT drop, it goes inline").
        let outcome = handle.enqueue(job()).await;
        assert!(
            !matches!(outcome, Enqueued::Queued),
            "queue-full bypass must persist inline, got {outcome:?}"
        );
        assert_eq!(handle.stats.inline_persists.load(Ordering::Relaxed), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn channel_closed_falls_back_to_inline() {
        let dir = unique_tmp_dir("closed");
        let mut cfg = test_cfg(&dir);
        cfg.max_attempts = 1;
        let (tx, rx) = mpsc::channel::<PersistJob>(cfg.queue_capacity);
        drop(rx); // close the channel → every send is Closed
        let handle = PersistHandle {
            tx,
            db: lazy_pool(),
            cfg: cfg.clone(),
            stats: Arc::new(PersistStats::default()),
        };
        let outcome = handle.enqueue(job()).await;
        assert!(
            !matches!(outcome, Enqueued::Queued),
            "channel-closed bypass must persist inline, got {outcome:?}"
        );
        assert_eq!(handle.stats.inline_persists.load(Ordering::Relaxed), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn dead_letter_path_env_resolution() {
        assert_eq!(
            dead_letter_path_from(Some("/data/dl.jsonl".into())),
            PathBuf::from("/data/dl.jsonl")
        );
        // Unset or blank falls back to the default.
        assert_eq!(
            dead_letter_path_from(None),
            PathBuf::from(DEFAULT_DEAD_LETTER_PATH)
        );
        assert_eq!(
            dead_letter_path_from(Some("  ".into())),
            PathBuf::from(DEFAULT_DEAD_LETTER_PATH)
        );
    }

    #[test]
    fn probe_reports_writability_without_destroying_records() {
        let dir = unique_tmp_dir("probe");
        let path = dir.join("dead_letter.jsonl");
        assert!(probe_dead_letter_path(&path));
        // The probe may leave an empty file behind; what it must NEVER do is
        // unlink one, since a peer sharing the path could have appended real
        // captures between any check and a removal.
        std::fs::write(&path, b"{}\n").unwrap();
        assert!(probe_dead_letter_path(&path));
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{}\n",
            "the probe must never destroy existing dead-letter records"
        );
        // Unwritable (missing parent directory) reports false, does not panic.
        assert!(!probe_dead_letter_path(
            &dir.join("missing").join("dl.jsonl")
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A capture that never reached disk must not be reported as a capture:
    /// `dead_lettered` stays 0 and the loss lands on the failure counter.
    #[tokio::test]
    async fn failed_dead_letter_write_counts_as_failure_not_capture() {
        let dir = unique_tmp_dir("dl-fail");
        let mut cfg = test_cfg(&dir);
        cfg.dead_letter_path = dir.join("missing-subdir").join("dead_letter.jsonl");
        let stats = PersistStats::default();
        let persist = |_db: DbPool, _job: PersistJob| async { Err(sqlx::Error::RowNotFound) };
        let outcome = persist_with_retry(&lazy_pool(), &job(), &cfg, &stats, persist).await;
        assert_eq!(outcome, PersistOutcome::DeadLetteredPermanent);
        assert_eq!(
            stats.dead_lettered.load(Ordering::Relaxed),
            0,
            "nothing landed on disk — must not be counted as captured"
        );
        assert_eq!(stats.dead_letter_failures.load(Ordering::Relaxed), 1);
        assert!(!cfg.dead_letter_path.exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A messageId conflict is a REJECTED send, not a failed one: it must not
    /// consume the dead-letter budget.
    ///
    /// The id can never be inserted (the unique index considers it taken by
    /// different bytes), so a captured copy could never be replayed — while the
    /// sender picks the id, making it an attacker-chosen input. Routing it
    /// through the permanent-error path let one socket append a full message
    /// body per collision: measured at 13.7 MiB/s, exhausting the ceiling in
    /// ~19s, after which a genuine DB outage loses its captures too.
    #[tokio::test]
    async fn id_conflict_is_rejected_without_consuming_the_dead_letter_budget() {
        let dir = unique_tmp_dir("dl-conflict");
        let cfg = test_cfg(&dir);
        let stats = PersistStats::default();
        let persist = |_db: DbPool, _job: PersistJob| async { Ok(InsertOutcome::IdConflict) };

        let outcome = persist_with_retry(&lazy_pool(), &job(), &cfg, &stats, persist).await;

        assert_eq!(outcome, PersistOutcome::Rejected);
        assert_eq!(
            stats.id_conflicts.load(Ordering::Relaxed),
            1,
            "the rejection must be counted so a spike is visible"
        );
        assert_eq!(stats.dead_lettered.load(Ordering::Relaxed), 0);
        assert_eq!(stats.dead_letter_failures.load(Ordering::Relaxed), 0);
        assert!(
            !cfg.dead_letter_path.exists(),
            "a conflict must not write to the dead-letter file at all"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Capture is a safety net, not an unbounded sink. Each record carries the
    /// full message body, so any input that reliably produces a permanent DB
    /// error would otherwise let a send loop consume the host's disk — which
    /// takes down the whole service, unlike losing captures past a ceiling.
    #[tokio::test]
    async fn dead_letter_stops_at_the_size_ceiling_instead_of_filling_the_disk() {
        let dir = unique_tmp_dir("dl-ceiling");
        let cfg = test_cfg(&dir);

        // Pre-fill the file to the ceiling.
        std::fs::write(
            &cfg.dead_letter_path,
            vec![b'x'; DEAD_LETTER_MAX_BYTES as usize],
        )
        .expect("prefill");

        let stats = PersistStats::default();
        let persist = |_db: DbPool, _job: PersistJob| async { Err(sqlx::Error::RowNotFound) };
        let outcome = persist_with_retry(&lazy_pool(), &job(), &cfg, &stats, persist).await;

        assert_eq!(outcome, PersistOutcome::DeadLetteredPermanent);
        assert_eq!(
            stats.dead_lettered.load(Ordering::Relaxed),
            0,
            "a refused write is not a capture"
        );
        assert_eq!(
            stats.dead_letter_failures.load(Ordering::Relaxed),
            1,
            "the loss must be counted so it is alertable"
        );
        assert_eq!(
            std::fs::metadata(&cfg.dead_letter_path)
                .expect("still there")
                .len(),
            DEAD_LETTER_MAX_BYTES,
            "the file must not grow past the ceiling"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
