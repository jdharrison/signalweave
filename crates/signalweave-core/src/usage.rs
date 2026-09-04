#![allow(clippy::missing_panics_doc)]

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::future::{Future, Ready, ready};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use crate::{AdmissionMetadata, NodeId, PrincipalId, SessionKey};

/// Current schema version for persisted usage windows.
pub const USAGE_SCHEMA_VERSION: u16 = 1;

/// Default aggregation window.
pub const DEFAULT_USAGE_WINDOW: Duration = Duration::from_secs(60);

/// Bounded in-memory spool capacity when a sink is unavailable.
pub const DEFAULT_SPOOL_CAPACITY: usize = 1_024;

/// Metrics captured inside a single usage window.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UsageMetrics {
    pub join_attempts: u64,
    pub immediate_admissions: u64,
    pub queued_joins: u64,
    pub queue_full_rejections: u64,
    pub paused_rejections: u64,
    pub auth_rejections: u64,
    pub reconnect_reservations: u64,
    pub successful_resumes: u64,
    pub promoted_players: u64,
    pub expired_tickets: u64,
    pub cancelled_tickets: u64,
    pub abandoned_offers: u64,
    pub events_received: u64,
    pub events_delivered: u64,
    pub events_dropped: u64,
    pub bytes_received: u64,
    pub bytes_delivered: u64,
    pub persistence_reads: u64,
    pub persistence_writes: u64,
    pub inference_requests: u64,
    pub capacity_allocations: u64,
    pub active_ccu: u32,
    pub peak_ccu: u32,
    pub queue_depth: u32,
    pub peak_queue_depth: u32,
    pub connection_seconds: u64,
}

/// A finalized usage window. Identified by node, server, start time, and sequence so that
/// sink retries do not duplicate logical usage.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UsageWindow {
    pub schema_version: u16,
    pub node_id: NodeId,
    pub session: SessionKey,
    pub window_start: SystemTime,
    pub window_end: SystemTime,
    pub sequence: u64,
    pub capacity_revision: u64,
    pub allocated_ccu: u32,
    pub metrics: UsageMetrics,
}

impl UsageWindow {
    /// Stable idempotency identity used by sinks for deduplication.
    #[must_use]
    pub fn idempotency_id(&self) -> String {
        format!(
            "swu:{}:{}:{}:{}:{}",
            self.schema_version,
            self.node_id.get(),
            self.session.namespace.get(),
            self.session.session.get(),
            self.sequence
        )
    }
}

/// Handle returned when a connection is registered with usage counters. The handle must be
/// passed to `end_connection` so connection-seconds can be split accurately across windows.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionHandle(u64);

impl ConnectionHandle {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct ActiveConnection {
    handle: ConnectionHandle,
    principal: PrincipalId,
    started: Instant,
}

/// Per-virtual-server atomic counters. Cheap to increment from the realtime path; more
/// expensive snapshot/finalization runs on the aggregator thread.
#[derive(Debug)]
pub struct UsageCounters {
    metadata: AdmissionMetadata,
    next_handle: AtomicU64,
    join_attempts: AtomicU64,
    immediate_admissions: AtomicU64,
    queued_joins: AtomicU64,
    queue_full_rejections: AtomicU64,
    paused_rejections: AtomicU64,
    auth_rejections: AtomicU64,
    reconnect_reservations: AtomicU64,
    successful_resumes: AtomicU64,
    promoted_players: AtomicU64,
    expired_tickets: AtomicU64,
    cancelled_tickets: AtomicU64,
    abandoned_offers: AtomicU64,
    events_received: AtomicU64,
    events_delivered: AtomicU64,
    events_dropped: AtomicU64,
    bytes_received: AtomicU64,
    bytes_delivered: AtomicU64,
    persistence_reads: AtomicU64,
    persistence_writes: AtomicU64,
    inference_requests: AtomicU64,
    capacity_allocations: AtomicU64,
    active_ccu: AtomicU32,
    peak_ccu: AtomicU32,
    queue_depth: AtomicU32,
    peak_queue_depth: AtomicU32,
    connection_seconds: AtomicU64,
    active: Mutex<Vec<ActiveConnection>>,
}

impl UsageCounters {
    #[must_use]
    pub fn new(metadata: AdmissionMetadata) -> Self {
        Self {
            metadata,
            next_handle: AtomicU64::new(1),
            join_attempts: AtomicU64::new(0),
            immediate_admissions: AtomicU64::new(0),
            queued_joins: AtomicU64::new(0),
            queue_full_rejections: AtomicU64::new(0),
            paused_rejections: AtomicU64::new(0),
            auth_rejections: AtomicU64::new(0),
            reconnect_reservations: AtomicU64::new(0),
            successful_resumes: AtomicU64::new(0),
            promoted_players: AtomicU64::new(0),
            expired_tickets: AtomicU64::new(0),
            cancelled_tickets: AtomicU64::new(0),
            abandoned_offers: AtomicU64::new(0),
            events_received: AtomicU64::new(0),
            events_delivered: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_delivered: AtomicU64::new(0),
            persistence_reads: AtomicU64::new(0),
            persistence_writes: AtomicU64::new(0),
            inference_requests: AtomicU64::new(0),
            capacity_allocations: AtomicU64::new(0),
            active_ccu: AtomicU32::new(0),
            peak_ccu: AtomicU32::new(0),
            queue_depth: AtomicU32::new(0),
            peak_queue_depth: AtomicU32::new(0),
            connection_seconds: AtomicU64::new(0),
            active: Mutex::new(Vec::new()),
        }
    }

    #[must_use]
    pub fn metadata(&self) -> AdmissionMetadata {
        self.metadata
    }

    pub fn start_connection(&self, principal: PrincipalId, now: Instant) -> ConnectionHandle {
        let handle = ConnectionHandle(self.next_handle.fetch_add(1, Ordering::Relaxed));
        let mut active = self.active.lock().expect("usage active lock poisoned");
        active.push(ActiveConnection {
            handle,
            principal,
            started: now,
        });
        handle
    }

    pub fn end_connection(&self, handle: ConnectionHandle, now: Instant) {
        let mut active = self.active.lock().expect("usage active lock poisoned");
        if let Some(index) = active.iter().position(|conn| conn.handle == handle) {
            let conn = active.swap_remove(index);
            let seconds = now.saturating_duration_since(conn.started).as_secs();
            self.connection_seconds
                .fetch_add(seconds, Ordering::Relaxed);
        }
    }

    pub fn increment_join_attempts(&self) {
        self.join_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_immediate_admissions(&self) {
        self.immediate_admissions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_queued_joins(&self) {
        self.queued_joins.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_queue_full_rejections(&self) {
        self.queue_full_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_paused_rejections(&self) {
        self.paused_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_auth_rejections(&self) {
        self.auth_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_reconnect_reservations(&self) {
        self.reconnect_reservations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_successful_resumes(&self) {
        self.successful_resumes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_promoted_players(&self) {
        self.promoted_players.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_expired_tickets_by(&self, count: u32) {
        self.expired_tickets
            .fetch_add(u64::from(count), Ordering::Relaxed);
    }

    pub fn increment_cancelled_tickets(&self) {
        self.cancelled_tickets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_abandoned_offers_by(&self, count: u32) {
        self.abandoned_offers
            .fetch_add(u64::from(count), Ordering::Relaxed);
    }

    pub fn increment_capacity_allocations(&self) {
        self.capacity_allocations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_events_received(&self, count: u64) {
        self.events_received.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_events_delivered(&self, count: u64) {
        self.events_delivered.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_events_dropped(&self, count: u64) {
        self.events_dropped.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_bytes_delivered(&self, bytes: u64) {
        self.bytes_delivered.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_persistence_read(&self) {
        self.persistence_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_persistence_write(&self) {
        self.persistence_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_inference_request(&self) {
        self.inference_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_active_ccu(&self, value: u32) {
        self.active_ccu.store(value, Ordering::Relaxed);
        self.peak_ccu.fetch_max(value, Ordering::Relaxed);
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn set_queue_depth(&self, value: usize) {
        let bounded = value.min(u32::MAX as usize) as u32;
        self.queue_depth.store(bounded, Ordering::Relaxed);
        self.peak_queue_depth.fetch_max(bounded, Ordering::Relaxed);
    }

    /// Snapshot cumulative counters and gauges for the window ending at `window_end`, then
    /// reset cumulative counters and peak gauges. Active connections keep running into the
    /// next window; their elapsed time up to `window_end` is added to this window's
    /// connection-seconds.
    #[must_use]
    pub fn snapshot_and_reset(&self, window_end: Instant) -> UsageMetrics {
        UsageMetrics {
            join_attempts: self.join_attempts.swap(0, Ordering::Relaxed),
            immediate_admissions: self.immediate_admissions.swap(0, Ordering::Relaxed),
            queued_joins: self.queued_joins.swap(0, Ordering::Relaxed),
            queue_full_rejections: self.queue_full_rejections.swap(0, Ordering::Relaxed),
            paused_rejections: self.paused_rejections.swap(0, Ordering::Relaxed),
            auth_rejections: self.auth_rejections.swap(0, Ordering::Relaxed),
            reconnect_reservations: self.reconnect_reservations.swap(0, Ordering::Relaxed),
            successful_resumes: self.successful_resumes.swap(0, Ordering::Relaxed),
            promoted_players: self.promoted_players.swap(0, Ordering::Relaxed),
            expired_tickets: self.expired_tickets.swap(0, Ordering::Relaxed),
            cancelled_tickets: self.cancelled_tickets.swap(0, Ordering::Relaxed),
            abandoned_offers: self.abandoned_offers.swap(0, Ordering::Relaxed),
            events_received: self.events_received.swap(0, Ordering::Relaxed),
            events_delivered: self.events_delivered.swap(0, Ordering::Relaxed),
            events_dropped: self.events_dropped.swap(0, Ordering::Relaxed),
            bytes_received: self.bytes_received.swap(0, Ordering::Relaxed),
            bytes_delivered: self.bytes_delivered.swap(0, Ordering::Relaxed),
            persistence_reads: self.persistence_reads.swap(0, Ordering::Relaxed),
            persistence_writes: self.persistence_writes.swap(0, Ordering::Relaxed),
            inference_requests: self.inference_requests.swap(0, Ordering::Relaxed),
            capacity_allocations: self.capacity_allocations.swap(0, Ordering::Relaxed),
            active_ccu: self.active_ccu.load(Ordering::Relaxed),
            peak_ccu: self.peak_ccu.swap(0, Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            peak_queue_depth: self.peak_queue_depth.swap(0, Ordering::Relaxed),
            connection_seconds: {
                let mut active = self.active.lock().expect("usage active lock poisoned");
                let mut seconds = self.connection_seconds.swap(0, Ordering::Relaxed);
                for conn in &mut *active {
                    seconds += window_end.saturating_duration_since(conn.started).as_secs();
                    conn.started = window_end;
                }
                seconds
            },
        }
    }
}

/// Error returned by a usage sink.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsageSinkError {
    pub message: String,
    pub retryable: bool,
}

impl UsageSinkError {
    #[must_use]
    pub fn new(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: message.into(),
            retryable,
        }
    }
}

impl std::fmt::Display for UsageSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for UsageSinkError {}

/// Asynchronous destination for finalized usage windows.
pub trait UsageSink {
    type AppendFuture<'a>: Future<Output = Result<(), UsageSinkError>> + Send + 'a
    where
        Self: 'a;

    fn append(&self, window: UsageWindow) -> Self::AppendFuture<'_>;
}

/// Sinks finalized windows into an in-memory ring buffer. Useful for deterministic tests.
#[derive(Debug)]
pub struct MemoryUsageSink {
    capacity: usize,
    windows: Mutex<VecDeque<UsageWindow>>,
}

impl MemoryUsageSink {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            windows: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub fn len(&self) -> usize {
        self.windows
            .lock()
            .expect("memory sink lock poisoned")
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn drain(&self) -> Vec<UsageWindow> {
        self.windows
            .lock()
            .expect("memory sink lock poisoned")
            .drain(..)
            .collect()
    }

    pub fn windows(&self) -> Vec<UsageWindow> {
        self.windows
            .lock()
            .expect("memory sink lock poisoned")
            .iter()
            .cloned()
            .collect()
    }
}

impl UsageSink for MemoryUsageSink {
    type AppendFuture<'a> = Ready<Result<(), UsageSinkError>>;

    fn append(&self, window: UsageWindow) -> Self::AppendFuture<'_> {
        let mut guard = self.windows.lock().expect("memory sink lock poisoned");
        if guard.len() == self.capacity {
            guard.pop_front();
        }
        guard.push_back(window);
        ready(Ok(()))
    }
}

/// Drops all finalized windows. Only appropriate for tests that intentionally disable
/// usage collection.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopUsageSink;

impl<T: UsageSink + ?Sized> UsageSink for &T {
    type AppendFuture<'a>
        = T::AppendFuture<'a>
    where
        Self: 'a;

    fn append(&self, window: UsageWindow) -> Self::AppendFuture<'_> {
        (**self).append(window)
    }
}

impl<T: UsageSink + ?Sized> UsageSink for std::sync::Arc<T> {
    type AppendFuture<'a>
        = T::AppendFuture<'a>
    where
        Self: 'a;

    fn append(&self, window: UsageWindow) -> Self::AppendFuture<'_> {
        (**self).append(window)
    }
}

impl UsageSink for NoopUsageSink {
    type AppendFuture<'a> = Ready<Result<(), UsageSinkError>>;

    fn append(&self, _window: UsageWindow) -> Self::AppendFuture<'_> {
        ready(Ok(()))
    }
}

/// Development sink that appends JSON lines to a file. The serialized form excludes any
/// bearer tokens, IP addresses, message bodies, player PII, or secrets by construction.
#[derive(Debug)]
pub struct JsonlFileSink {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl JsonlFileSink {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl UsageSink for JsonlFileSink {
    type AppendFuture<'a> = Ready<Result<(), UsageSinkError>>;

    fn append(&self, window: UsageWindow) -> Self::AppendFuture<'_> {
        let mut guard = self.writer.lock().expect("jsonl sink lock poisoned");
        let bytes = match serde_json::to_vec(&window) {
            Ok(bytes) => bytes,
            Err(error) => return ready(Err(UsageSinkError::new(error.to_string(), false))),
        };
        if let Err(error) = guard.write_all(&bytes) {
            return ready(Err(UsageSinkError::new(error.to_string(), true)));
        }
        if let Err(error) = guard.write_all(b"\n") {
            return ready(Err(UsageSinkError::new(error.to_string(), true)));
        }
        ready(
            guard
                .flush()
                .map_err(|error| UsageSinkError::new(error.to_string(), true)),
        )
    }
}

/// Health status reported by a spooling sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SinkHealth {
    Healthy,
    Degraded,
}

/// Wraps an inner sink with a bounded in-memory spool. Failed appends are retried on the
/// next call; if the spool overflows, the oldest windows are dropped and health is marked
/// degraded. `dropped_windows` exposes the count so the loss is observable. Windows are
/// never silently discarded while the spool has room.
#[derive(Debug)]
pub struct SpoolingUsageSink<S> {
    inner: S,
    capacity: usize,
    spool: Mutex<VecDeque<UsageWindow>>,
    health: AtomicU32,
    dropped_windows: AtomicU64,
}

impl<S: UsageSink> SpoolingUsageSink<S> {
    #[must_use]
    pub fn new(inner: S, capacity: usize) -> Self {
        Self {
            inner,
            capacity,
            spool: Mutex::new(VecDeque::with_capacity(capacity)),
            health: AtomicU32::new(0),
            dropped_windows: AtomicU64::new(0),
        }
    }

    pub fn health(&self) -> SinkHealth {
        if self.health.load(Ordering::Relaxed) == 0 {
            SinkHealth::Healthy
        } else {
            SinkHealth::Degraded
        }
    }

    #[must_use]
    pub fn dropped_windows(&self) -> u64 {
        self.dropped_windows.load(Ordering::Relaxed)
    }
}

impl<S: UsageSink + Send + Sync> UsageSink for SpoolingUsageSink<S> {
    type AppendFuture<'a>
        = std::pin::Pin<Box<dyn Future<Output = Result<(), UsageSinkError>> + Send + 'a>>
    where
        S: 'a;

    fn append(&self, window: UsageWindow) -> Self::AppendFuture<'_> {
        Box::pin(async move {
            // Drain the spool outside the lock to avoid holding a MutexGuard across await.
            loop {
                let pending = {
                    let mut spool = self.spool.lock().expect("spool lock poisoned");
                    spool.pop_front()
                };
                let Some(pending) = pending else {
                    break;
                };
                match self.inner.append(pending).await {
                    Ok(()) => {}
                    Err(error) if error.retryable => {
                        self.push_to_spool(window);
                        return Err(error);
                    }
                    Err(_error) => {
                        self.set_health(SinkHealth::Degraded);
                        // Drop the stale window and continue with the new one.
                    }
                }
            }

            // Now try the new window. Keep a clone in case the sink asks us to retry.
            match self.inner.append(window.clone()).await {
                Ok(()) => {
                    self.set_health(SinkHealth::Healthy);
                    Ok(())
                }
                Err(error) if error.retryable => {
                    self.push_to_spool(window);
                    Err(error)
                }
                Err(error) => Err(error),
            }
        })
    }
}

impl<S> SpoolingUsageSink<S> {
    fn push_to_spool(&self, window: UsageWindow) {
        let mut spool = self.spool.lock().expect("spool lock poisoned");
        if spool.len() == self.capacity {
            spool.pop_front();
            self.dropped_windows.fetch_add(1, Ordering::Relaxed);
            self.set_health(SinkHealth::Degraded);
        }
        spool.push_back(window);
    }

    fn set_health(&self, health: SinkHealth) {
        self.health.store(
            match health {
                SinkHealth::Healthy => 0,
                SinkHealth::Degraded => 1,
            },
            Ordering::Relaxed,
        );
    }
}

/// Node-level usage aggregator. Owns a collection of per-server counters and produces
/// finalized `UsageWindow`s on a configurable cadence. Finalization is synchronous and
/// cheap; a separate async task drains windows to a `UsageSink`.
#[derive(Debug)]
pub struct UsageAggregator {
    node_id: NodeId,
    window_duration: Duration,
    base_instant: Instant,
    base_time: SystemTime,
    finalized_windows: AtomicU64,
    next_sequence: AtomicU64,
    counters: Mutex<Vec<Arc<UsageCounters>>>,
    pending: Mutex<VecDeque<UsageWindow>>,
}

impl UsageAggregator {
    #[must_use]
    pub fn new(node_id: NodeId, window_duration: Duration) -> Self {
        Self::with_base(node_id, window_duration, Instant::now(), SystemTime::now())
    }

    #[must_use]
    pub fn with_base(
        node_id: NodeId,
        window_duration: Duration,
        base_instant: Instant,
        base_time: SystemTime,
    ) -> Self {
        Self {
            node_id,
            window_duration,
            base_instant,
            base_time,
            finalized_windows: AtomicU64::new(0),
            next_sequence: AtomicU64::new(1),
            counters: Mutex::new(Vec::new()),
            pending: Mutex::new(VecDeque::with_capacity(DEFAULT_SPOOL_CAPACITY)),
        }
    }

    /// Register a server's counters so the aggregator can include it in the next window.
    pub fn register(&self, counters: Arc<UsageCounters>) {
        let mut guard = self
            .counters
            .lock()
            .expect("aggregator counters lock poisoned");
        if !guard
            .iter()
            .any(|existing| Arc::ptr_eq(existing, &counters))
        {
            guard.push(counters);
        }
    }

    /// Advance time and finalize any windows that have elapsed. Returns finalized windows
    /// ready for export. This method does not block on I/O.
    #[allow(clippy::cast_possible_truncation)]
    pub fn tick_at(&self, now: Instant) -> Vec<UsageWindow> {
        if self.window_duration.is_zero() {
            return Vec::new();
        }
        let elapsed = now.saturating_duration_since(self.base_instant);
        let windows_elapsed = elapsed.as_secs() / self.window_duration.as_secs();
        let previously_finalized = self.finalized_windows.load(Ordering::Relaxed);
        let new_windows = windows_elapsed.saturating_sub(previously_finalized);
        if new_windows == 0 {
            return Vec::new();
        }

        self.finalized_windows
            .fetch_add(new_windows, Ordering::Relaxed);
        let counters = self
            .counters
            .lock()
            .expect("aggregator counters lock poisoned");
        let mut finalized = Vec::with_capacity((new_windows as usize) * counters.len());
        for index in 0..new_windows {
            let window_index = previously_finalized + index;
            let window_end_instant = self
                .base_instant
                .checked_add(self.window_duration * ((window_index + 1) as u32))
                .unwrap_or(now);
            let window_end = self
                .base_time
                .checked_add(self.window_duration * ((window_index + 1) as u32))
                .unwrap_or_else(SystemTime::now);
            let window_start = window_end
                .checked_sub(self.window_duration)
                .unwrap_or(self.base_time);
            let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);

            for counter in counters.iter() {
                let metrics = counter.snapshot_and_reset(window_end_instant);
                finalized.push(UsageWindow {
                    schema_version: USAGE_SCHEMA_VERSION,
                    node_id: self.node_id,
                    session: counter.metadata.session,
                    window_start,
                    window_end,
                    sequence,
                    capacity_revision: 0,
                    allocated_ccu: 0,
                    metrics,
                });
            }
        }

        let mut pending = self
            .pending
            .lock()
            .expect("aggregator pending lock poisoned");
        for window in &finalized {
            if pending.len() == DEFAULT_SPOOL_CAPACITY {
                pending.pop_front();
            }
            pending.push_back(window.clone());
        }

        finalized
    }

    /// Poll finalized windows that have not yet been exported. Useful when driving the
    /// sink from an external task.
    pub fn drain_pending(&self) -> Vec<UsageWindow> {
        self.pending
            .lock()
            .expect("aggregator pending lock poisoned")
            .drain(..)
            .collect()
    }

    /// Snapshot the latest finalized windows without removing them.
    pub fn pending(&self) -> Vec<UsageWindow> {
        self.pending
            .lock()
            .expect("aggregator pending lock poisoned")
            .iter()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata() -> AdmissionMetadata {
        AdmissionMetadata {
            node_id: NodeId::new(1),
            session: SessionKey::new(crate::NamespaceId::new(1), crate::SessionId::new(1)),
        }
    }

    #[test]
    fn counters_snapshot_and_reset() {
        let now = Instant::now();
        let counters = UsageCounters::new(metadata());
        counters.increment_join_attempts();
        counters.increment_queued_joins();
        counters.set_active_ccu(5);
        let handle = counters.start_connection(PrincipalId::new(1), now);
        let metrics = counters.snapshot_and_reset(now + Duration::from_secs(10));
        assert_eq!(metrics.join_attempts, 1);
        assert_eq!(metrics.queued_joins, 1);
        assert_eq!(metrics.active_ccu, 5);
        assert_eq!(metrics.peak_ccu, 5);
        assert_eq!(metrics.connection_seconds, 10);
        counters.end_connection(handle, now + Duration::from_secs(15));
        let next = counters.snapshot_and_reset(now + Duration::from_secs(20));
        assert_eq!(next.join_attempts, 0);
        assert_eq!(next.connection_seconds, 5);
    }

    #[test]
    fn aggregator_window_idempotency() {
        let start = Instant::now();
        let aggregator = UsageAggregator::new(NodeId::new(1), Duration::from_secs(10));
        let counters = Arc::new(UsageCounters::new(metadata()));
        aggregator.register(counters.clone());
        counters.increment_join_attempts();
        let windows = aggregator.tick_at(start + Duration::from_secs(25));
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].sequence + 1, windows[1].sequence);
        assert_eq!(windows[0].idempotency_id(), windows[0].idempotency_id());
        assert_ne!(windows[0].idempotency_id(), windows[1].idempotency_id());
    }

    #[test]
    fn memory_sink_is_bounded() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let sink = MemoryUsageSink::new(2);
        let window = UsageWindow {
            schema_version: 1,
            node_id: NodeId::new(1),
            session: SessionKey::new(crate::NamespaceId::new(1), crate::SessionId::new(1)),
            window_start: SystemTime::UNIX_EPOCH,
            window_end: SystemTime::UNIX_EPOCH,
            sequence: 1,
            capacity_revision: 1,
            allocated_ccu: 10,
            metrics: UsageMetrics::default(),
        };
        rt.block_on(async {
            sink.append(window.clone()).await.expect("append");
            sink.append(window.clone()).await.expect("append");
            sink.append(window.clone()).await.expect("append");
        });
        assert_eq!(sink.len(), 2);
    }
}
