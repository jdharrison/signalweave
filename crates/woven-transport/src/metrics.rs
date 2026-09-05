//! Always-on, production-safe counters for capacity and cost observability.
//!
//! These are cheap relaxed atomic increments taken on the same command path as the
//! debug-only activity log, so they cost nothing extra to reach — just an add instead of a
//! (cfg'd-out) tracing call. Rendering into any wire format (Prometheus text, JSON, ...)
//! is left to the caller; this module only counts.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative, monotonically increasing counters for the life of one server process.
///
/// Field names double as their eventual Prometheus metric names (the `_total` suffix is the
/// Prometheus counter-naming convention), so it's kept despite clippy's pedantic objection.
#[derive(Default)]
#[allow(clippy::struct_field_names)]
pub struct ServerMetrics {
    connections_total: AtomicU64,
    authenticate_rejected_total: AtomicU64,
    join_rejected_total: AtomicU64,
    publishes_total: AtomicU64,
    transform_publishes_total: AtomicU64,
    publish_bytes_received_total: AtomicU64,
    publish_bytes_delivered_total: AtomicU64,
    events_delivered_total: AtomicU64,
    queue_dropped_total: AtomicU64,
    queue_evicted_total: AtomicU64,
}

/// Point-in-time snapshot of [`ServerMetrics`]' counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct ServerMetricsSnapshot {
    pub connections_total: u64,
    pub authenticate_rejected_total: u64,
    pub join_rejected_total: u64,
    pub publishes_total: u64,
    pub transform_publishes_total: u64,
    pub publish_bytes_received_total: u64,
    pub publish_bytes_delivered_total: u64,
    pub events_delivered_total: u64,
    pub queue_dropped_total: u64,
    pub queue_evicted_total: u64,
}

/// Live counts read directly from core state, not accumulated — always exact, never drifts.
#[derive(Clone, Copy, Debug, Default)]
pub struct LiveCounts {
    pub connections_active: usize,
    pub sessions_active: usize,
}

impl ServerMetrics {
    pub(crate) fn record_connected(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_authenticate_rejected(&self) {
        self.authenticate_rejected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_join_rejected(&self) {
        self.join_rejected_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_publish(&self, payload_bytes: u64, is_transform: bool) {
        self.publishes_total.fetch_add(1, Ordering::Relaxed);
        self.publish_bytes_received_total
            .fetch_add(payload_bytes, Ordering::Relaxed);
        if is_transform {
            self.transform_publishes_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_publish_outcome(
        &self,
        payload_bytes: u64,
        recipient_attempts: u64,
        dropped: u64,
        evicted: u64,
    ) {
        self.events_delivered_total
            .fetch_add(recipient_attempts, Ordering::Relaxed);
        self.publish_bytes_delivered_total.fetch_add(
            payload_bytes.saturating_mul(recipient_attempts),
            Ordering::Relaxed,
        );
        self.queue_dropped_total
            .fetch_add(dropped, Ordering::Relaxed);
        self.queue_evicted_total
            .fetch_add(evicted, Ordering::Relaxed);
    }

    /// Read every counter's current value. Cheap; safe to call on every `/metrics` scrape.
    #[must_use]
    pub fn snapshot(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            connections_total: self.connections_total.load(Ordering::Relaxed),
            authenticate_rejected_total: self.authenticate_rejected_total.load(Ordering::Relaxed),
            join_rejected_total: self.join_rejected_total.load(Ordering::Relaxed),
            publishes_total: self.publishes_total.load(Ordering::Relaxed),
            transform_publishes_total: self.transform_publishes_total.load(Ordering::Relaxed),
            publish_bytes_received_total: self.publish_bytes_received_total.load(Ordering::Relaxed),
            publish_bytes_delivered_total: self
                .publish_bytes_delivered_total
                .load(Ordering::Relaxed),
            events_delivered_total: self.events_delivered_total.load(Ordering::Relaxed),
            queue_dropped_total: self.queue_dropped_total.load(Ordering::Relaxed),
            queue_evicted_total: self.queue_evicted_total.load(Ordering::Relaxed),
        }
    }
}
