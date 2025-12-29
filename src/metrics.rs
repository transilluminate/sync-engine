//! Metrics instrumentation for sync-engine.
//!
//! Uses the `metrics` crate for backend-agnostic metrics collection.
//! The parent daemon is responsible for choosing the exporter (Prometheus, OTEL, etc.)
//!
//! # Metric Naming Convention
//! - `sync_engine_` prefix for all metrics
//! - `_total` suffix for counters
//! - `_seconds` suffix for duration histograms
//! - `_bytes` suffix for size histograms
//!
//! # Labels
//! - `tier`: L1, L2, L3
//! - `operation`: get, put, delete, batch
//! - `status`: success, error, rejected

use metrics::{counter, gauge, histogram};
use std::time::{Duration, Instant};

/// Record a successful sync operation
pub fn record_operation(tier: &str, operation: &str, status: &str) {
    counter!(
        "sync_engine_operations_total",
        "tier" => tier.to_string(),
        "operation" => operation.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

/// Record operation latency
pub fn record_latency(tier: &str, operation: &str, duration: Duration) {
    histogram!(
        "sync_engine_operation_seconds",
        "tier" => tier.to_string(),
        "operation" => operation.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Record batch size
pub fn record_batch_size(tier: &str, count: usize) {
    histogram!(
        "sync_engine_batch_size",
        "tier" => tier.to_string()
    )
    .record(count as f64);
}

/// Record batch bytes
pub fn record_batch_bytes(tier: &str, bytes: usize) {
    histogram!(
        "sync_engine_batch_bytes",
        "tier" => tier.to_string()
    )
    .record(bytes as f64);
}

/// Set current L1 cache size in bytes
pub fn set_l1_cache_bytes(bytes: usize) {
    gauge!("sync_engine_l1_cache_bytes").set(bytes as f64);
}

/// Set current L1 cache item count
pub fn set_l1_cache_items(count: usize) {
    gauge!("sync_engine_l1_cache_items").set(count as f64);
}

/// Set WAL pending entries
pub fn set_wal_entries(count: usize) {
    gauge!("sync_engine_wal_entries").set(count as f64);
}

/// Set WAL file size in bytes
pub fn set_wal_bytes(bytes: u64) {
    gauge!("sync_engine_wal_bytes").set(bytes as f64);
}

/// Set cuckoo filter capacity utilization (0.0 - 1.0)
pub fn set_cuckoo_filter_load(filter: &str, load: f64) {
    gauge!(
        "sync_engine_cuckoo_filter_load",
        "filter" => filter.to_string()
    )
    .set(load);
}

/// Set cuckoo filter entry count
pub fn set_cuckoo_filter_entries(filter: &str, count: usize) {
    gauge!(
        "sync_engine_cuckoo_filter_entries",
        "filter" => filter.to_string()
    )
    .set(count as f64);
}

/// Record eviction event
pub fn record_eviction(count: usize, bytes: usize) {
    counter!("sync_engine_evictions_total").increment(count as u64);
    counter!("sync_engine_evicted_bytes_total").increment(bytes as u64);
}

/// Set memory pressure level (0.0 - 1.0)
pub fn set_memory_pressure(pressure: f64) {
    gauge!("sync_engine_memory_pressure").set(pressure);
}

/// Set backpressure level (0 = None, 1 = Low, 2 = Medium, 3 = High, 4 = Critical)
pub fn set_backpressure_level(level: u8) {
    gauge!("sync_engine_backpressure_level").set(level as f64);
}

/// Record circuit breaker state change
pub fn set_circuit_state(circuit: &str, state: u8) {
    gauge!(
        "sync_engine_circuit_breaker_state",
        "circuit" => circuit.to_string()
    )
    .set(state as f64);
}

/// Record data corruption detection
pub fn record_corruption(id: &str) {
    counter!(
        "sync_engine_corruption_detected_total",
        "id" => id.to_string()
    )
    .increment(1);
}

/// Record circuit breaker call
pub fn record_circuit_call(circuit: &str, outcome: &str) {
    counter!(
        "sync_engine_circuit_breaker_calls_total",
        "circuit" => circuit.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

/// Set engine state (for monitoring state machine transitions)
pub fn set_engine_state(state: &str) {
    // Use a simple counter to track state transitions
    counter!(
        "sync_engine_state_transitions_total",
        "state" => state.to_string()
    )
    .increment(1);
}

/// Record WAL drain operation
pub fn record_wal_drain(count: usize, success: bool) {
    let status = if success { "success" } else { "failure" };
    counter!(
        "sync_engine_wal_drain_total",
        "status" => status
    )
    .increment(1);
    
    if success {
        counter!("sync_engine_wal_drained_items_total").increment(count as u64);
    }
}

/// Record merkle tree operation
pub fn record_merkle_operation(store: &str, operation: &str, success: bool) {
    let status = if success { "success" } else { "failure" };
    counter!(
        "sync_engine_merkle_operations_total",
        "store" => store.to_string(),
        "operation" => operation.to_string(),
        "status" => status
    )
    .increment(1);
}

/// A timing guard that records latency on drop
pub struct LatencyTimer {
    tier: &'static str,
    operation: &'static str,
    start: Instant,
}

impl LatencyTimer {
    /// Start a new latency timer
    pub fn new(tier: &'static str, operation: &'static str) -> Self {
        Self {
            tier,
            operation,
            start: Instant::now(),
        }
    }
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        record_latency(self.tier, self.operation, self.start.elapsed());
    }
}

/// Convenience macro for timing operations
#[macro_export]
macro_rules! time_operation {
    ($tier:expr, $op:expr) => {
        $crate::metrics::LatencyTimer::new($tier, $op)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Note: These tests verify the API compiles and doesn't panic.
    // In production, you'd use metrics-util's Recorder for assertions.
    
    #[test]
    fn test_record_operation() {
        record_operation("L1", "get", "success");
        record_operation("L2", "put", "error");
        record_operation("L3", "delete", "success");
    }
    
    #[test]
    fn test_record_latency() {
        record_latency("L1", "get", Duration::from_micros(100));
        record_latency("L2", "put", Duration::from_millis(5));
        record_latency("L3", "batch", Duration::from_millis(50));
    }
    
    #[test]
    fn test_record_batch() {
        record_batch_size("L2", 100);
        record_batch_bytes("L2", 1024 * 50);
    }
    
    #[test]
    fn test_gauges() {
        set_l1_cache_bytes(1024 * 1024);
        set_l1_cache_items(5000);
        set_wal_entries(42);
        set_wal_bytes(1024 * 100);
        set_memory_pressure(0.75);
        set_backpressure_level(2);
    }
    
    #[test]
    fn test_cuckoo_filter_metrics() {
        set_cuckoo_filter_load("L2", 0.65);
        set_cuckoo_filter_load("L3", 0.45);
        set_cuckoo_filter_entries("L2", 65000);
        set_cuckoo_filter_entries("L3", 45000);
    }
    
    #[test]
    fn test_eviction_metrics() {
        record_eviction(10, 1024 * 50);
    }
    
    #[test]
    fn test_circuit_breaker_metrics() {
        set_circuit_state("redis", 0);
        set_circuit_state("mysql", 2);
        record_circuit_call("redis", "success");
        record_circuit_call("mysql", "rejected");
    }
    
    #[test]
    fn test_wal_drain_metrics() {
        record_wal_drain(50, true);
        record_wal_drain(0, false);
    }
    
    #[test]
    fn test_merkle_metrics() {
        record_merkle_operation("sql", "insert", true);
        record_merkle_operation("redis", "batch", false);
    }
    
    #[test]
    fn test_latency_timer() {
        {
            let _timer = LatencyTimer::new("L1", "get");
            // Simulate some work
            std::thread::sleep(Duration::from_micros(10));
        }
        // Timer recorded on drop
    }
    
    #[test]
    fn test_engine_state_tracking() {
        set_engine_state("Created");
        set_engine_state("Connecting");
        set_engine_state("Running");
    }
}
