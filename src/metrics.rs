// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

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

// ═══════════════════════════════════════════════════════════════════════════
// ERROR TRACKING - Categorized error counters for alerting
// ═══════════════════════════════════════════════════════════════════════════

/// Record an error with category for alerting
pub fn record_error(tier: &str, operation: &str, error_type: &str) {
    counter!(
        "sync_engine_errors_total",
        "tier" => tier.to_string(),
        "operation" => operation.to_string(),
        "error_type" => error_type.to_string()
    )
    .increment(1);
}

/// Record a connection/backend error
pub fn record_connection_error(backend: &str) {
    counter!(
        "sync_engine_connection_errors_total",
        "backend" => backend.to_string()
    )
    .increment(1);
}

/// Record a timeout error
pub fn record_timeout(tier: &str, operation: &str) {
    counter!(
        "sync_engine_timeouts_total",
        "tier" => tier.to_string(),
        "operation" => operation.to_string()
    )
    .increment(1);
}

// ═══════════════════════════════════════════════════════════════════════════
// THROUGHPUT - Bytes and items processed
// ═══════════════════════════════════════════════════════════════════════════

/// Record bytes written to a tier
pub fn record_bytes_written(tier: &str, bytes: usize) {
    counter!(
        "sync_engine_bytes_written_total",
        "tier" => tier.to_string()
    )
    .increment(bytes as u64);
}

/// Record bytes read from a tier
pub fn record_bytes_read(tier: &str, bytes: usize) {
    counter!(
        "sync_engine_bytes_read_total",
        "tier" => tier.to_string()
    )
    .increment(bytes as u64);
}

/// Record items written
pub fn record_items_written(tier: &str, count: usize) {
    counter!(
        "sync_engine_items_written_total",
        "tier" => tier.to_string()
    )
    .increment(count as u64);
}

// ═══════════════════════════════════════════════════════════════════════════
// QUEUE DEPTHS - Pending work
// ═══════════════════════════════════════════════════════════════════════════

/// Set batch queue depth (items pending flush)
pub fn set_batch_queue_items(count: usize) {
    gauge!("sync_engine_batch_queue_items").set(count as f64);
}

/// Set batch queue size in bytes
pub fn set_batch_queue_bytes(bytes: usize) {
    gauge!("sync_engine_batch_queue_bytes").set(bytes as f64);
}

// ═══════════════════════════════════════════════════════════════════════════
// BACKEND HEALTH - Connection status
// ═══════════════════════════════════════════════════════════════════════════

/// Set backend health status (1 = healthy, 0 = unhealthy)
pub fn set_backend_healthy(backend: &str, healthy: bool) {
    gauge!(
        "sync_engine_backend_healthy",
        "backend" => backend.to_string()
    )
    .set(if healthy { 1.0 } else { 0.0 });
}

// ═══════════════════════════════════════════════════════════════════════════
// CIRCUIT BREAKER - Resilience metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Record circuit breaker call outcome
pub fn record_circuit_breaker_call(circuit: &str, outcome: &str) {
    counter!(
        "sync_engine_circuit_breaker_calls_total",
        "circuit" => circuit.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

// ═══════════════════════════════════════════════════════════════════════════
// CUCKOO FILTER - Accuracy tracking
// ═══════════════════════════════════════════════════════════════════════════

/// Record cuckoo filter false positive
pub fn record_cuckoo_false_positive(filter: &str) {
    counter!(
        "sync_engine_cuckoo_false_positive_total",
        "filter" => filter.to_string()
    )
    .increment(1);
}

/// Record cuckoo filter check
pub fn record_cuckoo_check(filter: &str, result: &str) {
    counter!(
        "sync_engine_cuckoo_checks_total",
        "filter" => filter.to_string(),
        "result" => result.to_string()
    )
    .increment(1);
}

// ═══════════════════════════════════════════════════════════════════════════
// STARTUP - Timing for cold start monitoring
// ═══════════════════════════════════════════════════════════════════════════

/// Record startup phase duration
pub fn record_startup_phase(phase: &str, duration: Duration) {
    histogram!(
        "sync_engine_startup_seconds",
        "phase" => phase.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Record total startup time
pub fn record_startup_total(duration: Duration) {
    histogram!("sync_engine_startup_total_seconds").record(duration.as_secs_f64());
}

// ═══════════════════════════════════════════════════════════════════════════
// BATCH FLUSH - Detailed flush metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Record batch flush duration
pub fn record_flush_duration(duration: Duration) {
    histogram!("sync_engine_flush_seconds").record(duration.as_secs_f64());
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

// ═══════════════════════════════════════════════════════════════════════════
// CDC STREAM - Change Data Capture metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Record CDC entries emitted to stream
pub fn record_cdc_entries(op: &str, count: usize) {
    counter!(
        "sync_engine_cdc_entries_total",
        "op" => op.to_string()
    )
    .increment(count as u64);
}

// ═══════════════════════════════════════════════════════════════════════════
// SEARCH - RediSearch and SQL search metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Record a search query execution
pub fn record_search_query(backend: &str, status: &str) {
    counter!(
        "sync_engine_search_queries_total",
        "backend" => backend.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

/// Record search query latency
pub fn record_search_latency(backend: &str, duration: Duration) {
    histogram!(
        "sync_engine_search_seconds",
        "backend" => backend.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Record search result count
pub fn record_search_results(count: usize) {
    histogram!("sync_engine_search_results").record(count as f64);
}

/// Record search cache hit/miss
pub fn record_search_cache(hit: bool) {
    let outcome = if hit { "hit" } else { "miss" };
    counter!(
        "sync_engine_search_cache_total",
        "outcome" => outcome
    )
    .increment(1);
}

/// Set search cache stats gauge
pub fn set_search_cache_stats(entries: usize, hit_rate: f64) {
    gauge!("sync_engine_search_cache_entries").set(entries as f64);
    gauge!("sync_engine_search_cache_hit_rate").set(hit_rate);
}

/// Record index creation/drop
pub fn record_search_index_operation(operation: &str, success: bool) {
    let status = if success { "success" } else { "failure" };
    counter!(
        "sync_engine_search_index_operations_total",
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
    
    #[test]
    fn test_search_metrics() {
        // Search queries
        record_search_query("redis", "success");
        record_search_query("sql", "success");
        record_search_query("redis", "error");
        
        // Search latency
        record_search_latency("redis", Duration::from_micros(500));
        record_search_latency("sql", Duration::from_millis(5));
        record_search_latency("cache", Duration::from_micros(10));
        
        // Search results
        record_search_results(42);
        record_search_results(0);
        
        // Cache stats
        record_search_cache(true);
        record_search_cache(false);
        set_search_cache_stats(100, 0.85);
        
        // Index operations
        record_search_index_operation("create", true);
        record_search_index_operation("drop", true);
        record_search_index_operation("create", false);
    }
}
