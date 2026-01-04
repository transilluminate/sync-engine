// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Public types for the sync engine coordinator.

use crate::backpressure::BackpressureLevel;
use crate::cuckoo::filter_manager::FilterTrust;

// ═══════════════════════════════════════════════════════════════════════════
// HEALTH CHECK - Comprehensive health status for /ready and /health endpoints
// ═══════════════════════════════════════════════════════════════════════════

/// Comprehensive health status of the sync engine.
///
/// This struct provides all the information needed for:
/// - `/ready` endpoint: Just check `healthy` field
/// - `/health` endpoint: Full diagnostics with data sources documented
/// - Metrics dashboards: Memory pressure, cache stats, backend latencies
///
/// # Data Sources
///
/// Each field documents where its value comes from:
/// - **Live probe**: Fresh check performed during `health_check()` call
/// - **Cached**: From internal state, no I/O required
/// - **Derived**: Computed from other fields
///
/// # Example
///
/// ```rust,ignore
/// let health = engine.health_check().await;
/// if health.healthy {
///     // Ready for traffic
/// } else {
///     // Log diagnostics
///     eprintln!("Engine unhealthy: state={:?}, redis={}, sql={}",
///         health.state, health.redis_connected, health.sql_connected);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct HealthCheck {
    // ═══════════════════════════════════════════════════════════════════════
    // ENGINE STATE (cached from internal state machine)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Current engine lifecycle state.
    /// **Source**: Cached from `state_tx` watch channel.
    pub state: EngineState,
    
    /// Whether the engine is in `Running` state and ready for traffic.
    /// **Source**: Derived from `state == Running`.
    pub ready: bool,
    
    // ═══════════════════════════════════════════════════════════════════════
    // MEMORY & BACKPRESSURE (cached from atomics/config)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Current memory pressure ratio (0.0 to 1.0).
    /// **Source**: Cached from `l1_size_bytes / config.l1_max_bytes`.
    pub memory_pressure: f64,
    
    /// Current backpressure level based on thresholds.
    /// **Source**: Derived from memory_pressure vs config thresholds.
    pub backpressure_level: BackpressureLevel,
    
    /// Whether the engine is accepting write operations.
    /// **Source**: Derived from `backpressure_level != Critical`.
    pub accepting_writes: bool,
    
    // ═══════════════════════════════════════════════════════════════════════
    // L1 CACHE (cached from DashMap/atomics)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Number of items in L1 cache.
    /// **Source**: Cached from `l1_cache.len()`.
    pub l1_items: usize,
    
    /// Total bytes in L1 cache.
    /// **Source**: Cached from `l1_size_bytes` atomic.
    pub l1_bytes: usize,
    
    // ═══════════════════════════════════════════════════════════════════════
    // CUCKOO FILTER (cached from FilterManager)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Number of items tracked in L3 cuckoo filter.
    /// **Source**: Cached from `l3_filter.len()`.
    pub filter_items: usize,
    
    /// Cuckoo filter trust status.
    /// **Source**: Cached from `l3_filter.trust()`.
    pub filter_trust: FilterTrust,
    
    // ═══════════════════════════════════════════════════════════════════════
    // BACKEND CONNECTIVITY (live probes during health_check)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Whether Redis (L2) responded to PING.
    /// **Source**: Live probe - `redis::cmd("PING")`.
    /// `None` if Redis is not configured.
    pub redis_connected: Option<bool>,
    
    /// Redis PING latency in milliseconds.
    /// **Source**: Live probe - timed PING command.
    /// `None` if Redis is not configured or PING failed.
    pub redis_latency_ms: Option<u64>,
    
    /// Whether SQL (L3) responded to `SELECT 1`.
    /// **Source**: Live probe - `sqlx::query("SELECT 1")`.
    /// `None` if SQL is not configured.
    pub sql_connected: Option<bool>,
    
    /// SQL `SELECT 1` latency in milliseconds.
    /// **Source**: Live probe - timed query.
    /// `None` if SQL is not configured or query failed.
    pub sql_latency_ms: Option<u64>,
    
    // ═══════════════════════════════════════════════════════════════════════
    // WAL STATUS (cached from WriteAheadLog)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Number of items pending in WAL (awaiting drain to SQL).
    /// **Source**: Cached from `l3_wal.stats().pending_items`.
    /// `None` if WAL is not configured.
    pub wal_pending_items: Option<u64>,
    
    // ═══════════════════════════════════════════════════════════════════════
    // OVERALL VERDICT (derived)
    // ═══════════════════════════════════════════════════════════════════════
    
    /// Overall health verdict for load balancer decisions.
    /// **Source**: Derived from:
    /// - `state == Running`
    /// - `redis_connected != Some(false)` (connected or not configured)
    /// - `sql_connected != Some(false)` (connected or not configured)
    /// - `backpressure_level != Critical`
    pub healthy: bool,
}

/// Engine lifecycle state.
///
/// The engine progresses through states during startup and shutdown.
/// Use [`super::SyncEngine::state()`] to check current state or
/// [`super::SyncEngine::state_receiver()`] to watch for changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    /// Just created, not yet started
    Created,
    /// Connecting to backends (L2/L3)
    Connecting,
    /// Draining WAL to SQL (blocking, before accepting writes)
    DrainingWal,
    /// Syncing Redis from SQL (branch diff)
    SyncingRedis,
    /// Warming up cuckoo filters
    WarmingUp,
    /// Ready to accept data
    Ready,
    /// Running normally
    Running,
    /// Graceful shutdown in progress
    ShuttingDown,
}

impl std::fmt::Display for EngineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Connecting => write!(f, "Connecting"),
            Self::DrainingWal => write!(f, "DrainingWal"),
            Self::SyncingRedis => write!(f, "SyncingRedis"),
            Self::WarmingUp => write!(f, "WarmingUp"),
            Self::Ready => write!(f, "Ready"),
            Self::Running => write!(f, "Running"),
            Self::ShuttingDown => write!(f, "ShuttingDown"),
        }
    }
}

/// Sync status of an item across storage tiers.
///
/// Used by [`super::SyncEngine::status()`] to report where an item exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemStatus {
    /// Item is synced (exists in at least one tier)
    Synced {
        /// Present in L1 (DashMap)
        in_l1: bool,
        /// Present in L2 (Redis)
        in_l2: bool,
        /// Present in L3 (MySQL/SQLite)
        in_l3: bool,
    },
    /// Item is queued for sync but not yet persisted
    Pending,
    /// Item does not exist
    Missing,
}

impl std::fmt::Display for ItemStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Synced { in_l1, in_l2, in_l3 } => {
                write!(f, "Synced(L1={}, L2={}, L3={})", in_l1, in_l2, in_l3)
            }
            Self::Pending => write!(f, "Pending"),
            Self::Missing => write!(f, "Missing"),
        }
    }
}

/// Result of a batch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchResult {
    /// Total items in the batch
    pub total: usize,
    /// Successfully processed items
    pub succeeded: usize,
    /// Failed items
    pub failed: usize,
}

impl BatchResult {
    /// Check if all items succeeded
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.failed == 0
    }
}

/// Where a write was persisted (internal use)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum WriteTarget {
    /// Written to MySQL (L3)
    L3,
    /// Written to local WAL (will drain to L3 when MySQL is back)
    Wal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_state_display() {
        assert_eq!(format!("{}", EngineState::Created), "Created");
        assert_eq!(format!("{}", EngineState::Running), "Running");
        assert_eq!(format!("{}", EngineState::ShuttingDown), "ShuttingDown");
    }

    #[test]
    fn test_item_status_display() {
        let synced = ItemStatus::Synced { in_l1: true, in_l2: false, in_l3: true };
        assert_eq!(format!("{}", synced), "Synced(L1=true, L2=false, L3=true)");
        
        assert_eq!(format!("{}", ItemStatus::Pending), "Pending");
        assert_eq!(format!("{}", ItemStatus::Missing), "Missing");
    }

    #[test]
    fn test_batch_result_is_success() {
        let success = BatchResult { total: 10, succeeded: 10, failed: 0 };
        assert!(success.is_success());
        
        let partial = BatchResult { total: 10, succeeded: 8, failed: 2 };
        assert!(!partial.is_success());
    }
}
