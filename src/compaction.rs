//! CRDT-aware compaction for storage lifecycle management.
//!
//! When the `crdt` feature is enabled, this module provides:
//! - Retention policies for CRDT operation logs
//! - Merge/compaction using CRDT semantics
//! - Background compaction scheduling
//!
//! # Feature Flag
//!
//! ```toml
//! [dependencies]
//! sync-engine = { version = "0.1", features = ["crdt"] }
//! ```
//!
//! # Compaction Model
//!
//! CRDTs accumulate operations over time. After the retention period,
//! operations are merged into a single snapshot per entity:
//!
//! - **Days 0-7** (configurable): Keep all operations for replay/audit
//! - **Day 7+**: Compact N operations → 1 CRDT snapshot
//! - **Never delete**: Snapshots preserved for self-healing/rebuild
//!
//! This ensures we always have the ability to rebuild state from
//! the compacted CRDT snapshot if corruption is detected.
//!
//! # Example
//!
//! ```rust,ignore
//! use sync_engine::compaction::{CompactionConfig, CompactionPolicy};
//!
//! let config = CompactionConfig {
//!     retention_full_days: 7,  // Keep full history for 7 days
//!     compact_batch_size: 1000,
//!     ..Default::default()
//! };
//! ```

use std::time::Duration;

/// Compaction configuration.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Days to keep full operation history before compacting.
    /// After this period, operations are merged into a single CRDT snapshot.
    /// The snapshot is kept forever (never deleted) to support self-healing.
    pub retention_full_days: u32,

    /// Number of items to process per compaction batch
    pub compact_batch_size: usize,

    /// Minimum interval between compaction runs
    pub compact_interval: Duration,

    /// Whether to compress compacted data (requires `compression` feature)
    pub compress_compacted: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            retention_full_days: 7,
            compact_batch_size: 1000,
            compact_interval: Duration::from_secs(3600), // 1 hour
            compress_compacted: true,
        }
    }
}

/// Compaction policy for CRDT data.
///
/// Note: We intentionally don't support "delete" or "keep latest" policies
/// because they would destroy CRDT semantics and prevent self-healing
/// from snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompactionPolicy {
    /// Keep all operations indefinitely (no compaction).
    /// Use for audit trails or when full history is required.
    KeepAll,
    /// Merge operations using CRDT semantics after retention period.
    /// This is the recommended policy - compacts N ops → 1 snapshot.
    #[default]
    MergeCrdt,
}

/// Result of a compaction run.
#[derive(Debug, Clone, Default)]
pub struct CompactionResult {
    /// Number of items scanned
    pub scanned: usize,
    /// Number of items compacted (N ops → 1 snapshot)
    pub compacted: usize,
    /// Bytes saved by compaction
    pub bytes_saved: usize,
    /// Duration of compaction run
    pub duration: Duration,
}

impl CompactionResult {
    /// Check if any work was done.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.compacted == 0
    }
}

// ============================================================================
// CRDT-specific compaction (requires `crdt` feature)
// ============================================================================

#[cfg(feature = "crdt")]
pub mod crdt_compact {
    //! CRDT-aware compaction using `crdt-data-types`.
    //!
    //! This module re-exports and wraps the compaction functions from the
    //! `crdt-data-types` crate, providing a unified interface for the sync engine.
    
    use crdt_data_types::compaction::{compact_capnp_bytes, compact_json_values};
    use crdt_data_types::CrdtError;
    use serde_json::Value;

    /// Compact multiple CRDT JSON values into a single merged value.
    ///
    /// Dispatches to the appropriate CRDT merge logic based on `crdt_type`.
    ///
    /// # Supported Types
    ///
    /// - `GCounter`, `PNCounter`
    /// - `GSet`, `ORSet`, `LWWSet`
    /// - `LWWRegister`, `FWWRegister`, `MVRegister`
    /// - `LWWMap`, `ORMap`
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use sync_engine::compaction::crdt_compact::compact_json;
    /// use serde_json::json;
    ///
    /// let values = vec![
    ///     json!({"counters": {"node_a": 10}, "vclock": {"clocks": {}}}),
    ///     json!({"counters": {"node_b": 20}, "vclock": {"clocks": {}}}),
    /// ];
    ///
    /// let result = compact_json("GCounter", &values)?;
    /// ```
    pub fn compact_json(crdt_type: &str, values: &[Value]) -> Result<Value, CrdtError> {
        compact_json_values(crdt_type, values)
    }

    /// Compact multiple Cap'n Proto byte buffers into a single buffer.
    ///
    /// This is the high-performance pathway for binary storage, avoiding
    /// JSON serialization overhead entirely.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use sync_engine::compaction::crdt_compact::compact_bytes;
    /// use crdt_data_types::{GCounter, Crdt};
    ///
    /// let mut gc1 = GCounter::new();
    /// gc1.increment("node_a", 10);
    /// let bytes1 = gc1.to_capnp_bytes();
    ///
    /// let mut gc2 = GCounter::new();
    /// gc2.increment("node_b", 20);
    /// let bytes2 = gc2.to_capnp_bytes();
    ///
    /// let compacted = compact_bytes("GCounter", &[&bytes1, &bytes2])?;
    /// ```
    pub fn compact_bytes(crdt_type: &str, buffers: &[&[u8]]) -> Result<Vec<u8>, CrdtError> {
        compact_capnp_bytes(crdt_type, buffers)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crdt_data_types::{Crdt, GCounter};
        use serde_json::json;

        #[test]
        fn test_compact_json_gcounter() {
            let values = vec![
                json!({"counters": {"node_a": 10}, "vclock": {"clocks": {"node_a": [1, 100]}}}),
                json!({"counters": {"node_b": 20}, "vclock": {"clocks": {"node_b": [1, 200]}}}),
            ];

            let result = compact_json("GCounter", &values).unwrap();
            // After merge, should have both nodes' counters
            assert!(result["counters"]["node_a"].as_i64().is_some());
            assert!(result["counters"]["node_b"].as_i64().is_some());
        }

        #[test]
        fn test_compact_bytes_gcounter() {
            let mut gc1 = GCounter::new();
            gc1.increment("node_a", 10);
            let bytes1 = gc1.to_capnp_bytes();

            let mut gc2 = GCounter::new();
            gc2.increment("node_b", 20);
            let bytes2 = gc2.to_capnp_bytes();

            let compacted = compact_bytes("GCounter", &[&bytes1, &bytes2]).unwrap();
            assert!(!compacted.is_empty());
        }

        #[test]
        fn test_compact_empty() {
            let result = compact_json("GCounter", &[]).unwrap();
            assert!(result.is_null());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.retention_full_days, 7);
        assert_eq!(config.compact_batch_size, 1000);
        assert!(config.compress_compacted);
    }

    #[test]
    fn test_compaction_policy_default() {
        let policy = CompactionPolicy::default();
        assert_eq!(policy, CompactionPolicy::MergeCrdt);
    }

    #[test]
    fn test_compaction_result_is_empty() {
        let empty = CompactionResult::default();
        assert!(empty.is_empty());

        let with_work = CompactionResult {
            compacted: 10,
            ..Default::default()
        };
        assert!(!with_work.is_empty());
    }
}
