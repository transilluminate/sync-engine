//! Configuration for the sync engine.
//!
//! # Example
//!
//! ```
//! use sync_engine::SyncEngineConfig;
//!
//! // Minimal config (uses defaults)
//! let config = SyncEngineConfig::default();
//! assert_eq!(config.l1_max_bytes, 256 * 1024 * 1024); // 256 MB
//!
//! // Full config
//! let config = SyncEngineConfig {
//!     redis_url: Some("redis://localhost:6379".into()),
//!     sql_url: Some("mysql://user:pass@localhost/db".into()),
//!     l1_max_bytes: 128 * 1024 * 1024, // 128 MB
//!     batch_flush_count: 100,
//!     batch_flush_ms: 50,
//!     ..Default::default()
//! };
//! ```

use serde::Deserialize;

/// Configuration for the sync engine.
///
/// All fields have sensible defaults. At minimum, you should configure
/// `redis_url` and `sql_url` for production use.
#[derive(Debug, Clone, Deserialize)]
pub struct SyncEngineConfig {
    /// Redis connection string (e.g., "redis://localhost:6379")
    #[serde(default)]
    pub redis_url: Option<String>,
    
    /// SQL connection string (e.g., "sqlite:sync.db" or "mysql://user:pass@host/db")
    #[serde(default)]
    pub sql_url: Option<String>,
    
    /// L1 cache max size in bytes (default: 256 MB)
    #[serde(default = "default_l1_max_bytes")]
    pub l1_max_bytes: usize,
    
    /// Backpressure thresholds
    #[serde(default = "default_backpressure_warn")]
    pub backpressure_warn: f64,
    #[serde(default = "default_backpressure_critical")]
    pub backpressure_critical: f64,
    
    /// Batch flush settings
    #[serde(default = "default_batch_flush_ms")]
    pub batch_flush_ms: u64,
    #[serde(default = "default_batch_flush_count")]
    pub batch_flush_count: usize,
    #[serde(default = "default_batch_flush_bytes")]
    pub batch_flush_bytes: usize,
    
    /// CRDT retention
    #[serde(default = "default_retention_full_days")]
    pub retention_full_days: u32,
    #[serde(default = "default_retention_compact_days")]
    pub retention_compact_days: u32,
    
    /// Cuckoo filter warmup
    #[serde(default = "default_cuckoo_warmup_batch_size")]
    pub cuckoo_warmup_batch_size: usize,
    
    /// WAL path (SQLite file for durability during MySQL outages)
    #[serde(default)]
    pub wal_path: Option<String>,
    
    /// WAL max items before backpressure
    #[serde(default)]
    pub wal_max_items: Option<u64>,
    
    /// WAL drain batch size
    #[serde(default = "default_wal_drain_batch_size")]
    pub wal_drain_batch_size: usize,
    
    /// CF snapshot interval in seconds (0 = disabled)
    #[serde(default = "default_cf_snapshot_interval_secs")]
    pub cf_snapshot_interval_secs: u64,
    
    /// CF snapshot after N inserts (0 = disabled)
    #[serde(default = "default_cf_snapshot_insert_threshold")]
    pub cf_snapshot_insert_threshold: u64,
}

fn default_l1_max_bytes() -> usize { 256 * 1024 * 1024 } // 256 MB
fn default_backpressure_warn() -> f64 { 0.7 }
fn default_backpressure_critical() -> f64 { 0.9 }
fn default_batch_flush_ms() -> u64 { 100 }
fn default_batch_flush_count() -> usize { 1000 }
fn default_batch_flush_bytes() -> usize { 1024 * 1024 } // 1 MB
fn default_retention_full_days() -> u32 { 7 }
fn default_retention_compact_days() -> u32 { 90 }
fn default_cuckoo_warmup_batch_size() -> usize { 10000 }
fn default_wal_drain_batch_size() -> usize { 100 }
fn default_cf_snapshot_interval_secs() -> u64 { 30 }
fn default_cf_snapshot_insert_threshold() -> u64 { 10_000 }

impl Default for SyncEngineConfig {
    fn default() -> Self {
        Self {
            redis_url: None,
            sql_url: None,
            l1_max_bytes: default_l1_max_bytes(),
            backpressure_warn: default_backpressure_warn(),
            backpressure_critical: default_backpressure_critical(),
            batch_flush_ms: default_batch_flush_ms(),
            batch_flush_count: default_batch_flush_count(),
            batch_flush_bytes: default_batch_flush_bytes(),
            retention_full_days: default_retention_full_days(),
            retention_compact_days: default_retention_compact_days(),
            cuckoo_warmup_batch_size: default_cuckoo_warmup_batch_size(),
            wal_path: None,
            wal_max_items: None,
            wal_drain_batch_size: default_wal_drain_batch_size(),
            cf_snapshot_interval_secs: default_cf_snapshot_interval_secs(),
            cf_snapshot_insert_threshold: default_cf_snapshot_insert_threshold(),
        }
    }
}
