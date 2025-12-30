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
    
    /// Redis key prefix for namespacing (e.g., "myapp:" → keys become "myapp:user.alice")
    /// Allows sync-engine to coexist with other data in the same Redis instance.
    #[serde(default)]
    pub redis_prefix: Option<String>,
    
    /// SQL connection string (e.g., "sqlite:sync.db" or "mysql://user:pass@host/db")
    #[serde(default)]
    pub sql_url: Option<String>,
    
    /// L1 cache max size in bytes (default: 256 MB)
    #[serde(default = "default_l1_max_bytes")]
    pub l1_max_bytes: usize,
    
    /// Maximum payload size in bytes (default: 16 MB)
    /// 
    /// Payloads larger than this will be rejected with an error.
    /// This prevents a single large item (e.g., 1TB file) from exhausting
    /// the L1 cache. Set to 0 for unlimited (not recommended).
    /// 
    /// **Important**: This is a safety limit. Developers should choose a value
    /// appropriate for their use case. For binary blobs, consider using
    /// external object storage (S3, GCS) and storing only references here.
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,
    
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
    
    /// Redis eviction: enable proactive eviction before Redis LRU kicks in
    #[serde(default = "default_redis_eviction_enabled")]
    pub redis_eviction_enabled: bool,
    
    /// Redis eviction: pressure threshold to start evicting (0.0-1.0, default: 0.75)
    #[serde(default = "default_redis_eviction_start")]
    pub redis_eviction_start: f64,
    
    /// Redis eviction: target pressure after eviction (0.0-1.0, default: 0.60)
    #[serde(default = "default_redis_eviction_target")]
    pub redis_eviction_target: f64,
}

fn default_l1_max_bytes() -> usize { 256 * 1024 * 1024 } // 256 MB
fn default_max_payload_bytes() -> usize { 16 * 1024 * 1024 } // 16 MB
fn default_backpressure_warn() -> f64 { 0.7 }
fn default_backpressure_critical() -> f64 { 0.9 }
fn default_batch_flush_ms() -> u64 { 100 }
fn default_batch_flush_count() -> usize { 1000 }
fn default_batch_flush_bytes() -> usize { 1024 * 1024 } // 1 MB
fn default_cuckoo_warmup_batch_size() -> usize { 10000 }
fn default_wal_drain_batch_size() -> usize { 100 }
fn default_cf_snapshot_interval_secs() -> u64 { 30 }
fn default_cf_snapshot_insert_threshold() -> u64 { 10_000 }
fn default_redis_eviction_enabled() -> bool { true }
fn default_redis_eviction_start() -> f64 { 0.75 }
fn default_redis_eviction_target() -> f64 { 0.60 }

impl Default for SyncEngineConfig {
    fn default() -> Self {
        Self {
            redis_url: None,
            sql_url: None,
            redis_prefix: None,
            l1_max_bytes: default_l1_max_bytes(),
            max_payload_bytes: default_max_payload_bytes(),
            backpressure_warn: default_backpressure_warn(),
            backpressure_critical: default_backpressure_critical(),
            batch_flush_ms: default_batch_flush_ms(),
            batch_flush_count: default_batch_flush_count(),
            batch_flush_bytes: default_batch_flush_bytes(),
            cuckoo_warmup_batch_size: default_cuckoo_warmup_batch_size(),
            wal_path: None,
            wal_max_items: None,
            wal_drain_batch_size: default_wal_drain_batch_size(),
            cf_snapshot_interval_secs: default_cf_snapshot_interval_secs(),
            cf_snapshot_insert_threshold: default_cf_snapshot_insert_threshold(),
            redis_eviction_enabled: default_redis_eviction_enabled(),
            redis_eviction_start: default_redis_eviction_start(),
            redis_eviction_target: default_redis_eviction_target(),
        }
    }
}
