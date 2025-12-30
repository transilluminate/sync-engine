//! Sync engine coordinator.
//!
//! The [`SyncEngine`] is the main orchestrator that ties together all components:
//! - L1 in-memory cache with eviction
//! - L2 Redis cache with batch writes
//! - L3 MySQL/SQLite archive with WAL durability
//! - Cuckoo filters for existence checks
//! - Merkle trees for sync verification
//!
//! # Lifecycle
//!
//! ```text
//! Created → Connecting → DrainingWal → SyncingRedis → WarmingUp → Ready → Running → ShuttingDown
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use sync_engine::{SyncEngine, SyncEngineConfig, SyncItem, EngineState};
//! use serde_json::json;
//! use tokio::sync::watch;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let config = SyncEngineConfig::default();
//! let (_tx, rx) = watch::channel(config.clone());
//! let mut engine = SyncEngine::new(config, rx);
//!
//! assert_eq!(engine.state(), EngineState::Created);
//!
//! // engine.start().await.expect("Start failed");
//! // assert!(engine.is_ready());
//! # }
//! ```

mod types;
mod api;
mod lifecycle;
mod flush;

pub use types::{EngineState, ItemStatus, BatchResult};
#[allow(unused_imports)]
use types::WriteTarget;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::time::Instant;
use dashmap::DashMap;
use tokio::sync::{watch, Mutex};
use tracing::{info, warn, debug, error};

use crate::config::SyncEngineConfig;
use crate::sync_item::SyncItem;
use crate::submit_options::SubmitOptions;
use crate::backpressure::BackpressureLevel;
use crate::storage::traits::{CacheStore, ArchiveStore, StorageError};
use crate::cuckoo::filter_manager::{FilterManager, FilterTrust};
use crate::cuckoo::FilterPersistence;
use crate::batching::hybrid_batcher::{HybridBatcher, BatchConfig, SizedItem};
use crate::merkle::{RedisMerkleStore, SqlMerkleStore, MerkleBatch};
use crate::resilience::wal::{WriteAheadLog, MysqlHealthChecker};
use crate::eviction::tan_curve::{TanCurvePolicy, CacheEntry};

/// Main sync engine coordinator.
///
/// Manages the three-tier storage architecture:
/// - **L1**: In-memory DashMap with pressure-based eviction
/// - **L2**: Redis cache with batch writes
/// - **L3**: MySQL/SQLite archive (ground truth)
///
/// # Thread Safety
///
/// The engine is `Send + Sync` and designed for concurrent access.
/// Internal state uses atomic operations and concurrent data structures.
pub struct SyncEngine {
    /// Configuration (can be updated at runtime via watch channel)
    pub(super) config: SyncEngineConfig,

    /// Runtime config updates
    #[allow(dead_code)]
    pub(super) config_rx: watch::Receiver<SyncEngineConfig>,

    /// Engine state (broadcast to watchers)
    pub(super) state: watch::Sender<EngineState>,

    /// Engine state receiver (for internal use)
    pub(super) state_rx: watch::Receiver<EngineState>,

    /// L1: In-memory cache
    pub(super) l1_cache: Arc<DashMap<String, SyncItem>>,

    /// L1 size tracking (bytes)
    pub(super) l1_size_bytes: Arc<AtomicUsize>,

    /// L2: Redis cache (optional)
    pub(super) l2_store: Option<Arc<dyn CacheStore>>,

    /// L3: MySQL/SQLite archive (optional)
    pub(super) l3_store: Option<Arc<dyn ArchiveStore>>,

    /// L3 Cuckoo filter (L2 has no filter - TTL makes it unreliable)
    pub(super) l3_filter: Arc<FilterManager>,

    /// Filter persistence (for fast startup)
    pub(super) filter_persistence: Option<FilterPersistence>,

    /// CF snapshot tracking
    pub(super) cf_inserts_since_snapshot: AtomicU64,
    pub(super) cf_last_snapshot: Mutex<Instant>,

    /// Hybrid batcher for L2 writes
    pub(super) l2_batcher: Mutex<HybridBatcher<SyncItem>>,

    /// Redis merkle store
    pub(super) redis_merkle: Option<RedisMerkleStore>,

    /// SQL merkle store
    pub(super) sql_merkle: Option<SqlMerkleStore>,

    /// Write-ahead log for L3 durability
    pub(super) l3_wal: Option<WriteAheadLog>,

    /// MySQL health checker
    pub(super) mysql_health: MysqlHealthChecker,

    /// Eviction policy
    pub(super) eviction_policy: TanCurvePolicy,
}

impl SyncEngine {
    /// Create a new sync engine.
    ///
    /// The engine starts in `Created` state. Call [`start()`](Self::start)
    /// to connect to backends and transition to `Ready`.
    pub fn new(config: SyncEngineConfig, config_rx: watch::Receiver<SyncEngineConfig>) -> Self {
        let (state_tx, state_rx) = watch::channel(EngineState::Created);

        let batch_config = BatchConfig {
            flush_ms: config.batch_flush_ms,
            flush_count: config.batch_flush_count,
            flush_bytes: config.batch_flush_bytes,
        };

        Self {
            config: config.clone(),
            config_rx,
            state: state_tx,
            state_rx,
            l1_cache: Arc::new(DashMap::new()),
            l1_size_bytes: Arc::new(AtomicUsize::new(0)),
            l2_store: None,
            l3_store: None,
            l3_filter: Arc::new(FilterManager::new("sync-engine-l3", 100_000)),
            filter_persistence: None,
            cf_inserts_since_snapshot: AtomicU64::new(0),
            cf_last_snapshot: Mutex::new(Instant::now()),
            l2_batcher: Mutex::new(HybridBatcher::new(batch_config)),
            redis_merkle: None,
            sql_merkle: None,
            l3_wal: None,
            mysql_health: MysqlHealthChecker::new(),
            eviction_policy: TanCurvePolicy::default(),
        }
    }

    /// Get current engine state.
    #[must_use]
    pub fn state(&self) -> EngineState {
        *self.state_rx.borrow()
    }

    /// Get a receiver to watch state changes.
    #[must_use]
    pub fn state_receiver(&self) -> watch::Receiver<EngineState> {
        self.state_rx.clone()
    }

    /// Check if engine is ready to accept requests.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self.state(), EngineState::Ready | EngineState::Running)
    }

    /// Get current memory pressure (0.0 - 1.0+).
    #[must_use]
    pub fn memory_pressure(&self) -> f64 {
        let used = self.l1_size_bytes.load(Ordering::Acquire);
        let max = self.config.l1_max_bytes;
        if max == 0 {
            0.0
        } else {
            used as f64 / max as f64
        }
    }

    /// Get current backpressure level.
    #[must_use]
    pub fn pressure(&self) -> BackpressureLevel {
        BackpressureLevel::from_pressure(self.memory_pressure())
    }

    /// Check if the engine should accept writes (based on pressure).
    #[must_use]
    pub fn should_accept_writes(&self) -> bool {
        let pressure = self.pressure();
        !matches!(pressure, BackpressureLevel::Emergency | BackpressureLevel::Shutdown)
    }

    // --- Core CRUD Operations ---

    /// Get an item by ID.
    ///
    /// Checks storage tiers in order: L1 → L2 → L3.
    /// Updates access count and promotes to L1 on hit.
    #[tracing::instrument(skip(self), fields(tier))]
    pub async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        // 1. Check L1 (in-memory)
        if let Some(mut item) = self.l1_cache.get_mut(id) {
            item.access_count = item.access_count.saturating_add(1);
            item.last_accessed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            tracing::Span::current().record("tier", "L1");
            debug!("L1 hit");
            return Ok(Some(item.clone()));
        }

        // 2. Try L2 (Redis) - no filter, just try it
        if let Some(ref l2) = self.l2_store {
            match l2.get(id).await {
                Ok(Some(item)) => {
                    // Promote to L1
                    self.insert_l1(item.clone());
                    tracing::Span::current().record("tier", "L2");
                    debug!("L2 hit, promoted to L1");
                    return Ok(Some(item));
                }
                Ok(None) => {
                    // Not in Redis
                    debug!("L2 miss");
                }
                Err(e) => {
                    warn!(error = %e, "L2 lookup failed");
                }
            }
        }

        // 3. Check L3 filter before hitting MySQL
        if self.l3_filter.should_check_l3(id) {
            if let Some(ref l3) = self.l3_store {
                match l3.get(id).await {
                    Ok(Some(item)) => {
                        // Promote to L1
                        if self.memory_pressure() < 1.0 {
                            self.insert_l1(item.clone());
                        }
                        tracing::Span::current().record("tier", "L3");
                        debug!("L3 hit, promoted to L1");
                        return Ok(Some(item));
                    }
                    Ok(None) => {
                        // False positive in filter
                        debug!("L3 filter false positive");
                    }
                    Err(e) => {
                        warn!(error = %e, "L3 lookup failed");
                    }
                }
            }
        }

        tracing::Span::current().record("tier", "miss");
        debug!("Cache miss");
        Ok(None)
    }

    /// Get an item with hash verification.
    ///
    /// If the item has a non-empty `merkle_root`, the content hash is verified.
    /// Returns `StorageError::Corruption` if the hash doesn't match.
    #[tracing::instrument(skip(self), fields(verified))]
    pub async fn get_verified(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        let item = match self.get(id).await? {
            Some(item) => item,
            None => return Ok(None),
        };

        // Verify hash if item has merkle_root set
        if !item.merkle_root.is_empty() {
            use sha2::{Sha256, Digest};
            
            let computed = Sha256::digest(&item.content);
            let computed_hex = hex::encode(computed);
            
            if computed_hex != item.merkle_root {
                tracing::Span::current().record("verified", false);
                warn!(
                    id = %id,
                    expected = %item.merkle_root,
                    actual = %computed_hex,
                    "Data corruption detected!"
                );
                
                // Record corruption metric
                crate::metrics::record_corruption(id);
                
                return Err(StorageError::Corruption {
                    id: id.to_string(),
                    expected: item.merkle_root.clone(),
                    actual: computed_hex,
                });
            }
            
            tracing::Span::current().record("verified", true);
            debug!(id = %id, "Hash verification passed");
        }

        Ok(Some(item))
    }

    /// Submit an item for sync.
    ///
    /// The item is immediately stored in L1 and queued for batch write to L2/L3.
    /// Uses default options: Redis + SQL (both enabled).
    /// Filters are updated only on successful writes in flush_batch_internal().
    ///
    /// For custom routing, use [`submit_with`](Self::submit_with).
    #[tracing::instrument(skip(self, item), fields(object_id = %item.object_id))]
    pub async fn submit(&self, item: SyncItem) -> Result<(), StorageError> {
        self.submit_with(item, SubmitOptions::default()).await
    }

    /// Submit an item with custom routing options.
    ///
    /// The item is immediately stored in L1 and queued for batch write.
    /// Items are batched by compatible options for efficient pipelined writes.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, SyncItem, SubmitOptions, CacheTtl};
    /// # async fn example(engine: &SyncEngine) -> Result<(), sync_engine::StorageError> {
    /// // Cache-only with 1 minute TTL (no SQL write)
    /// let item = SyncItem::new("cache.key".into(), b"data".to_vec());
    /// engine.submit_with(item, SubmitOptions::cache(CacheTtl::Minute)).await?;
    ///
    /// // SQL-only durable storage (no Redis)
    /// let item = SyncItem::new("archive.key".into(), b"data".to_vec());
    /// engine.submit_with(item, SubmitOptions::durable()).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(skip(self, item, options), fields(object_id = %item.object_id, redis = options.redis, sql = options.sql))]
    pub async fn submit_with(&self, mut item: SyncItem, options: SubmitOptions) -> Result<(), StorageError> {
        if !self.should_accept_writes() {
            return Err(StorageError::Backend(format!(
                "Rejecting write: engine state={}, pressure={}",
                self.state(),
                self.pressure()
            )));
        }

        let id = item.object_id.clone();

        // Attach options to item (travels through batch pipeline)
        item.submit_options = Some(options);

        // Insert into L1 (immediate, in-memory)
        self.insert_l1(item.clone());
        
        // NOTE: We do NOT insert into L2/L3 filters here!
        // Filters are updated only on SUCCESSFUL writes in flush_batch_internal()
        // This prevents filter/storage divergence if writes fail.
        
        // Queue for batched L2/L3 persistence
        self.l2_batcher.lock().await.add(item);

        debug!(id = %id, "Item submitted to L1 and batch queue");
        Ok(())
    }

    /// Delete an item from all storage tiers.
    /// 
    /// Deletes are more complex than writes because the item may exist in:
    /// - L1 (DashMap) - immediate removal
    /// - L2 (Redis) - async removal
    /// - L3 (MySQL) - async removal  
    /// - Cuckoo filters (L2/L3) - remove from both
    /// - Merkle trees - update with deletion marker
    #[tracing::instrument(skip(self), fields(object_id = %id))]
    pub async fn delete(&self, id: &str) -> Result<bool, StorageError> {
        if !self.should_accept_writes() {
            return Err(StorageError::Backend(format!(
                "Rejecting delete: engine state={}, pressure={}",
                self.state(),
                self.pressure()
            )));
        }

        let mut found = false;

        // 1. Remove from L1 (immediate)
        if let Some((_, item)) = self.l1_cache.remove(id) {
            let size = Self::item_size(&item);
            self.l1_size_bytes.fetch_sub(size, Ordering::Release);
            found = true;
            debug!("Deleted from L1");
        }

        // 2. Remove from L3 cuckoo filter only (no L2 filter - TTL makes it unreliable)
        self.l3_filter.remove(id);

        // 3. Delete from L2 (Redis) - best effort
        if let Some(ref l2) = self.l2_store {
            match l2.delete(id).await {
                Ok(()) => {
                    found = true;
                    debug!("Deleted from L2 (Redis)");
                }
                Err(e) => {
                    warn!(error = %e, "Failed to delete from L2 (Redis)");
                }
            }
        }

        // 4. Delete from L3 (MySQL) - ground truth
        if let Some(ref l3) = self.l3_store {
            match l3.delete(id).await {
                Ok(()) => {
                    found = true;
                    debug!("Deleted from L3 (MySQL)");
                }
                Err(e) => {
                    error!(error = %e, "Failed to delete from L3 (MySQL)");
                    // Don't return error - item may not exist in L3
                }
            }
        }

        // 5. Update merkle trees with deletion marker
        let mut merkle_batch = MerkleBatch::new();
        merkle_batch.delete(id.to_string());

        if let Some(ref sql_merkle) = self.sql_merkle {
            if let Err(e) = sql_merkle.apply_batch(&merkle_batch).await {
                error!(error = %e, "Failed to update SQL Merkle tree for deletion");
            }
        }

        if let Some(ref redis_merkle) = self.redis_merkle {
            if let Err(e) = redis_merkle.apply_batch(&merkle_batch).await {
                warn!(error = %e, "Failed to update Redis Merkle tree for deletion");
            }
        }

        info!(found, "Delete operation completed");
        Ok(found)
    }

    // --- Internal helpers ---

    /// Insert or update an item in L1, correctly tracking size.
    fn insert_l1(&self, item: SyncItem) {
        let new_size = Self::item_size(&item);
        let key = item.object_id.clone();
        
        // Use entry API to handle insert vs update atomically
        if let Some(old_item) = self.l1_cache.insert(key, item) {
            // Update: subtract old size, add new size
            let old_size = Self::item_size(&old_item);
            // Use wrapping operations to avoid underflow if sizes are estimated
            let current = self.l1_size_bytes.load(Ordering::Acquire);
            let new_total = current.saturating_sub(old_size).saturating_add(new_size);
            self.l1_size_bytes.store(new_total, Ordering::Release);
        } else {
            // Insert: just add new size
            self.l1_size_bytes.fetch_add(new_size, Ordering::Release);
        }
    }

    /// Calculate approximate size of an item in bytes.
    #[inline]
    fn item_size(item: &SyncItem) -> usize {
        // Use cached size if available, otherwise compute
        item.size_bytes()
    }

    fn maybe_evict(&self) {
        let pressure = self.memory_pressure();
        if pressure < self.config.backpressure_warn {
            return;
        }

        let level = BackpressureLevel::from_pressure(pressure);
        debug!(pressure = %pressure, level = %level, "Memory pressure detected, running eviction");
        
        // Collect cache entries for scoring
        let now = std::time::Instant::now();
        let entries: Vec<CacheEntry> = self.l1_cache.iter()
            .map(|ref_multi| {
                let item = ref_multi.value();
                let id = ref_multi.key().clone();
                
                // Convert epoch millis to Instant-relative age
                let now_millis = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let age_secs = if item.last_accessed > 0 {
                    (now_millis.saturating_sub(item.last_accessed)) as f64 / 1000.0
                } else {
                    3600.0 // Default 1 hour if never accessed
                };
                
                CacheEntry {
                    id,
                    size_bytes: item.size_bytes(),
                    created_at: now - std::time::Duration::from_secs_f64(age_secs),
                    last_access: now - std::time::Duration::from_secs_f64(age_secs),
                    access_count: item.access_count,
                    is_dirty: false, // All items in L1 are assumed flushed to L2/L3
                }
            })
            .collect();
        
        if entries.is_empty() {
            return;
        }
        
        // Calculate how many to evict based on pressure level
        let evict_count = match level {
            BackpressureLevel::Normal => 0,
            BackpressureLevel::Warn => entries.len() / 20,    // 5%
            BackpressureLevel::Throttle => entries.len() / 10, // 10%
            BackpressureLevel::Critical => entries.len() / 5,  // 20%
            BackpressureLevel::Emergency => entries.len() / 3, // 33%
            BackpressureLevel::Shutdown => entries.len() / 2,  // 50%
        }.max(1);
        
        // Select victims using tan curve algorithm
        let victims = self.eviction_policy.select_victims(&entries, evict_count, pressure);
        
        // Evict victims
        let mut evicted_bytes = 0usize;
        for victim_id in &victims {
            if let Some((_, item)) = self.l1_cache.remove(victim_id) {
                evicted_bytes += item.size_bytes();
            }
        }
        
        // Update size tracking
        self.l1_size_bytes.fetch_sub(evicted_bytes, Ordering::Release);
        
        info!(
            evicted = victims.len(),
            evicted_bytes = evicted_bytes,
            pressure = %pressure,
            level = %level,
            "Evicted entries from L1 cache"
        );
    }

    /// Get L1 cache stats
    pub fn l1_stats(&self) -> (usize, usize) {
        (
            self.l1_cache.len(),
            self.l1_size_bytes.load(Ordering::Acquire),
        )
    }

    /// Get L3 filter stats (entries, capacity, trust_state)
    #[must_use]
    pub fn l3_filter_stats(&self) -> (usize, usize, FilterTrust) {
        self.l3_filter.stats()
    }

    /// Get access to the L3 filter (for warmup/verification)
    pub fn l3_filter(&self) -> &Arc<FilterManager> {
        &self.l3_filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SyncEngineConfig;
    use tokio::sync::watch;
    use serde_json::json;

    fn create_test_engine() -> SyncEngine {
        let config = SyncEngineConfig::default();
        let (_tx, rx) = watch::channel(config.clone());
        SyncEngine::new(config, rx)
    }

    fn create_test_item(id: &str) -> SyncItem {
        SyncItem::from_json(
            id.to_string(),
            json!({"data": "test"}),
        )
    }

    #[test]
    fn test_engine_created_state() {
        let engine = create_test_engine();
        assert_eq!(engine.state(), EngineState::Created);
        assert!(!engine.is_ready());
    }

    #[test]
    fn test_memory_pressure_calculation() {
        let config = SyncEngineConfig {
            l1_max_bytes: 1000,
            ..Default::default()
        };
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);

        assert_eq!(engine.memory_pressure(), 0.0);

        // Simulate adding items
        let item = create_test_item("test1");
        engine.insert_l1(item);

        // Pressure should be > 0 now
        assert!(engine.memory_pressure() > 0.0);
    }

    #[test]
    fn test_l1_insert_and_size_tracking() {
        let engine = create_test_engine();
        
        let item = create_test_item("test1");
        let expected_size = item.size_bytes();
        
        engine.insert_l1(item);
        
        let (count, size) = engine.l1_stats();
        assert_eq!(count, 1);
        assert_eq!(size, expected_size);
    }

    #[test]
    fn test_l1_update_size_tracking() {
        let engine = create_test_engine();
        
        let item1 = create_test_item("test1");
        engine.insert_l1(item1);
        let (_, _size1) = engine.l1_stats();
        
        // Insert larger item with same ID
        let item2 = SyncItem::from_json(
            "test1".to_string(),
            json!({"data": "much larger content here for testing size changes"}),
        );
        let size2_expected = item2.size_bytes();
        engine.insert_l1(item2);
        
        let (count, size2) = engine.l1_stats();
        assert_eq!(count, 1); // Still one item
        assert_eq!(size2, size2_expected); // Size should be updated
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let engine = create_test_engine();
        let result = engine.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_from_l1() {
        let engine = create_test_engine();
        let item = create_test_item("test1");
        engine.insert_l1(item.clone());

        let result = engine.get("test1").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().object_id, "test1");
    }

    #[tokio::test]
    async fn test_delete_from_l1() {
        let engine = create_test_engine();
        let item = create_test_item("test1");
        engine.insert_l1(item);

        let (count_before, _) = engine.l1_stats();
        assert_eq!(count_before, 1);

        let deleted = engine.delete("test1").await.unwrap();
        assert!(deleted);

        let (count_after, size_after) = engine.l1_stats();
        assert_eq!(count_after, 0);
        assert_eq!(size_after, 0);
    }

    #[test]
    fn test_filter_stats() {
        let engine = create_test_engine();
        
        let (entries, capacity, _trust) = engine.l3_filter_stats();
        assert_eq!(entries, 0);
        assert!(capacity > 0);
    }

    #[test]
    fn test_should_accept_writes() {
        let engine = create_test_engine();
        assert!(engine.should_accept_writes());
    }

    #[test]
    fn test_pressure_level() {
        let engine = create_test_engine();
        assert_eq!(engine.pressure(), BackpressureLevel::Normal);
    }
}
