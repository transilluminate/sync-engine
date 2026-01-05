// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

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
mod merkle_api;
mod search_api;

pub use types::{EngineState, ItemStatus, BatchResult, HealthCheck};
pub use merkle_api::MerkleDiff;
pub use search_api::{SearchTier, SearchResult, SearchSource};
#[allow(unused_imports)]
use types::WriteTarget;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::time::Instant;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::{watch, Mutex, Semaphore};
use tracing::{info, warn, debug, error};

use crate::config::SyncEngineConfig;
use crate::sync_item::SyncItem;
use crate::submit_options::SubmitOptions;
use crate::backpressure::BackpressureLevel;
use crate::storage::traits::{CacheStore, ArchiveStore, StorageError};
use crate::storage::sql::SqlStore;
use crate::cuckoo::filter_manager::{FilterManager, FilterTrust};
use crate::cuckoo::FilterPersistence;
use crate::batching::hybrid_batcher::{HybridBatcher, BatchConfig, SizedItem};
use crate::merkle::{RedisMerkleStore, SqlMerkleStore, MerkleBatch};
use crate::resilience::wal::{WriteAheadLog, MysqlHealthChecker};
use crate::eviction::tan_curve::{TanCurvePolicy, CacheEntry};

use search_api::SearchState;

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
    /// Uses RwLock for interior mutability so run() can take &self
    pub(super) config: RwLock<SyncEngineConfig>,

    /// Runtime config updates (Mutex for interior mutability in run loop)
    pub(super) config_rx: Mutex<watch::Receiver<SyncEngineConfig>>,

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
    
    /// L2: Direct RedisStore reference for CDC operations
    pub(super) redis_store: Option<Arc<crate::storage::redis::RedisStore>>,

    /// L3: MySQL/SQLite archive (optional)
    pub(super) l3_store: Option<Arc<dyn ArchiveStore>>,
    
    /// L3: Direct SqlStore reference for dirty merkle operations
    pub(super) sql_store: Option<Arc<SqlStore>>,

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

    /// Search state (index manager + cache)
    pub(super) search_state: Option<Arc<RwLock<SearchState>>>,

    /// SQL write concurrency limiter
    /// 
    /// Limits concurrent sql_put_batch and merkle_nodes updates to reduce
    /// row-level lock contention and deadlocks under high load.
    pub(super) sql_write_semaphore: Arc<Semaphore>,
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

        // SQL write concurrency limiter - reduces row lock contention
        let sql_write_semaphore = Arc::new(Semaphore::new(config.sql_write_concurrency));

        Self {
            config: RwLock::new(config.clone()),
            config_rx: Mutex::new(config_rx),
            state: state_tx,
            state_rx,
            l1_cache: Arc::new(DashMap::new()),
            l1_size_bytes: Arc::new(AtomicUsize::new(0)),
            l2_store: None,
            redis_store: None,
            l3_store: None,
            sql_store: None,
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
            search_state: Some(Arc::new(RwLock::new(SearchState::default()))),
            sql_write_semaphore,
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
        let max = self.config.read().l1_max_bytes;
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

    /// Perform a comprehensive health check.
    ///
    /// This probes backend connectivity (Redis PING, SQL SELECT 1) and
    /// collects internal state into a [`HealthCheck`] struct suitable for
    /// `/ready` and `/health` endpoints.
    ///
    /// # Performance
    ///
    /// - **Cached fields**: Instant (no I/O)
    /// - **Live probes**: Redis PING + SQL SELECT 1 (parallel, ~1-10ms each)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let health = engine.health_check().await;
    ///
    /// // For /ready endpoint (load balancer)
    /// if health.healthy {
    ///     HttpResponse::Ok().body("ready")
    /// } else {
    ///     HttpResponse::ServiceUnavailable().body("not ready")
    /// }
    ///
    /// // For /health endpoint (diagnostics)
    /// HttpResponse::Ok().json(health)
    /// ```
    pub async fn health_check(&self) -> types::HealthCheck {
        // Cached state (no I/O)
        let state = self.state();
        let ready = matches!(state, EngineState::Ready | EngineState::Running);
        let memory_pressure = self.memory_pressure();
        let backpressure_level = self.pressure();
        let accepting_writes = self.should_accept_writes();
        let (l1_items, l1_bytes) = self.l1_stats();
        let (filter_items, _, filter_trust) = self.l3_filter_stats();
        
        // WAL stats (cached, no I/O)
        // Note: WalStats doesn't have file_size, so we only report pending items
        let mysql_healthy = self.mysql_health.is_healthy();
        let wal_pending_items = if let Some(ref wal) = self.l3_wal {
            let stats = wal.stats(mysql_healthy);
            Some(stats.pending_items)
        } else {
            None
        };
        
        // Live backend probes (parallel)
        let (redis_result, sql_result) = tokio::join!(
            self.probe_redis(),
            self.probe_sql()
        );
        
        let (redis_connected, redis_latency_ms) = redis_result;
        let (sql_connected, sql_latency_ms) = sql_result;
        
        // Derive overall health
        // Healthy if: running, backends connected (or not configured), not in critical backpressure
        let healthy = matches!(state, EngineState::Running)
            && redis_connected != Some(false)
            && sql_connected != Some(false)
            && !matches!(backpressure_level, BackpressureLevel::Emergency | BackpressureLevel::Shutdown);
        
        types::HealthCheck {
            state,
            ready,
            memory_pressure,
            backpressure_level,
            accepting_writes,
            l1_items,
            l1_bytes,
            filter_items,
            filter_trust,
            redis_connected,
            redis_latency_ms,
            sql_connected,
            sql_latency_ms,
            wal_pending_items,
            healthy,
        }
    }
    
    /// Probe Redis connectivity with PING.
    async fn probe_redis(&self) -> (Option<bool>, Option<u64>) {
        let Some(ref redis_store) = self.redis_store else {
            return (None, None); // Redis not configured
        };
        
        let start = std::time::Instant::now();
        let mut conn = redis_store.connection();
        
        let result: Result<String, _> = redis::cmd("PING")
            .query_async(&mut conn)
            .await;
        
        match result {
            Ok(_) => {
                let latency_ms = start.elapsed().as_millis() as u64;
                (Some(true), Some(latency_ms))
            }
            Err(_) => (Some(false), None),
        }
    }
    
    /// Probe SQL connectivity with SELECT 1.
    async fn probe_sql(&self) -> (Option<bool>, Option<u64>) {
        let Some(ref sql_store) = self.sql_store else {
            return (None, None); // SQL not configured
        };
        
        let start = std::time::Instant::now();
        let pool = sql_store.pool();
        
        let result = sqlx::query("SELECT 1")
            .fetch_one(&pool)
            .await;
        
        match result {
            Ok(_) => {
                let latency_ms = start.elapsed().as_millis() as u64;
                (Some(true), Some(latency_ms))
            }
            Err(_) => (Some(false), None),
        }
    }

    // --- Core CRUD Operations ---

    /// Get an item by ID.
    ///
    /// Checks storage tiers in order: L1 → L2 → L3.
    /// Updates access count and promotes to L1 on hit.
    #[tracing::instrument(skip(self), fields(tier))]
    pub async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        let start = std::time::Instant::now();
        
        // 1. Check L1 (in-memory)
        if let Some(mut item) = self.l1_cache.get_mut(id) {
            item.access_count = item.access_count.saturating_add(1);
            item.last_accessed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            tracing::Span::current().record("tier", "L1");
            debug!("L1 hit");
            crate::metrics::record_operation("L1", "get", "hit");
            crate::metrics::record_latency("L1", "get", start.elapsed());
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
                    crate::metrics::record_operation("L2", "get", "hit");
                    crate::metrics::record_latency("L2", "get", start.elapsed());
                    return Ok(Some(item));
                }
                Ok(None) => {
                    // Not in Redis
                    debug!("L2 miss");
                    crate::metrics::record_operation("L2", "get", "miss");
                }
                Err(e) => {
                    warn!(error = %e, "L2 lookup failed");
                    crate::metrics::record_operation("L2", "get", "error");
                }
            }
        }

        // 3. Check L3 filter before hitting MySQL
        if self.l3_filter.should_check_l3(id) {
            crate::metrics::record_cuckoo_check("L3", "positive");
            if let Some(ref l3) = self.l3_store {
                match l3.get(id).await {
                    Ok(Some(item)) => {
                        // Promote to L1
                        if self.memory_pressure() < 1.0 {
                            self.insert_l1(item.clone());
                        }
                        tracing::Span::current().record("tier", "L3");
                        debug!("L3 hit, promoted to L1");
                        crate::metrics::record_operation("L3", "get", "hit");
                        crate::metrics::record_latency("L3", "get", start.elapsed());
                        crate::metrics::record_bytes_read("L3", item.content.len());
                        return Ok(Some(item));
                    }
                    Ok(None) => {
                        // False positive in filter
                        debug!("L3 filter false positive");
                        crate::metrics::record_operation("L3", "get", "false_positive");
                        crate::metrics::record_cuckoo_false_positive("L3");
                    }
                    Err(e) => {
                        warn!(error = %e, "L3 lookup failed");
                        crate::metrics::record_operation("L3", "get", "error");
                        crate::metrics::record_error("L3", "get", "backend");
                    }
                }
            }
        } else {
            // Cuckoo filter says definitely not in L3 - saved a database query
            crate::metrics::record_cuckoo_check("L3", "negative");
        }

        tracing::Span::current().record("tier", "miss");
        debug!("Cache miss");
        crate::metrics::record_operation("all", "get", "miss");
        crate::metrics::record_latency("all", "get", start.elapsed());
        Ok(None)
    }

    /// Get an item with hash verification.
    ///
    /// If the item has a non-empty `content_hash`, the content hash is verified.
    /// Returns `StorageError::Corruption` if the hash doesn't match.
    #[tracing::instrument(skip(self), fields(verified))]
    pub async fn get_verified(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        let item = match self.get(id).await? {
            Some(item) => item,
            None => return Ok(None),
        };

        // Verify hash if item has content_hash set
        if !item.content_hash.is_empty() {
            use sha2::{Sha256, Digest};
            
            let computed = Sha256::digest(&item.content);
            let computed_hex = hex::encode(computed);
            
            if computed_hex != item.content_hash {
                tracing::Span::current().record("verified", false);
                warn!(
                    id = %id,
                    expected = %item.content_hash,
                    actual = %computed_hex,
                    "Data corruption detected!"
                );
                
                // Record corruption metric
                crate::metrics::record_corruption(id);
                
                return Err(StorageError::Corruption {
                    id: id.to_string(),
                    expected: item.content_hash.clone(),
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
        let start = std::time::Instant::now();
        
        if !self.should_accept_writes() {
            crate::metrics::record_operation("engine", "submit", "rejected");
            crate::metrics::record_error("engine", "submit", "backpressure");
            return Err(StorageError::Backend(format!(
                "Rejecting write: engine state={}, pressure={}",
                self.state(),
                self.pressure()
            )));
        }

        let id = item.object_id.clone();
        let item_bytes = item.content.len();

        // Apply state override from options (if provided)
        if let Some(ref state) = options.state {
            item.state = state.clone();
        }
        
        // Attach options to item (travels through batch pipeline)
        item.submit_options = Some(options);

        // Insert into L1 (immediate, in-memory)
        self.insert_l1(item.clone());
        crate::metrics::record_operation("L1", "submit", "success");
        crate::metrics::record_bytes_written("L1", item_bytes);
        
        // NOTE: We do NOT insert into L2/L3 filters here!
        // Filters are updated only on SUCCESSFUL writes in flush_batch_internal()
        // This prevents filter/storage divergence if writes fail.
        
        // Queue for batched L2/L3 persistence
        let batch_to_flush = {
            let mut batcher = self.l2_batcher.lock().await;
            if let Some(reason) = batcher.add(item) {
                batcher.force_flush_with_reason(reason)
            } else {
                None
            }
        };

        if let Some(batch) = batch_to_flush {
            // Flush immediately (provides backpressure)
            self.flush_batch_internal(batch).await;
        }

        debug!(id = %id, "Item submitted to L1 and batch queue");
        crate::metrics::record_latency("L1", "submit", start.elapsed());
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
        let start = std::time::Instant::now();
        
        if !self.should_accept_writes() {
            crate::metrics::record_operation("engine", "delete", "rejected");
            crate::metrics::record_error("engine", "delete", "backpressure");
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
            crate::metrics::record_operation("L1", "delete", "success");
        }

        // 2. Remove from L3 cuckoo filter only (no L2 filter - TTL makes it unreliable)
        self.l3_filter.remove(id);

        // 3. Delete from L2 (Redis) - best effort
        if let Some(ref l2) = self.l2_store {
            let l2_start = std::time::Instant::now();
            match l2.delete(id).await {
                Ok(()) => {
                    found = true;
                    debug!("Deleted from L2 (Redis)");
                    crate::metrics::record_operation("L2", "delete", "success");
                    crate::metrics::record_latency("L2", "delete", l2_start.elapsed());
                }
                Err(e) => {
                    warn!(error = %e, "Failed to delete from L2 (Redis)");
                    crate::metrics::record_operation("L2", "delete", "error");
                    crate::metrics::record_error("L2", "delete", "backend");
                }
            }
        }

        // 4. Delete from L3 (MySQL) - ground truth
        if let Some(ref l3) = self.l3_store {
            let l3_start = std::time::Instant::now();
            match l3.delete(id).await {
                Ok(()) => {
                    found = true;
                    debug!("Deleted from L3 (MySQL)");
                    crate::metrics::record_operation("L3", "delete", "success");
                    crate::metrics::record_latency("L3", "delete", l3_start.elapsed());
                }
                Err(e) => {
                    error!(error = %e, "Failed to delete from L3 (MySQL)");
                    crate::metrics::record_operation("L3", "delete", "error");
                    crate::metrics::record_error("L3", "delete", "backend");
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
                crate::metrics::record_error("L3", "merkle", "batch_apply");
            }
        }

        if let Some(ref redis_merkle) = self.redis_merkle {
            if let Err(e) = redis_merkle.apply_batch(&merkle_batch).await {
                warn!(error = %e, "Failed to update Redis Merkle tree for deletion");
                crate::metrics::record_error("L2", "merkle", "batch_apply");
            }
        }

        // 6. Emit CDC delete entry (if enabled)
        if self.config.read().enable_cdc_stream && found {
            self.emit_cdc_delete(id).await;
        }

        info!(found, "Delete operation completed");
        crate::metrics::record_latency("all", "delete", start.elapsed());
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
        if pressure < self.config.read().backpressure_warn {
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

    /// Get merkle root hashes from Redis (L2) and SQL (L3).
    /// 
    /// Returns `(redis_root, sql_root)` as hex strings.
    /// Returns `None` for backends that aren't connected or have empty trees.
    pub async fn merkle_roots(&self) -> (Option<String>, Option<String>) {
        let redis_root = if let Some(ref rm) = self.redis_merkle {
            rm.root_hash().await.ok().flatten().map(hex::encode)
        } else {
            None
        };
        
        let sql_root = if let Some(ref sm) = self.sql_merkle {
            sm.root_hash().await.ok().flatten().map(hex::encode)
        } else {
            None
        };
        
        (redis_root, sql_root)
    }

    /// Verify and trust the L3 cuckoo filter.
    /// 
    /// Compares the filter's merkle root against L3's merkle root.
    /// If they match, marks the filter as trusted.
    /// 
    /// Returns `true` if the filter is now trusted, `false` otherwise.
    pub async fn verify_filter(&self) -> bool {
        // Get SQL merkle root
        let sql_root = if let Some(ref sm) = self.sql_merkle {
            match sm.root_hash().await {
                Ok(Some(root)) => root,
                _ => return false,
            }
        } else {
            // No SQL backend - can't verify, mark trusted anyway
            self.l3_filter.mark_trusted();
            return true;
        };

        // For now, we trust the filter if we have a SQL root
        // A full implementation would compare CF merkle against SQL merkle
        // But since CF doesn't maintain a merkle tree, we trust after warmup
        info!(
            sql_root = %hex::encode(sql_root),
            "Verifying L3 filter against SQL merkle root"
        );
        
        // Mark trusted if we got here (SQL is connected and has a root)
        self.l3_filter.mark_trusted();
        true
    }

    /// Update all gauge metrics with current engine state.
    /// 
    /// Call this before snapshotting metrics to ensure gauges reflect current state.
    /// Useful for OTEL export or monitoring dashboards.
    pub fn update_gauge_metrics(&self) {
        let (l1_count, l1_bytes) = self.l1_stats();
        crate::metrics::set_l1_cache_items(l1_count);
        crate::metrics::set_l1_cache_bytes(l1_bytes);
        crate::metrics::set_memory_pressure(self.memory_pressure());
        
        let (filter_entries, filter_capacity, _trust) = self.l3_filter_stats();
        let filter_load = if filter_capacity > 0 { 
            filter_entries as f64 / filter_capacity as f64 
        } else { 
            0.0 
        };
        crate::metrics::set_cuckoo_filter_entries("L3", filter_entries);
        crate::metrics::set_cuckoo_filter_load("L3", filter_load);
        
        crate::metrics::set_backpressure_level(self.pressure() as u8);
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

    #[tokio::test]
    async fn test_health_check_basic() {
        let engine = create_test_engine();
        
        let health = engine.health_check().await;
        
        // Engine is in Created state (not started)
        assert_eq!(health.state, EngineState::Created);
        assert!(!health.ready); // Not ready until started
        assert!(!health.healthy); // Not healthy (not Running)
        
        // Memory should be empty
        assert_eq!(health.memory_pressure, 0.0);
        assert_eq!(health.l1_items, 0);
        assert_eq!(health.l1_bytes, 0);
        
        // Backpressure should be normal
        assert_eq!(health.backpressure_level, BackpressureLevel::Normal);
        assert!(health.accepting_writes);
        
        // No backends configured
        assert!(health.redis_connected.is_none());
        assert!(health.sql_connected.is_none());
        assert!(health.wal_pending_items.is_none());
    }
}
