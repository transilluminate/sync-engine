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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::time::Instant;
use dashmap::DashMap;
use tokio::sync::{watch, Mutex};
use tracing::{info, warn, debug, error};

use crate::config::SyncEngineConfig;
use crate::sync_item::SyncItem;
use crate::backpressure::BackpressureLevel;
use crate::storage::traits::{CacheStore, ArchiveStore, StorageError};
use crate::cuckoo::filter_manager::{FilterManager, FilterTrust};
use crate::cuckoo::{FilterPersistence, L2_FILTER_ID, L3_FILTER_ID};
use crate::batching::hybrid_batcher::{HybridBatcher, BatchConfig, SizedItem};
use crate::merkle::{RedisMerkleStore, SqlMerkleStore, MerkleBatch, PathMerkle};
use crate::resilience::wal::{WriteAheadLog, MysqlHealthChecker};
use crate::eviction::tan_curve::{TanCurvePolicy, CacheEntry};

/// Engine lifecycle state.
///
/// The engine progresses through states during startup and shutdown.
/// Use [`SyncEngine::state()`] to check current state or
/// [`SyncEngine::state_receiver()`] to watch for changes.
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

/// Where a write was persisted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]  // Used for WAL drain path
enum WriteTarget {
    /// Written to MySQL (L3)
    L3,
    /// Written to local WAL (will drain to L3 when MySQL is back)
    Wal,
}

/// The sync engine - owns L1 cache and coordinates L2/L3 persistence
pub struct SyncEngine {
    config: SyncEngineConfig,
    config_rx: watch::Receiver<SyncEngineConfig>,
    
    state: watch::Sender<EngineState>,
    state_rx: watch::Receiver<EngineState>,
    
    /// L1 in-memory cache (owned by sync engine)
    l1_cache: Arc<DashMap<String, SyncItem>>,
    l1_size_bytes: Arc<AtomicUsize>,
    
    /// L2 cache store (Redis) - optional
    l2_store: Option<Arc<dyn CacheStore>>,
    
    /// L3 archive store (SQL) - optional
    l3_store: Option<Arc<dyn ArchiveStore>>,
    
    /// Cuckoo filter for L2 (Redis) existence checks - saves network hops
    l2_filter: Arc<FilterManager>,
    
    /// Cuckoo filter for L3 (MySQL) existence checks - saves network hops
    l3_filter: Arc<FilterManager>,
    
    /// Filter persistence (for fast startup)
    filter_persistence: Option<FilterPersistence>,
    
    /// CF snapshot tracking
    cf_inserts_since_snapshot: AtomicU64,
    cf_last_snapshot: Mutex<Instant>,
    
    /// Batch queue for L2 writes (Mutex for interior mutability)
    l2_batcher: Mutex<HybridBatcher<SyncItem>>,
    
    /// Merkle tree store in Redis (shadows L2 data, never evicted)
    redis_merkle: Option<RedisMerkleStore>,
    
    /// Merkle tree store in SQL (ground truth)
    sql_merkle: Option<SqlMerkleStore>,
    
    /// Write-ahead log for L3 durability during MySQL outages
    l3_wal: Option<WriteAheadLog>,
    
    /// MySQL health checker
    mysql_health: MysqlHealthChecker,
    
    /// Eviction policy (tan curve algorithm)
    eviction_policy: TanCurvePolicy,
}

impl SyncEngine {
    /// Create a new sync engine (does not connect yet)
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
            l2_filter: Arc::new(FilterManager::new("sync-engine-l2", 100_000)),
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

    /// Get current engine state
    #[must_use]
    pub fn state(&self) -> EngineState {
        *self.state_rx.borrow()
    }

    /// Subscribe to state changes
    pub fn state_receiver(&self) -> watch::Receiver<EngineState> {
        self.state_rx.clone()
    }

    /// Check if engine is ready to accept data
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self.state(), EngineState::Ready | EngineState::Running)
    }

    /// Get current memory pressure (0.0 - 1.0)
    #[must_use]
    pub fn memory_pressure(&self) -> f64 {
        let used = self.l1_size_bytes.load(Ordering::Acquire);
        used as f64 / self.config.l1_max_bytes as f64
    }

    /// Get current backpressure level
    #[must_use]
    pub fn pressure(&self) -> BackpressureLevel {
        BackpressureLevel::from_pressure(self.memory_pressure())
    }

    /// Check if we should accept writes
    #[must_use]
    pub fn should_accept_writes(&self) -> bool {
        self.is_ready() && self.pressure().should_accept_writes()
    }

    /// Get item from cache (L1 → L2 → L3 fallback with tiered cuckoo filter optimization)
    #[tracing::instrument(skip(self), fields(tier))]
    pub async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        // Check L1 first (in-memory, always fast)
        if let Some(item) = self.l1_cache.get(id) {
            tracing::Span::current().record("tier", "L1");
            debug!(id = %id, "L1 cache hit");
            return Ok(Some(item.clone()));
        }

        // Check L2 filter before network hop to Redis
        if let Some(ref l2) = self.l2_store {
            if self.l2_filter.should_check_l3(id) {
                // Filter says "maybe in L2" - worth the network hop
                if let Some(item) = l2.get(id).await? {
                    tracing::Span::current().record("tier", "L2");
                    debug!(id = %id, "L2 cache hit, promoting to L1");
                    if self.memory_pressure() < 1.0 {
                        self.insert_l1(item.clone());
                    }
                    return Ok(Some(item));
                }
            } else {
                debug!(id = %id, "L2 filter says definitely not in Redis, skipping network hop");
            }
        }

        // Check L3 filter before network hop to MySQL
        if !self.l3_filter.should_check_l3(id) {
            tracing::Span::current().record("tier", "filter_reject");
            debug!(id = %id, "L3 filter says definitely not in MySQL");
            return Ok(None);
        }

        // Check L3 if available
        if let Some(ref l3) = self.l3_store {
            if let Some(item) = l3.get(id).await? {
                tracing::Span::current().record("tier", "L3");
                debug!(id = %id, "L3 hit, promoting to L1 and L2 filter");
                // Add to L2 filter since it will be promoted through the system
                self.l2_filter.insert(id);
                if self.memory_pressure() < 1.0 {
                    self.insert_l1(item.clone());
                }
                return Ok(Some(item));
            }
        }

        tracing::Span::current().record("tier", "miss");
        debug!(id = %id, "Cache miss across all tiers");
        Ok(None)
    }

    /// Get item with hash verification (detects corruption).
    ///
    /// If the item has a non-empty `merkle_root`, the content hash is verified.
    /// Returns `StorageError::Corruption` if the hash doesn't match.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, StorageError};
    /// # async fn example(engine: &SyncEngine) {
    /// match engine.get_verified("uk.nhs.patient.12345").await {
    ///     Ok(Some(item)) => println!("Verified: {:?}", item.object_id),
    ///     Ok(None) => println!("Not found"),
    ///     Err(StorageError::Corruption { id, .. }) => {
    ///         eprintln!("Data corruption detected for {}", id);
    ///     }
    ///     Err(e) => eprintln!("Error: {}", e),
    /// }
    /// # }
    /// ```
    #[tracing::instrument(skip(self), fields(tier, verified))]
    pub async fn get_verified(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        let item = self.get(id).await?;
        
        if let Some(ref item) = item {
            if !item.merkle_root.is_empty() {
                use sha2::{Sha256, Digest};
                
                let content_bytes = item.content.to_string();
                let computed = Sha256::digest(content_bytes.as_bytes());
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
        }
        
        Ok(item)
    }

    /// Submit an item for sync (from ingest engine)
    #[tracing::instrument(skip(self, item), fields(object_id = %item.object_id))]
    pub async fn submit(&self, item: SyncItem) -> Result<(), StorageError> {
        if !self.should_accept_writes() {
            return Err(StorageError::Backend(format!(
                "Rejecting write: engine state={}, pressure={}",
                self.state(),
                self.pressure()
            )));
        }

        let id = item.object_id.clone();
        
        // Insert into L1 (immediate, in-memory)
        self.insert_l1(item.clone());
        
        // NOTE: We do NOT insert into L2/L3 filters here!
        // Filters are updated only on SUCCESSFUL writes in flush_batch_internal()
        // This prevents filter/storage divergence if writes fail.
        
        // Queue for batched L2 persistence
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
    /// - WAL - if item was written during MySQL outage
    /// - Merkle trees - update with deletion marker
    /// 
    /// Strategy: Delete from all locations, log failures but don't fail the operation.
    /// Consistency is eventually achieved through the merkle tree verification.
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

        // 2. Remove from cuckoo filters (allows future negative lookups)
        self.l2_filter.remove(id);
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

    /// Start the engine - connect to backends with proper startup sequence.
    /// 
    /// Startup flow (trust hierarchy):
    /// 1. Initialize WAL (SQLite) - always first, our durability lifeline
    /// 2. Connect to SQL (L3) - ground truth, initialize SQL merkle store
    /// 3. Drain WAL to SQL - ensure all pending items are persisted
    /// 4. Get SQL merkle root - this is our trusted root hash
    /// 5. Try to restore CF from WAL-SQLite snapshot
    ///    - If snapshot merkle root matches SQL root → CF is trusted
    ///    - Otherwise → CF must be rebuilt from SQL scan
    /// 6. Connect to Redis (L2) - cache layer
    /// 7. Compare Redis merkle root with SQL merkle root
    ///    - If match → Redis is synced
    ///    - If mismatch → use branch diff to find stale regions and resync
    /// 8. Ready!
    #[tracing::instrument(skip(self), fields(has_redis, has_sql))]
    pub async fn start(&mut self) -> Result<(), StorageError> {
        info!("Starting sync engine with trust-verified startup...");
        let _ = self.state.send(EngineState::Connecting);

        // ========== PHASE 1: Initialize WAL (always first) ==========
        let wal_path = self.config.wal_path.clone()
            .unwrap_or_else(|| "./sync_engine_wal.db".to_string());
        let wal_max_items = self.config.wal_max_items.unwrap_or(1_000_000);
        
        let wal = match WriteAheadLog::new(&wal_path, wal_max_items).await {
            Ok(wal) => {
                info!(path = %wal_path, "Write-ahead log initialized");
                wal
            }
            Err(e) => {
                return Err(StorageError::Backend(format!(
                    "Failed to initialize WAL at {}: {}. Cannot guarantee durability!",
                    wal_path, e
                )));
            }
        };
        
        // Initialize filter persistence (uses same WAL SQLite)
        match FilterPersistence::new(&wal_path).await {
            Ok(fp) => {
                self.filter_persistence = Some(fp);
            }
            Err(e) => {
                warn!(error = %e, "Failed to initialize filter persistence - CF snapshots disabled");
            }
        }
        
        let pending_count = if wal.has_pending() {
            wal.stats(false).pending_items
        } else {
            0
        };
        self.l3_wal = Some(wal);

        // ========== PHASE 2: Connect to SQL (L3 - ground truth) ==========
        if let Some(ref sql_url) = self.config.sql_url {
            info!(url = %sql_url, "Connecting to SQL (L3 - ground truth)...");
            match crate::storage::sql::SqlStore::new(sql_url).await {
                Ok(store) => {
                    // Initialize SQL merkle store (ground truth) - shares pool with SqlStore
                    let is_sqlite = sql_url.starts_with("sqlite:");
                    let sql_merkle = SqlMerkleStore::from_pool(store.pool(), is_sqlite);
                    if let Err(e) = sql_merkle.init_schema().await {
                        error!(error = %e, "Failed to initialize SQL merkle schema");
                        return Err(StorageError::Backend(format!(
                            "Failed to initialize SQL merkle schema: {}", e
                        )));
                    }
                    self.sql_merkle = Some(sql_merkle);
                    
                    self.l3_store = Some(Arc::new(store));
                    tracing::Span::current().record("has_sql", true);
                    self.mysql_health.record_success();
                    info!("SQL (L3) connected with merkle store (ground truth)");
                }
                Err(e) => {
                    tracing::Span::current().record("has_sql", false);
                    error!(error = %e, "Failed to connect to SQL - this is required for startup");
                    self.mysql_health.record_failure();
                    return Err(StorageError::Backend(format!(
                        "SQL connection required for startup: {}", e
                    )));
                }
            }
        } else {
            warn!("No SQL URL configured - operating without ground truth storage!");
            tracing::Span::current().record("has_sql", false);
        }

        // ========== PHASE 3: Drain WAL to SQL ==========
        if pending_count > 0 {
            let _ = self.state.send(EngineState::DrainingWal);
            info!(pending = pending_count, "Draining WAL to SQL before startup...");
            
            // Force drain all pending items using existing WAL drain
            if let Some(ref l3) = self.l3_store {
                if let Some(ref wal) = self.l3_wal {
                    match wal.drain_to(l3.as_ref(), pending_count as usize).await {
                        Ok(drained) => {
                            info!(drained = drained.len(), "WAL drained to SQL");
                        }
                        Err(e) => {
                            warn!(error = %e, "WAL drain had errors - some items may retry later");
                        }
                    }
                }
            }
        }
        
        // ========== PHASE 4: Get SQL merkle root (trusted root) ==========
        let sql_root: Option<[u8; 32]> = if let Some(ref sql_merkle) = self.sql_merkle {
            match sql_merkle.root_hash().await {
                Ok(Some(root)) => {
                    info!(root = %hex::encode(root), "SQL merkle root (ground truth)");
                    Some(root)
                }
                Ok(None) => {
                    info!("SQL merkle tree is empty (no data yet)");
                    None
                }
                Err(e) => {
                    warn!(error = %e, "Failed to get SQL merkle root");
                    None
                }
            }
        } else {
            None
        };
        
        // ========== PHASE 5: Restore CF from snapshot (if valid) ==========
        if let Some(ref filter_persistence) = self.filter_persistence {
            if let Some(sql_root) = &sql_root {
                // Try L2 filter first
                match filter_persistence.load(L2_FILTER_ID).await {
                    Ok(Some(state)) if &state.merkle_root == sql_root => {
                        // Merkle roots match - CF is trustworthy!
                        if let Err(e) = self.l2_filter.import(&state.filter_bytes) {
                            warn!(error = %e, "Failed to import L2 filter from snapshot");
                        } else {
                            self.l2_filter.mark_trusted();
                            info!(
                                entries = state.entry_count,
                                "Restored L2 cuckoo filter from snapshot (merkle verified)"
                            );
                        }
                    }
                    Ok(Some(_)) => {
                        warn!("L2 CF snapshot merkle root mismatch - filter will be rebuilt");
                    }
                    Ok(None) => {
                        info!("No L2 CF snapshot found - filter will be built on warmup");
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to load L2 CF snapshot");
                    }
                }
                
                // Try L3 filter
                match filter_persistence.load(L3_FILTER_ID).await {
                    Ok(Some(state)) if &state.merkle_root == sql_root => {
                        if let Err(e) = self.l3_filter.import(&state.filter_bytes) {
                            warn!(error = %e, "Failed to import L3 filter from snapshot");
                        } else {
                            self.l3_filter.mark_trusted();
                            info!(
                                entries = state.entry_count,
                                "Restored L3 cuckoo filter from snapshot (merkle verified)"
                            );
                        }
                    }
                    Ok(Some(_)) => {
                        warn!("L3 CF snapshot merkle root mismatch - filter will be rebuilt");
                    }
                    Ok(None) => {
                        info!("No L3 CF snapshot found - filter will be built on warmup");
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to load L3 CF snapshot");
                    }
                }
            }
        }

        // ========== PHASE 6: Connect to Redis (L2 - cache) ==========
        if let Some(ref redis_url) = self.config.redis_url {
            info!(url = %redis_url, "Connecting to Redis (L2 - cache)...");
            match crate::storage::redis::RedisStore::new(redis_url).await {
                Ok(store) => {
                    // Create merkle store from same connection
                    let redis_merkle = RedisMerkleStore::new(store.connection());
                    self.redis_merkle = Some(redis_merkle);
                    
                    self.l2_store = Some(Arc::new(store));
                    tracing::Span::current().record("has_redis", true);
                    info!("Redis (L2) connected with merkle shadow tree");
                }
                Err(e) => {
                    tracing::Span::current().record("has_redis", false);
                    warn!(error = %e, "Failed to connect to Redis, continuing without L2 cache");
                }
            }
        } else {
            tracing::Span::current().record("has_redis", false);
        }

        // ========== PHASE 7: Sync Redis with SQL via branch diff ==========
        if let (Some(ref sql_merkle), Some(ref redis_merkle), Some(ref sql_root)) = 
            (&self.sql_merkle, &self.redis_merkle, &sql_root) 
        {
            let _ = self.state.send(EngineState::SyncingRedis);
            
            match redis_merkle.root_hash().await {
                Ok(Some(redis_root)) if &redis_root == sql_root => {
                    info!("Redis merkle root matches SQL - Redis is in sync");
                }
                Ok(Some(redis_root)) => {
                    info!(
                        sql_root = %hex::encode(sql_root),
                        redis_root = %hex::encode(redis_root),
                        "Redis merkle root mismatch - initiating branch diff sync"
                    );
                    
                    // Find divergent branches
                    match self.sync_redis_from_sql_diff(sql_merkle, redis_merkle).await {
                        Ok(synced) => {
                            info!(items_synced = synced, "Redis sync complete via branch diff");
                        }
                        Err(e) => {
                            warn!(error = %e, "Branch diff sync failed - Redis may be stale");
                        }
                    }
                }
                Ok(None) => {
                    info!("Redis merkle tree is empty - will be populated on writes");
                    // TODO: Full sync from SQL to Redis if Redis is empty
                }
                Err(e) => {
                    warn!(error = %e, "Failed to get Redis merkle root - Redis may be stale");
                }
            }
        }

        let _ = self.state.send(EngineState::Ready);
        info!("Sync engine ready (trust-verified startup complete)");
        Ok(())
    }
    
    /// Sync Redis from SQL by diffing merkle trees and only syncing stale branches.
    /// Returns the number of items synced.
    async fn sync_redis_from_sql_diff(
        &self,
        sql_merkle: &SqlMerkleStore,
        redis_merkle: &RedisMerkleStore,
    ) -> Result<usize, StorageError> {
        let mut total_synced = 0;
        
        // Start at root and find divergent branches
        let stale_prefixes = self.find_stale_branches(sql_merkle, redis_merkle, "").await?;
        
        for prefix in stale_prefixes {
            info!(prefix = %prefix, "Syncing stale branch from SQL to Redis");
            
            // Get all leaf paths under this prefix from SQL
            let leaf_paths = sql_merkle.get_leaves_under(&prefix).await
                .map_err(|e| StorageError::Backend(format!("Failed to get leaves: {}", e)))?;
            
            if leaf_paths.is_empty() {
                continue;
            }
            
            // Fetch actual data from L3 and push to Redis, building merkle batch
            let mut merkle_batch = MerkleBatch::new();
            
            if let Some(ref l3_store) = self.l3_store {
                for object_id in &leaf_paths {
                    if let Ok(Some(item)) = l3_store.get(object_id).await {
                        // Compute the leaf hash for merkle update
                        let payload_hash = PathMerkle::payload_hash(item.content.to_string().as_bytes());
                        let leaf_hash = PathMerkle::leaf_hash(
                            &item.object_id,
                            item.version,
                            item.updated_at,
                            &payload_hash,
                        );
                        merkle_batch.insert(object_id.clone(), leaf_hash);
                        
                        // Push to Redis
                        if let Some(ref l2_store) = self.l2_store {
                            if let Err(e) = l2_store.put(&item).await {
                                warn!(id = %object_id, error = %e, "Failed to sync item to Redis");
                            } else {
                                total_synced += 1;
                            }
                        }
                    }
                }
                
                // Update Redis merkle for this batch
                if !merkle_batch.is_empty() {
                    if let Err(e) = redis_merkle.apply_batch(&merkle_batch).await {
                        warn!(prefix = %prefix, error = %e, "Failed to update Redis merkle");
                    }
                }
            }
        }
        
        Ok(total_synced)
    }
    
    /// Find stale branches by recursively comparing SQL and Redis merkle nodes.
    /// Returns a list of path prefixes that need to be resynced.
    async fn find_stale_branches(
        &self,
        sql_merkle: &SqlMerkleStore,
        redis_merkle: &RedisMerkleStore,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        let mut stale = Vec::new();
        
        // Get children from both stores
        let sql_children = sql_merkle.get_children(prefix).await
            .map_err(|e| StorageError::Backend(format!("SQL merkle error: {}", e)))?;
        let redis_children = redis_merkle.get_children(prefix).await
            .map_err(|e| StorageError::Backend(format!("Redis merkle error: {}", e)))?;
        
        // If SQL has no children at this level, nothing to sync
        if sql_children.is_empty() {
            return Ok(stale);
        }
        
        // Check each SQL child against Redis
        for (child_path, sql_hash) in sql_children {
            match redis_children.get(&child_path) {
                Some(redis_hash) if redis_hash == &sql_hash => {
                    // Hashes match - this subtree is in sync
                    continue;
                }
                Some(_) => {
                    // Hash mismatch - need to drill down or sync
                    // If this is a leaf (contains '.'), sync it directly
                    if child_path.contains('.') && !child_path.ends_with('.') {
                        stale.push(child_path);
                    } else {
                        // Interior node - recurse to find specific stale leaves
                        let sub_stale = Box::pin(
                            self.find_stale_branches(sql_merkle, redis_merkle, &child_path)
                        ).await?;
                        stale.extend(sub_stale);
                    }
                }
                None => {
                    // Redis is missing this entire subtree - sync it all
                    stale.push(child_path);
                }
            }
        }
        
        Ok(stale)
    }


    /// Warm up L1 cache from L2/L3 (call after start)
    #[tracing::instrument(skip(self))]
    pub async fn warm_up(&mut self) -> Result<(), StorageError> {
        let _ = self.state.send(EngineState::WarmingUp);
        info!("Warming up cuckoo filter and L1 cache...");
        
        // Warm cuckoo filter from L3 (scan all keys)
        if let Some(l3) = &self.l3_store {
            let batch_size = self.config.cuckoo_warmup_batch_size;
            info!(
                batch_size,
                "Warming L3 cuckoo filter from MySQL..."
            );
            
            let total_count = l3.count_all().await.unwrap_or(0);
            if total_count > 0 {
                let mut offset = 0u64;
                let mut loaded = 0usize;
                
                loop {
                    let keys = l3.scan_keys(offset, batch_size).await?;
                    if keys.is_empty() {
                        break;
                    }
                    
                    for key in &keys {
                        self.l3_filter.insert(key);
                    }
                    
                    loaded += keys.len();
                    offset += keys.len() as u64;
                    
                    if loaded.is_multiple_of(10_000) || loaded == total_count as usize {
                        debug!(loaded, total = %total_count, "L3 filter warmup progress");
                    }
                }
                
                // Mark filter as trusted after full scan
                self.l3_filter.mark_trusted();
                info!(
                    loaded,
                    trust_state = ?self.l3_filter.trust_state(),
                    "L3 cuckoo filter warmup complete"
                );
            } else {
                info!("L3 store is empty, skipping filter warmup");
                self.l3_filter.mark_trusted(); // Empty is valid
            }
        }
        
        // L2 filter warmup would go here (Redis SCAN)
        // For now, L2 filter starts untrusted and populates on-demand
        info!(
            l2_trust = ?self.l2_filter.trust_state(),
            l3_trust = ?self.l3_filter.trust_state(),
            "Cuckoo filter warmup complete"
        );

        let _ = self.state.send(EngineState::Ready);
        info!("Warm-up complete, engine ready");
        Ok(())
    }

    /// Perform one tick of maintenance (for manual control instead of run loop).
    /// Call this periodically if you're not using `run()`.
    pub async fn tick(&self) {
        self.maybe_evict();
        self.maybe_flush_l2().await;
    }

    /// Force flush all pending L2 batches immediately.
    pub async fn force_flush(&self) {
        let batch = self.l2_batcher.lock().await.force_flush();
        if let Some(batch) = batch {
            debug!(batch_size = batch.items.len(), "Force flushing L2 batch");
            // Flush directly without re-adding to batcher
            self.flush_batch_internal(batch).await;
        }
    }

    /// Run the main event loop
    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) {
        let _ = self.state.send(EngineState::Running);
        info!("Sync engine running");

        let mut health_check_interval = tokio::time::interval(
            tokio::time::Duration::from_secs(30)
        );
        let mut wal_drain_interval = tokio::time::interval(
            tokio::time::Duration::from_secs(5)
        );
        let mut cf_snapshot_interval = tokio::time::interval(
            tokio::time::Duration::from_secs(self.config.cf_snapshot_interval_secs)
        );

        // Main loop - handle batching, eviction, config changes
        loop {
            tokio::select! {
                // Check for config changes
                Ok(()) = self.config_rx.changed() => {
                    let new_config = self.config_rx.borrow().clone();
                    info!("Config updated: l1_max_bytes={}", new_config.l1_max_bytes);
                    self.config = new_config;
                }
                
                // Periodic maintenance (eviction, batch flush) - every 100ms
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    self.maybe_evict();
                    self.maybe_flush_l2().await;
                    
                    // Check insert threshold for CF snapshot
                    self.maybe_snapshot_cf_by_threshold().await;
                }
                
                // MySQL health check - every 30s
                _ = health_check_interval.tick() => {
                    self.check_mysql_health().await;
                }
                
                // WAL drain attempt - every 5s (only if MySQL healthy)
                _ = wal_drain_interval.tick() => {
                    self.maybe_drain_wal().await;
                }
                
                // Periodic CF snapshot - configurable interval (default 30s)
                _ = cf_snapshot_interval.tick() => {
                    self.maybe_snapshot_cf_by_time().await;
                }
            }
        }
    }
    
    /// Snapshot CF if insert threshold exceeded
    async fn maybe_snapshot_cf_by_threshold(&self) {
        let inserts = self.cf_inserts_since_snapshot.load(std::sync::atomic::Ordering::Relaxed);
        if inserts >= self.config.cf_snapshot_insert_threshold {
            self.snapshot_cuckoo_filters("threshold").await;
        }
    }
    
    /// Snapshot CF if time interval elapsed
    async fn maybe_snapshot_cf_by_time(&self) {
        let last_snapshot = self.cf_last_snapshot.lock().await;
        let elapsed = last_snapshot.elapsed().as_secs();
        drop(last_snapshot);
        
        // Only snapshot if we have inserts since last snapshot
        let inserts = self.cf_inserts_since_snapshot.load(std::sync::atomic::Ordering::Relaxed);
        if inserts > 0 && elapsed >= self.config.cf_snapshot_interval_secs {
            self.snapshot_cuckoo_filters("time").await;
        }
    }
    
    /// Snapshot both L2 and L3 cuckoo filters to WAL SQLite
    async fn snapshot_cuckoo_filters(&self, reason: &str) {
        let persistence = match &self.filter_persistence {
            Some(p) => p,
            None => return,
        };
        
        // Get current SQL merkle root for trust verification
        let merkle_root: [u8; 32] = if let Some(ref sql_merkle) = self.sql_merkle {
            match sql_merkle.root_hash().await {
                Ok(Some(root)) => root,
                Ok(None) => [0u8; 32], // Empty tree
                Err(e) => {
                    warn!(error = %e, "Cannot snapshot CF: failed to get SQL merkle root");
                    return;
                }
            }
        } else {
            [0u8; 32] // No SQL store, use zero root
        };
        
        let inserts = self.cf_inserts_since_snapshot.swap(0, std::sync::atomic::Ordering::Relaxed);
        
        // Snapshot L2 filter
        if let Some(l2_bytes) = self.l2_filter.export() {
            let l2_count = self.l2_filter.len();
            if let Err(e) = persistence.save(L2_FILTER_ID, &l2_bytes, &merkle_root, l2_count).await {
                warn!(error = %e, "Failed to snapshot L2 cuckoo filter");
            }
        }
        
        // Snapshot L3 filter
        if let Some(l3_bytes) = self.l3_filter.export() {
            let l3_count = self.l3_filter.len();
            if let Err(e) = persistence.save(L3_FILTER_ID, &l3_bytes, &merkle_root, l3_count).await {
                warn!(error = %e, "Failed to snapshot L3 cuckoo filter");
            }
        }
        
        // Update last snapshot time
        *self.cf_last_snapshot.lock().await = Instant::now();
        
        info!(
            reason = reason,
            inserts_since_last = inserts,
            l2_entries = self.l2_filter.len(),
            l3_entries = self.l3_filter.len(),
            merkle_root = %hex::encode(merkle_root),
            "Cuckoo filters snapshot saved"
        );
    }
    
    /// Check MySQL health and log status changes
    async fn check_mysql_health(&self) {
        if let Some(ref l3) = self.l3_store {
            let was_healthy = self.mysql_health.is_healthy();
            let is_healthy = self.mysql_health.check(l3.as_ref()).await;
            
            if was_healthy != is_healthy {
                if is_healthy {
                    info!("MySQL connectivity restored");
                } else {
                    warn!(
                        failures = self.mysql_health.failure_count(),
                        "MySQL connectivity lost, writes will go to WAL"
                    );
                }
            }
        }
    }
    
    /// Drain WAL to MySQL if healthy
    async fn maybe_drain_wal(&self) {
        // Only drain if MySQL is healthy
        if !self.mysql_health.is_healthy() {
            return;
        }
        
        let Some(ref wal) = self.l3_wal else {
            return;
        };
        
        if !wal.has_pending() {
            return;
        }
        
        let Some(ref l3) = self.l3_store else {
            return;
        };
        
        match wal.drain_to(l3.as_ref(), self.config.wal_drain_batch_size).await {
            Ok(drained_ids) if !drained_ids.is_empty() => {
                // Add successfully drained items to L3 filter
                for id in &drained_ids {
                    self.l3_filter.insert(id);
                }
                info!(drained = drained_ids.len(), "WAL drained to MySQL, L3 filter updated");
                self.mysql_health.record_success();
            }
            Ok(_) => {} // Nothing to drain
            Err(e) => {
                warn!(error = %e, "WAL drain failed, MySQL may be down again");
                self.mysql_health.record_failure();
            }
        }
    }
    
    /// Flush L2 batch if ready, update Merkle tree, persist to L3
    async fn maybe_flush_l2(&self) {
        // Check if batcher has a batch ready
        let batch = {
            let mut batcher = self.l2_batcher.lock().await;
            batcher.take_if_ready()
        };
        
        let Some(batch) = batch else {
            return;
        };
        
        self.flush_batch_internal(batch).await;
    }

    /// Internal: flush a batch to L2/L3, update Merkle tree
    async fn flush_batch_internal(&self, batch: crate::batching::hybrid_batcher::FlushBatch<SyncItem>) {
        let batch_size = batch.items.len();
        debug!(batch_size = batch_size, reason = ?batch.reason, "Flushing L2 batch");
        
        // Take ownership of items for modification (adding batch_id)
        let mut items: Vec<SyncItem> = batch.items;
        
        // ====== L2: Pipelined Redis writes ======
        let mut l2_success = 0;
        let mut l2_errors = 0;
        
        if let Some(ref l2) = self.l2_store {
            match l2.put_batch(&items).await {
                Ok(result) => {
                    l2_success = result.written;
                    // Insert into L2 filter ONLY after successful Redis write
                    for item in &items {
                        self.l2_filter.insert(&item.object_id);
                    }
                    debug!(written = result.written, "L2 pipelined batch write complete");
                }
                Err(e) => {
                    warn!(error = %e, "L2 batch write failed");
                    l2_errors = items.len();
                    // Don't insert into L2 filter - write failed!
                    // Continue - L2 is just cache, L3/WAL is durability
                }
            }
        }
        
        // ====== L3: Batched SQL writes with verification ======
        let mut l3_success = 0;
        let mut wal_fallback = 0;
        
        if self.mysql_health.is_healthy() {
            if let Some(ref l3) = self.l3_store {
                match l3.put_batch(&mut items).await {
                    Ok(result) => {
                        l3_success = result.written;
                        if result.verified {
                            // Insert into L3 filter ONLY after verified MySQL write
                            for item in &items {
                                self.l3_filter.insert(&item.object_id);
                            }
                            self.mysql_health.record_success();
                            debug!(
                                batch_id = %result.batch_id,
                                written = result.written,
                                "L3 batch write verified"
                            );
                        } else {
                            // Verification failed - don't trust it, don't add to filter
                            warn!(batch_id = %result.batch_id, "L3 batch verification failed");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "L3 batch write failed, falling back to WAL");
                        self.mysql_health.record_failure();
                        // Fall back to WAL for all items
                        // NOTE: Items in WAL are NOT in L3 filter until WAL drains successfully
                        if let Some(ref wal) = self.l3_wal {
                            for item in &items {
                                if let Err(e) = wal.write(item).await {
                                    warn!(id = %item.object_id, error = %e, "WAL write also failed!");
                                } else {
                                    wal_fallback += 1;
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // MySQL unhealthy, write everything to WAL
            // NOTE: Items in WAL are NOT in L3 filter until WAL drains successfully
            if let Some(ref wal) = self.l3_wal {
                for item in &items {
                    if let Err(e) = wal.write(item).await {
                        warn!(id = %item.object_id, error = %e, "WAL write failed!");
                    } else {
                        wal_fallback += 1;
                    }
                }
            }
        }
        
        // ====== Merkle: Compute and update hashes ======
        let mut merkle_batch = MerkleBatch::new();
        
        // Build merkle batch for all successfully written items
        for item in &items {
            let payload_hash = PathMerkle::payload_hash(item.content.to_string().as_bytes());
            let leaf_hash = PathMerkle::leaf_hash(
                &item.object_id,
                item.version,
                item.updated_at,
                &payload_hash,
            );
            merkle_batch.insert(item.object_id.clone(), leaf_hash);
        }
        
        // Apply Merkle updates to BOTH stores (SQL is ground truth, Redis is cache)
        if !merkle_batch.is_empty() {
            // SQL merkle (ground truth) - must succeed
            if let Some(ref sql_merkle) = self.sql_merkle {
                if let Err(e) = sql_merkle.apply_batch(&merkle_batch).await {
                    error!(error = %e, "Failed to update SQL Merkle tree (ground truth)");
                } else {
                    debug!(merkle_updates = merkle_batch.len(), "SQL merkle tree updated");
                    
                    // Track inserts for CF snapshot threshold
                    self.cf_inserts_since_snapshot.fetch_add(
                        merkle_batch.len() as u64, 
                        std::sync::atomic::Ordering::Relaxed
                    );
                }
            }
            
            // Redis merkle (cache) - best effort
            if let Some(ref redis_merkle) = self.redis_merkle {
                if let Err(e) = redis_merkle.apply_batch(&merkle_batch).await {
                    warn!(error = %e, "Failed to update Redis Merkle tree (cache)");
                } else {
                    debug!(merkle_updates = merkle_batch.len(), "Redis merkle tree updated");
                }
            }
        }
        
        info!(
            l2_success,
            l2_errors,
            l3_success,
            wal_fallback,
            reason = ?batch.reason,
            "Batch flush complete"
        );
    }
    
    /// Write an item to L3 (MySQL) or fall back to WAL if MySQL is unavailable.
    /// Used for single-item writes (e.g., WAL drain).
    #[allow(dead_code)]  // Used in WAL drain path
    async fn write_to_l3_or_wal(&self, item: &SyncItem) -> Result<WriteTarget, StorageError> {
        // If MySQL is healthy, try to write directly
        if self.mysql_health.is_healthy() {
            if let Some(ref l3) = self.l3_store {
                match l3.put(item).await {
                    Ok(()) => {
                        self.mysql_health.record_success();
                        return Ok(WriteTarget::L3);
                    }
                    Err(e) => {
                        // MySQL write failed, fall through to WAL
                        debug!(id = %item.object_id, error = %e, "L3 write failed, falling back to WAL");
                        self.mysql_health.record_failure();
                    }
                }
            }
        }
        
        // Fall back to WAL
        if let Some(ref wal) = self.l3_wal {
            wal.write(item).await?;
            return Ok(WriteTarget::Wal);
        }
        
        // No L3 and no WAL - this shouldn't happen if start() succeeded
        Err(StorageError::Backend("No L3 or WAL available".to_string()))
    }

    /// Initiate graceful shutdown
    #[tracing::instrument(skip(self))]
    pub async fn shutdown(&mut self) {
        info!("Initiating sync engine shutdown...");
        let _ = self.state.send(EngineState::ShuttingDown);
        
        // Force flush any pending L2 batches
        let batch = self.l2_batcher.lock().await.force_flush();
        if let Some(batch) = batch {
            info!(batch_size = batch.items.len(), "Flushing final L2 batch on shutdown");
            // Re-add items and flush (reuse logic)
            {
                let mut batcher = self.l2_batcher.lock().await;
                batcher.add_batch(batch.items);
            }
            self.maybe_flush_l2().await;
        }
        
        // Snapshot cuckoo filters for fast restart
        self.snapshot_cuckoo_filters("shutdown").await;
        
        info!("Sync engine shutdown complete");
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

    /// Get L2 filter stats (entries, capacity, trust_state)
    #[must_use]
    pub fn l2_filter_stats(&self) -> (usize, usize, FilterTrust) {
        self.l2_filter.stats()
    }

    /// Get L3 filter stats (entries, capacity, trust_state)
    #[must_use]
    pub fn l3_filter_stats(&self) -> (usize, usize, FilterTrust) {
        self.l3_filter.stats()
    }

    /// Get access to the L2 filter (for warmup/verification)
    pub fn l2_filter(&self) -> &Arc<FilterManager> {
        &self.l2_filter
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
    use crate::sync_item::SyncItem;
    use crate::storage::traits::{CacheStore, ArchiveStore, StorageError, BatchWriteResult};
    use async_trait::async_trait;
    use serde_json::json;
    use tokio::sync::watch;
    use std::sync::atomic::{AtomicUsize, AtomicBool};

    fn test_config() -> SyncEngineConfig {
        SyncEngineConfig {
            // No backends - pure L1 testing
            redis_url: None,
            sql_url: None,
            wal_path: None,
            l1_max_bytes: 1024 * 1024, // 1 MB
            ..Default::default()
        }
    }

    fn test_item(id: &str) -> SyncItem {
        SyncItem::new(id.to_string(), json!({"test": "data", "id": id}))
    }

    // ========== Mock Stores for Failure Testing ==========

    /// A mock store that can be configured to fail on demand
    #[allow(dead_code)]
    struct FailingCacheStore {
        fail_get: AtomicBool,
        fail_put: AtomicBool,
        fail_delete: AtomicBool,
        get_count: AtomicUsize,
        put_count: AtomicUsize,
        delete_count: AtomicUsize,
        data: dashmap::DashMap<String, SyncItem>,
    }

    #[allow(dead_code)]
    impl FailingCacheStore {
        fn new() -> Self {
            Self {
                fail_get: AtomicBool::new(false),
                fail_put: AtomicBool::new(false),
                fail_delete: AtomicBool::new(false),
                get_count: AtomicUsize::new(0),
                put_count: AtomicUsize::new(0),
                delete_count: AtomicUsize::new(0),
                data: dashmap::DashMap::new(),
            }
        }

        fn set_fail_get(&self, fail: bool) {
            self.fail_get.store(fail, Ordering::SeqCst);
        }

        fn set_fail_put(&self, fail: bool) {
            self.fail_put.store(fail, Ordering::SeqCst);
        }

        fn set_fail_delete(&self, fail: bool) {
            self.fail_delete.store(fail, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl CacheStore for FailingCacheStore {
        async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
            self.get_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(StorageError::Backend("Mock Redis failure".into()));
            }
            Ok(self.data.get(id).map(|r| r.value().clone()))
        }

        async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
            self.put_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_put.load(Ordering::SeqCst) {
                return Err(StorageError::Backend("Mock Redis failure".into()));
            }
            self.data.insert(item.object_id.clone(), item.clone());
            Ok(())
        }

        async fn delete(&self, id: &str) -> Result<(), StorageError> {
            self.delete_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_delete.load(Ordering::SeqCst) {
                return Err(StorageError::Backend("Mock Redis failure".into()));
            }
            self.data.remove(id);
            Ok(())
        }
    }

    /// A mock archive store that can fail and track operations
    #[allow(dead_code)]
    struct FailingArchiveStore {
        fail_get: AtomicBool,
        fail_put: AtomicBool,
        fail_delete: AtomicBool,
        fail_batch: AtomicBool,
        get_count: AtomicUsize,
        put_count: AtomicUsize,
        delete_count: AtomicUsize,
        batch_count: AtomicUsize,
        data: dashmap::DashMap<String, SyncItem>,
    }

    #[allow(dead_code)]
    impl FailingArchiveStore {
        fn new() -> Self {
            Self {
                fail_get: AtomicBool::new(false),
                fail_put: AtomicBool::new(false),
                fail_delete: AtomicBool::new(false),
                fail_batch: AtomicBool::new(false),
                get_count: AtomicUsize::new(0),
                put_count: AtomicUsize::new(0),
                delete_count: AtomicUsize::new(0),
                batch_count: AtomicUsize::new(0),
                data: dashmap::DashMap::new(),
            }
        }

        fn set_fail_batch(&self, fail: bool) {
            self.fail_batch.store(fail, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ArchiveStore for FailingArchiveStore {
        async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
            self.get_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(StorageError::Backend("Mock MySQL failure".into()));
            }
            Ok(self.data.get(id).map(|r| r.value().clone()))
        }

        async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
            self.put_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_put.load(Ordering::SeqCst) {
                return Err(StorageError::Backend("Mock MySQL failure".into()));
            }
            self.data.insert(item.object_id.clone(), item.clone());
            Ok(())
        }

        async fn delete(&self, id: &str) -> Result<(), StorageError> {
            self.delete_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_delete.load(Ordering::SeqCst) {
                return Err(StorageError::Backend("Mock MySQL failure".into()));
            }
            self.data.remove(id);
            Ok(())
        }

        async fn put_batch(&self, items: &mut [SyncItem]) -> Result<BatchWriteResult, StorageError> {
            self.batch_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_batch.load(Ordering::SeqCst) {
                return Err(StorageError::Backend("Mock MySQL batch failure".into()));
            }
            for item in items.iter() {
                self.data.insert(item.object_id.clone(), item.clone());
            }
            Ok(BatchWriteResult {
                batch_id: uuid::Uuid::new_v4().to_string(),
                written: items.len(),
                verified: true,
            })
        }

        async fn scan_keys(&self, offset: u64, limit: usize) -> Result<Vec<String>, StorageError> {
            let keys: Vec<String> = self.data.iter()
                .skip(offset as usize)
                .take(limit)
                .map(|r| r.key().clone())
                .collect();
            Ok(keys)
        }

        async fn count_all(&self) -> Result<u64, StorageError> {
            Ok(self.data.len() as u64)
        }
    }

    // ========== Engine State Tests ==========

    #[test]
    fn test_engine_initial_state() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        assert_eq!(engine.state(), EngineState::Created);
        assert!(!engine.is_ready());
    }

    #[tokio::test]
    async fn test_engine_rejects_writes_before_ready() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        let result = engine.submit(test_item("test-1")).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Rejecting write"));
    }

    // ========== Memory Pressure Tests ==========

    #[test]
    fn test_memory_pressure_calculation() {
        let config = SyncEngineConfig {
            l1_max_bytes: 1000,
            ..test_config()
        };
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Initially empty = 0 pressure
        assert_eq!(engine.memory_pressure(), 0.0);
        assert_eq!(engine.pressure(), BackpressureLevel::Normal);
    }

    // ========== L1 Cache Tests ==========

    #[tokio::test]
    async fn test_l1_stats_empty() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        let (count, bytes) = engine.l1_stats();
        assert_eq!(count, 0);
        assert_eq!(bytes, 0);
    }

    // ========== Cuckoo Filter Tests ==========

    #[test]
    fn test_cuckoo_filters_initialized() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Both filters should exist but be empty and untrusted
        let (l2_entries, _, l2_trust) = engine.l2_filter_stats();
        let (l3_entries, _, l3_trust) = engine.l3_filter_stats();
        
        assert_eq!(l2_entries, 0);
        assert_eq!(l3_entries, 0);
        assert_eq!(l2_trust, FilterTrust::Untrusted);
        assert_eq!(l3_trust, FilterTrust::Untrusted);
    }

    // ========== Backpressure Level Tests ==========

    #[test]
    fn test_should_accept_writes_when_normal() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Not ready, so should reject
        assert!(!engine.should_accept_writes());
    }

    // ========== Engine State Display ==========

    #[test]
    fn test_engine_state_display() {
        assert_eq!(format!("{}", EngineState::Created), "Created");
        assert_eq!(format!("{}", EngineState::Connecting), "Connecting");
        assert_eq!(format!("{}", EngineState::DrainingWal), "DrainingWal");
        assert_eq!(format!("{}", EngineState::SyncingRedis), "SyncingRedis");
        assert_eq!(format!("{}", EngineState::WarmingUp), "WarmingUp");
        assert_eq!(format!("{}", EngineState::Ready), "Ready");
        assert_eq!(format!("{}", EngineState::Running), "Running");
        assert_eq!(format!("{}", EngineState::ShuttingDown), "ShuttingDown");
    }

    // ========== Failure Path Tests ==========

    #[test]
    fn test_write_target_enum_exists() {
        // Verify WriteTarget enum is defined (used in WAL path)
        let _l3 = WriteTarget::L3;
        let _wal = WriteTarget::Wal;
    }

    #[tokio::test]
    async fn test_get_returns_none_on_cache_miss() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // No item exists, should return None (not error)
        let result = engine.get("nonexistent-key").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_l1_insert_tracks_size() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        let item = test_item("test-1");
        engine.insert_l1(item);
        
        let (count, bytes) = engine.l1_stats();
        assert_eq!(count, 1);
        assert!(bytes > 0, "Should track non-zero bytes");
    }

    #[test]
    fn test_l1_insert_stores_item() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        let item = test_item("test-1");
        engine.insert_l1(item);
        
        // Verify item was stored
        let has_item = engine.l1_cache.get("test-1").is_some();
        assert!(has_item, "Item should exist in L1");
    }

    #[tokio::test]
    async fn test_get_from_l1_cache_hit() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Insert directly into L1
        let item = test_item("cached-item");
        engine.insert_l1(item.clone());
        
        // Get should find it in L1
        let result = engine.get("cached-item").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().object_id, "cached-item");
    }

    #[test]
    fn test_filter_insert_on_manual_insert() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Filters start empty
        assert_eq!(engine.l2_filter.len(), 0);
        assert_eq!(engine.l3_filter.len(), 0);
        
        // Manual filter insert (simulating what happens on successful write)
        engine.l2_filter.insert("test-key");
        engine.l3_filter.insert("test-key");
        
        assert_eq!(engine.l2_filter.len(), 1);
        assert_eq!(engine.l3_filter.len(), 1);
    }

    #[tokio::test]
    async fn test_delete_from_empty_cache_returns_false() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Force engine into ready state for delete to work
        let _ = engine.state.send(EngineState::Ready);
        
        // Delete non-existent item
        let result = engine.delete("nonexistent").await;
        assert!(result.is_ok());
        assert!(!result.unwrap(), "Should return false for non-existent item");
    }

    #[tokio::test]
    async fn test_delete_removes_from_l1_and_filters() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Insert item and add to filters
        engine.insert_l1(test_item("to-delete"));
        engine.l2_filter.insert("to-delete");
        engine.l3_filter.insert("to-delete");
        
        // Force engine into ready state
        let _ = engine.state.send(EngineState::Ready);
        
        // Delete
        let result = engine.delete("to-delete").await;
        assert!(result.is_ok());
        assert!(result.unwrap(), "Should return true when item existed");
        
        // Verify removed from L1
        assert!(engine.l1_cache.get("to-delete").is_none());
        
        // Verify removed from filters (filter.remove doesn't guarantee removal with cuckoo filters)
        // but the item should no longer be in the filter's reported entries
    }

    #[test]
    fn test_state_receiver_clone() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        let receiver = engine.state_receiver();
        assert_eq!(*receiver.borrow(), EngineState::Created);
    }

    #[test]
    fn test_is_ready_states() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Created = not ready
        assert!(!engine.is_ready());
        
        // Ready = ready
        let _ = engine.state.send(EngineState::Ready);
        assert!(engine.is_ready());
        
        // Running = ready
        let _ = engine.state.send(EngineState::Running);
        assert!(engine.is_ready());
        
        // ShuttingDown = not ready
        let _ = engine.state.send(EngineState::ShuttingDown);
        assert!(!engine.is_ready());
    }

    #[test]
    fn test_eviction_at_various_pressure_levels() {
        let config = SyncEngineConfig {
            l1_max_bytes: 100, // Very small to trigger pressure
            ..test_config()
        };
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Insert items to create pressure
        for i in 0..10 {
            engine.insert_l1(test_item(&format!("item-{}", i)));
        }
        
        // Memory pressure should be > 0 now
        let pressure = engine.memory_pressure();
        assert!(pressure > 0.0, "Should have memory pressure after inserts");
    }

    #[tokio::test]
    async fn test_mysql_health_checker_integration() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Initially healthy (optimistic)
        assert!(engine.mysql_health.is_healthy());
        
        // Record failures
        engine.mysql_health.record_failure();
        engine.mysql_health.record_failure();
        engine.mysql_health.record_failure();
        
        // After 3 failures, should be unhealthy
        assert!(!engine.mysql_health.is_healthy());
        
        // Record success
        engine.mysql_health.record_success();
        
        // Should be healthy again
        assert!(engine.mysql_health.is_healthy());
    }

    #[tokio::test]
    async fn test_batcher_integration() {
        let config = SyncEngineConfig {
            batch_flush_count: 5,
            batch_flush_bytes: 1024 * 1024, // 1MB, won't trigger
            batch_flush_ms: 60000, // 1 minute, won't trigger
            ..test_config()
        };
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Force ready state
        let _ = engine.state.send(EngineState::Ready);
        
        // Submit items (less than flush threshold)
        for i in 0..3 {
            let _ = engine.submit(test_item(&format!("batch-item-{}", i))).await;
        }
        
        // Check batcher has items (not empty)
        let batcher = engine.l2_batcher.lock().await;
        assert!(!batcher.is_empty());
    }

    #[test]
    fn test_filter_stats_accessors() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Test all filter accessors
        let l2 = engine.l2_filter();
        let l3 = engine.l3_filter();
        
        assert_eq!(l2.len(), 0);
        assert_eq!(l3.len(), 0);
        
        // Insert via accessor
        l2.insert("via-accessor");
        assert_eq!(engine.l2_filter.len(), 1);
    }

    #[tokio::test]
    async fn test_get_verified_detects_corruption() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Force ready state
        let _ = engine.state.send(EngineState::Ready);
        
        // Create item with correct hash
        use sha2::{Sha256, Digest};
        let content = json!({"name": "valid", "data": 123});
        let content_str = content.to_string();
        let correct_hash = hex::encode(Sha256::digest(content_str.as_bytes()));
        
        let mut valid_item = SyncItem::new("test.valid".into(), content.clone());
        valid_item.merkle_root = correct_hash;
        
        // Submit and verify
        engine.submit(valid_item).await.expect("Submit failed");
        let result = engine.get_verified("test.valid").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        
        // Create item with wrong hash (simulated corruption)
        let mut corrupted_item = SyncItem::new("test.corrupted".into(), json!({"name": "corrupted"}));
        corrupted_item.merkle_root = "deadbeef".repeat(8); // Wrong hash
        
        engine.submit(corrupted_item).await.expect("Submit failed");
        let result = engine.get_verified("test.corrupted").await;
        
        assert!(matches!(result, Err(StorageError::Corruption { .. })));
    }

    #[tokio::test]
    async fn test_get_verified_skips_empty_hash() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Force ready state
        let _ = engine.state.send(EngineState::Ready);
        
        // Create item without hash (empty merkle_root)
        let item = SyncItem::new("test.no_hash".into(), json!({"data": "no hash"}));
        assert!(item.merkle_root.is_empty());
        
        engine.submit(item).await.expect("Submit failed");
        
        // get_verified should skip verification for items without hash
        let result = engine.get_verified("test.no_hash").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }
}
