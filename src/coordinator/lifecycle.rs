//! Engine lifecycle management: start, shutdown, run loop.
//!
//! This module contains the startup sequence, main run loop, and shutdown logic.

use tracing::{info, warn, debug, error};

use crate::storage::traits::StorageError;
use crate::cuckoo::{FilterPersistence, L3_FILTER_ID};
use crate::merkle::{RedisMerkleStore, SqlMerkleStore, MerkleBatch, PathMerkle};
use crate::resilience::wal::WriteAheadLog;

use super::{SyncEngine, EngineState};
#[allow(unused_imports)]
use super::WriteTarget;

impl SyncEngine {
    /// Start the engine - connect to backends with proper startup sequence.
    /// 
    /// Startup flow (trust hierarchy):
    /// 1. Initialize WAL (SQLite) - always first, our durability lifeline
    /// 2. Connect to SQL (L3) - ground truth, initialize SQL merkle store
    /// 3. Drain any pending WAL entries to SQL (blocking)
    /// 4. Get SQL merkle root (this is our trusted root)
    /// 5. Load CF snapshots from SQLite:
    ///    - If snapshot merkle root matches SQL root → CF is trusted
    ///    - Otherwise → CF must be rebuilt from SQL scan
    /// 6. Connect to Redis (L2) - cache layer
    /// 7. Compare Redis merkle root with SQL merkle root
    ///    - If match → Redis is synced
    ///    - If mismatch → use branch diff to find stale regions and resync
    /// 8. Ready!
    #[tracing::instrument(skip(self), fields(has_redis, has_sql))]
    pub async fn start(&mut self) -> Result<(), StorageError> {
        let startup_start = std::time::Instant::now();
        info!("Starting sync engine with trust-verified startup...");
        let _ = self.state.send(EngineState::Connecting);

        // ========== PHASE 1: Initialize WAL (always first) ==========
        let phase_start = std::time::Instant::now();
        let wal_path = self.config.wal_path.clone()
            .unwrap_or_else(|| "./sync_engine_wal.db".to_string());
        let wal_max_items = self.config.wal_max_items.unwrap_or(1_000_000);
        
        let wal = match WriteAheadLog::new(&wal_path, wal_max_items).await {
            Ok(wal) => {
                info!(path = %wal_path, "Write-ahead log initialized");
                crate::metrics::record_startup_phase("wal_init", phase_start.elapsed());
                wal
            }
            Err(e) => {
                crate::metrics::record_error("WAL", "init", "sqlite");
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
                crate::metrics::record_error("filter", "init", "persistence");
            }
        }
        
        let pending_count = if wal.has_pending() {
            wal.stats(false).pending_items
        } else {
            0
        };
        self.l3_wal = Some(wal);

        // ========== PHASE 2: Connect to SQL (L3 - ground truth) ==========
        let phase_start = std::time::Instant::now();
        if let Some(ref sql_url) = self.config.sql_url {
            info!(url = %sql_url, "Connecting to SQL (L3 - ground truth)...");
            match crate::storage::sql::SqlStore::new(sql_url).await {
                Ok(store) => {
                    // Initialize SQL merkle store (ground truth) - shares pool with SqlStore
                    let is_sqlite = sql_url.starts_with("sqlite:");
                    let sql_merkle = SqlMerkleStore::from_pool(store.pool(), is_sqlite);
                    if let Err(e) = sql_merkle.init_schema().await {
                        error!(error = %e, "Failed to initialize SQL merkle schema");
                        crate::metrics::record_error("L3", "init", "merkle_schema");
                        return Err(StorageError::Backend(format!(
                            "Failed to initialize SQL merkle schema: {}", e
                        )));
                    }
                    self.sql_merkle = Some(sql_merkle);
                    
                    // Keep both Arc<dyn ArchiveStore> and Arc<SqlStore> for dirty merkle access
                    let store = std::sync::Arc::new(store);
                    self.sql_store = Some(store.clone());
                    self.l3_store = Some(store);
                    tracing::Span::current().record("has_sql", true);
                    self.mysql_health.record_success();
                    crate::metrics::set_backend_healthy("mysql", true);
                    crate::metrics::record_startup_phase("sql_connect", phase_start.elapsed());
                    info!("SQL (L3) connected with merkle store (ground truth)");
                }
                Err(e) => {
                    tracing::Span::current().record("has_sql", false);
                    error!(error = %e, "Failed to connect to SQL - this is required for startup");
                    self.mysql_health.record_failure();
                    crate::metrics::set_backend_healthy("mysql", false);
                    crate::metrics::record_connection_error("mysql");
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
            let phase_start = std::time::Instant::now();
            let _ = self.state.send(EngineState::DrainingWal);
            info!(pending = pending_count, "Draining WAL to SQL before startup...");
            
            if let Some(ref l3) = self.l3_store {
                if let Some(ref wal) = self.l3_wal {
                    match wal.drain_to(l3.as_ref(), pending_count as usize).await {
                        Ok(drained) => {
                            info!(drained = drained.len(), "WAL drained to SQL");
                            crate::metrics::record_items_written("L3", drained.len());
                        }
                        Err(e) => {
                            warn!(error = %e, "WAL drain had errors - some items may retry later");
                            crate::metrics::record_error("WAL", "drain", "partial");
                        }
                    }
                }
            }
            crate::metrics::record_startup_phase("wal_drain", phase_start.elapsed());
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
        let phase_start = std::time::Instant::now();
        self.restore_cuckoo_filters(&sql_root).await;
        crate::metrics::record_startup_phase("cf_restore", phase_start.elapsed());

        // ========== PHASE 6: Connect to Redis (L2 - cache) ==========
        let phase_start = std::time::Instant::now();
        if let Some(ref redis_url) = self.config.redis_url {
            info!(url = %redis_url, prefix = ?self.config.redis_prefix, "Connecting to Redis (L2 - cache)...");
            match crate::storage::redis::RedisStore::with_prefix(redis_url, self.config.redis_prefix.as_deref()).await {
                Ok(store) => {
                    let redis_merkle = RedisMerkleStore::with_prefix(
                        store.connection(),
                        self.config.redis_prefix.as_deref(),
                    );
                    self.redis_merkle = Some(redis_merkle);
                    let store = std::sync::Arc::new(store);
                    self.redis_store = Some(store.clone());  // Keep direct reference for CDC
                    self.l2_store = Some(store);
                    tracing::Span::current().record("has_redis", true);
                    crate::metrics::set_backend_healthy("redis", true);
                    crate::metrics::record_startup_phase("redis_connect", phase_start.elapsed());
                    info!("Redis (L2) connected with merkle shadow tree");
                }
                Err(e) => {
                    tracing::Span::current().record("has_redis", false);
                    warn!(error = %e, "Failed to connect to Redis, continuing without L2 cache");
                    crate::metrics::set_backend_healthy("redis", false);
                    crate::metrics::record_connection_error("redis");
                }
            }
        } else {
            tracing::Span::current().record("has_redis", false);
        }

        // ========== PHASE 7: Sync Redis with SQL via branch diff ==========
        if let (Some(ref sql_merkle), Some(ref redis_merkle), Some(ref sql_root)) = 
            (&self.sql_merkle, &self.redis_merkle, &sql_root) 
        {
            let phase_start = std::time::Instant::now();
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
                    
                    match self.sync_redis_from_sql_diff(sql_merkle, redis_merkle).await {
                        Ok(synced) => {
                            info!(items_synced = synced, "Redis sync complete via branch diff");
                            crate::metrics::record_items_written("L2", synced);
                        }
                        Err(e) => {
                            warn!(error = %e, "Branch diff sync failed - Redis may be stale");
                            crate::metrics::record_error("L2", "sync", "branch_diff");
                        }
                    }
                }
                Ok(None) => {
                    info!("Redis merkle tree is empty - will be populated on writes");
                }
                Err(e) => {
                    warn!(error = %e, "Failed to get Redis merkle root - Redis may be stale");
                    crate::metrics::record_error("L2", "merkle", "root_hash");
                }
            }
            crate::metrics::record_startup_phase("redis_sync", phase_start.elapsed());
        }

        let _ = self.state.send(EngineState::Ready);
        crate::metrics::record_startup_total(startup_start.elapsed());
        info!("Sync engine ready (trust-verified startup complete)");
        Ok(())
    }
    
    /// Restore cuckoo filters from snapshots if merkle roots match
    async fn restore_cuckoo_filters(&self, sql_root: &Option<[u8; 32]>) {
        let persistence = match &self.filter_persistence {
            Some(p) => p,
            None => return,
        };
        
        let sql_root = match sql_root {
            Some(r) => r,
            None => return,
        };
        
        // Note: L2 filter removed (TTL makes it untrustworthy - use Redis EXISTS)
        
        // Try L3 filter
        match persistence.load(L3_FILTER_ID).await {
            Ok(Some(state)) if &state.merkle_root == sql_root => {
                if let Err(e) = self.l3_filter.import(&state.filter_bytes) {
                    warn!(error = %e, "Failed to import L3 filter from snapshot");
                } else {
                    self.l3_filter.mark_trusted();
                    info!(entries = state.entry_count, "Restored L3 cuckoo filter from snapshot");
                }
            }
            Ok(Some(_)) => warn!("L3 CF snapshot merkle root mismatch - filter will be rebuilt"),
            Ok(None) => info!("No L3 CF snapshot found - filter will be built on warmup"),
            Err(e) => warn!(error = %e, "Failed to load L3 CF snapshot"),
        }
    }
    
    /// Sync Redis from SQL by diffing merkle trees and only syncing stale branches.
    async fn sync_redis_from_sql_diff(
        &self,
        sql_merkle: &SqlMerkleStore,
        redis_merkle: &RedisMerkleStore,
    ) -> Result<usize, StorageError> {
        let mut total_synced = 0;
        let stale_prefixes = self.find_stale_branches(sql_merkle, redis_merkle, "").await?;
        
        for prefix in stale_prefixes {
            info!(prefix = %prefix, "Syncing stale branch from SQL to Redis");
            
            let leaf_paths = sql_merkle.get_leaves_under(&prefix).await
                .map_err(|e| StorageError::Backend(format!("Failed to get leaves: {}", e)))?;
            
            if leaf_paths.is_empty() {
                continue;
            }
            
            let mut merkle_batch = MerkleBatch::new();
            
            if let Some(ref l3_store) = self.l3_store {
                for object_id in &leaf_paths {
                    if let Ok(Some(item)) = l3_store.get(object_id).await {
                        let payload_hash = PathMerkle::payload_hash(&item.content);
                        let leaf_hash = PathMerkle::leaf_hash(
                            &item.object_id,
                            item.version,
                            item.updated_at,
                            &payload_hash,
                        );
                        merkle_batch.insert(object_id.clone(), leaf_hash);
                        
                        if let Some(ref l2_store) = self.l2_store {
                            if let Err(e) = l2_store.put(&item).await {
                                warn!(id = %object_id, error = %e, "Failed to sync item to Redis");
                            } else {
                                total_synced += 1;
                            }
                        }
                    }
                }
                
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
    async fn find_stale_branches(
        &self,
        sql_merkle: &SqlMerkleStore,
        redis_merkle: &RedisMerkleStore,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        let mut stale = Vec::new();
        
        let sql_children = sql_merkle.get_children(prefix).await
            .map_err(|e| StorageError::Backend(format!("SQL merkle error: {}", e)))?;
        let redis_children = redis_merkle.get_children(prefix).await
            .map_err(|e| StorageError::Backend(format!("Redis merkle error: {}", e)))?;
        
        if sql_children.is_empty() {
            return Ok(stale);
        }
        
        for (child_path, sql_hash) in sql_children {
            match redis_children.get(&child_path) {
                Some(redis_hash) if redis_hash == &sql_hash => continue,
                Some(_) => {
                    if child_path.contains('.') && !child_path.ends_with('.') {
                        stale.push(child_path);
                    } else {
                        let sub_stale = Box::pin(
                            self.find_stale_branches(sql_merkle, redis_merkle, &child_path)
                        ).await?;
                        stale.extend(sub_stale);
                    }
                }
                None => stale.push(child_path),
            }
        }
        
        Ok(stale)
    }

    /// Warm up L1 cache from L2/L3 (call after start)
    #[tracing::instrument(skip(self))]
    pub async fn warm_up(&mut self) -> Result<(), StorageError> {
        let _ = self.state.send(EngineState::WarmingUp);
        info!("Warming up cuckoo filter and L1 cache...");
        
        if let Some(l3) = &self.l3_store {
            let batch_size = self.config.cuckoo_warmup_batch_size;
            info!(batch_size, "Warming L3 cuckoo filter from MySQL...");
            
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
                    
                    if loaded % 10_000 == 0 || loaded == total_count as usize {
                        debug!(loaded, total = %total_count, "L3 filter warmup progress");
                    }
                }
                
                self.l3_filter.mark_trusted();
                info!(loaded, trust_state = ?self.l3_filter.trust_state(), "L3 cuckoo filter warmup complete");
            } else {
                info!("L3 store is empty, skipping filter warmup");
                self.l3_filter.mark_trusted();
            }
        }
        
        info!(
            l3_trust = ?self.l3_filter.trust_state(),
            "Cuckoo filter warmup complete (L3 only, Redis uses EXISTS)"
        );

        let _ = self.state.send(EngineState::Ready);
        info!("Warm-up complete, engine ready");
        Ok(())
    }

    /// Perform one tick of maintenance (for manual control instead of run loop).
    pub async fn tick(&self) {
        self.maybe_evict();
        self.maybe_flush_l2().await;
    }

    /// Force flush all pending L2 batches immediately.
    pub async fn force_flush(&self) {
        let batch = self.l2_batcher.lock().await.force_flush();
        if let Some(batch) = batch {
            debug!(batch_size = batch.items.len(), "Force flushing L2 batch");
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

        loop {
            tokio::select! {
                Ok(()) = self.config_rx.changed() => {
                    let new_config = self.config_rx.borrow().clone();
                    info!("Config updated: l1_max_bytes={}", new_config.l1_max_bytes);
                    self.config = new_config;
                }
                
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    self.maybe_evict();
                    self.maybe_flush_l2().await;
                    self.maybe_snapshot_cf_by_threshold().await;
                }
                
                _ = health_check_interval.tick() => {
                    self.check_mysql_health().await;
                }
                
                _ = wal_drain_interval.tick() => {
                    self.maybe_drain_wal().await;
                }
                
                _ = cf_snapshot_interval.tick() => {
                    self.maybe_snapshot_cf_by_time().await;
                }
            }
        }
    }

    /// Initiate graceful shutdown
    #[tracing::instrument(skip(self))]
    pub async fn shutdown(&self) {
        use crate::FlushReason;
        
        let shutdown_start = std::time::Instant::now();
        info!("Initiating sync engine shutdown...");
        let _ = self.state.send(EngineState::ShuttingDown);
        
        let batch = self.l2_batcher.lock().await.force_flush_with_reason(FlushReason::Shutdown);
        if let Some(batch) = batch {
            let batch_size = batch.items.len();
            info!(batch_size, "Flushing final L2 batch on shutdown");
            {
                let mut batcher = self.l2_batcher.lock().await;
                batcher.add_batch(batch.items);
            }
            self.maybe_flush_l2().await;
            crate::metrics::record_items_written("L2", batch_size);
        }
        
        self.snapshot_cuckoo_filters("shutdown").await;
        
        crate::metrics::record_startup_phase("shutdown", shutdown_start.elapsed());
        info!("Sync engine shutdown complete");
    }
}
