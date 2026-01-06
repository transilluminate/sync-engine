// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Batch flushing and maintenance operations.
//!
//! Internal operations for flushing batches to L2/L3, WAL management,
//! cuckoo filter snapshots, and health checks.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{info, warn, debug, error};

use crate::cdc::CdcEntry;
use crate::storage::traits::StorageError;
use crate::sync_item::SyncItem;
use crate::submit_options::OptionsKey;
use crate::merkle::{MerkleBatch, PathMerkle};
use crate::batching::hybrid_batcher::{FlushBatch, FlushReason};
use crate::cuckoo::L3_FILTER_ID;

use super::{SyncEngine, WriteTarget};

impl SyncEngine {
    /// Snapshot CF if insert threshold exceeded
    pub(super) async fn maybe_snapshot_cf_by_threshold(&self) {
        let inserts = self.cf_inserts_since_snapshot.load(Ordering::Relaxed);
        if inserts >= self.config.read().cf_snapshot_insert_threshold {
            self.snapshot_cuckoo_filters("threshold").await;
        }
    }
    
    /// Snapshot CF if time interval elapsed
    pub(super) async fn maybe_snapshot_cf_by_time(&self) {
        let last_snapshot = self.cf_last_snapshot.lock().await;
        let elapsed = last_snapshot.elapsed().as_secs();
        drop(last_snapshot);
        
        let inserts = self.cf_inserts_since_snapshot.load(Ordering::Relaxed);
        if inserts > 0 && elapsed >= self.config.read().cf_snapshot_interval_secs {
            self.snapshot_cuckoo_filters("time").await;
        }
    }
    
    /// Snapshot both L2 and L3 cuckoo filters to WAL SQLite
    pub(super) async fn snapshot_cuckoo_filters(&self, reason: &str) {
        let persistence = match &self.filter_persistence {
            Some(p) => p,
            None => return,
        };
        
        let merkle_root: [u8; 32] = if let Some(ref sql_merkle) = self.sql_merkle {
            match sql_merkle.root_hash().await {
                Ok(Some(root)) => root,
                Ok(None) => [0u8; 32],
                Err(e) => {
                    warn!(error = %e, "Cannot snapshot CF: failed to get SQL merkle root");
                    return;
                }
            }
        } else {
            [0u8; 32]
        };
        
        let inserts = self.cf_inserts_since_snapshot.swap(0, Ordering::Relaxed);
        
        // Note: L2 filter removed (TTL makes it untrustworthy - use Redis EXISTS)
        
        // Snapshot L3 filter only
        if let Some(l3_bytes) = self.l3_filter.export() {
            let l3_count = self.l3_filter.len();
            if let Err(e) = persistence.save(L3_FILTER_ID, &l3_bytes, &merkle_root, l3_count).await {
                warn!(error = %e, "Failed to snapshot L3 cuckoo filter");
            }
        }
        
        *self.cf_last_snapshot.lock().await = Instant::now();
        
        info!(
            reason = reason,
            inserts_since_last = inserts,
            l3_entries = self.l3_filter.len(),
            merkle_root = %hex::encode(merkle_root),
            "Cuckoo filter snapshot saved (L3 only)"
        );
    }
    
    /// Check MySQL health and log status changes
    pub(super) async fn check_mysql_health(&self) {
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
    pub(super) async fn maybe_drain_wal(&self) {
        if !self.mysql_health.is_healthy() {
            return;
        }
        
        let Some(ref wal) = self.l3_wal else { return };
        if !wal.has_pending() { return }
        let Some(ref l3) = self.l3_store else { return };
        
        let batch_size = self.config.read().wal_drain_batch_size;
        match wal.drain_to(l3.as_ref(), batch_size).await {
            Ok(drained_ids) if !drained_ids.is_empty() => {
                for id in &drained_ids {
                    self.l3_filter.insert(id);
                }
                info!(drained = drained_ids.len(), "WAL drained to MySQL, L3 filter updated");
                self.mysql_health.record_success();
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "WAL drain failed, MySQL may be down again");
                self.mysql_health.record_failure();
            }
        }
    }
    
    /// Flush L2 batch if ready
    pub(super) async fn maybe_flush_l2(&self) {
        let batch = {
            let mut batcher = self.l2_batcher.lock().await;
            batcher.take_if_ready()
        };
        
        if let Some(batch) = batch {
            self.flush_batch_internal(batch).await;
        }
    }

    /// Maximum view batches to flush per tick.
    /// Limits work per iteration to avoid starving the main event loop.
    const VIEW_BATCHES_PER_TICK: usize = 5;

    /// Flush pending view batches from the dedicated queue.
    ///
    /// Drains up to [`VIEW_BATCHES_PER_TICK`] batches per call to avoid
    /// blocking the main loop for too long.
    pub(super) async fn maybe_flush_views(&self) {
        let mut rx = self.view_queue_rx.lock().await;
        
        let mut count = 0;
        while count < Self::VIEW_BATCHES_PER_TICK {
            match rx.try_recv() {
                Ok(items) => {
                    if !items.is_empty() {
                        let total_bytes = items.iter().map(|i| i.content.len()).sum();
                        let batch = FlushBatch {
                            items,
                            total_bytes,
                            reason: FlushReason::ViewQueue,
                        };
                        self.flush_batch_internal(batch).await;
                    }
                    count += 1;
                },
                Err(_) => break,
            }
        }
    }

    /// Internal: flush a batch to L2/L3, grouped by options for efficiency.
    ///
    /// Items are grouped by their `OptionsKey` so that:
    /// - All items with same options go in one Redis pipeline
    /// - All items with same options go in one SQL batch INSERT
    ///
    /// **Deduplication**: Under high concurrency, the same `object_id` may appear
    /// multiple times in a batch window. We deduplicate by `object_id`, keeping
    /// the latest version (last occurrence). This prevents:
    /// - SQL deadlocks from `INSERT ... ON DUPLICATE KEY UPDATE` on same rows
    /// - Verification mismatches from counting duplicates vs distinct rows
    pub(super) async fn flush_batch_internal(&self, batch: FlushBatch<SyncItem>) {
        let flush_start = std::time::Instant::now();
        let batch_size = batch.items.len();
        debug!(batch_size = batch_size, reason = ?batch.reason, "Flushing batch");
        
        // ====== Group items by compatible options, deduplicate by object_id ======
        // Using HashMap<object_id, SyncItem> ensures each object_id appears once.
        // Later items overwrite earlier ones, keeping the latest version.
        let mut groups: HashMap<OptionsKey, HashMap<String, SyncItem>> = HashMap::new();
        for item in batch.items {
            let key = OptionsKey::from(&item.effective_options());
            // .insert() replaces existing, keeping the latest version from the batch
            groups.entry(key).or_default().insert(item.object_id.clone(), item);
        }
        
        let group_count = groups.len();
        let deduped_total: usize = groups.values().map(|g| g.len()).sum();
        if deduped_total < batch_size {
            debug!(
                original = batch_size,
                deduped = deduped_total,
                "Deduplicated batch (same object_id updated multiple times)"
            );
        }
        debug!(group_count, items = deduped_total, "Grouped items by options");
        
        // ====== Flush each group ======
        let mut total_l2_success = 0;
        let mut total_l2_errors = 0;
        let mut total_l2_bytes = 0usize;
        let mut total_l3_success = 0;
        let mut total_l3_bytes = 0usize;
        let mut total_wal_fallback = 0;
        
        for (options_key, items_map) in groups {
            // Convert HashMap to Vec for downstream processing
            let mut items: Vec<SyncItem> = items_map.into_values().collect();
            let options = options_key.to_options();
            let group_size = items.len();
            let group_bytes: usize = items.iter().map(|i| i.content.len()).sum();
            
            // ====== L2: Pipelined Redis writes (if enabled for this group) ======
            if options.redis {
                if let Some(ref l2) = self.l2_store {
                    let l2_start = std::time::Instant::now();
                    // Get TTL in seconds from CacheTtl enum
                    let ttl_secs = options.redis_ttl.as_ref().map(|ttl| ttl.to_duration().as_secs());
                    match l2.put_batch_with_ttl(&items, ttl_secs).await {
                        Ok(result) => {
                            total_l2_success += result.written;
                            total_l2_bytes += group_bytes;
                            crate::metrics::record_latency("L2", "batch_write", l2_start.elapsed());
                            // Note: No L2 filter - TTL makes filters untrustworthy
                            debug!(written = result.written, ttl = ?ttl_secs, "L2 group write complete");
                            
                            // ====== CDC: Emit PUT entries after successful L2 write ======
                            if self.config.read().enable_cdc_stream {
                                self.emit_cdc_put_batch(&items).await;
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, group_size, "L2 group write failed");
                            total_l2_errors += group_size;
                            crate::metrics::record_error("L2", "batch_write", "backend");
                            crate::metrics::record_connection_error("redis");
                        }
                    }
                }
            }
            
            // ====== L3: Batched SQL writes (if enabled for this group) ======
            if options.sql {
                if self.mysql_health.is_healthy() {
                    if let Some(ref l3) = self.l3_store {
                        // Acquire semaphore permit to limit concurrent SQL writes
                        let _permit = self.sql_write_semaphore.acquire().await;
                        
                        let l3_start = std::time::Instant::now();
                        match l3.put_batch(&mut items).await {
                            Ok(result) => {
                                total_l3_success += result.written;
                                total_l3_bytes += group_bytes;
                                crate::metrics::record_latency("L3", "batch_write", l3_start.elapsed());
                                if result.verified {
                                    for item in &items {
                                        self.l3_filter.insert(&item.object_id);
                                    }
                                    self.mysql_health.record_success();
                                    crate::metrics::set_backend_healthy("mysql", true);
                                    debug!(batch_id = %result.batch_id, written = result.written, "L3 group write verified");
                                } else {
                                    warn!(batch_id = %result.batch_id, "L3 group verification failed");
                                    crate::metrics::record_error("L3", "batch_write", "verification");
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, group_size, "L3 group write failed, falling back to WAL");
                                self.mysql_health.record_failure();
                                crate::metrics::record_error("L3", "batch_write", "backend");
                                crate::metrics::record_connection_error("mysql");
                                crate::metrics::set_backend_healthy("mysql", false);
                                if let Some(ref wal) = self.l3_wal {
                                    for item in &items {
                                        if let Err(e) = wal.write(item).await {
                                            warn!(id = %item.object_id, error = %e, "WAL write also failed!");
                                            crate::metrics::record_error("WAL", "write", "io");
                                        } else {
                                            total_wal_fallback += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else if let Some(ref wal) = self.l3_wal {
                    // MySQL unhealthy - write directly to WAL
                    crate::metrics::set_backend_healthy("mysql", false);
                    for item in &items {
                        if let Err(e) = wal.write(item).await {
                            warn!(id = %item.object_id, error = %e, "WAL write failed!");
                            crate::metrics::record_error("WAL", "write", "io");
                        } else {
                            total_wal_fallback += 1;
                        }
                    }
                }
            }
            
            // No need to collect items for Merkle anymore - we use dirty flag in SQL
        }
        
        // ====== Merkle: Calculate for dirty items from SQL ======
        // Only run merkle calculation if enabled for this instance
        // In multi-instance deployments, only designated nodes calculate merkle
        let merkle_config = {
            let cfg = self.config.read();
            (cfg.merkle_calc_enabled, cfg.merkle_calc_jitter_ms, cfg.batch_flush_count)
        };
        if merkle_config.0 {
            // Apply jitter to reduce contention between instances
            if merkle_config.1 > 0 {
                let jitter = rand::random::<u64>() % merkle_config.1;
                tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;
            }
            
            // Fetch dirty items from SQL (up to batch size limit)
            let dirty_limit = merkle_config.2.max(1000);
            let dirty_items = if let Some(ref sql) = self.sql_store {
                match sql.get_dirty_merkle_items(dirty_limit).await {
                    Ok(items) => items,
                    Err(e) => {
                        warn!(error = %e, "Failed to fetch dirty merkle items");
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            };
            
            if !dirty_items.is_empty() {
                let mut merkle_batch = MerkleBatch::new();
                let mut processed_ids: Vec<String> = Vec::with_capacity(dirty_items.len());
                
                for item in &dirty_items {
                    let payload_hash = PathMerkle::payload_hash(&item.content);
                    let leaf_hash = PathMerkle::leaf_hash(
                        &item.object_id,
                        item.version,
                        &payload_hash,
                    );
                    merkle_batch.insert(item.object_id.clone(), leaf_hash);
                    processed_ids.push(item.object_id.clone());
                }
                
                // Apply merkle batch to SQL (ground truth) and Redis (cache)
                // Acquire semaphore to limit concurrent SQL merkle writes
                let mut sql_success = false;
                if let Some(ref sql_merkle) = self.sql_merkle {
                    let _permit = self.sql_write_semaphore.acquire().await;
                    if let Err(e) = sql_merkle.apply_batch(&merkle_batch).await {
                        error!(error = %e, "Failed to update SQL Merkle tree (ground truth)");
                    } else {
                        debug!(merkle_updates = merkle_batch.len(), "SQL merkle tree updated");
                        self.cf_inserts_since_snapshot.fetch_add(merkle_batch.len() as u64, Ordering::Relaxed);
                        sql_success = true;
                    }
                }
                
                if let Some(ref redis_merkle) = self.redis_merkle {
                    if let Err(e) = redis_merkle.apply_batch(&merkle_batch).await {
                        warn!(error = %e, "Failed to update Redis Merkle tree (cache)");
                        crate::metrics::record_error("merkle", "redis_update", "backend");
                    } else {
                        debug!(merkle_updates = merkle_batch.len(), "Redis merkle tree updated");
                        crate::metrics::record_merkle_operation("redis", "batch_update", true);
                    }
                }
                
                // Mark items clean ONLY if SQL merkle succeeded (ground truth)
                if sql_success {
                    if let Some(ref sql) = self.sql_store {
                        if let Err(e) = sql.mark_merkle_clean(&processed_ids).await {
                            warn!(error = %e, count = processed_ids.len(), "Failed to mark items merkle-clean");
                        } else {
                            debug!(count = processed_ids.len(), "Marked items merkle-clean");
                        }
                    }
                }
            }
        } else if batch_size > 0 {
            debug!(
                items = batch_size, 
                "Merkle calculation disabled for this instance"
            );
        }
        
        // Record comprehensive metrics for OTEL export
        let flush_duration = flush_start.elapsed();
        crate::metrics::record_flush_duration(flush_duration);
        
        // Batch sizes
        crate::metrics::record_batch_size("L2", total_l2_success);
        crate::metrics::record_batch_size("L3", total_l3_success);
        crate::metrics::record_batch_bytes("L2", total_l2_bytes);
        crate::metrics::record_batch_bytes("L3", total_l3_bytes);
        
        // Throughput
        crate::metrics::record_bytes_written("L2", total_l2_bytes);
        crate::metrics::record_bytes_written("L3", total_l3_bytes);
        crate::metrics::record_items_written("L2", total_l2_success);
        crate::metrics::record_items_written("L3", total_l3_success);
        
        // Operation outcomes
        if total_l2_success > 0 {
            crate::metrics::record_operation("L2", "batch_write", "success");
        }
        if total_l2_errors > 0 {
            crate::metrics::record_operation("L2", "batch_write", "error");
        }
        if total_l3_success > 0 {
            crate::metrics::record_operation("L3", "batch_write", "success");
        }
        if total_wal_fallback > 0 {
            crate::metrics::record_operation("WAL", "fallback", "success");
            crate::metrics::record_items_written("WAL", total_wal_fallback);
        }
        
        info!(
            l2_success = total_l2_success, 
            l2_errors = total_l2_errors, 
            l3_success = total_l3_success, 
            wal_fallback = total_wal_fallback, 
            groups = group_count,
            flush_ms = flush_duration.as_millis(),
            reason = ?batch.reason, 
            "Batch flush complete"
        );
    }
    
    /// Write an item to L3 (MySQL) or fall back to WAL if MySQL is unavailable.
    #[allow(dead_code)]
    pub(super) async fn write_to_l3_or_wal(&self, item: &SyncItem) -> Result<WriteTarget, StorageError> {
        if self.mysql_health.is_healthy() {
            if let Some(ref l3) = self.l3_store {
                match l3.put(item).await {
                    Ok(()) => {
                        self.mysql_health.record_success();
                        return Ok(WriteTarget::L3);
                    }
                    Err(e) => {
                        debug!(id = %item.object_id, error = %e, "L3 write failed, falling back to WAL");
                        self.mysql_health.record_failure();
                    }
                }
            }
        }
        
        if let Some(ref wal) = self.l3_wal {
            wal.write(item).await?;
            return Ok(WriteTarget::Wal);
        }
        
        Err(StorageError::Backend("No L3 or WAL available".to_string()))
    }
    
    // ========================================================================
    // CDC Stream Helpers
    // ========================================================================
    
    /// Emit CDC PUT entries for a batch of items.
    /// 
    /// Called after successful L2 write. Uses pipelining for efficiency.
    /// Failures are logged but don't block the main write path.
    pub(super) async fn emit_cdc_put_batch(&self, items: &[SyncItem]) {
        let Some(ref redis) = self.redis_store else { return };
        
        if items.is_empty() {
            return;
        }
        
        // Build CDC entries
        let entries: Vec<CdcEntry> = items.iter()
            .map(|item| {
                CdcEntry::put(
                    item.object_id.clone(),
                    item.content_hash.clone(),
                    &item.content,
                    item.content_type.as_str(),
                    item.version,
                    item.updated_at,
                    item.trace_parent.clone(),
                )
            })
            .collect();
        
        let cdc_start = std::time::Instant::now();
        let maxlen = self.config.read().cdc_stream_maxlen;
        match redis.xadd_cdc_batch(&entries, maxlen).await {
            Ok(ids) => {
                debug!(count = ids.len(), "CDC PUT entries emitted");
                crate::metrics::record_cdc_entries("PUT", ids.len());
                crate::metrics::record_latency("CDC", "put_batch", cdc_start.elapsed());
            }
            Err(e) => {
                // Log but don't fail - CDC is best-effort, merkle repair catches gaps
                warn!(error = %e, count = items.len(), "Failed to emit CDC entries");
                crate::metrics::record_error("CDC", "put", "backend");
            }
        }
    }
    
    /// Emit a CDC DELETE entry for a single item.
    /// 
    /// Called after successful delete. Best-effort, doesn't block delete.
    pub(super) async fn emit_cdc_delete(&self, id: &str) {
        let Some(ref redis) = self.redis_store else { return };
        
        let entry = CdcEntry::delete(id.to_string());
        
        let cdc_start = std::time::Instant::now();
        let maxlen = self.config.read().cdc_stream_maxlen;
        match redis.xadd_cdc(&entry, maxlen).await {
            Ok(_entry_id) => {
                debug!(id = %id, "CDC DEL entry emitted");
                crate::metrics::record_cdc_entries("DEL", 1);
                crate::metrics::record_latency("CDC", "delete", cdc_start.elapsed());
            }
            Err(e) => {
                warn!(error = %e, id = %id, "Failed to emit CDC delete entry");
                crate::metrics::record_error("CDC", "delete", "backend");
            }
        }
    }
}
