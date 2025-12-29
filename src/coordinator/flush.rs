//! Batch flushing and maintenance operations.
//!
//! Internal operations for flushing batches to L2/L3, WAL management,
//! cuckoo filter snapshots, and health checks.

use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{info, warn, debug, error};

use crate::storage::traits::StorageError;
use crate::sync_item::SyncItem;
use crate::merkle::{MerkleBatch, PathMerkle};
use crate::batching::hybrid_batcher::FlushBatch;
use crate::cuckoo::{L2_FILTER_ID, L3_FILTER_ID};

use super::{SyncEngine, WriteTarget};

impl SyncEngine {
    /// Snapshot CF if insert threshold exceeded
    pub(super) async fn maybe_snapshot_cf_by_threshold(&self) {
        let inserts = self.cf_inserts_since_snapshot.load(Ordering::Relaxed);
        if inserts >= self.config.cf_snapshot_insert_threshold {
            self.snapshot_cuckoo_filters("threshold").await;
        }
    }
    
    /// Snapshot CF if time interval elapsed
    pub(super) async fn maybe_snapshot_cf_by_time(&self) {
        let last_snapshot = self.cf_last_snapshot.lock().await;
        let elapsed = last_snapshot.elapsed().as_secs();
        drop(last_snapshot);
        
        let inserts = self.cf_inserts_since_snapshot.load(Ordering::Relaxed);
        if inserts > 0 && elapsed >= self.config.cf_snapshot_interval_secs {
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
        
        match wal.drain_to(l3.as_ref(), self.config.wal_drain_batch_size).await {
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

    /// Internal: flush a batch to L2/L3, update Merkle tree
    pub(super) async fn flush_batch_internal(&self, batch: FlushBatch<SyncItem>) {
        let batch_size = batch.items.len();
        debug!(batch_size = batch_size, reason = ?batch.reason, "Flushing L2 batch");
        
        let mut items: Vec<SyncItem> = batch.items;
        
        // ====== L2: Pipelined Redis writes ======
        let mut l2_success = 0;
        let mut l2_errors = 0;
        
        if let Some(ref l2) = self.l2_store {
            match l2.put_batch(&items).await {
                Ok(result) => {
                    l2_success = result.written;
                    for item in &items {
                        self.l2_filter.insert(&item.object_id);
                    }
                    debug!(written = result.written, "L2 pipelined batch write complete");
                }
                Err(e) => {
                    warn!(error = %e, "L2 batch write failed");
                    l2_errors = items.len();
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
                            for item in &items {
                                self.l3_filter.insert(&item.object_id);
                            }
                            self.mysql_health.record_success();
                            debug!(batch_id = %result.batch_id, written = result.written, "L3 batch write verified");
                        } else {
                            warn!(batch_id = %result.batch_id, "L3 batch verification failed");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "L3 batch write failed, falling back to WAL");
                        self.mysql_health.record_failure();
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
        } else if let Some(ref wal) = self.l3_wal {
            for item in &items {
                if let Err(e) = wal.write(item).await {
                    warn!(id = %item.object_id, error = %e, "WAL write failed!");
                } else {
                    wal_fallback += 1;
                }
            }
        }
        
        // ====== Merkle: Compute and update hashes ======
        let mut merkle_batch = MerkleBatch::new();
        
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
        
        if !merkle_batch.is_empty() {
            if let Some(ref sql_merkle) = self.sql_merkle {
                if let Err(e) = sql_merkle.apply_batch(&merkle_batch).await {
                    error!(error = %e, "Failed to update SQL Merkle tree (ground truth)");
                } else {
                    debug!(merkle_updates = merkle_batch.len(), "SQL merkle tree updated");
                    self.cf_inserts_since_snapshot.fetch_add(merkle_batch.len() as u64, Ordering::Relaxed);
                }
            }
            
            if let Some(ref redis_merkle) = self.redis_merkle {
                if let Err(e) = redis_merkle.apply_batch(&merkle_batch).await {
                    warn!(error = %e, "Failed to update Redis Merkle tree (cache)");
                } else {
                    debug!(merkle_updates = merkle_batch.len(), "Redis merkle tree updated");
                }
            }
        }
        
        info!(l2_success, l2_errors, l3_success, wal_fallback, reason = ?batch.reason, "Batch flush complete");
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
}
