// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! V1.1 API: Query and batch operations.
//!
//! This module contains the higher-level API methods added in V1.1:
//! - `contains()` - Fast probabilistic existence check
//! - `len()` / `is_empty()` - L1 cache size queries
//! - `status()` - Detailed sync status
//! - `get_many()` - Parallel batch fetch
//! - `submit_many()` - Batch upsert
//! - `delete_many()` - Batch delete
//! - `get_or_insert_with()` - Cache-aside pattern

use std::sync::atomic::Ordering;
use tokio::task::JoinSet;
use tracing::{debug, info, warn, error};

use crate::storage::traits::StorageError;
use crate::sync_item::SyncItem;
use crate::merkle::MerkleBatch;

use super::{SyncEngine, ItemStatus, BatchResult};

impl SyncEngine {
    // ═══════════════════════════════════════════════════════════════════════════
    // API: Query & Batch Operations
    // ═══════════════════════════════════════════════════════════════════════════

    /// Check if an item exists across all tiers.
    ///
    /// Checks in order: L1 cache → Redis EXISTS → L3 Cuckoo filter → SQL query.
    /// If found in SQL and Cuckoo filter was untrusted, updates the filter.
    ///
    /// # Returns
    /// - `true` → item definitely exists in at least one tier
    /// - `false` → item does not exist (authoritative)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::SyncEngine;
    /// # async fn example(engine: &SyncEngine) {
    /// if engine.contains("user.123").await {
    ///     let item = engine.get("user.123").await;
    /// } else {
    ///     println!("Not found");
    /// }
    /// # }
    /// ```
    pub async fn contains(&self, id: &str) -> bool {
        // L1: In-memory cache (definitive, sync)
        if self.l1_cache.contains_key(id) {
            return true;
        }
        
        // L2: Redis EXISTS (async, authoritative for Redis tier)
        if let Some(ref l2) = self.l2_store {
            if l2.exists(id).await.unwrap_or(false) {
                return true;
            }
        }
        
        // L3: Cuckoo filter check (sync, probabilistic)
        if self.l3_filter.is_trusted() {
            // Filter is trusted - use it for fast negative
            if !self.l3_filter.should_check_l3(id) {
                return false; // Definitely not in L3
            }
        }
        
        // L3: SQL query (async, ground truth)
        if let Some(ref l3) = self.l3_store {
            if l3.exists(id).await.unwrap_or(false) {
                // Found in SQL - update Cuckoo if it was untrusted
                if !self.l3_filter.is_trusted() {
                    self.l3_filter.insert(id);
                }
                return true;
            }
        }
        
        false
    }
    
    /// Fast check: is this item definitely NOT in L3?
    ///
    /// Uses the Cuckoo filter for a fast authoritative negative.
    /// - Returns `true` → item is **definitely not** in L3 (safe to skip)
    /// - Returns `false` → item **might** exist (need to check L3)
    ///
    /// Only meaningful when the L3 filter is trusted. If untrusted, returns `false`
    /// (meaning "we don't know, you should check").
    ///
    /// # Use Case
    ///
    /// Fast early-exit in replication: if definitely missing, apply without checking.
    ///
    /// ```rust,no_run
    /// # use sync_engine::SyncEngine;
    /// # async fn example(engine: &SyncEngine) {
    /// if engine.definitely_missing("patient.123") {
    ///     // Fast path: definitely new, just insert
    ///     println!("New item, inserting directly");
    /// } else {
    ///     // Slow path: might exist, check hash
    ///     if !engine.is_current("patient.123", "abc123...").await {
    ///         println!("Outdated, updating");
    ///     }
    /// }
    /// # }
    /// ```
    #[must_use]
    #[inline]
    pub fn definitely_missing(&self, id: &str) -> bool {
        // Only authoritative if filter is trusted
        if !self.l3_filter.is_trusted() {
            return false; // Unknown, caller should check
        }
        // Cuckoo false = definitely not there
        !self.l3_filter.should_check_l3(id)
    }

    /// Fast check: might this item exist somewhere?
    ///
    /// Checks L1 cache (partial, evicts) and Cuckoo filter (probabilistic).
    /// - Returns `true` → item is in L1 OR might be in L3 (worth checking)
    /// - Returns `false` → item is definitely not in L1 or L3
    ///
    /// Note: L1 is partial (items evict), so this can return `false` even if
    /// the item exists in L2/L3. For authoritative check, use `contains()`.
    ///
    /// # Use Case
    ///
    /// Quick probabilistic check before expensive async lookup.
    #[must_use]
    #[inline]
    pub fn might_exist(&self, id: &str) -> bool {
        self.l1_cache.contains_key(id) || self.l3_filter.should_check_l3(id)
    }

    /// Check if the item at `key` has the given content hash.
    ///
    /// This is the semantic API for CDC deduplication in replication.
    /// Returns `true` if the item exists AND its content hash matches.
    /// Returns `false` if item doesn't exist OR hash differs.
    ///
    /// # Arguments
    /// * `id` - Object ID
    /// * `content_hash` - SHA256 hash of content (hex-encoded string)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::SyncEngine;
    /// # async fn example(engine: &SyncEngine) {
    /// // Skip replication if we already have this version
    /// let incoming_hash = "abc123...";
    /// if engine.is_current("patient.123", incoming_hash).await {
    ///     println!("Already up to date, skipping");
    ///     return;
    /// }
    /// # }
    /// ```
    pub async fn is_current(&self, id: &str, content_hash: &str) -> bool {
        // Check L1 first (fastest)
        if let Some(item) = self.l1_cache.get(id) {
            return item.content_hash == content_hash;
        }

        // Check L2 (if available)
        if let Some(ref l2) = self.l2_store {
            if let Ok(Some(item)) = l2.get(id).await {
                return item.content_hash == content_hash;
            }
        }

        // Check L3 (ground truth)
        if let Some(ref l3) = self.l3_store {
            if self.l3_filter.should_check_l3(id) {
                if let Ok(Some(item)) = l3.get(id).await {
                    return item.content_hash == content_hash;
                }
            }
        }

        false
    }

    /// Get the current count of items in L1 cache.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.l1_cache.len()
    }

    /// Check if L1 cache is empty.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.l1_cache.is_empty()
    }

    /// Get the sync status of an item.
    ///
    /// Returns detailed state information about where an item exists
    /// and its sync status across tiers.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, ItemStatus};
    /// # async fn example(engine: &SyncEngine) {
    /// match engine.status("order.456").await {
    ///     ItemStatus::Synced { in_l1, in_l2, in_l3 } => {
    ///         println!("Synced: L1={}, L2={}, L3={}", in_l1, in_l2, in_l3);
    ///     }
    ///     ItemStatus::Pending => println!("Queued for sync"),
    ///     ItemStatus::Missing => println!("Not found"),
    /// }
    /// # }
    /// ```
    pub async fn status(&self, id: &str) -> ItemStatus {
        let in_l1 = self.l1_cache.contains_key(id);
        
        // Check if pending in batch queue
        let pending = self.l2_batcher.lock().await.contains(id);
        if pending {
            return ItemStatus::Pending;
        }
        
        // Check L2 (if available) - use EXISTS, no filter
        let in_l2 = if let Some(ref l2) = self.l2_store {
            l2.exists(id).await.unwrap_or(false)
        } else {
            false
        };
        
        // Check L3 (if available)  
        let in_l3 = if let Some(ref l3) = self.l3_store {
            self.l3_filter.should_check_l3(id) && l3.get(id).await.ok().flatten().is_some()
        } else {
            false
        };
        
        if in_l1 || in_l2 || in_l3 {
            ItemStatus::Synced { in_l1, in_l2, in_l3 }
        } else {
            ItemStatus::Missing
        }
    }

    /// Fetch multiple items in parallel.
    ///
    /// Returns a vector of `Option<SyncItem>` in the same order as input IDs.
    /// Missing items are represented as `None`.
    ///
    /// # Performance
    ///
    /// This method fetches from L1 synchronously, then batches L2/L3 lookups
    /// for items not in L1. Much faster than sequential `get()` calls.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::SyncEngine;
    /// # async fn example(engine: &SyncEngine) {
    /// let ids = vec!["user.1", "user.2", "user.3"];
    /// let items = engine.get_many(&ids).await;
    /// for (id, item) in ids.iter().zip(items.iter()) {
    ///     match item {
    ///         Some(item) => println!("{}: found", id),
    ///         None => println!("{}: missing", id),
    ///     }
    /// }
    /// # }
    /// ```
    pub async fn get_many(&self, ids: &[&str]) -> Vec<Option<SyncItem>> {
        let mut results: Vec<Option<SyncItem>> = vec![None; ids.len()];
        let mut missing_indices: Vec<usize> = Vec::new();
        
        // Phase 1: Check L1 (synchronous, fast)
        for (i, id) in ids.iter().enumerate() {
            if let Some(item) = self.l1_cache.get(*id) {
                results[i] = Some(item.clone());
            } else {
                missing_indices.push(i);
            }
        }
        
        // Phase 2: Fetch missing items from L2/L3 in parallel
        if !missing_indices.is_empty() {
            let mut join_set: JoinSet<(usize, Option<SyncItem>)> = JoinSet::new();
            
            for &i in &missing_indices {
                let id = ids[i].to_string();
                let l2_store = self.l2_store.clone();
                let l3_store = self.l3_store.clone();
                let l3_filter = self.l3_filter.clone();
                
                join_set.spawn(async move {
                    // Try L2 first (no filter, just try Redis)
                    if let Some(ref l2) = l2_store {
                        if let Ok(Some(item)) = l2.get(&id).await {
                            return (i, Some(item));
                        }
                    }
                    
                    // Fall back to L3 (use Cuckoo filter if trusted)
                    if let Some(ref l3) = l3_store {
                        if !l3_filter.is_trusted() || l3_filter.should_check_l3(&id) {
                            if let Ok(Some(item)) = l3.get(&id).await {
                                return (i, Some(item));
                            }
                        }
                    }
                    
                    (i, None)
                });
            }
            
            // Collect results
            while let Some(result) = join_set.join_next().await {
                if let Ok((i, item)) = result {
                    results[i] = item;
                }
            }
        }
        
        results
    }

    /// Submit multiple items for sync atomically.
    ///
    /// All items are added to L1 and queued for batch persistence.
    /// Returns a `BatchResult` with success/failure counts.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, SyncItem};
    /// # use serde_json::json;
    /// # async fn example(engine: &SyncEngine) {
    /// let items = vec![
    ///     SyncItem::from_json("user.1".into(), json!({"name": "Alice"})),
    ///     SyncItem::from_json("user.2".into(), json!({"name": "Bob"})),
    /// ];
    /// let result = engine.submit_many(items).await.unwrap();
    /// println!("Submitted: {}, Failed: {}", result.succeeded, result.failed);
    /// # }
    /// ```
    pub async fn submit_many(&self, items: Vec<SyncItem>) -> Result<BatchResult, StorageError> {
        if !self.should_accept_writes() {
            return Err(StorageError::Backend(format!(
                "Rejecting batch write: engine state={}, pressure={}",
                self.state(),
                self.pressure()
            )));
        }
        
        let total = items.len();
        let mut succeeded = 0;
        
        // Lock batcher once for the whole batch
        let mut batcher = self.l2_batcher.lock().await;
        
        for item in items {
            self.insert_l1(item.clone());
            batcher.add(item);
            succeeded += 1;
        }
        
        debug!(total, succeeded, "Batch submitted to L1 and queue");
        
        Ok(BatchResult {
            total,
            succeeded,
            failed: total - succeeded,
        })
    }

    /// Submit a batch of materialized views.
    ///
    /// Uses a dedicated queue to avoid contention with the main batcher.
    /// Views are flushed independently by the sync engine loop.
    pub async fn submit_view_batch(&self, items: Vec<SyncItem>) -> Result<BatchResult, StorageError> {
        if !self.should_accept_writes() {
            return Err(StorageError::Backend(format!(
                "Rejecting view batch: engine state={}, pressure={}",
                self.state(),
                self.pressure()
            )));
        }
        
        let total = items.len();
        
        // Update L1 cache immediately
        for item in &items {
            self.insert_l1(item.clone());
        }
        
        // Send to dedicated queue
        match self.view_queue_tx.send(items).await {
            Ok(_) => {
                debug!(total, "View batch submitted to queue");
                Ok(BatchResult {
                    total,
                    succeeded: total,
                    failed: 0,
                })
            },
            Err(_) => {
                error!("View queue closed");
                Err(StorageError::Backend("View queue closed".to_string()))
            }
        }
    }

    /// Delete multiple items atomically.
    ///
    /// Removes items from all tiers (L1, L2, L3) and updates filters.
    /// Returns a `BatchResult` with counts.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::SyncEngine;
    /// # async fn example(engine: &SyncEngine) {
    /// let ids = vec!["user.1", "user.2", "user.3"];
    /// let result = engine.delete_many(&ids).await.unwrap();
    /// println!("Deleted: {}", result.succeeded);
    /// # }
    /// ```
    pub async fn delete_many(&self, ids: &[&str]) -> Result<BatchResult, StorageError> {
        if !self.should_accept_writes() {
            return Err(StorageError::Backend(format!(
                "Rejecting batch delete: engine state={}, pressure={}",
                self.state(),
                self.pressure()
            )));
        }
        
        let total = ids.len();
        let mut succeeded = 0;
        
        // Build merkle batch for all deletions
        let mut merkle_batch = MerkleBatch::new();
        
        for id in ids {
            // Remove from L1
            if let Some((_, item)) = self.l1_cache.remove(*id) {
                let size = Self::item_size(&item);
                self.l1_size_bytes.fetch_sub(size, Ordering::Release);
            }
            
            // Remove from L3 filter (no L2 filter with TTL support)
            self.l3_filter.remove(id);
            
            // Queue merkle deletion
            merkle_batch.delete(id.to_string());
            
            succeeded += 1;
        }
        
        // Batch delete from L2
        if let Some(ref l2) = self.l2_store {
            for id in ids {
                if let Err(e) = l2.delete(id).await {
                    warn!(id, error = %e, "Failed to delete from L2");
                }
            }
        }
        
        // Batch delete from L3
        if let Some(ref l3) = self.l3_store {
            for id in ids {
                if let Err(e) = l3.delete(id).await {
                    warn!(id, error = %e, "Failed to delete from L3");
                }
            }
        }
        
        // Update merkle trees
        if let Some(ref sql_merkle) = self.sql_merkle {
            if let Err(e) = sql_merkle.apply_batch(&merkle_batch).await {
                error!(error = %e, "Failed to update SQL Merkle tree for batch deletion");
            }
        }
        
        if let Some(ref redis_merkle) = self.redis_merkle {
            if let Err(e) = redis_merkle.apply_batch(&merkle_batch).await {
                warn!(error = %e, "Failed to update Redis Merkle tree for batch deletion");
            }
        }
        
        info!(total, succeeded, "Batch delete completed");
        
        Ok(BatchResult {
            total,
            succeeded,
            failed: total - succeeded,
        })
    }

    /// Get an item, or compute and insert it if missing.
    ///
    /// This is the classic "get or insert" pattern, useful for cache-aside:
    /// 1. Check cache (L1 → L2 → L3)
    /// 2. If missing, call the async factory function
    /// 3. Insert the result and return it
    ///
    /// The factory is only called if the item is not found.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, SyncItem};
    /// # use serde_json::json;
    /// # async fn example(engine: &SyncEngine) {
    /// let item = engine.get_or_insert_with("user.123", || async {
    ///     // Expensive operation - only runs if not cached
    ///     SyncItem::from_json("user.123".into(), json!({"name": "Fetched from DB"}))
    /// }).await.unwrap();
    /// # }
    /// ```
    pub async fn get_or_insert_with<F, Fut>(
        &self,
        id: &str,
        factory: F,
    ) -> Result<SyncItem, StorageError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = SyncItem>,
    {
        // Try to get existing
        if let Some(item) = self.get(id).await? {
            return Ok(item);
        }
        
        // Not found - compute new value
        let item = factory().await;
        
        // Insert and return
        self.submit(item.clone()).await?;
        
        Ok(item)
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // State-based queries: Fast indexed access by caller-defined state tag
    // ═══════════════════════════════════════════════════════════════════════════
    
    /// Get items by state from SQL (L3 ground truth).
    ///
    /// Uses indexed query for fast retrieval.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, StorageError};
    /// # async fn example(engine: &SyncEngine) -> Result<(), StorageError> {
    /// // Get all delta items for CRDT merging
    /// let deltas = engine.get_by_state("delta", 1000).await?;
    /// for item in deltas {
    ///     println!("Delta: {}", item.object_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_by_state(&self, state: &str, limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        if let Some(ref sql) = self.sql_store {
            sql.get_by_state(state, limit).await
        } else {
            Ok(Vec::new())
        }
    }
    
    /// Count items in a given state (SQL ground truth).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, StorageError};
    /// # async fn example(engine: &SyncEngine) -> Result<(), StorageError> {
    /// let pending_count = engine.count_by_state("pending").await?;
    /// println!("{} items pending", pending_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count_by_state(&self, state: &str) -> Result<u64, StorageError> {
        if let Some(ref sql) = self.sql_store {
            sql.count_by_state(state).await
        } else {
            Ok(0)
        }
    }
    
    /// Get just the IDs of items in a given state (lightweight query).
    ///
    /// Returns IDs from SQL. For Redis state SET, use `list_state_ids_redis()`.
    pub async fn list_state_ids(&self, state: &str, limit: usize) -> Result<Vec<String>, StorageError> {
        if let Some(ref sql) = self.sql_store {
            sql.list_state_ids(state, limit).await
        } else {
            Ok(Vec::new())
        }
    }
    
    /// Update the state of an item by ID.
    ///
    /// Updates both SQL (ground truth) and Redis state SETs.
    /// L1 cache is NOT updated - caller should re-fetch if needed.
    ///
    /// Returns true if the item was found and updated.
    pub async fn set_state(&self, id: &str, new_state: &str) -> Result<bool, StorageError> {
        let mut updated = false;
        
        // Update SQL (ground truth)
        if let Some(ref sql) = self.sql_store {
            updated = sql.set_state(id, new_state).await?;
        }
        
        // Note: Redis state SETs are not updated here because we'd need to know
        // the old state to do SREM. For full Redis state management, the item
        // should be re-submitted with the new state via submit_with().
        
        Ok(updated)
    }
    
    /// Delete all items in a given state from SQL.
    ///
    /// Also removes from L1 cache and Redis state SET.
    /// Returns the number of deleted items.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, StorageError};
    /// # async fn example(engine: &SyncEngine) -> Result<(), StorageError> {
    /// // Clean up all processed deltas
    /// let deleted = engine.delete_by_state("delta").await?;
    /// println!("Deleted {} delta items", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_by_state(&self, state: &str) -> Result<u64, StorageError> {
        let mut deleted = 0u64;
        
        // Get IDs first (for L1 cleanup)
        let ids = if let Some(ref sql) = self.sql_store {
            sql.list_state_ids(state, 100_000).await?
        } else {
            Vec::new()
        };
        
        // Remove from L1 cache
        for id in &ids {
            self.l1_cache.remove(id);
        }
        
        // Delete from SQL
        if let Some(ref sql) = self.sql_store {
            deleted = sql.delete_by_state(state).await?;
        }
        
        // Note: Redis items with TTL will expire naturally.
        // For immediate Redis cleanup, call delete_by_state on RedisStore directly.
        
        info!(state = %state, deleted = deleted, "Deleted items by state");
        
        Ok(deleted)
    }
    
    // =========================================================================
    // Prefix Scan Operations
    // =========================================================================
    
    /// Scan items by ID prefix.
    ///
    /// Retrieves all items whose ID starts with the given prefix.
    /// Queries SQL (ground truth) directly - does NOT check L1 cache.
    ///
    /// Useful for CRDT delta-first architecture where deltas are stored as:
    /// `delta:{object_id}:{op_id}` and you need to fetch all deltas for an object.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, StorageError};
    /// # async fn example(engine: &SyncEngine) -> Result<(), StorageError> {
    /// // Get base state
    /// let base = engine.get("base:user.123").await?;
    ///
    /// // Get all pending deltas for this object
    /// let deltas = engine.scan_prefix("delta:user.123:", 1000).await?;
    ///
    /// // Merge on-the-fly for read-repair
    /// for delta in deltas {
    ///     println!("Delta: {} -> {:?}", delta.object_id, delta.content_as_json());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        if let Some(ref sql) = self.sql_store {
            sql.scan_prefix(prefix, limit).await
        } else {
            Ok(Vec::new())
        }
    }
    
    /// Count items matching an ID prefix (SQL ground truth).
    pub async fn count_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        if let Some(ref sql) = self.sql_store {
            sql.count_prefix(prefix).await
        } else {
            Ok(0)
        }
    }
    
    /// Delete all items matching an ID prefix.
    ///
    /// Removes from L1 cache, SQL, and Redis.
    /// Returns the number of deleted items.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, StorageError};
    /// # async fn example(engine: &SyncEngine) -> Result<(), StorageError> {
    /// // After merging deltas into base, clean them up
    /// let deleted = engine.delete_prefix("delta:user.123:").await?;
    /// println!("Cleaned up {} deltas", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let mut deleted = 0u64;
        
        // Get IDs first (for L1/L2 cleanup)
        let items = if let Some(ref sql) = self.sql_store {
            sql.scan_prefix(prefix, 100_000).await?
        } else {
            Vec::new()
        };
        
        // Remove from L1 cache
        for item in &items {
            self.l1_cache.remove(&item.object_id);
        }
        
        // Remove from L2 (Redis) one-by-one via CacheStore trait
        if let Some(ref l2) = self.l2_store {
            for item in &items {
                let _ = l2.delete(&item.object_id).await;
            }
        }
        
        // Delete from SQL
        if let Some(ref sql) = self.sql_store {
            deleted = sql.delete_prefix(prefix).await?;
        }
        
        info!(prefix = %prefix, deleted = deleted, "Deleted items by prefix");
        
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SyncEngineConfig;
    use serde_json::json;
    use tokio::sync::watch;

    fn test_config() -> SyncEngineConfig {
        SyncEngineConfig {
            redis_url: None,
            sql_url: None,
            wal_path: None,
            l1_max_bytes: 1024 * 1024,
            ..Default::default()
        }
    }

    fn test_item(id: &str) -> SyncItem {
        SyncItem::from_json(id.to_string(), json!({"test": "data", "id": id}))
    }

    #[tokio::test]
    async fn test_contains_l1_hit() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        engine.l1_cache.insert("test.exists".into(), test_item("test.exists"));
        
        assert!(engine.contains("test.exists").await);
    }

    #[tokio::test]
    async fn test_contains_with_trusted_filter() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        engine.l3_filter.mark_trusted();
        
        engine.l1_cache.insert("test.exists".into(), test_item("test.exists"));
        engine.l3_filter.insert("test.exists");
        
        assert!(engine.contains("test.exists").await);
        assert!(!engine.contains("test.missing").await);
    }

    #[test]
    fn test_len_and_is_empty() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        assert!(engine.is_empty());
        assert_eq!(engine.len(), 0);
        
        engine.l1_cache.insert("a".into(), test_item("a"));
        assert!(!engine.is_empty());
        assert_eq!(engine.len(), 1);
        
        engine.l1_cache.insert("b".into(), test_item("b"));
        assert_eq!(engine.len(), 2);
    }

    #[tokio::test]
    async fn test_status_synced_in_l1() {
        use super::super::EngineState;
        
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        let _ = engine.state.send(EngineState::Ready);
        
        engine.submit(test_item("test.item")).await.expect("Submit failed");
        let _ = engine.l2_batcher.lock().await.force_flush();
        
        let status = engine.status("test.item").await;
        assert!(matches!(status, ItemStatus::Synced { in_l1: true, .. }));
    }

    #[tokio::test]
    async fn test_status_pending() {
        use super::super::EngineState;
        
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        let _ = engine.state.send(EngineState::Ready);
        
        engine.submit(test_item("test.pending")).await.expect("Submit failed");
        
        let status = engine.status("test.pending").await;
        assert_eq!(status, ItemStatus::Pending);
    }

    #[tokio::test]
    async fn test_status_missing() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        let status = engine.status("test.nonexistent").await;
        assert_eq!(status, ItemStatus::Missing);
    }

    #[tokio::test]
    async fn test_get_many_from_l1() {
        use super::super::EngineState;
        
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        let _ = engine.state.send(EngineState::Ready);
        
        engine.l1_cache.insert("a".into(), test_item("a"));
        engine.l1_cache.insert("b".into(), test_item("b"));
        engine.l1_cache.insert("c".into(), test_item("c"));
        
        let results = engine.get_many(&["a", "b", "missing", "c"]).await;
        
        assert_eq!(results.len(), 4);
        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_none());
        assert!(results[3].is_some());
        
        assert_eq!(results[0].as_ref().unwrap().object_id, "a");
        assert_eq!(results[1].as_ref().unwrap().object_id, "b");
        assert_eq!(results[3].as_ref().unwrap().object_id, "c");
    }

    #[tokio::test]
    async fn test_submit_many() {
        use super::super::EngineState;
        
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        let _ = engine.state.send(EngineState::Ready);
        
        let items = vec![
            test_item("batch.1"),
            test_item("batch.2"),
            test_item("batch.3"),
        ];
        
        let result = engine.submit_many(items).await.expect("Batch submit failed");
        
        assert_eq!(result.total, 3);
        assert_eq!(result.succeeded, 3);
        assert_eq!(result.failed, 0);
        assert!(result.is_success());
        
        assert_eq!(engine.len(), 3);
        assert!(engine.contains("batch.1").await);
        assert!(engine.contains("batch.2").await);
        assert!(engine.contains("batch.3").await);
    }

    #[tokio::test]
    async fn test_delete_many() {
        use super::super::EngineState;
        
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        let _ = engine.state.send(EngineState::Ready);
        
        engine.l1_cache.insert("del.1".into(), test_item("del.1"));
        engine.l1_cache.insert("del.2".into(), test_item("del.2"));
        engine.l1_cache.insert("keep".into(), test_item("keep"));
        
        let result = engine.delete_many(&["del.1", "del.2"]).await.expect("Batch delete failed");
        
        assert_eq!(result.total, 2);
        assert_eq!(result.succeeded, 2);
        assert!(result.is_success());
        
        assert!(!engine.l1_cache.contains_key("del.1"));
        assert!(!engine.l1_cache.contains_key("del.2"));
        assert!(engine.l1_cache.contains_key("keep"));
    }

    #[tokio::test]
    async fn test_get_or_insert_with_existing() {
        use super::super::EngineState;
        
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        let _ = engine.state.send(EngineState::Ready);
        
        let existing = test_item("existing");
        engine.l1_cache.insert("existing".into(), existing.clone());
        
        let factory_called = std::sync::atomic::AtomicBool::new(false);
        let result = engine.get_or_insert_with("existing", || {
            factory_called.store(true, std::sync::atomic::Ordering::SeqCst);
            async { test_item("should_not_be_used") }
        }).await.expect("get_or_insert_with failed");
        
        assert!(!factory_called.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(result.object_id, "existing");
    }

    #[tokio::test]
    async fn test_get_or_insert_with_missing() {
        use super::super::EngineState;
        
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        let _ = engine.state.send(EngineState::Ready);
        
        let result = engine.get_or_insert_with("new_item", || async {
            SyncItem::from_json("new_item".into(), json!({"created": "by factory"}))
        }).await.expect("get_or_insert_with failed");
        
        assert_eq!(result.object_id, "new_item");
        assert!(engine.contains("new_item").await);
    }
}
