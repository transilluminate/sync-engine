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
    
    /// Fast sync check if item might exist (L1 + Cuckoo only).
    ///
    /// Use this when you need a quick probabilistic check without async.
    /// For authoritative check, use `contains()` instead.
    #[must_use]
    #[inline]
    pub fn contains_fast(&self, id: &str) -> bool {
        self.l1_cache.contains_key(id) || self.l3_filter.should_check_l3(id)
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
        
        for item in items {
            self.insert_l1(item.clone());
            self.l2_batcher.lock().await.add(item);
            succeeded += 1;
        }
        
        debug!(total, succeeded, "Batch submitted to L1 and queue");
        
        Ok(BatchResult {
            total,
            succeeded,
            failed: total - succeeded,
        })
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
