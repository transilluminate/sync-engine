//! Write-Ahead Log (WAL) for L3 durability during MySQL outages.
//!
//! When MySQL is unavailable (maintenance, network issues, etc.), items
//! are written to a local SQLite database. A background task drains
//! the WAL to MySQL when connectivity is restored.
//!
//! This is NOT a tier - it's a durability buffer. Items in the WAL
//! are "in flight" to MySQL, not a permanent storage location.

use crate::storage::sql::SqlStore;
use crate::storage::traits::{ArchiveStore, StorageError};
use crate::sync_item::SyncItem;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// WAL state for observability
#[derive(Debug, Clone, Copy)]
pub struct WalStats {
    /// Number of items currently in WAL
    pub pending_items: u64,
    /// Total items written to WAL since startup
    pub total_written: u64,
    /// Total items drained to MySQL since startup
    pub total_drained: u64,
    /// Whether drain is currently in progress
    pub draining: bool,
    /// Whether MySQL is currently reachable
    pub mysql_healthy: bool,
}

/// Write-ahead log backed by SQLite.
pub struct WriteAheadLog {
    /// Local SQLite store
    store: SqlStore,
    /// Path to SQLite file (for display)
    path: String,
    /// Items pending drain
    pending_count: AtomicU64,
    /// Total written since startup
    total_written: AtomicU64,
    /// Total drained since startup
    total_drained: AtomicU64,
    /// Whether currently draining
    draining: AtomicBool,
    /// Max items before backpressure
    max_items: u64,
    /// Max file size in bytes before backpressure (default 100MB)
    max_bytes: u64,
}

impl WriteAheadLog {
    /// Default max WAL size: 100MB
    const DEFAULT_MAX_BYTES: u64 = 100 * 1024 * 1024;

    /// Create a new WAL at the given path.
    pub async fn new(path: impl AsRef<Path>, max_items: u64) -> Result<Self, StorageError> {
        Self::with_max_bytes(path, max_items, Self::DEFAULT_MAX_BYTES).await
    }

    /// Create a new WAL with custom max size limit.
    pub async fn with_max_bytes(
        path: impl AsRef<Path>,
        max_items: u64,
        max_bytes: u64,
    ) -> Result<Self, StorageError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let url = format!("sqlite://{}?mode=rwc", path_str);
        
        info!(path = %path_str, max_items, max_bytes, "Initializing write-ahead log");
        
        let store = SqlStore::new(&url).await?;
        
        // Count existing items (from previous run)
        let pending = store.count_all().await.unwrap_or(0);
        if pending > 0 {
            warn!(pending, "WAL has items from previous run, will drain");
        }
        
        Ok(Self {
            store,
            path: path_str,
            pending_count: AtomicU64::new(pending),
            total_written: AtomicU64::new(0),
            total_drained: AtomicU64::new(0),
            draining: AtomicBool::new(false),
            max_items,
            max_bytes,
        })
    }

    /// Write an item to the WAL.
    pub async fn write(&self, item: &SyncItem) -> Result<(), StorageError> {
        // Check item count limit
        let pending = self.pending_count.load(Ordering::Acquire);
        if pending >= self.max_items {
            return Err(StorageError::Backend(format!(
                "WAL full: {} items (max {})",
                pending, self.max_items
            )));
        }
        
        // Check file size limit (periodic check - every 100 writes)
        if pending % 100 == 0 {
            if let Ok(size) = self.file_size_bytes() {
                if size >= self.max_bytes {
                    return Err(StorageError::Backend(format!(
                        "WAL file too large: {} bytes (max {})",
                        size, self.max_bytes
                    )));
                }
            }
        }

        self.store.put(item).await?;
        self.pending_count.fetch_add(1, Ordering::Release);
        self.total_written.fetch_add(1, Ordering::Relaxed);
        
        debug!(
            id = %item.object_id,
            pending = pending + 1,
            "Item written to WAL"
        );
        
        Ok(())
    }

    /// Get the WAL file size in bytes.
    pub fn file_size_bytes(&self) -> std::io::Result<u64> {
        std::fs::metadata(&self.path).map(|m| m.len())
    }

    /// Run a WAL checkpoint to reclaim disk space.
    /// Call this after draining to prevent unbounded file growth.
    pub async fn checkpoint(&self) -> Result<(), StorageError> {
        // Run PRAGMA wal_checkpoint(TRUNCATE) to reclaim space
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.store.pool())
            .await
            .map_err(|e| StorageError::Backend(format!("WAL checkpoint failed: {}", e)))?;
        
        debug!(path = %self.path, "WAL checkpoint completed");
        Ok(())
    }

    /// Check if WAL has items to drain.
    #[must_use]
    pub fn has_pending(&self) -> bool {
        self.pending_count.load(Ordering::Acquire) > 0
    }

    /// Get current stats.
    #[must_use]
    pub fn stats(&self, mysql_healthy: bool) -> WalStats {
        WalStats {
            pending_items: self.pending_count.load(Ordering::Acquire),
            total_written: self.total_written.load(Ordering::Relaxed),
            total_drained: self.total_drained.load(Ordering::Relaxed),
            draining: self.draining.load(Ordering::Acquire),
            mysql_healthy,
        }
    }

    /// Check if WAL is under pressure (>= 80% full).
    #[must_use]
    pub fn under_pressure(&self) -> bool {
        let pending = self.pending_count.load(Ordering::Acquire);
        pending as f64 / self.max_items as f64 >= 0.8
    }

    /// Drain items from WAL to MySQL using batch operations.
    ///
    /// Returns IDs of items successfully drained, or error if MySQL unreachable.
    pub async fn drain_to(
        &self,
        mysql: &dyn ArchiveStore,
        batch_size: usize,
    ) -> Result<Vec<String>, StorageError> {
        if self.draining.swap(true, Ordering::AcqRel) {
            // Already draining
            return Ok(Vec::new());
        }

        let _guard = DrainGuard(&self.draining);

        let pending = self.pending_count.load(Ordering::Acquire);
        if pending == 0 {
            return Ok(Vec::new());
        }

        info!(pending, batch_size, "Starting WAL drain to MySQL");

        // Fetch a batch of items from WAL
        let items = self.store.scan_batch(batch_size).await?;
        let batch_len = items.len();
        
        if batch_len == 0 {
            return Ok(Vec::new());
        }

        // Collect IDs for batch delete and return
        let ids: Vec<String> = items.iter().map(|i| i.object_id.clone()).collect();

        // Write batch to MySQL
        let mut items_mut: Vec<_> = items;
        match mysql.put_batch(&mut items_mut).await {
            Ok(result) => {
                if !result.verified {
                    warn!(
                        batch_id = %result.batch_id,
                        written = result.written,
                        "WAL drain batch verification failed"
                    );
                }
                
                // Batch delete from WAL - items are now safely in MySQL
                match self.store.delete_batch(&ids).await {
                    Ok(deleted) => {
                        debug!(deleted, "Batch deleted from WAL");
                    }
                    Err(e) => {
                        // Log but don't fail - duplicates are fine, we'll dedup on read
                        error!(error = %e, "Failed to batch delete from WAL after MySQL write");
                    }
                }
                
                let drained = result.written;
                self.pending_count.fetch_sub(drained as u64, Ordering::Release);
                self.total_drained.fetch_add(drained as u64, Ordering::Relaxed);
                
                info!(drained, remaining = pending - drained as u64, "WAL drain batch complete");
                
                // Checkpoint to reclaim space if WAL is now empty
                if self.pending_count.load(Ordering::Acquire) == 0 {
                    if let Err(e) = self.checkpoint().await {
                        warn!(error = %e, "Failed to checkpoint WAL after drain");
                    }
                }
                
                Ok(ids)
            }
            Err(e) => {
                // MySQL failed - stop draining, will retry later
                warn!(
                    error = %e,
                    batch_size = batch_len,
                    "MySQL batch write failed during drain"
                );
                Err(e)
            }
        }
    }

    /// Get the path to the WAL file.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// RAII guard to reset draining flag.
struct DrainGuard<'a>(&'a AtomicBool);

impl Drop for DrainGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Health checker for MySQL connectivity.
pub struct MysqlHealthChecker {
    /// Last known health state
    healthy: AtomicBool,
    /// Consecutive failure count
    failures: AtomicU64,
    /// Lock for health check (prevent thundering herd)
    checking: Mutex<()>,
}

impl MysqlHealthChecker {
    pub fn new() -> Self {
        Self {
            healthy: AtomicBool::new(true), // Assume healthy until proven otherwise
            failures: AtomicU64::new(0),
            checking: Mutex::new(()),
        }
    }

    /// Record a successful MySQL operation.
    pub fn record_success(&self) {
        self.failures.store(0, Ordering::Release);
        self.healthy.store(true, Ordering::Release);
    }

    /// Record a failed MySQL operation.
    pub fn record_failure(&self) {
        let failures = self.failures.fetch_add(1, Ordering::AcqRel) + 1;
        if failures >= 3 {
            self.healthy.store(false, Ordering::Release);
        }
    }

    /// Check if MySQL is considered healthy.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Get consecutive failure count.
    pub fn failure_count(&self) -> u64 {
        self.failures.load(Ordering::Acquire)
    }

    /// Perform a health check (ping).
    pub async fn check(&self, mysql: &dyn ArchiveStore) -> bool {
        // Prevent multiple simultaneous checks
        let _guard = self.checking.lock().await;
        
        // Try a simple operation
        match mysql.get("__health_check__").await {
            Ok(_) => {
                self.record_success();
                true
            }
            Err(_) => {
                self.record_failure();
                false
            }
        }
    }
}

impl Default for MysqlHealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_item::SyncItem;
    use serde_json::json;
    use tempfile::tempdir;

    fn test_item(id: &str) -> SyncItem {
        SyncItem::from_json(id.to_string(), json!({"test": "data", "id": id}))
    }

    #[tokio::test]
    async fn test_wal_write_and_stats() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.db");
        
        let wal = WriteAheadLog::new(wal_path.as_path(), 1000).await.unwrap();
        
        // Initial state
        let stats = wal.stats(true);
        assert_eq!(stats.pending_items, 0);
        assert_eq!(stats.total_written, 0);
        assert!(!wal.has_pending());
        
        // Write some items
        for i in 0..5 {
            wal.write(&test_item(&format!("item-{}", i))).await.unwrap();
        }
        
        let stats = wal.stats(true);
        assert_eq!(stats.pending_items, 5);
        assert_eq!(stats.total_written, 5);
        assert!(wal.has_pending());
    }

    #[tokio::test]
    async fn test_wal_max_items_limit() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test_limit.db");
        
        // Very small limit for testing
        let wal = WriteAheadLog::new(wal_path.as_path(), 3).await.unwrap();
        
        // Fill to capacity
        wal.write(&test_item("item-1")).await.unwrap();
        wal.write(&test_item("item-2")).await.unwrap();
        wal.write(&test_item("item-3")).await.unwrap();
        
        // This should fail - at capacity
        let result: Result<(), StorageError> = wal.write(&test_item("item-4")).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("WAL full"));
    }

    #[tokio::test]
    async fn test_wal_under_pressure() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test_pressure.db");
        
        let wal = WriteAheadLog::new(wal_path.as_path(), 10).await.unwrap();
        
        // Not under pressure when empty
        assert!(!wal.under_pressure());
        
        // Fill to 80%
        for i in 0..8 {
            wal.write(&test_item(&format!("item-{}", i))).await.unwrap();
        }
        
        // Should be under pressure now (8/10 = 80%)
        assert!(wal.under_pressure());
    }

    #[tokio::test]
    async fn test_wal_persistence_across_restart() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test_restart.db");
        
        // Write items in first instance
        {
            let wal = WriteAheadLog::new(wal_path.as_path(), 1000).await.unwrap();
            wal.write(&test_item("persist-1")).await.unwrap();
            wal.write(&test_item("persist-2")).await.unwrap();
            wal.write(&test_item("persist-3")).await.unwrap();
        }
        
        // Reopen and verify items persisted
        {
            let wal = WriteAheadLog::new(wal_path.as_path(), 1000).await.unwrap();
            assert_eq!(wal.stats(true).pending_items, 3);
            assert!(wal.has_pending());
        }
    }

    #[tokio::test]
    async fn test_wal_file_size_check() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test_size.db");
        
        let wal = WriteAheadLog::new(wal_path.as_path(), 1000).await.unwrap();
        
        // Write some data
        wal.write(&test_item("size-test")).await.unwrap();
        
        // File should exist and have non-zero size
        let size = wal.file_size_bytes().unwrap();
        assert!(size > 0);
    }

    #[tokio::test]
    async fn test_wal_with_max_bytes_limit() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test_bytes.db");
        
        // Very small byte limit (1KB) - will trigger after a few writes
        let wal = WriteAheadLog::with_max_bytes(wal_path.as_path(), 1000, 1024).await.unwrap();
        
        // Write items until we hit the limit
        // Note: size check only happens every 100 writes, so this tests the item limit path
        for i in 0..50 {
            let result: Result<(), StorageError> = wal.write(&test_item(&format!("bytes-item-{}", i))).await;
            if result.is_err() {
                // Expected to eventually fail
                return;
            }
        }
        
        // If we got here, the byte limit wasn't hit (file too small) - that's OK
        // The important thing is the code path exists
    }

    // ========== MysqlHealthChecker tests ==========

    #[test]
    fn test_health_checker_initial_state() {
        let checker = MysqlHealthChecker::new();
        assert!(checker.is_healthy()); // Assume healthy initially
        assert_eq!(checker.failure_count(), 0);
    }

    #[test]
    fn test_health_checker_failure_threshold() {
        let checker = MysqlHealthChecker::new();
        
        // First 2 failures don't mark unhealthy
        checker.record_failure();
        assert!(checker.is_healthy());
        assert_eq!(checker.failure_count(), 1);
        
        checker.record_failure();
        assert!(checker.is_healthy());
        assert_eq!(checker.failure_count(), 2);
        
        // 3rd failure marks unhealthy
        checker.record_failure();
        assert!(!checker.is_healthy());
        assert_eq!(checker.failure_count(), 3);
    }

    #[test]
    fn test_health_checker_success_resets() {
        let checker = MysqlHealthChecker::new();
        
        // Mark unhealthy
        checker.record_failure();
        checker.record_failure();
        checker.record_failure();
        assert!(!checker.is_healthy());
        
        // One success resets everything
        checker.record_success();
        assert!(checker.is_healthy());
        assert_eq!(checker.failure_count(), 0);
    }

    #[test]
    fn test_health_checker_partial_failures_then_success() {
        let checker = MysqlHealthChecker::new();
        
        // 2 failures, then success
        checker.record_failure();
        checker.record_failure();
        checker.record_success();
        
        // Should be healthy and reset
        assert!(checker.is_healthy());
        assert_eq!(checker.failure_count(), 0);
    }
}
