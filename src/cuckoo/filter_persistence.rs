//! Cuckoo filter persistence for fast startup.
//!
//! Stores filter state alongside the merkle root that was current when
//! the filter was saved. On startup, if the current merkle root matches
//! the saved root, the filter is immediately trusted.
//!
//! # Storage
//!
//! Uses the WAL SQLite database for storage (reuses existing connection).
//!
//! ```sql
//! CREATE TABLE cf_state (
//!     filter_id TEXT PRIMARY KEY,      -- "l2" or "l3"
//!     filter_bytes BLOB NOT NULL,      -- Exported filter data
//!     merkle_root BLOB NOT NULL,       -- 32-byte root hash when saved
//!     entry_count INTEGER NOT NULL,    -- Number of entries at save time
//!     saved_at INTEGER NOT NULL        -- Unix timestamp
//! );
//! ```

use crate::StorageError;
use sqlx::{AnyPool, Row, any::AnyPoolOptions};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Filter IDs for persistence
pub const L2_FILTER_ID: &str = "l2";
pub const L3_FILTER_ID: &str = "l3";

/// Saved filter state
#[derive(Debug, Clone)]
pub struct SavedFilterState {
    /// Exported filter bytes
    pub filter_bytes: Vec<u8>,
    /// Merkle root when filter was saved
    pub merkle_root: [u8; 32],
    /// Number of entries at save time
    pub entry_count: usize,
    /// Unix timestamp when saved
    pub saved_at: u64,
}

/// Cuckoo filter persistence manager.
///
/// Stores and retrieves filter state from SQLite for fast startup.
pub struct FilterPersistence {
    pool: AnyPool,
}

impl FilterPersistence {
    /// Create a new filter persistence manager.
    ///
    /// Uses the same SQLite file as the WAL for simplicity.
    pub async fn new(sqlite_path: &str) -> Result<Self, StorageError> {
        sqlx::any::install_default_drivers();
        
        let url = format!("sqlite://{}?mode=rwc", sqlite_path);
        
        let pool = AnyPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&url)
            .await
            .map_err(|e| StorageError::Backend(format!(
                "Failed to connect to filter persistence DB: {}", e
            )))?;

        let persistence = Self { pool };
        persistence.init_schema().await?;
        
        Ok(persistence)
    }

    /// Create from existing pool (share with WAL).
    pub fn from_pool(pool: AnyPool) -> Self {
        Self { pool }
    }

    async fn init_schema(&self) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cf_state (
                filter_id TEXT PRIMARY KEY,
                filter_bytes BLOB NOT NULL,
                merkle_root BLOB NOT NULL,
                entry_count INTEGER NOT NULL,
                saved_at INTEGER NOT NULL
            )
            "#
        )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!(
                "Failed to create cf_state table: {}", e
            )))?;

        Ok(())
    }

    /// Save filter state with current merkle root.
    pub async fn save(
        &self,
        filter_id: &str,
        filter_bytes: &[u8],
        merkle_root: &[u8; 32],
        entry_count: usize,
    ) -> Result<(), StorageError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        sqlx::query(
            r#"
            INSERT INTO cf_state (filter_id, filter_bytes, merkle_root, entry_count, saved_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(filter_id) DO UPDATE SET
                filter_bytes = excluded.filter_bytes,
                merkle_root = excluded.merkle_root,
                entry_count = excluded.entry_count,
                saved_at = excluded.saved_at
            "#
        )
            .bind(filter_id)
            .bind(filter_bytes)
            .bind(merkle_root.as_slice())
            .bind(entry_count as i64)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!(
                "Failed to save filter state: {}", e
            )))?;

        info!(
            filter_id,
            entry_count,
            bytes = filter_bytes.len(),
            "Filter state saved"
        );
        Ok(())
    }

    /// Load filter state if it exists.
    pub async fn load(&self, filter_id: &str) -> Result<Option<SavedFilterState>, StorageError> {
        let result = sqlx::query(
            "SELECT filter_bytes, merkle_root, entry_count, saved_at FROM cf_state WHERE filter_id = ?"
        )
            .bind(filter_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!(
                "Failed to load filter state: {}", e
            )))?;

        match result {
            Some(row) => {
                let filter_bytes: Vec<u8> = row.try_get("filter_bytes")
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
                let root_bytes: Vec<u8> = row.try_get("merkle_root")
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
                let entry_count: i64 = row.try_get("entry_count")
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
                let saved_at: i64 = row.try_get("saved_at")
                    .map_err(|e| StorageError::Backend(e.to_string()))?;

                if root_bytes.len() != 32 {
                    warn!(
                        filter_id,
                        len = root_bytes.len(),
                        "Invalid merkle root length in saved filter state"
                    );
                    return Ok(None);
                }

                let mut merkle_root = [0u8; 32];
                merkle_root.copy_from_slice(&root_bytes);

                debug!(
                    filter_id,
                    entry_count,
                    bytes = filter_bytes.len(),
                    "Loaded saved filter state"
                );

                Ok(Some(SavedFilterState {
                    filter_bytes,
                    merkle_root,
                    entry_count: entry_count as usize,
                    saved_at: saved_at as u64,
                }))
            }
            None => Ok(None),
        }
    }

    /// Delete saved filter state.
    pub async fn delete(&self, filter_id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM cf_state WHERE filter_id = ?")
            .bind(filter_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!(
                "Failed to delete filter state: {}", e
            )))?;

        Ok(())
    }

    /// Check if a saved filter matches the current merkle root.
    ///
    /// If it matches, the filter can be trusted immediately.
    pub async fn is_filter_current(
        &self,
        filter_id: &str,
        current_root: &[u8; 32],
    ) -> Result<bool, StorageError> {
        match self.load(filter_id).await? {
            Some(saved) => Ok(&saved.merkle_root == current_root),
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    
    fn temp_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("cf_test_{}.db", name))
    }

    #[tokio::test]
    async fn test_filter_persistence_roundtrip() {
        let db_path = temp_db_path("roundtrip");
        let _ = std::fs::remove_file(&db_path); // Clean up any old test
        
        let persistence = FilterPersistence::new(db_path.to_str().unwrap()).await.unwrap();
        
        let filter_bytes = vec![1, 2, 3, 4, 5];
        let merkle_root = [42u8; 32];
        let entry_count = 100;

        // Save
        persistence.save(L3_FILTER_ID, &filter_bytes, &merkle_root, entry_count)
            .await.unwrap();

        // Load
        let loaded = persistence.load(L3_FILTER_ID).await.unwrap();
        assert!(loaded.is_some());

        let state = loaded.unwrap();
        assert_eq!(state.filter_bytes, filter_bytes);
        assert_eq!(state.merkle_root, merkle_root);
        assert_eq!(state.entry_count, entry_count);
        
        let _ = std::fs::remove_file(&db_path); // Clean up
    }

    #[tokio::test]
    async fn test_filter_current_check() {
        let db_path = temp_db_path("current_check");
        let _ = std::fs::remove_file(&db_path); // Clean up any old test
        
        let persistence = FilterPersistence::new(db_path.to_str().unwrap()).await.unwrap();
        
        let merkle_root = [42u8; 32];
        persistence.save(L3_FILTER_ID, &[1, 2, 3], &merkle_root, 10)
            .await.unwrap();

        // Same root = current
        assert!(persistence.is_filter_current(L3_FILTER_ID, &merkle_root).await.unwrap());

        // Different root = not current
        let different_root = [99u8; 32];
        assert!(!persistence.is_filter_current(L3_FILTER_ID, &different_root).await.unwrap());
        
        let _ = std::fs::remove_file(&db_path); // Clean up
    }
}
