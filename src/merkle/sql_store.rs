// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! SQL storage for Merkle tree nodes (ground truth).
//!
//! The SQL merkle store is the authoritative source for merkle hashes.
//! On startup, we trust SQL merkle root over Redis.
//!
//! # Schema
//!
//! ```sql
//! CREATE TABLE merkle_nodes (
//!     path VARCHAR(255) PRIMARY KEY,    -- "" = root, "uk", "uk.nhs", etc
//!     parent_path VARCHAR(255),          -- Parent node path (NULL for root)
//!     merkle_hash BINARY(32) NOT NULL,   -- SHA256 hash
//!     object_count INT NOT NULL DEFAULT 0,
//!     is_leaf BOOLEAN DEFAULT FALSE,
//!     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//! );
//! ```

use super::path_tree::{MerkleBatch, MerkleNode, PathMerkle};
use crate::StorageError;
use sqlx::{AnyPool, Row, any::AnyPoolOptions};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{debug, info, instrument};

/// SQL key for root hash
const ROOT_PATH: &str = "";

/// SQL-backed Merkle tree storage (ground truth).
#[derive(Clone)]
pub struct SqlMerkleStore {
    pool: AnyPool,
    is_sqlite: bool,
}

impl SqlMerkleStore {
    /// Create a new SQL merkle store, initializing schema if needed.
    pub async fn new(connection_string: &str) -> Result<Self, StorageError> {
        // Ensure drivers are installed
        sqlx::any::install_default_drivers();
        
        let is_sqlite = connection_string.starts_with("sqlite:");
        
        let pool = AnyPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .connect(connection_string)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to connect to SQL merkle store: {}", e)))?;

        let store = Self { pool, is_sqlite };
        store.init_schema().await?;
        
        info!("SQL merkle store initialized");
        Ok(store)
    }

    /// Create from existing pool (e.g., share with SqlStore).
    pub fn from_pool(pool: AnyPool, is_sqlite: bool) -> Self {
        Self { pool, is_sqlite }
    }

    /// Initialize the schema (creates tables if not exist).
    /// Called automatically by `new()`, but must be called manually after `from_pool()`.
    pub async fn init_schema(&self) -> Result<(), StorageError> {
        // Use TEXT/VARCHAR for merkle_hash (stored as 64-char hex) for SQLx Any driver compatibility
        let sql = if self.is_sqlite {
            r#"
            CREATE TABLE IF NOT EXISTS merkle_nodes (
                path TEXT PRIMARY KEY,
                parent_path TEXT,
                merkle_hash TEXT NOT NULL,
                object_count INTEGER NOT NULL DEFAULT 0,
                is_leaf INTEGER DEFAULT 0,
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            );
            CREATE INDEX IF NOT EXISTS idx_merkle_parent ON merkle_nodes(parent_path);
            "#
        } else {
            // Use VARCHAR for merkle_hash to avoid SQLx Any driver binary type mapping issues
            // Hash is stored as 64-char hex string
            r#"
            CREATE TABLE IF NOT EXISTS merkle_nodes (
                path VARCHAR(255) PRIMARY KEY,
                parent_path VARCHAR(255),
                merkle_hash VARCHAR(64) NOT NULL,
                object_count INT NOT NULL DEFAULT 0,
                is_leaf INT DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_merkle_parent (parent_path)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        };

        // SQLite needs separate statements
        if self.is_sqlite {
            for stmt in sql.split(';').filter(|s| !s.trim().is_empty()) {
                sqlx::query(stmt)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Backend(format!("Failed to init merkle schema: {}", e)))?;
            }
        } else {
            sqlx::query(sql)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(format!("Failed to init merkle schema: {}", e)))?;
        }

        Ok(())
    }

    /// Get the root hash (ground truth).
    #[instrument(skip(self))]
    pub async fn root_hash(&self) -> Result<Option<[u8; 32]>, StorageError> {
        self.get_hash(ROOT_PATH).await
    }

    /// Get hash for a specific path.
    pub async fn get_hash(&self, path: &str) -> Result<Option<[u8; 32]>, StorageError> {
        let result = sqlx::query("SELECT merkle_hash FROM merkle_nodes WHERE path = ?")
            .bind(path)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to get merkle hash: {}", e)))?;

        match result {
            Some(row) => {
                // For MySQL we store as hex VARCHAR, for SQLite as BLOB
                // Try reading as string first (works for both), then decode
                let hash_str: String = row.try_get("merkle_hash")
                    .map_err(|e| StorageError::Backend(format!("Failed to read merkle hash: {}", e)))?;
                
                // If it looks like hex (64 chars), decode it
                // Otherwise it might be raw bytes that got stringified
                let bytes = if hash_str.len() == 64 && hash_str.chars().all(|c| c.is_ascii_hexdigit()) {
                    hex::decode(&hash_str).map_err(|e| StorageError::Backend(format!(
                        "Invalid merkle hash hex: {}", e
                    )))?
                } else {
                    // SQLite might return raw bytes
                    hash_str.into_bytes()
                };
                
                if bytes.len() != 32 {
                    return Err(StorageError::Backend(format!(
                        "Invalid merkle hash length: {}", bytes.len()
                    )));
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&bytes);
                Ok(Some(hash))
            }
            None => Ok(None),
        }
    }

    /// Get children of an interior node.
    pub async fn get_children(&self, path: &str) -> Result<BTreeMap<String, [u8; 32]>, StorageError> {
        let rows = sqlx::query(
            "SELECT path, merkle_hash FROM merkle_nodes WHERE parent_path = ?"
        )
            .bind(path)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to get merkle children: {}", e)))?;

        let mut children = BTreeMap::new();
        for row in rows {
            let child_path: String = row.try_get("path")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            
            // Read hash as string (works for both MySQL VARCHAR and SQLite BLOB-as-text)
            let hash_str: String = row.try_get("merkle_hash")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            
            // Decode from hex if it looks like hex
            let bytes = if hash_str.len() == 64 && hash_str.chars().all(|c| c.is_ascii_hexdigit()) {
                hex::decode(&hash_str).unwrap_or_else(|_| hash_str.into_bytes())
            } else {
                hash_str.into_bytes()
            };
            
            if bytes.len() == 32 {
                // Extract just the segment name from path
                let segment = if path.is_empty() {
                    child_path.clone()
                } else {
                    child_path.strip_prefix(&format!("{}.", path))
                        .unwrap_or(&child_path)
                        .to_string()
                };
                
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&bytes);
                children.insert(segment, hash);
            }
        }

        Ok(children)
    }

    /// Get a full node (hash + children).
    pub async fn get_node(&self, path: &str) -> Result<Option<MerkleNode>, StorageError> {
        let hash = self.get_hash(path).await?;
        
        match hash {
            Some(h) => {
                let children = self.get_children(path).await?;
                Ok(Some(if children.is_empty() {
                    MerkleNode::leaf(h)
                } else {
                    MerkleNode {
                        hash: h,
                        children,
                        is_leaf: false,
                    }
                }))
            }
            None => Ok(None),
        }
    }

    /// Apply a batch of merkle updates atomically.
    ///
    /// This stores leaf hashes and recomputes affected interior nodes.
    /// Note: Session-level READ COMMITTED is set in SqlStore::new() for MySQL.
    #[instrument(skip(self, batch), fields(batch_size = batch.len()))]
    pub async fn apply_batch(&self, batch: &MerkleBatch) -> Result<(), StorageError> {
        if batch.is_empty() {
            return Ok(());
        }

        // Start transaction (READ COMMITTED already set at session level for MySQL)
        let mut tx = self.pool.begin().await
            .map_err(|e| StorageError::Backend(format!("Failed to begin transaction: {}", e)))?;

        // Step 1: Apply leaf updates
        for (object_id, maybe_hash) in &batch.leaves {
            let parent = PathMerkle::parent_prefix(object_id);
            
            match maybe_hash {
                Some(hash) => {
                    // Store hash as hex string for cross-DB compatibility
                    let hash_hex = hex::encode(hash);
                    let sql = if self.is_sqlite {
                        "INSERT INTO merkle_nodes (path, parent_path, merkle_hash, is_leaf, object_count) 
                         VALUES (?, ?, ?, 1, 1)
                         ON CONFLICT(path) DO UPDATE SET 
                            merkle_hash = excluded.merkle_hash,
                            updated_at = strftime('%s', 'now')"
                    } else {
                        "INSERT INTO merkle_nodes (path, parent_path, merkle_hash, is_leaf, object_count) 
                         VALUES (?, ?, ?, 1, 1)
                         ON DUPLICATE KEY UPDATE 
                            merkle_hash = VALUES(merkle_hash),
                            updated_at = CURRENT_TIMESTAMP"
                    };
                    
                    sqlx::query(sql)
                        .bind(object_id)
                        .bind(parent)
                        .bind(&hash_hex)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Backend(format!("Failed to upsert merkle leaf: {}", e)))?;
                }
                None => {
                    sqlx::query("DELETE FROM merkle_nodes WHERE path = ?")
                        .bind(object_id)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Backend(format!("Failed to delete merkle leaf: {}", e)))?;
                }
            }
        }

        // Commit leaf changes first
        tx.commit().await
            .map_err(|e| StorageError::Backend(format!("Failed to commit merkle leaves: {}", e)))?;

        // Step 2: Recompute affected interior nodes (bottom-up)
        let affected_prefixes = batch.affected_prefixes();
        for prefix in affected_prefixes {
            self.recompute_interior_node(&prefix).await?;
        }

        // Step 3: Recompute root
        self.recompute_interior_node(ROOT_PATH).await?;

        debug!(updates = batch.len(), "SQL merkle batch applied");
        Ok(())
    }

    /// Recompute an interior node's hash from its children.
    async fn recompute_interior_node(&self, path: &str) -> Result<(), StorageError> {
        // Get all direct children
        let children = self.get_children(path).await?;
        
        if children.is_empty() {
            // No children, remove this interior node (unless it's a leaf)
            let result = sqlx::query("SELECT is_leaf FROM merkle_nodes WHERE path = ?")
                .bind(path)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            
            if let Some(row) = result {
                // Both SQLite and MySQL now store is_leaf as INT
                let is_leaf: i32 = row.try_get("is_leaf").unwrap_or(0);
                
                if is_leaf == 0 {
                    sqlx::query("DELETE FROM merkle_nodes WHERE path = ?")
                        .bind(path)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| StorageError::Backend(e.to_string()))?;
                }
            }
            return Ok(());
        }

        // Compute new interior hash
        let node = MerkleNode::interior(children);
        let hash_hex = hex::encode(node.hash); // Store as hex string
        let parent = if path.is_empty() { None } else { Some(PathMerkle::parent_prefix(path)) };
        let object_count = node.children.len() as i32;

        let sql = if self.is_sqlite {
            "INSERT INTO merkle_nodes (path, parent_path, merkle_hash, is_leaf, object_count) 
             VALUES (?, ?, ?, 0, ?)
             ON CONFLICT(path) DO UPDATE SET 
                merkle_hash = excluded.merkle_hash,
                object_count = excluded.object_count,
                updated_at = strftime('%s', 'now')"
        } else {
            "INSERT INTO merkle_nodes (path, parent_path, merkle_hash, is_leaf, object_count) 
             VALUES (?, ?, ?, 0, ?)
             ON DUPLICATE KEY UPDATE 
                merkle_hash = VALUES(merkle_hash),
                object_count = VALUES(object_count),
                updated_at = CURRENT_TIMESTAMP"
        };

        sqlx::query(sql)
            .bind(path)
            .bind(parent)
            .bind(&hash_hex)
            .bind(object_count)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to update interior node: {}", e)))?;

        debug!(path = %path, children = node.children.len(), "Recomputed interior node");
        Ok(())
    }

    /// Compare with another merkle store and find differing branches.
    ///
    /// Returns prefixes where hashes differ (for sync).
    #[instrument(skip(self, their_children))]
    pub async fn diff_children(
        &self,
        path: &str,
        their_children: &BTreeMap<String, [u8; 32]>,
    ) -> Result<Vec<String>, StorageError> {
        let our_children = self.get_children(path).await?;
        let mut diffs = Vec::new();

        let prefix_with_dot = if path.is_empty() {
            String::new()
        } else {
            format!("{}.", path)
        };

        // Find segments where hashes differ or we have but they don't
        for (segment, our_hash) in &our_children {
            match their_children.get(segment) {
                Some(their_hash) if their_hash != our_hash => {
                    diffs.push(format!("{}{}", prefix_with_dot, segment));
                }
                None => {
                    // We have it, they don't
                    diffs.push(format!("{}{}", prefix_with_dot, segment));
                }
                _ => {} // Hashes match
            }
        }

        // Find segments they have but we don't
        for segment in their_children.keys() {
            if !our_children.contains_key(segment) {
                diffs.push(format!("{}{}", prefix_with_dot, segment));
            }
        }

        Ok(diffs)
    }

    /// Get all leaf paths under a prefix (for sync).
    pub async fn get_leaves_under(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let pattern = if prefix.is_empty() {
            "%".to_string()
        } else {
            format!("{}%", prefix)
        };

        // Both SQLite and MySQL now use INT for is_leaf
        let sql = "SELECT path FROM merkle_nodes WHERE path LIKE ? AND is_leaf = 1";

        let rows = sqlx::query(sql)
            .bind(&pattern)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        let mut leaves = Vec::with_capacity(rows.len());
        for row in rows {
            let path: String = row.try_get("path")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            leaves.push(path);
        }

        Ok(leaves)
    }

    /// Count total objects (leaves) in the tree.
    pub async fn count_leaves(&self) -> Result<u64, StorageError> {
        // Both SQLite and MySQL now use INT for is_leaf
        let sql = "SELECT COUNT(*) as cnt FROM merkle_nodes WHERE is_leaf = 1";

        let row = sqlx::query(sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        let count: i64 = row.try_get("cnt")
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    
    fn temp_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("merkle_test_{}.db", name))
    }

    #[tokio::test]
    async fn test_sql_merkle_basic() {
        let db_path = temp_db_path("basic");
        let _ = std::fs::remove_file(&db_path); // Clean up any old test
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlMerkleStore::new(&url).await.unwrap();
        
        // Initially empty
        assert!(store.root_hash().await.unwrap().is_none());
        
        // Insert a leaf
        let mut batch = MerkleBatch::new();
        batch.insert("uk.nhs.patient.123".to_string(), [1u8; 32]);
        store.apply_batch(&batch).await.unwrap();
        
        // Root should now exist
        let root = store.root_hash().await.unwrap();
        assert!(root.is_some());
        
        // Leaf should exist
        let leaf = store.get_hash("uk.nhs.patient.123").await.unwrap();
        assert_eq!(leaf, Some([1u8; 32]));
        
        let _ = std::fs::remove_file(&db_path); // Clean up
    }

    #[tokio::test]
    async fn test_sql_merkle_diff() {
        let db_path = temp_db_path("diff");
        let _ = std::fs::remove_file(&db_path); // Clean up any old test
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlMerkleStore::new(&url).await.unwrap();
        
        // Insert some leaves
        let mut batch = MerkleBatch::new();
        batch.insert("uk.a.1".to_string(), [1u8; 32]);
        batch.insert("uk.b.2".to_string(), [2u8; 32]);
        store.apply_batch(&batch).await.unwrap();
        
        // Compare with "their" children where uk.a differs
        let mut their_children = BTreeMap::new();
        their_children.insert("a".to_string(), [99u8; 32]); // Different!
        their_children.insert("b".to_string(), store.get_hash("uk.b").await.unwrap().unwrap()); // Same
        
        let diffs = store.diff_children("uk", &their_children).await.unwrap();
        assert!(diffs.contains(&"uk.a".to_string()));
        assert!(!diffs.contains(&"uk.b".to_string()));
        
        let _ = std::fs::remove_file(&db_path); // Clean up
    }
}
