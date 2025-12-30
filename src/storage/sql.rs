//! SQL storage backend for L3 archive.
//!
//! Stores `SyncItem` as raw bytes in a BLOB column. The caller is responsible
//! for any compression/serialization before calling submit().

use async_trait::async_trait;
use sqlx::{AnyPool, Row, any::AnyPoolOptions};
use crate::sync_item::SyncItem;
use super::traits::{ArchiveStore, BatchWriteResult, StorageError};
use crate::resilience::retry::{retry, RetryConfig};
use std::sync::Once;
use std::time::Duration;

// SQLx `Any` driver requires runtime installation
static INSTALL_DRIVERS: Once = Once::new();

fn install_drivers() {
    INSTALL_DRIVERS.call_once(|| {
        sqlx::any::install_default_drivers();
    });
}

pub struct SqlStore {
    pool: AnyPool,
    is_sqlite: bool,
}

impl SqlStore {
    /// Create a new SQL store with startup-mode retry (fails fast if config is wrong).
    pub async fn new(connection_string: &str) -> Result<Self, StorageError> {
        install_drivers();
        
        let is_sqlite = connection_string.starts_with("sqlite:");
        
        let pool = retry("sql_connect", &RetryConfig::startup(), || async {
            AnyPoolOptions::new()
                .max_connections(20)
                .acquire_timeout(Duration::from_secs(10))
                .idle_timeout(Duration::from_secs(300))
                .connect(connection_string)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))
        })
        .await?;

        let store = Self { pool, is_sqlite };
        store.init_schema().await?;
        Ok(store)
    }
    
    /// Get a clone of the connection pool for sharing with other stores.
    pub fn pool(&self) -> AnyPool {
        self.pool.clone()
    }

    async fn init_schema(&self) -> Result<(), StorageError> {
        let sql = if self.is_sqlite {
            r#"
            CREATE TABLE IF NOT EXISTS sync_items (
                id TEXT PRIMARY KEY,
                content BLOB NOT NULL,
                updated_at INTEGER NOT NULL,
                batch_id TEXT
            )
            "#
        } else {
            // MySQL - use MEDIUMBLOB for up to 16MB items
            r#"
            CREATE TABLE IF NOT EXISTS sync_items (
                id VARCHAR(255) PRIMARY KEY,
                content MEDIUMBLOB NOT NULL,
                updated_at BIGINT NOT NULL,
                batch_id VARCHAR(36),
                INDEX idx_batch_id (batch_id)
            )
            "#
        };

        retry("sql_init_schema", &RetryConfig::startup(), || async {
            sqlx::query(sql)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))
        })
        .await?;

        Ok(())
    }
}

#[async_trait]
impl ArchiveStore for SqlStore {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        let id = id.to_string();
        
        retry("sql_get", &RetryConfig::query(), || async {
            let result = sqlx::query("SELECT content FROM sync_items WHERE id = ?")
                .bind(&id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            match result {
                Some(row) => {
                    // Read raw bytes from BLOB
                    let content_bytes: Vec<u8> = row.try_get("content")
                        .map_err(|e| StorageError::Backend(e.to_string()))?;
                    
                    // Deserialize SyncItem from bytes
                    let item: SyncItem = serde_json::from_slice(&content_bytes)
                        .map_err(|e| StorageError::Backend(e.to_string()))?;
                    Ok(Some(item))
                }
                None => Ok(None),
            }
        })
        .await
    }

    async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
        let id = item.object_id.clone();
        let content_bytes = serde_json::to_vec(item)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let updated_at = item.last_accessed as i64;
        let batch_id = item.batch_id.clone();

        let sql = if self.is_sqlite {
            "INSERT INTO sync_items (id, content, updated_at, batch_id) VALUES (?, ?, ?, ?) 
             ON CONFLICT(id) DO UPDATE SET content = excluded.content, updated_at = excluded.updated_at, batch_id = excluded.batch_id"
        } else {
            "INSERT INTO sync_items (id, content, updated_at, batch_id) VALUES (?, ?, ?, ?) 
             ON DUPLICATE KEY UPDATE content = VALUES(content), updated_at = VALUES(updated_at), batch_id = VALUES(batch_id)"
        };

        retry("sql_put", &RetryConfig::query(), || async {
            sqlx::query(sql)
                .bind(&id)
                .bind(&content_bytes)
                .bind(updated_at)
                .bind(&batch_id)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn delete(&self, id: &str) -> Result<(), StorageError> {
        let id = id.to_string();
        retry("sql_delete", &RetryConfig::query(), || async {
            sqlx::query("DELETE FROM sync_items WHERE id = ?")
                .bind(&id)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn exists(&self, id: &str) -> Result<bool, StorageError> {
        let id = id.to_string();
        retry("sql_exists", &RetryConfig::query(), || async {
            let result = sqlx::query("SELECT 1 FROM sync_items WHERE id = ? LIMIT 1")
                .bind(&id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            Ok(result.is_some())
        })
        .await
    }
    
    /// Write a batch of items in a single multi-row INSERT with verification.
    async fn put_batch(&self, items: &mut [SyncItem]) -> Result<BatchWriteResult, StorageError> {
        if items.is_empty() {
            return Ok(BatchWriteResult {
                batch_id: String::new(),
                written: 0,
                verified: true,
            });
        }

        // Generate a unique batch ID
        let batch_id = uuid::Uuid::new_v4().to_string();
        
        // Stamp all items with the batch_id
        for item in items.iter_mut() {
            item.batch_id = Some(batch_id.clone());
        }

        // MySQL max_allowed_packet is typically 16MB, so chunk into ~500 item batches
        const CHUNK_SIZE: usize = 500;
        let mut total_written = 0usize;

        for chunk in items.chunks(CHUNK_SIZE) {
            let written = self.put_batch_chunk(chunk, &batch_id).await?;
            total_written += written;
        }

        // Verify the batch was written
        let verified_count = self.verify_batch(&batch_id).await?;
        let verified = verified_count == items.len();

        if !verified {
            tracing::warn!(
                batch_id = %batch_id,
                expected = items.len(),
                actual = verified_count,
                "Batch verification mismatch"
            );
        }

        Ok(BatchWriteResult {
            batch_id,
            written: total_written,
            verified,
        })
    }

    async fn scan_keys(&self, offset: u64, limit: usize) -> Result<Vec<String>, StorageError> {
        let rows = sqlx::query("SELECT id FROM sync_items ORDER BY id LIMIT ? OFFSET ?")
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            keys.push(id);
        }
        
        Ok(keys)
    }

    async fn count_all(&self) -> Result<u64, StorageError> {
        let result = sqlx::query("SELECT COUNT(*) as cnt FROM sync_items")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let count: i64 = result.try_get("cnt")
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(count as u64)
    }
}

impl SqlStore {
    /// Write a single chunk of items
    async fn put_batch_chunk(&self, chunk: &[SyncItem], batch_id: &str) -> Result<usize, StorageError> {
        let placeholders: Vec<String> = (0..chunk.len())
            .map(|_| "(?, ?, ?, ?)".to_string())
            .collect();
        
        let sql = if self.is_sqlite {
            format!(
                "INSERT INTO sync_items (id, content, updated_at, batch_id) VALUES {} \
                 ON CONFLICT(id) DO UPDATE SET content = excluded.content, updated_at = excluded.updated_at, batch_id = excluded.batch_id",
                placeholders.join(", ")
            )
        } else {
            format!(
                "INSERT INTO sync_items (id, content, updated_at, batch_id) VALUES {} \
                 ON DUPLICATE KEY UPDATE content = VALUES(content), updated_at = VALUES(updated_at), batch_id = VALUES(batch_id)",
                placeholders.join(", ")
            )
        };

        // Serialize all items upfront as bytes
        let serialized: Result<Vec<_>, _> = chunk.iter()
            .map(|item| {
                let bytes = serde_json::to_vec(item)
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
                Ok((item.object_id.clone(), bytes, item.last_accessed as i64))
            })
            .collect();
        let serialized = serialized?;
        let batch_id = batch_id.to_string();

        retry("sql_put_batch", &RetryConfig::query(), || {
            let sql = sql.clone();
            let serialized = serialized.clone();
            let batch_id = batch_id.clone();
            async move {
                let mut query = sqlx::query(&sql);
                
                for (id, content, updated_at) in &serialized {
                    query = query.bind(id).bind(content).bind(*updated_at).bind(&batch_id);
                }
                
                query.execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
                
                Ok(())
            }
        })
        .await?;

        Ok(chunk.len())
    }

    /// Verify a batch was written by counting items with the given batch_id
    async fn verify_batch(&self, batch_id: &str) -> Result<usize, StorageError> {
        let batch_id = batch_id.to_string();
        
        let result = sqlx::query("SELECT COUNT(*) as cnt FROM sync_items WHERE batch_id = ?")
            .bind(&batch_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let count: i64 = result.try_get("cnt")
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(count as usize)
    }

    /// Scan a batch of items (for WAL drain).
    pub async fn scan_batch(&self, limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        let rows = sqlx::query("SELECT content FROM sync_items ORDER BY updated_at ASC LIMIT ?")
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let content_bytes: Vec<u8> = row.try_get("content")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let item: SyncItem = serde_json::from_slice(&content_bytes)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            items.push(item);
        }
        
        Ok(items)
    }

    /// Delete multiple items by ID in a single query.
    pub async fn delete_batch(&self, ids: &[String]) -> Result<usize, StorageError> {
        if ids.is_empty() {
            return Ok(0);
        }

        let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
        let sql = format!(
            "DELETE FROM sync_items WHERE id IN ({})",
            placeholders.join(", ")
        );

        retry("sql_delete_batch", &RetryConfig::query(), || {
            let sql = sql.clone();
            let ids = ids.to_vec();
            async move {
                let mut query = sqlx::query(&sql);
                for id in &ids {
                    query = query.bind(id);
                }
                
                let result = query.execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Backend(e.to_string()))?;
                
                Ok(result.rows_affected() as usize)
            }
        })
        .await
    }
}
