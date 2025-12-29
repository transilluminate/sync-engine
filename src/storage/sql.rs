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
        // Install the default drivers (MySQL, SQLite, etc based on Cargo features)
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
        // Ensure drivers are installed before any connection
        install_drivers();
        
        let is_sqlite = connection_string.starts_with("sqlite:");
        
        // Use startup() retry config - fail fast if connection is misconfigured
        // rather than retrying forever during initial connection
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
    
    /// Get a clone of the connection pool for sharing with other stores (e.g., SqlMerkleStore)
    pub fn pool(&self) -> AnyPool {
        self.pool.clone()
    }

    async fn init_schema(&self) -> Result<(), StorageError> {
        let sql = if self.is_sqlite {
            r#"
            CREATE TABLE IF NOT EXISTS sync_items (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                batch_id TEXT
            )
            "#
        } else {
            // Assume MySQL/Postgres
            r#"
            CREATE TABLE IF NOT EXISTS sync_items (
                id VARCHAR(255) PRIMARY KEY,
                content JSON NOT NULL,
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
                    // sqlx::Any doesn't support Json<T> directly, so read as String
                    let content_str: String = row.try_get("content")
                        .map_err(|e| StorageError::Backend(e.to_string()))?;
                    let item: SyncItem = serde_json::from_str(&content_str)
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
        // sqlx::Any doesn't support Json<T> directly, so serialize to String for both backends
        let content_str = serde_json::to_string(item)
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
                .bind(&content_str)
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
    
    /// Write a batch of items in a single multi-row INSERT with verification.
    /// 
    /// This is much more efficient than individual puts, and includes a batch_id
    /// that can be queried back to verify the write completed.
    async fn put_batch(&self, items: &mut [SyncItem]) -> Result<BatchWriteResult, StorageError> {
        self.put_batch_impl(items).await
    }

    async fn scan_keys(&self, offset: u64, limit: usize) -> Result<Vec<String>, StorageError> {
        self.scan_keys_impl(offset, limit).await
    }

    async fn count_all(&self) -> Result<u64, StorageError> {
        self.count_all_impl().await
    }
}

impl SqlStore {
    /// Write a batch of items in a single multi-row INSERT with verification.
    /// 
    /// This is much more efficient than individual puts, and includes a batch_id
    /// that can be queried back to verify the write completed.
    /// 
    /// Returns the batch_id and count of items written.
    async fn put_batch_impl(&self, items: &mut [SyncItem]) -> Result<BatchWriteResult, StorageError> {
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
        // to stay well under the limit while still being efficient
        const CHUNK_SIZE: usize = 500;
        let mut total_written = 0usize;

        for chunk in items.chunks(CHUNK_SIZE) {
            let written = self.put_batch_chunk(chunk, &batch_id).await?;
            total_written += written;
        }

        // Verify the batch was written by counting items with our batch_id
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

    /// Write a single chunk of items
    async fn put_batch_chunk(&self, chunk: &[SyncItem], batch_id: &str) -> Result<usize, StorageError> {
        // Build multi-row INSERT
        // MySQL: INSERT INTO ... VALUES (?, ?, ?, ?), (?, ?, ?, ?), ... ON DUPLICATE KEY UPDATE ...
        // SQLite: INSERT INTO ... VALUES (?, ?, ?, ?), (?, ?, ?, ?), ... ON CONFLICT DO UPDATE ...
        
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

        // Serialize all items upfront
        let serialized: Result<Vec<_>, _> = chunk.iter()
            .map(|item| {
                serde_json::to_string(item)
                    .map(|s| (item.object_id.clone(), s, item.last_accessed as i64))
                    .map_err(|e| StorageError::Backend(e.to_string()))
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

    /// Count all items in the store (public convenience wrapper).
    pub async fn count_all(&self) -> Result<u64, StorageError> {
        self.count_all_impl().await
    }

    async fn count_all_impl(&self) -> Result<u64, StorageError> {
        let result = sqlx::query("SELECT COUNT(*) as cnt FROM sync_items")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let count: i64 = result.try_get("cnt")
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(count as u64)
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
            let content_str: String = row.try_get("content")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let item: SyncItem = serde_json::from_str(&content_str)
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            items.push(item);
        }
        
        Ok(items)
    }

    /// Scan all keys (for cuckoo filter warmup).
    pub async fn scan_keys_impl(&self, offset: u64, limit: usize) -> Result<Vec<String>, StorageError> {
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

    /// Delete multiple items by ID in a single query.
    /// 
    /// Much more efficient than individual deletes for WAL drain.
    pub async fn delete_batch(&self, ids: &[String]) -> Result<usize, StorageError> {
        if ids.is_empty() {
            return Ok(0);
        }

        // Build IN clause with placeholders
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
