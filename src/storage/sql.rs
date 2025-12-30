//! SQL storage backend for L3 archive.
//!
//! Content-type aware storage with proper columns for queryability:
//! - **JSON content** → Stored in `payload` TEXT column (queryable via JSON_EXTRACT)
//! - **Binary content** → Stored in `payload_blob` MEDIUMBLOB column
//!
//! Schema mirrors Redis structure:
//! ```sql
//! CREATE TABLE sync_items (
//!   id VARCHAR(255) PRIMARY KEY,
//!   version BIGINT NOT NULL,
//!   timestamp BIGINT NOT NULL,
//!   payload_hash VARCHAR(64),
//!   payload LONGTEXT,        -- JSON as text (sqlx Any driver limitation)
//!   payload_blob MEDIUMBLOB, -- For binary content
//!   audit TEXT               -- Operational metadata: {batch, trace, home}
//! )
//! ```
//!
//! ## sqlx Any Driver Quirks
//! 
//! We use TEXT instead of native JSON type because sqlx's `Any` driver:
//! 1. Doesn't support MySQL's JSON type mapping
//! 2. Treats LONGTEXT/TEXT as BLOB (requires reading as `Vec<u8>` then converting)
//!
//! JSON functions still work on TEXT columns:
//!
//! ```sql
//! -- Find users named Alice
//! SELECT * FROM sync_items WHERE JSON_EXTRACT(payload, '$.name') = 'Alice';
//! 
//! -- Find all items from a batch
//! SELECT * FROM sync_items WHERE JSON_EXTRACT(audit, '$.batch') = 'abc-123';
//! ```

use async_trait::async_trait;
use sqlx::{AnyPool, Row, any::AnyPoolOptions};
use crate::sync_item::{SyncItem, ContentType};
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
        // Note: We use TEXT/LONGTEXT instead of native JSON type because sqlx's
        // `Any` driver doesn't support MySQL's JSON type mapping. The data is still
        // valid JSON and can be queried with JSON_EXTRACT() in MySQL.
        let sql = if self.is_sqlite {
            r#"
            CREATE TABLE IF NOT EXISTS sync_items (
                id TEXT PRIMARY KEY,
                version INTEGER NOT NULL DEFAULT 1,
                timestamp INTEGER NOT NULL,
                payload_hash TEXT,
                payload TEXT,
                payload_blob BLOB,
                audit TEXT
            )
            "#
        } else {
            // MySQL - use LONGTEXT for JSON (sqlx Any driver doesn't support native JSON)
            // JSON functions like JSON_EXTRACT() still work on TEXT columns containing valid JSON
            r#"
            CREATE TABLE IF NOT EXISTS sync_items (
                id VARCHAR(255) PRIMARY KEY,
                version BIGINT NOT NULL DEFAULT 1,
                timestamp BIGINT NOT NULL,
                payload_hash VARCHAR(64),
                payload LONGTEXT,
                payload_blob MEDIUMBLOB,
                audit TEXT,
                INDEX idx_timestamp (timestamp)
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
    
    /// Build the audit JSON object for operational metadata.
    fn build_audit_json(item: &SyncItem) -> Option<String> {
        let mut audit = serde_json::Map::new();
        
        if let Some(ref batch_id) = item.batch_id {
            audit.insert("batch".to_string(), serde_json::Value::String(batch_id.clone()));
        }
        if let Some(ref trace_parent) = item.trace_parent {
            audit.insert("trace".to_string(), serde_json::Value::String(trace_parent.clone()));
        }
        if let Some(ref home) = item.home_instance_id {
            audit.insert("home".to_string(), serde_json::Value::String(home.clone()));
        }
        
        if audit.is_empty() {
            None
        } else {
            serde_json::to_string(&serde_json::Value::Object(audit)).ok()
        }
    }
    
    /// Parse audit JSON back into SyncItem fields.
    fn parse_audit_json(audit_str: Option<String>) -> (Option<String>, Option<String>, Option<String>) {
        match audit_str {
            Some(s) => {
                if let Ok(audit) = serde_json::from_str::<serde_json::Value>(&s) {
                    let batch_id = audit.get("batch").and_then(|v| v.as_str()).map(String::from);
                    let trace_parent = audit.get("trace").and_then(|v| v.as_str()).map(String::from);
                    let home_instance_id = audit.get("home").and_then(|v| v.as_str()).map(String::from);
                    (batch_id, trace_parent, home_instance_id)
                } else {
                    (None, None, None)
                }
            }
            None => (None, None, None),
        }
    }
}

#[async_trait]
impl ArchiveStore for SqlStore {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        let id = id.to_string();
        
        retry("sql_get", &RetryConfig::query(), || async {
            let result = sqlx::query(
                "SELECT version, timestamp, payload_hash, payload, payload_blob, audit FROM sync_items WHERE id = ?"
            )
                .bind(&id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            match result {
                Some(row) => {
                    let version: i64 = row.try_get("version").unwrap_or(1);
                    let timestamp: i64 = row.try_get("timestamp").unwrap_or(0);
                    let payload_hash: Option<String> = row.try_get("payload_hash").ok();
                    
                    // sqlx Any driver treats MySQL LONGTEXT as BLOB, so we read as bytes
                    // and convert to String for JSON payload
                    let payload_bytes: Option<Vec<u8>> = row.try_get("payload").ok();
                    let payload_json: Option<String> = payload_bytes.and_then(|bytes| {
                        String::from_utf8(bytes).ok()
                    });
                    
                    let payload_blob: Option<Vec<u8>> = row.try_get("payload_blob").ok();
                    let audit_bytes: Option<Vec<u8>> = row.try_get("audit").ok();
                    let audit_json: Option<String> = audit_bytes.and_then(|bytes| {
                        String::from_utf8(bytes).ok()
                    });
                    
                    // Determine content and content_type
                    let (content, content_type) = if let Some(ref json_str) = payload_json {
                        // JSON content - parse and re-serialize to bytes
                        let content = json_str.as_bytes().to_vec();
                        (content, ContentType::Json)
                    } else if let Some(blob) = payload_blob {
                        // Binary content
                        (blob, ContentType::Binary)
                    } else {
                        return Err(StorageError::Backend("No payload in row".to_string()));
                    };
                    
                    // Parse audit fields
                    let (batch_id, trace_parent, home_instance_id) = Self::parse_audit_json(audit_json);
                    
                    let item = SyncItem::reconstruct(
                        id.clone(),
                        version as u64,
                        timestamp,
                        content_type,
                        content,
                        batch_id,
                        trace_parent,
                        payload_hash.unwrap_or_default(),
                        home_instance_id,
                    );
                    Ok(Some(item))
                }
                None => Ok(None),
            }
        })
        .await
    }

    async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
        let id = item.object_id.clone();
        let version = item.version as i64;
        let timestamp = item.updated_at;
        let payload_hash = if item.merkle_root.is_empty() { None } else { Some(item.merkle_root.clone()) };
        let audit_json = Self::build_audit_json(item);
        
        // Determine payload storage based on content type
        let (payload_json, payload_blob): (Option<String>, Option<Vec<u8>>) = match item.content_type {
            ContentType::Json => {
                let json_str = String::from_utf8_lossy(&item.content).to_string();
                (Some(json_str), None)
            }
            ContentType::Binary => {
                (None, Some(item.content.clone()))
            }
        };

        let sql = if self.is_sqlite {
            "INSERT INTO sync_items (id, version, timestamp, payload_hash, payload, payload_blob, audit) 
             VALUES (?, ?, ?, ?, ?, ?, ?) 
             ON CONFLICT(id) DO UPDATE SET 
                version = excluded.version, 
                timestamp = excluded.timestamp, 
                payload_hash = excluded.payload_hash, 
                payload = excluded.payload, 
                payload_blob = excluded.payload_blob, 
                audit = excluded.audit"
        } else {
            "INSERT INTO sync_items (id, version, timestamp, payload_hash, payload, payload_blob, audit) 
             VALUES (?, ?, ?, ?, ?, ?, ?) 
             ON DUPLICATE KEY UPDATE 
                version = VALUES(version), 
                timestamp = VALUES(timestamp), 
                payload_hash = VALUES(payload_hash), 
                payload = VALUES(payload), 
                payload_blob = VALUES(payload_blob), 
                audit = VALUES(audit)"
        };

        retry("sql_put", &RetryConfig::query(), || async {
            sqlx::query(sql)
                .bind(&id)
                .bind(version)
                .bind(timestamp)
                .bind(&payload_hash)
                .bind(&payload_json)
                .bind(&payload_blob)
                .bind(&audit_json)
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
    /// Write a single chunk of items with content-type aware storage.
    /// The batch_id is already embedded in each item's audit JSON.
    async fn put_batch_chunk(&self, chunk: &[SyncItem], _batch_id: &str) -> Result<usize, StorageError> {
        let placeholders: Vec<String> = (0..chunk.len())
            .map(|_| "(?, ?, ?, ?, ?, ?, ?)".to_string())
            .collect();
        
        let sql = if self.is_sqlite {
            format!(
                "INSERT INTO sync_items (id, version, timestamp, payload_hash, payload, payload_blob, audit) VALUES {} \
                 ON CONFLICT(id) DO UPDATE SET \
                    version = excluded.version, \
                    timestamp = excluded.timestamp, \
                    payload_hash = excluded.payload_hash, \
                    payload = excluded.payload, \
                    payload_blob = excluded.payload_blob, \
                    audit = excluded.audit",
                placeholders.join(", ")
            )
        } else {
            format!(
                "INSERT INTO sync_items (id, version, timestamp, payload_hash, payload, payload_blob, audit) VALUES {} \
                 ON DUPLICATE KEY UPDATE \
                    version = VALUES(version), \
                    timestamp = VALUES(timestamp), \
                    payload_hash = VALUES(payload_hash), \
                    payload = VALUES(payload), \
                    payload_blob = VALUES(payload_blob), \
                    audit = VALUES(audit)",
                placeholders.join(", ")
            )
        };

        // Prepare all items with their fields
        #[derive(Clone)]
        struct PreparedRow {
            id: String,
            version: i64,
            timestamp: i64,
            payload_hash: Option<String>,
            payload_json: Option<String>,
            payload_blob: Option<Vec<u8>>,
            audit_json: Option<String>,
        }
        
        let prepared: Vec<PreparedRow> = chunk.iter()
            .map(|item| {
                let (payload_json, payload_blob) = match item.content_type {
                    ContentType::Json => {
                        let json_str = String::from_utf8_lossy(&item.content).to_string();
                        (Some(json_str), None)
                    }
                    ContentType::Binary => {
                        (None, Some(item.content.clone()))
                    }
                };
                
                PreparedRow {
                    id: item.object_id.clone(),
                    version: item.version as i64,
                    timestamp: item.updated_at,
                    payload_hash: if item.merkle_root.is_empty() { None } else { Some(item.merkle_root.clone()) },
                    payload_json,
                    payload_blob,
                    audit_json: Self::build_audit_json(item),
                }
            })
            .collect();

        retry("sql_put_batch", &RetryConfig::query(), || {
            let sql = sql.clone();
            let prepared = prepared.clone();
            async move {
                let mut query = sqlx::query(&sql);
                
                for row in &prepared {
                    query = query
                        .bind(&row.id)
                        .bind(row.version)
                        .bind(row.timestamp)
                        .bind(&row.payload_hash)
                        .bind(&row.payload_json)
                        .bind(&row.payload_blob)
                        .bind(&row.audit_json);
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

    /// Verify a batch was written by counting items with the given batch_id (in audit JSON).
    async fn verify_batch(&self, batch_id: &str) -> Result<usize, StorageError> {
        let batch_id = batch_id.to_string();
        
        // Query varies by DB - MySQL has native JSON functions, SQLite uses string matching
        let sql = if self.is_sqlite {
            "SELECT COUNT(*) as cnt FROM sync_items WHERE audit LIKE ?"
        } else {
            "SELECT COUNT(*) as cnt FROM sync_items WHERE JSON_EXTRACT(audit, '$.batch') = ?"
        };
        
        let bind_value = if self.is_sqlite {
            format!("%\"batch\":\"{}%", batch_id)
        } else {
            batch_id.clone()
        };
        
        let result = sqlx::query(sql)
            .bind(&bind_value)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let count: i64 = result.try_get("cnt")
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(count as usize)
    }

    /// Scan a batch of items (for WAL drain).
    pub async fn scan_batch(&self, limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        let rows = sqlx::query(
            "SELECT id, version, timestamp, payload_hash, payload, payload_blob, audit FROM sync_items ORDER BY timestamp ASC LIMIT ?"
        )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let version: i64 = row.try_get("version").unwrap_or(1);
            let timestamp: i64 = row.try_get("timestamp").unwrap_or(0);
            let payload_hash: Option<String> = row.try_get("payload_hash").ok();
            
            // sqlx Any driver treats MySQL LONGTEXT/TEXT as BLOB
            let payload_bytes: Option<Vec<u8>> = row.try_get("payload").ok();
            let payload_json: Option<String> = payload_bytes.and_then(|b| String::from_utf8(b).ok());
            let payload_blob: Option<Vec<u8>> = row.try_get("payload_blob").ok();
            let audit_bytes: Option<Vec<u8>> = row.try_get("audit").ok();
            let audit_json: Option<String> = audit_bytes.and_then(|b| String::from_utf8(b).ok());
            
            let (content, content_type) = if let Some(ref json_str) = payload_json {
                (json_str.as_bytes().to_vec(), ContentType::Json)
            } else if let Some(blob) = payload_blob {
                (blob, ContentType::Binary)
            } else {
                continue; // Skip rows with no payload
            };
            
            let (batch_id, trace_parent, home_instance_id) = Self::parse_audit_json(audit_json);
            
            let item = SyncItem::reconstruct(
                id,
                version as u64,
                timestamp,
                content_type,
                content,
                batch_id,
                trace_parent,
                payload_hash.unwrap_or_default(),
                home_instance_id,
            );
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
