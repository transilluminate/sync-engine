// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

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
//!   audit TEXT,              -- Operational metadata: {batch, trace, home}
//!   access_count BIGINT,     -- Local access frequency (for eviction)
//!   last_accessed BIGINT     -- Last access timestamp (for eviction)
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
        
        // Enable WAL mode for SQLite (better concurrency, faster writes)
        if is_sqlite {
            store.enable_wal_mode().await?;
        }
        
        store.init_schema().await?;
        Ok(store)
    }
    
    /// Get a clone of the connection pool for sharing with other stores.
    pub fn pool(&self) -> AnyPool {
        self.pool.clone()
    }
    
    /// Enable WAL (Write-Ahead Logging) mode for SQLite.
    /// 
    /// Benefits:
    /// - Concurrent reads during writes (readers don't block writers)
    /// - Better write performance (single fsync instead of two)
    /// - More predictable performance under load
    async fn enable_wal_mode(&self) -> Result<(), StorageError> {
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to enable WAL mode: {}", e)))?;
        
        // Also set synchronous to NORMAL for better performance while still safe
        // (FULL is default but WAL mode is safe with NORMAL)
        sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to set synchronous mode: {}", e)))?;
        
        Ok(())
    }

    async fn init_schema(&self) -> Result<(), StorageError> {
        // Note: We use TEXT/LONGTEXT instead of native JSON type because sqlx's
        // `Any` driver doesn't support MySQL's JSON type mapping. The data is still
        // valid JSON and can be queried with JSON_EXTRACT() in MySQL.
        //
        // merkle_dirty: Set to 1 on write, background task sets to 0 after merkle recalc.
        // This enables efficient multi-instance coordination without locking.
        //
        // state: Arbitrary caller-defined state tag (e.g., "delta", "base", "pending").
        // Indexed for fast state-based queries.
        let sql = if self.is_sqlite {
            r#"
            CREATE TABLE IF NOT EXISTS sync_items (
                id TEXT PRIMARY KEY,
                version INTEGER NOT NULL DEFAULT 1,
                timestamp INTEGER NOT NULL,
                payload_hash TEXT,
                payload TEXT,
                payload_blob BLOB,
                audit TEXT,
                merkle_dirty INTEGER NOT NULL DEFAULT 1,
                state TEXT NOT NULL DEFAULT 'default',
                access_count INTEGER NOT NULL DEFAULT 0,
                last_accessed INTEGER NOT NULL DEFAULT 0
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
                merkle_dirty TINYINT NOT NULL DEFAULT 1,
                state VARCHAR(32) NOT NULL DEFAULT 'default',
                access_count BIGINT NOT NULL DEFAULT 0,
                last_accessed BIGINT NOT NULL DEFAULT 0,
                INDEX idx_timestamp (timestamp),
                INDEX idx_merkle_dirty (merkle_dirty),
                INDEX idx_state (state)
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
                "SELECT version, timestamp, payload_hash, payload, payload_blob, audit, state, access_count, last_accessed FROM sync_items WHERE id = ?"
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
                    
                    // Try reading payload as String first (SQLite TEXT), then as bytes (MySQL LONGTEXT)
                    let payload_json: Option<String> = row.try_get::<String, _>("payload").ok()
                        .or_else(|| {
                            row.try_get::<Vec<u8>, _>("payload").ok()
                                .and_then(|bytes| String::from_utf8(bytes).ok())
                        });
                    
                    let payload_blob: Option<Vec<u8>> = row.try_get("payload_blob").ok();
                    
                    // Try reading audit as String first (SQLite TEXT), then as bytes (MySQL LONGTEXT)
                    let audit_json: Option<String> = row.try_get::<String, _>("audit").ok()
                        .or_else(|| {
                            row.try_get::<Vec<u8>, _>("audit").ok()
                                .and_then(|bytes| String::from_utf8(bytes).ok())
                        });
                    
                    // State field - try String first (SQLite), then bytes (MySQL)
                    let state: String = row.try_get::<String, _>("state").ok()
                        .or_else(|| {
                            row.try_get::<Vec<u8>, _>("state").ok()
                                .and_then(|bytes| String::from_utf8(bytes).ok())
                        })
                        .unwrap_or_else(|| "default".to_string());
                    
                    // Access metadata (local eviction stats, not replicated)
                    let access_count: i64 = row.try_get("access_count").unwrap_or(0);
                    let last_accessed: i64 = row.try_get("last_accessed").unwrap_or(0);
                    
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
                        state,
                        access_count as u64,
                        last_accessed as u64,
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
        let payload_hash = if item.content_hash.is_empty() { None } else { Some(item.content_hash.clone()) };
        let audit_json = Self::build_audit_json(item);
        let state = item.state.clone();
        
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
            "INSERT INTO sync_items (id, version, timestamp, payload_hash, payload, payload_blob, audit, merkle_dirty, state, access_count, last_accessed) 
             VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?) 
             ON CONFLICT(id) DO UPDATE SET 
                version = excluded.version, 
                timestamp = excluded.timestamp, 
                payload_hash = excluded.payload_hash, 
                payload = excluded.payload, 
                payload_blob = excluded.payload_blob, 
                audit = excluded.audit, 
                merkle_dirty = 1, 
                state = excluded.state,
                access_count = excluded.access_count,
                last_accessed = excluded.last_accessed"
        } else {
            "INSERT INTO sync_items (id, version, timestamp, payload_hash, payload, payload_blob, audit, merkle_dirty, state, access_count, last_accessed) 
             VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?) 
             ON DUPLICATE KEY UPDATE 
                version = VALUES(version), 
                timestamp = VALUES(timestamp), 
                payload_hash = VALUES(payload_hash), 
                payload = VALUES(payload), 
                payload_blob = VALUES(payload_blob), 
                audit = VALUES(audit), 
                merkle_dirty = 1, 
                state = VALUES(state),
                access_count = VALUES(access_count),
                last_accessed = VALUES(last_accessed)"
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
                .bind(&state)
                .bind(item.access_count as i64)
                .bind(item.last_accessed as i64)
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

        // Generate a unique batch ID (for audit trail, not verification)
        let batch_id = uuid::Uuid::new_v4().to_string();
        
        // Stamp all items with the batch_id
        for item in items.iter_mut() {
            item.batch_id = Some(batch_id.clone());
        }

        // Collect IDs for verification
        let item_ids: Vec<String> = items.iter().map(|i| i.object_id.clone()).collect();

        // MySQL max_allowed_packet is typically 16MB, so chunk into ~500 item batches
        const CHUNK_SIZE: usize = 500;
        let mut total_written = 0usize;

        for chunk in items.chunks(CHUNK_SIZE) {
            let written = self.put_batch_chunk(chunk, &batch_id).await?;
            total_written += written;
        }

        // Verify ALL items exist (not by batch_id - that's unreliable under concurrency)
        let verified_count = self.verify_batch_ids(&item_ids).await?;
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
            .map(|_| "(?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)".to_string())
            .collect();
        
        let sql = if self.is_sqlite {
            format!(
                "INSERT INTO sync_items (id, version, timestamp, payload_hash, payload, payload_blob, audit, merkle_dirty, state, access_count, last_accessed) VALUES {} \
                 ON CONFLICT(id) DO UPDATE SET \
                    version = excluded.version, \
                    timestamp = excluded.timestamp, \
                    payload_hash = excluded.payload_hash, \
                    payload = excluded.payload, \
                    payload_blob = excluded.payload_blob, \
                    audit = excluded.audit, \
                    merkle_dirty = 1, \
                    state = excluded.state, \
                    access_count = excluded.access_count, \
                    last_accessed = excluded.last_accessed",
                placeholders.join(", ")
            )
        } else {
            format!(
                "INSERT INTO sync_items (id, version, timestamp, payload_hash, payload, payload_blob, audit, merkle_dirty, state, access_count, last_accessed) VALUES {} \
                 ON DUPLICATE KEY UPDATE \
                    version = VALUES(version), \
                    timestamp = VALUES(timestamp), \
                    payload_hash = VALUES(payload_hash), \
                    payload = VALUES(payload), \
                    payload_blob = VALUES(payload_blob), \
                    audit = VALUES(audit), \
                    merkle_dirty = 1, \
                    state = VALUES(state), \
                    access_count = VALUES(access_count), \
                    last_accessed = VALUES(last_accessed)",
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
            state: String,
            access_count: i64,
            last_accessed: i64,
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
                    payload_hash: if item.content_hash.is_empty() { None } else { Some(item.content_hash.clone()) },
                    payload_json,
                    payload_blob,
                    audit_json: Self::build_audit_json(item),
                    state: item.state.clone(),
                    access_count: item.access_count as i64,
                    last_accessed: item.last_accessed as i64,
                }
            })
            .collect();

        retry("sql_put_batch", &RetryConfig::batch_write(), || {
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
                        .bind(&row.audit_json)
                        .bind(&row.state)
                        .bind(row.access_count)
                        .bind(row.last_accessed);
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

    /// Verify a batch was written by checking all IDs exist.
    /// This is more reliable than batch_id verification under concurrent writes.
    async fn verify_batch_ids(&self, ids: &[String]) -> Result<usize, StorageError> {
        if ids.is_empty() {
            return Ok(0);
        }

        // Use chunked EXISTS queries to avoid overly large IN clauses
        const CHUNK_SIZE: usize = 500;
        let mut total_found = 0usize;

        for chunk in ids.chunks(CHUNK_SIZE) {
            let placeholders: Vec<&str> = (0..chunk.len()).map(|_| "?").collect();
            let sql = format!(
                "SELECT COUNT(*) as cnt FROM sync_items WHERE id IN ({})",
                placeholders.join(", ")
            );

            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(id);
            }

            let result = query
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;

            let count: i64 = result
                .try_get("cnt")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            total_found += count as usize;
        }

        Ok(total_found)
    }

    /// Legacy batch_id verification (kept for reference, but not used under concurrency)
    #[allow(dead_code)]
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
            "SELECT id, version, timestamp, payload_hash, payload, payload_blob, audit, state, access_count, last_accessed FROM sync_items ORDER BY timestamp ASC LIMIT ?"
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
            
            let state_bytes: Option<Vec<u8>> = row.try_get("state").ok();
            let state: String = state_bytes
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .unwrap_or_else(|| "default".to_string());
            
            // Access metadata
            let access_count: i64 = row.try_get("access_count").unwrap_or(0);
            let last_accessed: i64 = row.try_get("last_accessed").unwrap_or(0);
            
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
                state,
                access_count as u64,
                last_accessed as u64,
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
    
    // ═══════════════════════════════════════════════════════════════════════════
    // Merkle Dirty Flag: For deferred merkle calculation in multi-instance setups
    // ═══════════════════════════════════════════════════════════════════════════
    
    /// Get IDs of items with merkle_dirty = 1 (need merkle recalculation).
    ///
    /// Used by background merkle processor to batch recalculate affected trees.
    pub async fn get_dirty_merkle_ids(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        let rows = sqlx::query(
            "SELECT id FROM sync_items WHERE merkle_dirty = 1 LIMIT ?"
        )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to get dirty merkle ids: {}", e)))?;
        
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            ids.push(id);
        }
        
        Ok(ids)
    }
    
    /// Count items with merkle_dirty = 1.
    pub async fn count_dirty_merkle(&self) -> Result<u64, StorageError> {
        let result = sqlx::query("SELECT COUNT(*) as cnt FROM sync_items WHERE merkle_dirty = 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let count: i64 = result.try_get("cnt")
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(count as u64)
    }
    
    /// Mark items as merkle-clean after recalculation.
    pub async fn mark_merkle_clean(&self, ids: &[String]) -> Result<usize, StorageError> {
        if ids.is_empty() {
            return Ok(0);
        }
        
        let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
        let sql = format!(
            "UPDATE sync_items SET merkle_dirty = 0 WHERE id IN ({})",
            placeholders.join(", ")
        );
        
        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }
        
        let result = query.execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(result.rows_affected() as usize)
    }
    
    /// Check if there are any dirty merkle items.
    pub async fn has_dirty_merkle(&self) -> Result<bool, StorageError> {
        let result = sqlx::query("SELECT 1 FROM sync_items WHERE merkle_dirty = 1 LIMIT 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(result.is_some())
    }
    
    /// Get full SyncItems with merkle_dirty = 1 (need merkle recalculation).
    ///
    /// Returns the items themselves so merkle can be calculated.
    /// Use `mark_merkle_clean()` after processing to clear the flag.
    pub async fn get_dirty_merkle_items(&self, limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        let rows = sqlx::query(
            "SELECT id, version, timestamp, payload_hash, payload, payload_blob, audit, state, access_count, last_accessed 
             FROM sync_items WHERE merkle_dirty = 1 LIMIT ?"
        )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to get dirty merkle items: {}", e)))?;
        
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let version: i64 = row.try_get("version").unwrap_or(1);
            let timestamp: i64 = row.try_get("timestamp").unwrap_or(0);
            let payload_hash: Option<String> = row.try_get("payload_hash").ok();
            
            // Handle JSON payload (MySQL returns as bytes, SQLite as string)
            let payload_bytes: Option<Vec<u8>> = row.try_get("payload").ok();
            let payload_json: Option<String> = payload_bytes.and_then(|bytes| {
                String::from_utf8(bytes).ok()
            });
            
            let payload_blob: Option<Vec<u8>> = row.try_get("payload_blob").ok();
            let audit_bytes: Option<Vec<u8>> = row.try_get("audit").ok();
            let audit_json: Option<String> = audit_bytes.and_then(|bytes| {
                String::from_utf8(bytes).ok()
            });
            
            // State field
            let state_bytes: Option<Vec<u8>> = row.try_get("state").ok();
            let state: String = state_bytes
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .unwrap_or_else(|| "default".to_string());
            
            // Access metadata
            let access_count: i64 = row.try_get("access_count").unwrap_or(0);
            let last_accessed: i64 = row.try_get("last_accessed").unwrap_or(0);
            
            // Determine content and content_type
            let (content, content_type) = if let Some(ref json_str) = payload_json {
                (json_str.as_bytes().to_vec(), ContentType::Json)
            } else if let Some(blob) = payload_blob {
                (blob, ContentType::Binary)
            } else {
                continue; // Skip items with no payload
            };
            
            // Parse audit fields
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
                state,
                access_count as u64,
                last_accessed as u64,
            );
            items.push(item);
        }
        
        Ok(items)
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // State-based queries: Fast indexed access by caller-defined state tag
    // ═══════════════════════════════════════════════════════════════════════════
    
    /// Get items by state (e.g., "delta", "base", "pending").
    ///
    /// Uses indexed query for fast retrieval.
    pub async fn get_by_state(&self, state: &str, limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        let rows = sqlx::query(
            "SELECT id, version, timestamp, payload_hash, payload, payload_blob, audit, state, access_count, last_accessed 
             FROM sync_items WHERE state = ? LIMIT ?"
        )
            .bind(state)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to get items by state: {}", e)))?;
        
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let version: i64 = row.try_get("version").unwrap_or(1);
            let timestamp: i64 = row.try_get("timestamp").unwrap_or(0);
            let payload_hash: Option<String> = row.try_get("payload_hash").ok();
            
            // Try reading payload as String first (SQLite TEXT), then as bytes (MySQL LONGTEXT)
            let payload_json: Option<String> = row.try_get::<String, _>("payload").ok()
                .or_else(|| {
                    row.try_get::<Vec<u8>, _>("payload").ok()
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                });
            
            let payload_blob: Option<Vec<u8>> = row.try_get("payload_blob").ok();
            
            // Try reading audit as String first (SQLite), then bytes (MySQL)
            let audit_json: Option<String> = row.try_get::<String, _>("audit").ok()
                .or_else(|| {
                    row.try_get::<Vec<u8>, _>("audit").ok()
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                });
            
            // State field - try String first (SQLite), then bytes (MySQL)
            let state: String = row.try_get::<String, _>("state").ok()
                .or_else(|| {
                    row.try_get::<Vec<u8>, _>("state").ok()
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                })
                .unwrap_or_else(|| "default".to_string());
            
            // Access metadata (local-only, not replicated)
            let access_count: i64 = row.try_get("access_count").unwrap_or(0);
            let last_accessed: i64 = row.try_get("last_accessed").unwrap_or(0);
            
            let (content, content_type) = if let Some(ref json_str) = payload_json {
                (json_str.as_bytes().to_vec(), ContentType::Json)
            } else if let Some(blob) = payload_blob {
                (blob, ContentType::Binary)
            } else {
                continue;
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
                state,
                access_count as u64,
                last_accessed as u64,
            );
            items.push(item);
        }
        
        Ok(items)
    }
    
    /// Count items in a given state.
    pub async fn count_by_state(&self, state: &str) -> Result<u64, StorageError> {
        let result = sqlx::query("SELECT COUNT(*) as cnt FROM sync_items WHERE state = ?")
            .bind(state)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let count: i64 = result.try_get("cnt")
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(count as u64)
    }
    
    /// Get just the IDs of items in a given state (lightweight query).
    pub async fn list_state_ids(&self, state: &str, limit: usize) -> Result<Vec<String>, StorageError> {
        let rows = sqlx::query("SELECT id FROM sync_items WHERE state = ? LIMIT ?")
            .bind(state)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to list state IDs: {}", e)))?;
        
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            ids.push(id);
        }
        
        Ok(ids)
    }
    
    /// Update the state of an item by ID.
    pub async fn set_state(&self, id: &str, new_state: &str) -> Result<bool, StorageError> {
        let result = sqlx::query("UPDATE sync_items SET state = ? WHERE id = ?")
            .bind(new_state)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(result.rows_affected() > 0)
    }
    
    /// Delete all items in a given state.
    ///
    /// Returns the number of deleted items.
    pub async fn delete_by_state(&self, state: &str) -> Result<u64, StorageError> {
        let result = sqlx::query("DELETE FROM sync_items WHERE state = ?")
            .bind(state)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(result.rows_affected())
    }
    
    /// Scan items by ID prefix.
    ///
    /// Efficiently retrieves all items whose ID starts with the given prefix.
    /// Uses SQL `LIKE 'prefix%'` which leverages the primary key index.
    ///
    /// # Example
    /// ```rust,ignore
    /// // Get all deltas for object user.123
    /// let deltas = store.scan_prefix("delta:user.123:", 1000).await?;
    /// ```
    pub async fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        // Build LIKE pattern: "prefix%"
        let pattern = format!("{}%", prefix);
        
        let rows = sqlx::query(
            "SELECT id, version, timestamp, payload_hash, payload, payload_blob, audit, state, access_count, last_accessed 
             FROM sync_items WHERE id LIKE ? ORDER BY id LIMIT ?"
        )
            .bind(&pattern)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to scan by prefix: {}", e)))?;
        
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let version: i64 = row.try_get("version").unwrap_or(1);
            let timestamp: i64 = row.try_get("timestamp").unwrap_or(0);
            let payload_hash: Option<String> = row.try_get("payload_hash").ok();
            
            // Try reading payload as String first (SQLite TEXT), then as bytes (MySQL LONGTEXT)
            let payload_json: Option<String> = row.try_get::<String, _>("payload").ok()
                .or_else(|| {
                    row.try_get::<Vec<u8>, _>("payload").ok()
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                });
            
            let payload_blob: Option<Vec<u8>> = row.try_get("payload_blob").ok();
            
            // Try reading audit as String first (SQLite), then bytes (MySQL)
            let audit_json: Option<String> = row.try_get::<String, _>("audit").ok()
                .or_else(|| {
                    row.try_get::<Vec<u8>, _>("audit").ok()
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                });
            
            // State field - try String first (SQLite), then bytes (MySQL)
            let state: String = row.try_get::<String, _>("state").ok()
                .or_else(|| {
                    row.try_get::<Vec<u8>, _>("state").ok()
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                })
                .unwrap_or_else(|| "default".to_string());
            
            // Access metadata (local-only, not replicated)
            let access_count: i64 = row.try_get("access_count").unwrap_or(0);
            let last_accessed: i64 = row.try_get("last_accessed").unwrap_or(0);
            
            let (content, content_type) = if let Some(ref json_str) = payload_json {
                (json_str.as_bytes().to_vec(), ContentType::Json)
            } else if let Some(blob) = payload_blob {
                (blob, ContentType::Binary)
            } else {
                continue;
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
                state,
                access_count as u64,
                last_accessed as u64,
            );
            items.push(item);
        }
        
        Ok(items)
    }
    
    /// Count items matching an ID prefix.
    pub async fn count_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let pattern = format!("{}%", prefix);
        
        let result = sqlx::query("SELECT COUNT(*) as cnt FROM sync_items WHERE id LIKE ?")
            .bind(&pattern)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        let count: i64 = result.try_get("cnt")
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(count as u64)
    }
    
    /// Delete all items matching an ID prefix.
    ///
    /// Returns the number of deleted items.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let pattern = format!("{}%", prefix);
        
        let result = sqlx::query("DELETE FROM sync_items WHERE id LIKE ?")
            .bind(&pattern)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use serde_json::json;
    
    fn temp_db_path(name: &str) -> PathBuf {
        // Use local temp/ folder (gitignored) instead of system temp
        PathBuf::from("temp").join(format!("sql_test_{}.db", name))
    }
    
    /// Clean up SQLite database and its WAL files
    fn cleanup_db(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
    
    fn test_item(id: &str, state: &str) -> SyncItem {
        SyncItem::from_json(id.to_string(), json!({"id": id}))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_state_stored_and_retrieved() {
        let db_path = temp_db_path("stored");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Store item with custom state
        let item = test_item("item1", "delta");
        store.put(&item).await.unwrap();
        
        // Retrieve and verify state
        let retrieved = store.get("item1").await.unwrap().unwrap();
        assert_eq!(retrieved.state, "delta");
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_state_default_value() {
        let db_path = temp_db_path("default");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Store item with default state
        let item = SyncItem::from_json("item1".into(), json!({"test": true}));
        store.put(&item).await.unwrap();
        
        let retrieved = store.get("item1").await.unwrap().unwrap();
        assert_eq!(retrieved.state, "default");
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_get_by_state() {
        let db_path = temp_db_path("get_by_state");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Insert items in different states
        store.put(&test_item("delta1", "delta")).await.unwrap();
        store.put(&test_item("delta2", "delta")).await.unwrap();
        store.put(&test_item("base1", "base")).await.unwrap();
        store.put(&test_item("pending1", "pending")).await.unwrap();
        
        // Query by state
        let deltas = store.get_by_state("delta", 100).await.unwrap();
        assert_eq!(deltas.len(), 2);
        assert!(deltas.iter().all(|i| i.state == "delta"));
        
        let bases = store.get_by_state("base", 100).await.unwrap();
        assert_eq!(bases.len(), 1);
        assert_eq!(bases[0].object_id, "base1");
        
        // Empty result for non-existent state
        let none = store.get_by_state("nonexistent", 100).await.unwrap();
        assert!(none.is_empty());
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_get_by_state_with_limit() {
        let db_path = temp_db_path("get_by_state_limit");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Insert 10 items
        for i in 0..10 {
            store.put(&test_item(&format!("item{}", i), "batch")).await.unwrap();
        }
        
        // Query with limit
        let limited = store.get_by_state("batch", 5).await.unwrap();
        assert_eq!(limited.len(), 5);
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_count_by_state() {
        let db_path = temp_db_path("count_by_state");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Insert items
        store.put(&test_item("a1", "alpha")).await.unwrap();
        store.put(&test_item("a2", "alpha")).await.unwrap();
        store.put(&test_item("a3", "alpha")).await.unwrap();
        store.put(&test_item("b1", "beta")).await.unwrap();
        
        assert_eq!(store.count_by_state("alpha").await.unwrap(), 3);
        assert_eq!(store.count_by_state("beta").await.unwrap(), 1);
        assert_eq!(store.count_by_state("gamma").await.unwrap(), 0);
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_list_state_ids() {
        let db_path = temp_db_path("list_state_ids");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        store.put(&test_item("id1", "pending")).await.unwrap();
        store.put(&test_item("id2", "pending")).await.unwrap();
        store.put(&test_item("id3", "done")).await.unwrap();
        
        let pending_ids = store.list_state_ids("pending", 100).await.unwrap();
        assert_eq!(pending_ids.len(), 2);
        assert!(pending_ids.contains(&"id1".to_string()));
        assert!(pending_ids.contains(&"id2".to_string()));
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_set_state() {
        let db_path = temp_db_path("set_state");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        store.put(&test_item("item1", "pending")).await.unwrap();
        
        // Verify initial state
        let before = store.get("item1").await.unwrap().unwrap();
        assert_eq!(before.state, "pending");
        
        // Update state
        let updated = store.set_state("item1", "approved").await.unwrap();
        assert!(updated);
        
        // Verify new state
        let after = store.get("item1").await.unwrap().unwrap();
        assert_eq!(after.state, "approved");
        
        // Non-existent item returns false
        let not_found = store.set_state("nonexistent", "x").await.unwrap();
        assert!(!not_found);
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_delete_by_state() {
        let db_path = temp_db_path("delete_by_state");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        store.put(&test_item("keep1", "keep")).await.unwrap();
        store.put(&test_item("keep2", "keep")).await.unwrap();
        store.put(&test_item("del1", "delete_me")).await.unwrap();
        store.put(&test_item("del2", "delete_me")).await.unwrap();
        store.put(&test_item("del3", "delete_me")).await.unwrap();
        
        // Delete by state
        let deleted = store.delete_by_state("delete_me").await.unwrap();
        assert_eq!(deleted, 3);
        
        // Verify deleted
        assert!(store.get("del1").await.unwrap().is_none());
        assert!(store.get("del2").await.unwrap().is_none());
        
        // Verify others remain
        assert!(store.get("keep1").await.unwrap().is_some());
        assert!(store.get("keep2").await.unwrap().is_some());
        
        // Delete non-existent state returns 0
        let zero = store.delete_by_state("nonexistent").await.unwrap();
        assert_eq!(zero, 0);
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_multiple_puts_preserve_state() {
        let db_path = temp_db_path("multi_put_state");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Put multiple items with different states
        store.put(&test_item("a", "state_a")).await.unwrap();
        store.put(&test_item("b", "state_b")).await.unwrap();
        store.put(&test_item("c", "state_c")).await.unwrap();
        
        assert_eq!(store.get("a").await.unwrap().unwrap().state, "state_a");
        assert_eq!(store.get("b").await.unwrap().unwrap().state, "state_b");
        assert_eq!(store.get("c").await.unwrap().unwrap().state, "state_c");
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_scan_prefix() {
        let db_path = temp_db_path("scan_prefix");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Insert items with different prefixes (CRDT pattern)
        store.put(&test_item("delta:user.123:op001", "delta")).await.unwrap();
        store.put(&test_item("delta:user.123:op002", "delta")).await.unwrap();
        store.put(&test_item("delta:user.123:op003", "delta")).await.unwrap();
        store.put(&test_item("delta:user.456:op001", "delta")).await.unwrap();
        store.put(&test_item("base:user.123", "base")).await.unwrap();
        store.put(&test_item("base:user.456", "base")).await.unwrap();
        
        // Scan specific object's deltas
        let user123_deltas = store.scan_prefix("delta:user.123:", 100).await.unwrap();
        assert_eq!(user123_deltas.len(), 3);
        assert!(user123_deltas.iter().all(|i| i.object_id.starts_with("delta:user.123:")));
        
        // Scan different object
        let user456_deltas = store.scan_prefix("delta:user.456:", 100).await.unwrap();
        assert_eq!(user456_deltas.len(), 1);
        
        // Scan all deltas
        let all_deltas = store.scan_prefix("delta:", 100).await.unwrap();
        assert_eq!(all_deltas.len(), 4);
        
        // Scan all bases
        let bases = store.scan_prefix("base:", 100).await.unwrap();
        assert_eq!(bases.len(), 2);
        
        // Empty result for non-matching prefix
        let none = store.scan_prefix("nonexistent:", 100).await.unwrap();
        assert!(none.is_empty());
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_scan_prefix_with_limit() {
        let db_path = temp_db_path("scan_prefix_limit");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Insert 20 items
        for i in 0..20 {
            store.put(&test_item(&format!("delta:obj:op{:03}", i), "delta")).await.unwrap();
        }
        
        // Query with limit
        let limited = store.scan_prefix("delta:obj:", 5).await.unwrap();
        assert_eq!(limited.len(), 5);
        
        // Verify we can get all with larger limit
        let all = store.scan_prefix("delta:obj:", 100).await.unwrap();
        assert_eq!(all.len(), 20);
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_count_prefix() {
        let db_path = temp_db_path("count_prefix");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Insert items with different prefixes
        store.put(&test_item("delta:user.123:op001", "delta")).await.unwrap();
        store.put(&test_item("delta:user.123:op002", "delta")).await.unwrap();
        store.put(&test_item("delta:user.123:op003", "delta")).await.unwrap();
        store.put(&test_item("delta:user.456:op001", "delta")).await.unwrap();
        store.put(&test_item("base:user.123", "base")).await.unwrap();
        
        // Count by prefix
        assert_eq!(store.count_prefix("delta:user.123:").await.unwrap(), 3);
        assert_eq!(store.count_prefix("delta:user.456:").await.unwrap(), 1);
        assert_eq!(store.count_prefix("delta:").await.unwrap(), 4);
        assert_eq!(store.count_prefix("base:").await.unwrap(), 1);
        assert_eq!(store.count_prefix("nonexistent:").await.unwrap(), 0);
        
        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn test_delete_prefix() {
        let db_path = temp_db_path("delete_prefix");
        cleanup_db(&db_path);
        
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let store = SqlStore::new(&url).await.unwrap();
        
        // Insert items with different prefixes
        store.put(&test_item("delta:user.123:op001", "delta")).await.unwrap();
        store.put(&test_item("delta:user.123:op002", "delta")).await.unwrap();
        store.put(&test_item("delta:user.123:op003", "delta")).await.unwrap();
        store.put(&test_item("delta:user.456:op001", "delta")).await.unwrap();
        store.put(&test_item("base:user.123", "base")).await.unwrap();
        
        // Delete one object's deltas
        let deleted = store.delete_prefix("delta:user.123:").await.unwrap();
        assert_eq!(deleted, 3);
        
        // Verify deleted
        assert!(store.get("delta:user.123:op001").await.unwrap().is_none());
        assert!(store.get("delta:user.123:op002").await.unwrap().is_none());
        
        // Verify other deltas remain
        assert!(store.get("delta:user.456:op001").await.unwrap().is_some());
        
        // Verify bases remain
        assert!(store.get("base:user.123").await.unwrap().is_some());
        
        // Delete non-existent prefix returns 0
        let zero = store.delete_prefix("nonexistent:").await.unwrap();
        assert_eq!(zero, 0);
        
        cleanup_db(&db_path);
    }
}
