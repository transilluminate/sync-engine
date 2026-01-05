// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Redis storage backend for L2 cache.
//!
//! Content-type aware storage using RedisJSON (Redis Stack):
//! - **JSON content** → `JSON.SET` with full structure preserved → RedisSearch indexable!
//! - **Binary content** → Redis STRING (SET) for efficient blob storage
//!
//! JSON documents are stored with a clean, flat structure:
//! ```json
//! {
//!   "version": 1,
//!   "timestamp": 1767084657058,
//!   "payload_hash": "abc123...",
//!   "payload": {"name": "Alice", "role": "admin"},
//!   "audit": {"batch": "...", "trace": "...", "home": "..."}
//! }
//! ```
//!
//! This enables powerful search with RediSearch ON JSON:
//! ```text
//! FT.CREATE idx ON JSON PREFIX 1 sync: SCHEMA $.payload.name AS name TEXT
//! FT.SEARCH idx '@name:Alice'
//! ```

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{Client, AsyncCommands, pipe, cmd};
use crate::sync_item::{SyncItem, ContentType};
use super::traits::{BatchWriteResult, CacheStore, StorageError};
use crate::resilience::retry::{retry, RetryConfig};

pub struct RedisStore {
    connection: ConnectionManager,
    /// Optional key prefix for namespacing (e.g., "myapp:" → "myapp:user.alice")
    prefix: String,
}

impl RedisStore {
    /// Create a new Redis store without a key prefix.
    pub async fn new(connection_string: &str) -> Result<Self, StorageError> {
        Self::with_prefix(connection_string, None).await
    }
    
    /// Create a new Redis store with an optional key prefix.
    /// 
    /// The prefix is prepended to all keys, enabling namespacing when
    /// sharing a Redis instance with other applications.
    /// 
    /// # Example
    /// 
    /// ```rust,no_run
    /// # use sync_engine::storage::redis::RedisStore;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Keys will be prefixed: "myapp:user.alice", "myapp:config.app"
    /// let store = RedisStore::with_prefix("redis://localhost", Some("myapp:")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_prefix(connection_string: &str, prefix: Option<&str>) -> Result<Self, StorageError> {
        let client = Client::open(connection_string)
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        // Use startup config: fast-fail after ~30s, don't hang forever
        let connection = retry("redis_connect", &RetryConfig::startup(), || async {
            ConnectionManager::new(client.clone()).await
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))?;

        Ok(Self { 
            connection,
            prefix: prefix.unwrap_or("").to_string(),
        })
    }
    
    /// Apply the prefix to a key.
    #[inline]
    fn prefixed_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }
    
    /// Strip the prefix from a key (for returning clean IDs).
    /// Will be used when implementing key iteration/scanning.
    #[inline]
    #[allow(dead_code)]
    fn strip_prefix<'a>(&self, key: &'a str) -> &'a str {
        if self.prefix.is_empty() {
            key
        } else {
            key.strip_prefix(&self.prefix).unwrap_or(key)
        }
    }

    /// Get a clone of the connection manager (for sharing with MerkleStore)
    pub fn connection(&self) -> ConnectionManager {
        self.connection.clone()
    }
    
    /// Get the configured prefix
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
    
    /// Build the JSON document for RedisJSON storage.
    /// 
    /// Structure (flat with nested audit):
    /// ```json
    /// {
    ///   "version": 1,
    ///   "timestamp": 1767084657058,
    ///   "payload_hash": "abc123...",
    ///   "state": "default",
    ///   "access_count": 5,
    ///   "last_accessed": 1767084660000,
    ///   "payload": {"name": "Alice", ...},
    ///   "audit": {"batch": "...", "trace": "...", "home": "..."}
    /// }
    /// ```
    fn build_json_document(item: &SyncItem) -> Result<String, StorageError> {
        // Parse user content as JSON
        let payload: serde_json::Value = serde_json::from_slice(&item.content)
            .map_err(|e| StorageError::Backend(format!("Invalid JSON content: {}", e)))?;
        
        // Build audit object (internal operational metadata)
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
        
        // Build final document (flat structure)
        let mut doc = serde_json::json!({
            "version": item.version,
            "timestamp": item.updated_at,
            "state": item.state,
            "access_count": item.access_count,
            "last_accessed": item.last_accessed,
            "payload": payload
        });
        
        // Only include payload_hash if non-empty
        if !item.content_hash.is_empty() {
            doc["payload_hash"] = serde_json::Value::String(item.content_hash.clone());
        }
        
        // Only include audit if there's something in it
        if !audit.is_empty() {
            doc["audit"] = serde_json::Value::Object(audit);
        }
        
        serde_json::to_string(&doc)
            .map_err(|e| StorageError::Backend(e.to_string()))
    }
    
    /// Parse a RedisJSON document back into a SyncItem.
    fn parse_json_document(id: &str, json_str: &str) -> Result<SyncItem, StorageError> {
        let doc: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| StorageError::Backend(format!("Invalid JSON document: {}", e)))?;
        
        // Top-level fields
        let version = doc.get("version").and_then(|v| v.as_u64()).unwrap_or(1);
        let updated_at = doc.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
        let content_hash = doc.get("payload_hash").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let state = doc.get("state").and_then(|v| v.as_str()).unwrap_or("default").to_string();
        
        // Access metadata (local-only, not replicated)
        let access_count = doc.get("access_count").and_then(|v| v.as_u64()).unwrap_or(0);
        let last_accessed = doc.get("last_accessed").and_then(|v| v.as_u64()).unwrap_or(0);
        
        // Audit fields (nested)
        let audit = doc.get("audit");
        let batch_id = audit.and_then(|a| a.get("batch")).and_then(|v| v.as_str()).map(String::from);
        let trace_parent = audit.and_then(|a| a.get("trace")).and_then(|v| v.as_str()).map(String::from);
        let home_instance_id = audit.and_then(|a| a.get("home")).and_then(|v| v.as_str()).map(String::from);
        
        // Extract payload and serialize back to bytes
        let payload = doc.get("payload").cloned().unwrap_or(serde_json::Value::Null);
        let content = serde_json::to_vec(&payload)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        
        Ok(SyncItem::reconstruct(
            id.to_string(),
            version,
            updated_at,
            ContentType::Json,
            content,
            batch_id,
            trace_parent,
            content_hash,
            home_instance_id,
            state,
            access_count,
            last_accessed,
        ))
    }

    /// Parse FT.SEARCH NOCONTENT response into a list of keys.
    /// Response format: [count, key1, key2, ...]
    fn parse_ft_search_response(value: redis::Value) -> Result<Vec<String>, redis::RedisError> {
        match value {
            redis::Value::Array(arr) => {
                if arr.is_empty() {
                    return Ok(vec![]);
                }
                // First element is count, rest are keys
                let keys: Vec<String> = arr.into_iter()
                    .skip(1) // Skip count
                    .filter_map(|v| match v {
                        redis::Value::BulkString(bytes) => String::from_utf8(bytes).ok(),
                        redis::Value::SimpleString(s) => Some(s),
                        _ => None,
                    })
                    .collect();
                Ok(keys)
            }
            _ => Ok(vec![]),
        }
    }
}

#[async_trait]
impl CacheStore for RedisStore {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        let conn = self.connection.clone();
        let prefixed_id = self.prefixed_key(id);
        let original_id = id.to_string();
        
        // Check the type of the key to determine how to read it
        let key_type: Option<String> = retry("redis_type", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let key = prefixed_id.clone();
            async move {
                let t: String = redis::cmd("TYPE").arg(&key).query_async(&mut conn).await?;
                Ok(if t == "none" { None } else { Some(t) })
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))?;
        
        match key_type.as_deref() {
            None => Ok(None), // Key doesn't exist
            Some("ReJSON-RL") => {
                // RedisJSON document - use JSON.GET
                let json_str: Option<String> = retry("redis_json_get", &RetryConfig::query(), || {
                    let mut conn = conn.clone();
                    let key = prefixed_id.clone();
                    async move {
                        let data: Option<String> = cmd("JSON.GET")
                            .arg(&key)
                            .query_async(&mut conn)
                            .await?;
                        Ok(data)
                    }
                })
                .await
                .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))?;
                
                match json_str {
                    Some(s) => Self::parse_json_document(&original_id, &s).map(Some),
                    None => Ok(None),
                }
            }
            Some("string") => {
                // Binary content or legacy format - read as bytes
                let data: Option<Vec<u8>> = retry("redis_get", &RetryConfig::query(), || {
                    let mut conn = conn.clone();
                    let key = prefixed_id.clone();
                    async move {
                        let data: Option<Vec<u8>> = conn.get(&key).await?;
                        Ok(data)
                    }
                })
                .await
                .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))?;
                
                data.map(|bytes| serde_json::from_slice(&bytes).map_err(|e| StorageError::Backend(e.to_string())))
                    .transpose()
            }
            Some(other) => {
                Err(StorageError::Backend(format!("Unexpected Redis key type: {}", other)))
            }
        }
    }

    async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
        let conn = self.connection.clone();
        let prefixed_id = self.prefixed_key(&item.object_id);
        
        match item.content_type {
            ContentType::Json => {
                // Build JSON document with metadata wrapper
                let json_doc = Self::build_json_document(item)?;
                
                retry("redis_json_set", &RetryConfig::query(), || {
                    let mut conn = conn.clone();
                    let key = prefixed_id.clone();
                    let doc = json_doc.clone();
                    async move {
                        // JSON.SET key $ <json>
                        let _: () = cmd("JSON.SET")
                            .arg(&key)
                            .arg("$")
                            .arg(&doc)
                            .query_async(&mut conn)
                            .await?;
                        Ok(())
                    }
                })
                .await
                .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
            }
            ContentType::Binary => {
                // Store as serialized blob (binary content)
                let data = serde_json::to_vec(item)
                    .map_err(|e| StorageError::Backend(e.to_string()))?;

                retry("redis_set", &RetryConfig::query(), || {
                    let mut conn = conn.clone();
                    let key = prefixed_id.clone();
                    let data = data.clone();
                    async move {
                        let _: () = conn.set(&key, &data).await?;
                        Ok(())
                    }
                })
                .await
                .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
            }
        }
    }

    async fn delete(&self, id: &str) -> Result<(), StorageError> {
        let conn = self.connection.clone();
        let prefixed_id = self.prefixed_key(id);

        retry("redis_delete", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let key = prefixed_id.clone();
            async move {
                let _: () = conn.del(&key).await?;
                Ok(())
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }

    async fn exists(&self, id: &str) -> Result<bool, StorageError> {
        let conn = self.connection.clone();
        let prefixed_id = self.prefixed_key(id);

        retry("redis_exists", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let key = prefixed_id.clone();
            async move {
                let exists: bool = conn.exists(&key).await?;
                Ok(exists)
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }

    /// Write a batch of items using Redis pipeline (atomic, much faster than individual SETs).
    async fn put_batch(&self, items: &[SyncItem]) -> Result<BatchWriteResult, StorageError> {
        self.put_batch_with_ttl(items, None).await
    }
    
    /// Write a batch of items with optional TTL.
    async fn put_batch_with_ttl(&self, items: &[SyncItem], ttl_secs: Option<u64>) -> Result<BatchWriteResult, StorageError> {
        self.put_batch_impl(items, ttl_secs).await
    }

    /// Create a RediSearch index (FT.CREATE).
    async fn ft_create(&self, args: &[String]) -> Result<(), StorageError> {
        let conn = self.connection.clone();

        retry("redis_ft_create", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let args = args.to_vec();
            async move {
                let mut cmd = cmd("FT.CREATE");
                for arg in &args {
                    cmd.arg(arg);
                }
                let _: () = cmd.query_async(&mut conn).await?;
                Ok(())
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }

    /// Drop a RediSearch index (FT.DROPINDEX).
    async fn ft_dropindex(&self, index: &str) -> Result<(), StorageError> {
        let conn = self.connection.clone();
        let index = index.to_string();

        retry("redis_ft_dropindex", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let index = index.clone();
            async move {
                let _: () = cmd("FT.DROPINDEX")
                    .arg(&index)
                    .query_async(&mut conn)
                    .await?;
                Ok(())
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }

    /// Search using RediSearch (FT.SEARCH).
    async fn ft_search(&self, index: &str, query: &str, limit: usize) -> Result<Vec<String>, StorageError> {
        let conn = self.connection.clone();
        let index = index.to_string();
        let query = query.to_string();

        retry("redis_ft_search", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let index = index.clone();
            let query = query.clone();
            async move {
                // FT.SEARCH index query LIMIT 0 limit NOCONTENT
                // NOCONTENT returns only keys, not document content
                let result: redis::Value = cmd("FT.SEARCH")
                    .arg(&index)
                    .arg(&query)
                    .arg("LIMIT")
                    .arg(0)
                    .arg(limit)
                    .arg("NOCONTENT")
                    .query_async(&mut conn)
                    .await?;

                // Parse FT.SEARCH response: [count, key1, key2, ...]
                Self::parse_ft_search_response(result)
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }

    /// Search using RediSearch with binary parameters (for vector KNN search).
    /// Uses FT.SEARCH index query PARAMS n name blob... LIMIT offset count NOCONTENT
    async fn ft_search_with_params(
        &self,
        index: &str,
        query: &str,
        params: &[(String, Vec<u8>)],
        limit: usize,
    ) -> Result<Vec<String>, StorageError> {
        let conn = self.connection.clone();
        let index = index.to_string();
        let query = query.to_string();
        let params: Vec<(String, Vec<u8>)> = params.to_vec();

        retry("redis_ft_search_knn", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let index = index.clone();
            let query = query.clone();
            let params = params.clone();
            async move {
                // FT.SEARCH index query PARAMS n name1 blob1 name2 blob2... LIMIT 0 limit NOCONTENT
                let mut command = cmd("FT.SEARCH");
                command.arg(&index).arg(&query);

                // Add PARAMS section: PARAMS {count} {name} {blob}...
                if !params.is_empty() {
                    command.arg("PARAMS").arg(params.len() * 2);
                    for (name, blob) in &params {
                        command.arg(name).arg(blob.as_slice());
                    }
                }

                command.arg("LIMIT").arg(0).arg(limit).arg("NOCONTENT");

                let result: redis::Value = command.query_async(&mut conn).await?;
                Self::parse_ft_search_response(result)
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }
}

impl RedisStore {
    /// Pipelined batch write implementation with content-type aware storage.
    /// Uses JSON.SET for JSON content, SET for binary blobs.
    /// Also adds items to state SETs for fast state-based queries.
    async fn put_batch_impl(&self, items: &[SyncItem], ttl_secs: Option<u64>) -> Result<BatchWriteResult, StorageError> {
        if items.is_empty() {
            return Ok(BatchWriteResult {
                batch_id: String::new(),
                written: 0,
                verified: true,
            });
        }

        // Prepare items: JSON → JSON.SET document, Binary → serialized bytes
        #[derive(Clone)]
        enum PreparedItem {
            Json { key: String, id: String, state: String, doc: String },
            Blob { key: String, id: String, state: String, data: Vec<u8> },
        }
        
        let prepared: Result<Vec<_>, _> = items.iter()
            .map(|item| {
                let prefixed_key = self.prefixed_key(&item.object_id);
                let id = item.object_id.clone();
                let state = item.state.clone();
                match item.content_type {
                    ContentType::Json => {
                        Self::build_json_document(item)
                            .map(|doc| PreparedItem::Json { key: prefixed_key, id, state, doc })
                    }
                    ContentType::Binary => {
                        serde_json::to_vec(item)
                            .map(|bytes| PreparedItem::Blob { key: prefixed_key, id, state, data: bytes })
                            .map_err(|e| StorageError::Backend(e.to_string()))
                    }
                }
            })
            .collect();
        let prepared = prepared?;
        let count = prepared.len();

        let conn = self.connection.clone();
        let prefix = self.prefix.clone();
        
        retry("redis_put_batch", &RetryConfig::batch_write(), || {
            let mut conn = conn.clone();
            let prepared = prepared.clone();
            let prefix = prefix.clone();
            async move {
                let mut pipeline = pipe();
                
                for item in &prepared {
                    match item {
                        PreparedItem::Json { key, id, state, doc } => {
                            // JSON.SET key $ <json>
                            pipeline.cmd("JSON.SET").arg(key).arg("$").arg(doc);
                            if let Some(ttl) = ttl_secs {
                                pipeline.expire(key, ttl as i64);
                            }
                            // Add to state SET: sync:state:{state}
                            let state_key = format!("{}state:{}", prefix, state);
                            pipeline.cmd("SADD").arg(&state_key).arg(id);
                        }
                        PreparedItem::Blob { key, id, state, data } => {
                            // SET for binary content
                            if let Some(ttl) = ttl_secs {
                                pipeline.cmd("SETEX").arg(key).arg(ttl as i64).arg(data.as_slice());
                            } else {
                                pipeline.set(key, data.as_slice());
                            }
                            // Add to state SET
                            let state_key = format!("{}state:{}", prefix, state);
                            pipeline.cmd("SADD").arg(&state_key).arg(id);
                        }
                    }
                }
                
                pipeline.query_async::<()>(&mut conn).await?;
                Ok(())
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))?;

        Ok(BatchWriteResult {
            batch_id: String::new(),
            written: count,
            verified: true,
        })
    }

    /// Check if multiple keys exist in Redis (pipelined).
    /// Returns a vec of bools matching the input order.
    pub async fn exists_batch(&self, ids: &[String]) -> Result<Vec<bool>, StorageError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.connection.clone();
        // Apply prefix to all keys
        let prefixed_ids: Vec<String> = ids.iter().map(|id| self.prefixed_key(id)).collect();

        retry("redis_exists_batch", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let prefixed_ids = prefixed_ids.clone();
            async move {
                let mut pipeline = pipe();
                for key in &prefixed_ids {
                    pipeline.exists(key);
                }
                
                let results: Vec<bool> = pipeline.query_async(&mut conn).await?;
                Ok(results)
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // State SET operations: O(1) membership, fast iteration by state
    // ═══════════════════════════════════════════════════════════════════════════
    
    /// Get all IDs in a given state (from Redis SET).
    ///
    /// Returns IDs without prefix - ready to use with `get()`.
    pub async fn list_state_ids(&self, state: &str) -> Result<Vec<String>, StorageError> {
        let mut conn = self.connection.clone();
        let state_key = format!("{}state:{}", self.prefix, state);
        
        let ids: Vec<String> = cmd("SMEMBERS")
            .arg(&state_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to get state members: {}", e)))?;
        
        Ok(ids)
    }
    
    /// Count items in a given state (SET cardinality).
    pub async fn count_by_state(&self, state: &str) -> Result<u64, StorageError> {
        let mut conn = self.connection.clone();
        let state_key = format!("{}state:{}", self.prefix, state);
        
        let count: u64 = cmd("SCARD")
            .arg(&state_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to count state: {}", e)))?;
        
        Ok(count)
    }
    
    /// Check if an ID is in a given state (SET membership).
    pub async fn is_in_state(&self, id: &str, state: &str) -> Result<bool, StorageError> {
        let mut conn = self.connection.clone();
        let state_key = format!("{}state:{}", self.prefix, state);
        
        let is_member: bool = cmd("SISMEMBER")
            .arg(&state_key)
            .arg(id)
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to check state membership: {}", e)))?;
        
        Ok(is_member)
    }
    
    /// Move an ID from one state to another (atomic SMOVE).
    ///
    /// Returns true if the item was moved, false if it wasn't in the source state.
    pub async fn move_state(&self, id: &str, from_state: &str, to_state: &str) -> Result<bool, StorageError> {
        let mut conn = self.connection.clone();
        let from_key = format!("{}state:{}", self.prefix, from_state);
        let to_key = format!("{}state:{}", self.prefix, to_state);
        
        let moved: bool = cmd("SMOVE")
            .arg(&from_key)
            .arg(&to_key)
            .arg(id)
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to move state: {}", e)))?;
        
        Ok(moved)
    }
    
    /// Remove an ID from a state SET.
    pub async fn remove_from_state(&self, id: &str, state: &str) -> Result<bool, StorageError> {
        let mut conn = self.connection.clone();
        let state_key = format!("{}state:{}", self.prefix, state);
        
        let removed: u32 = cmd("SREM")
            .arg(&state_key)
            .arg(id)
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to remove from state: {}", e)))?;
        
        Ok(removed > 0)
    }
    
    /// Delete all items in a state (both the SET and the actual keys).
    ///
    /// Returns the number of items deleted.
    pub async fn delete_by_state(&self, state: &str) -> Result<u64, StorageError> {
        let mut conn = self.connection.clone();
        let state_key = format!("{}state:{}", self.prefix, state);
        
        // Get all IDs in this state
        let ids: Vec<String> = cmd("SMEMBERS")
            .arg(&state_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to get state members: {}", e)))?;
        
        if ids.is_empty() {
            return Ok(0);
        }
        
        let count = ids.len() as u64;
        
        // Delete all the keys and the state SET
        let mut pipeline = pipe();
        for id in &ids {
            let key = self.prefixed_key(id);
            pipeline.del(&key);
        }
        pipeline.del(&state_key);
        
        pipeline.query_async::<()>(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to delete state items: {}", e)))?;
        
        Ok(count)
    }
    
    /// Scan items by ID prefix using Redis SCAN.
    ///
    /// Uses cursor-based SCAN with MATCH pattern for safe iteration.
    /// Does NOT block the server (unlike KEYS).
    ///
    /// # Example
    /// ```rust,ignore
    /// // Get all deltas for object user.123
    /// let deltas = store.scan_prefix("delta:user.123:", 1000).await?;
    /// ```
    pub async fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        let mut conn = self.connection.clone();
        
        // Build match pattern: "{store_prefix}{user_prefix}*"
        let pattern = format!("{}{}*", self.prefix, prefix);
        
        let mut items = Vec::new();
        let mut cursor: u64 = 0;
        
        // SCAN iteration (cursor-based, non-blocking)
        loop {
            // SCAN cursor MATCH pattern COUNT batch_size
            let (new_cursor, keys): (u64, Vec<String>) = cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100) // Batch size per iteration
                .query_async(&mut conn)
                .await
                .map_err(|e| StorageError::Backend(format!("SCAN failed: {}", e)))?;
            
            // Fetch each key using JSON.GET
            for key in keys {
                if items.len() >= limit {
                    break;
                }
                
                let json_opt: Option<String> = cmd("JSON.GET")
                    .arg(&key)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| StorageError::Backend(format!("JSON.GET failed: {}", e)))?;
                
                if let Some(json_str) = json_opt {
                    // Strip prefix to get clean ID
                    let id = self.strip_prefix(&key);
                    if let Ok(item) = Self::parse_json_document(id, &json_str) {
                        items.push(item);
                    }
                }
            }
            
            cursor = new_cursor;
            
            // Stop if cursor is 0 (complete) or we have enough items
            if cursor == 0 || items.len() >= limit {
                break;
            }
        }
        
        Ok(items)
    }
    
    /// Count items matching an ID prefix.
    ///
    /// Note: This requires scanning all matching keys, so use sparingly.
    pub async fn count_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let mut conn = self.connection.clone();
        let pattern = format!("{}{}*", self.prefix, prefix);
        
        let mut count: u64 = 0;
        let mut cursor: u64 = 0;
        
        loop {
            let (new_cursor, keys): (u64, Vec<String>) = cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut conn)
                .await
                .map_err(|e| StorageError::Backend(format!("SCAN failed: {}", e)))?;
            
            count += keys.len() as u64;
            cursor = new_cursor;
            
            if cursor == 0 {
                break;
            }
        }
        
        Ok(count)
    }
    
    /// Delete all items matching an ID prefix.
    ///
    /// Returns the number of deleted items.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<u64, StorageError> {
        let mut conn = self.connection.clone();
        let pattern = format!("{}{}*", self.prefix, prefix);
        
        let mut deleted: u64 = 0;
        let mut cursor: u64 = 0;
        
        loop {
            let (new_cursor, keys): (u64, Vec<String>) = cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut conn)
                .await
                .map_err(|e| StorageError::Backend(format!("SCAN failed: {}", e)))?;
            
            if !keys.is_empty() {
                // Batch delete
                let mut pipeline = pipe();
                for key in &keys {
                    pipeline.del(key);
                }
                pipeline.query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| StorageError::Backend(format!("DEL failed: {}", e)))?;
                
                deleted += keys.len() as u64;
            }
            
            cursor = new_cursor;
            
            if cursor == 0 {
                break;
            }
        }
        
        Ok(deleted)
    }
    
    // ========================================================================
    // CDC Stream Methods
    // ========================================================================
    
    /// Write a CDC entry to the stream.
    /// 
    /// Uses XADD with MAXLEN ~ for bounded stream size.
    /// The stream key is `{prefix}__local__:cdc`.
    pub async fn xadd_cdc(
        &self, 
        entry: &crate::cdc::CdcEntry, 
        maxlen: u64
    ) -> Result<String, StorageError> {
        let stream_key = crate::cdc::cdc_stream_key(if self.prefix.is_empty() { None } else { Some(&self.prefix) });
        let fields = entry.to_redis_fields();
        
        let mut conn = self.connection.clone();
        
        // Build XADD command: XADD key MAXLEN ~ maxlen * field1 value1 field2 value2 ...
        let mut command = cmd("XADD");
        command.arg(&stream_key);
        command.arg("MAXLEN");
        command.arg("~");
        command.arg(maxlen);
        command.arg("*"); // Auto-generate ID
        
        for (field, value) in fields {
            command.arg(field);
            command.arg(value.as_bytes());
        }
        
        let entry_id: String = command
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("XADD CDC failed: {}", e)))?;
        
        Ok(entry_id)
    }
    
    /// Write multiple CDC entries to the stream in a pipeline.
    /// 
    /// Returns the stream entry IDs for each write.
    pub async fn xadd_cdc_batch(
        &self, 
        entries: &[crate::cdc::CdcEntry], 
        maxlen: u64
    ) -> Result<Vec<String>, StorageError> {
        if entries.is_empty() {
            return Ok(vec![]);
        }
        
        let stream_key = crate::cdc::cdc_stream_key(if self.prefix.is_empty() { None } else { Some(&self.prefix) });
        let mut conn = self.connection.clone();
        
        let mut pipeline = pipe();
        
        for entry in entries {
            let fields = entry.to_redis_fields();
            
            // Start XADD command in pipeline
            let mut command = cmd("XADD");
            command.arg(&stream_key);
            command.arg("MAXLEN");
            command.arg("~");
            command.arg(maxlen);
            command.arg("*");
            
            for (field, value) in fields {
                command.arg(field);
                command.arg(value.as_bytes());
            }
            
            pipeline.add_command(command);
        }
        
        let ids: Vec<String> = pipeline
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Backend(format!("XADD CDC batch failed: {}", e)))?;
        
        Ok(ids)
    }
}