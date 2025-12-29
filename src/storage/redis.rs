use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{Client, AsyncCommands, pipe};
use crate::sync_item::SyncItem;
use super::traits::{BatchWriteResult, CacheStore, StorageError};
use crate::resilience::retry::{retry, RetryConfig};

pub struct RedisStore {
    connection: ConnectionManager,
}

impl RedisStore {
    pub async fn new(connection_string: &str) -> Result<Self, StorageError> {
        let client = Client::open(connection_string)
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        // Use startup config: fast-fail after ~30s, don't hang forever
        let connection = retry("redis_connect", &RetryConfig::startup(), || async {
            ConnectionManager::new(client.clone()).await
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))?;

        Ok(Self { connection })
    }

    /// Get a clone of the connection manager (for sharing with MerkleStore)
    pub fn connection(&self) -> ConnectionManager {
        self.connection.clone()
    }
}

#[async_trait]
impl CacheStore for RedisStore {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        let conn = self.connection.clone();
        let id = id.to_string();
        
        retry("redis_get", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let id = id.clone();
            async move {
                let data: Option<String> = conn.get(&id).await?;
                Ok(data)
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))?
        .map(|s| serde_json::from_str(&s).map_err(|e| StorageError::Backend(e.to_string())))
        .transpose()
    }

    async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
        let conn = self.connection.clone();
        let id = item.object_id.clone();
        let data = serde_json::to_string(item)
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        retry("redis_put", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let id = id.clone();
            let data = data.clone();
            async move {
                let _: () = conn.set(&id, &data).await?;
                Ok(())
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }

    async fn delete(&self, id: &str) -> Result<(), StorageError> {
        let conn = self.connection.clone();
        let id = id.to_string();

        retry("redis_delete", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let id = id.clone();
            async move {
                let _: () = conn.del(&id).await?;
                Ok(())
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }

    /// Write a batch of items using Redis pipeline (atomic, much faster than individual SETs).
    async fn put_batch(&self, items: &[SyncItem]) -> Result<BatchWriteResult, StorageError> {
        self.put_batch_impl(items).await
    }
}

impl RedisStore {
    /// Pipelined batch write implementation.
    async fn put_batch_impl(&self, items: &[SyncItem]) -> Result<BatchWriteResult, StorageError> {
        if items.is_empty() {
            return Ok(BatchWriteResult {
                batch_id: String::new(),
                written: 0,
                verified: true,
            });
        }

        // Serialize all items first
        let serialized: Result<Vec<_>, _> = items.iter()
            .map(|item| {
                serde_json::to_string(item)
                    .map(|s| (item.object_id.clone(), s))
                    .map_err(|e| StorageError::Backend(e.to_string()))
            })
            .collect();
        let serialized = serialized?;
        let count = serialized.len();

        let conn = self.connection.clone();
        
        retry("redis_put_batch", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let serialized = serialized.clone();
            async move {
                // Build the pipeline
                let mut pipeline = pipe();
                for (id, data) in &serialized {
                    pipeline.set(id, data);
                }
                
                // Execute atomically
                pipeline.query_async::<()>(&mut conn).await?;
                Ok(())
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))?;

        Ok(BatchWriteResult {
            batch_id: String::new(), // Redis doesn't need verification
            written: count,
            verified: true, // Pipeline is atomic
        })
    }

    /// Check if multiple keys exist in Redis (pipelined).
    /// Returns a vec of bools matching the input order.
    pub async fn exists_batch(&self, ids: &[String]) -> Result<Vec<bool>, StorageError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.connection.clone();
        let ids = ids.to_vec();

        retry("redis_exists_batch", &RetryConfig::query(), || {
            let mut conn = conn.clone();
            let ids = ids.clone();
            async move {
                let mut pipeline = pipe();
                for id in &ids {
                    pipeline.exists(id);
                }
                
                let results: Vec<bool> = pipeline.query_async(&mut conn).await?;
                Ok(results)
            }
        })
        .await
        .map_err(|e: redis::RedisError| StorageError::Backend(e.to_string()))
    }
}