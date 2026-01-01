use async_trait::async_trait;
use crate::sync_item::SyncItem;
use crate::search::SqlParam;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Item not found")]
    NotFound,
    #[error("Storage backend error: {0}")]
    Backend(String),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Data corruption detected for '{id}': expected hash {expected}, got {actual}")]
    Corruption {
        id: String,
        expected: String,
        actual: String,
    },
}

/// Result of a batch write operation with verification
#[derive(Debug)]
pub struct BatchWriteResult {
    /// Unique batch ID for this write (for SQL verification)
    pub batch_id: String,
    /// Number of items successfully written
    pub written: usize,
    /// Whether the write was verified (for SQL, reads back count; for Redis, always true)
    pub verified: bool,
}

#[async_trait]
pub trait CacheStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError>;
    async fn put(&self, item: &SyncItem) -> Result<(), StorageError>;
    async fn delete(&self, id: &str) -> Result<(), StorageError>;
    
    /// Check if an item exists (Redis EXISTS command - fast, no data transfer).
    async fn exists(&self, id: &str) -> Result<bool, StorageError>;
    
    /// Write a batch of items atomically (pipelined for Redis).
    /// Default implementation falls back to sequential puts.
    async fn put_batch(&self, items: &[SyncItem]) -> Result<BatchWriteResult, StorageError> {
        self.put_batch_with_ttl(items, None).await
    }
    
    /// Write a batch of items with optional TTL (in seconds).
    /// For Redis: uses SETEX when ttl is Some, SET when None.
    async fn put_batch_with_ttl(&self, items: &[SyncItem], ttl_secs: Option<u64>) -> Result<BatchWriteResult, StorageError> {
        // Default: ignore TTL, just do sequential puts
        let _ = ttl_secs;
        for item in items {
            self.put(item).await?;
        }
        Ok(BatchWriteResult {
            batch_id: String::new(),
            written: items.len(),
            verified: true,
        })
    }

    /// Create a RediSearch index (FT.CREATE).
    async fn ft_create(&self, args: &[String]) -> Result<(), StorageError> {
        let _ = args;
        Err(StorageError::Backend("FT.CREATE not supported".into()))
    }

    /// Drop a RediSearch index (FT.DROPINDEX).
    async fn ft_dropindex(&self, index: &str) -> Result<(), StorageError> {
        let _ = index;
        Err(StorageError::Backend("FT.DROPINDEX not supported".into()))
    }

    /// Search using RediSearch (FT.SEARCH).
    /// Returns matching keys.
    async fn ft_search(&self, index: &str, query: &str, limit: usize) -> Result<Vec<String>, StorageError> {
        let _ = (index, query, limit);
        Err(StorageError::Backend("FT.SEARCH not supported".into()))
    }
}

#[async_trait]
pub trait ArchiveStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError>;
    async fn put(&self, item: &SyncItem) -> Result<(), StorageError>;
    async fn delete(&self, id: &str) -> Result<(), StorageError>;
    
    /// Check if an item exists (SQL EXISTS query - fast, no data transfer).
    async fn exists(&self, id: &str) -> Result<bool, StorageError>;
    
    /// Write a batch of items with verification.
    /// The batch_id is stamped on items and can be queried back for verification.
    /// Default implementation falls back to sequential puts.
    async fn put_batch(&self, items: &mut [SyncItem]) -> Result<BatchWriteResult, StorageError> {
        for item in items.iter() {
            self.put(item).await?;
        }
        Ok(BatchWriteResult {
            batch_id: String::new(),
            written: items.len(),
            verified: true,
        })
    }

    /// Scan keys for cuckoo filter warmup (paginated).
    /// Returns empty vec when offset exceeds total count.
    async fn scan_keys(&self, offset: u64, limit: usize) -> Result<Vec<String>, StorageError>;
    
    /// Count total items in store.
    async fn count_all(&self) -> Result<u64, StorageError>;

    /// Search using SQL WHERE clause with JSON_EXTRACT.
    /// Returns matching items.
    async fn search(&self, where_clause: &str, params: &[SqlParam], limit: usize) -> Result<Vec<SyncItem>, StorageError> {
        let _ = (where_clause, params, limit);
        Err(StorageError::Backend("SQL search not supported".into()))
    }
}
