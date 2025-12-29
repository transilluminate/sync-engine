use async_trait::async_trait;
use crate::sync_item::SyncItem;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Item not found")]
    NotFound,
    #[error("Storage backend error: {0}")]
    Backend(String),
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
    
    /// Write a batch of items atomically (pipelined for Redis).
    /// Default implementation falls back to sequential puts.
    async fn put_batch(&self, items: &[SyncItem]) -> Result<BatchWriteResult, StorageError> {
        for item in items {
            self.put(item).await?;
        }
        Ok(BatchWriteResult {
            batch_id: String::new(),
            written: items.len(),
            verified: true,
        })
    }
}

#[async_trait]
pub trait ArchiveStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError>;
    async fn put(&self, item: &SyncItem) -> Result<(), StorageError>;
    async fn delete(&self, id: &str) -> Result<(), StorageError>;
    
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
}
