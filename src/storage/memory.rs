use async_trait::async_trait;
use dashmap::DashMap;
use crate::sync_item::SyncItem;
use super::traits::{CacheStore, StorageError};

pub struct InMemoryStore {
    data: DashMap<String, SyncItem>,
}

impl InMemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }

    /// Get current item count
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear all items
    pub fn clear(&self) {
        self.data.clear();
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CacheStore for InMemoryStore {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        Ok(self.data.get(id).map(|r| r.value().clone()))
    }

    async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
        self.data.insert(item.object_id.clone(), item.clone());
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.data.remove(id);
        Ok(())
    }

    async fn exists(&self, id: &str) -> Result<bool, StorageError> {
        Ok(self.data.contains_key(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_item(id: &str) -> SyncItem {
        SyncItem::from_json(id.to_string(), json!({"test": "data", "id": id}))
    }

    #[tokio::test]
    async fn test_new_store_is_empty() {
        let store = InMemoryStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let store = InMemoryStore::new();
        let item = test_item("item-1");
        
        store.put(&item).await.unwrap();
        
        let result = store.get("item-1").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().object_id, "item-1");
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let store = InMemoryStore::new();
        
        let result = store.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let store = InMemoryStore::new();
        let item = test_item("to-delete");
        
        store.put(&item).await.unwrap();
        assert_eq!(store.len(), 1);
        
        store.delete("to-delete").await.unwrap();
        assert_eq!(store.len(), 0);
        
        let result = store.get("to-delete").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_is_ok() {
        let store = InMemoryStore::new();
        
        // Should not error
        let result = store.delete("nonexistent").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_overwrites() {
        let store = InMemoryStore::new();
        
        let item1 = SyncItem::from_json("same-id".to_string(), json!({"version": 1}));
        let item2 = SyncItem::from_json("same-id".to_string(), json!({"version": 2}));
        
        store.put(&item1).await.unwrap();
        store.put(&item2).await.unwrap();
        
        assert_eq!(store.len(), 1);
        
        let result = store.get("same-id").await.unwrap().unwrap();
        let content = result.content_as_json().unwrap();
        assert_eq!(content["version"], 2);
    }

    #[tokio::test]
    async fn test_clear() {
        let store = InMemoryStore::new();
        
        for i in 0..10 {
            store.put(&test_item(&format!("item-{}", i))).await.unwrap();
        }
        
        assert_eq!(store.len(), 10);
        
        store.clear();
        
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn test_default_trait() {
        let store = InMemoryStore::default();
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn test_put_batch_via_trait() {
        let store = InMemoryStore::new();
        
        let items: Vec<SyncItem> = (0..5)
            .map(|i| test_item(&format!("batch-{}", i)))
            .collect();
        
        // Use trait default implementation
        let result = store.put_batch(&items).await.unwrap();
        
        assert_eq!(result.written, 5);
        assert!(result.verified);
        assert_eq!(store.len(), 5);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        use std::sync::Arc;
        
        let store = Arc::new(InMemoryStore::new());
        let mut handles = vec![];
        
        // Spawn 10 tasks that each insert 10 items
        for batch in 0..10 {
            let store_clone = store.clone();
            let handle = tokio::spawn(async move {
                for i in 0..10 {
                    let item = test_item(&format!("batch-{}-item-{}", batch, i));
                    store_clone.put(&item).await.unwrap();
                }
            });
            handles.push(handle);
        }
        
        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }
        
        // Should have all 100 items
        assert_eq!(store.len(), 100);
    }
}
