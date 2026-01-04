// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Search Result Cache
//!
//! Caches SQL search results with merkle-based invalidation.
//! When the merkle root for a prefix changes, cached results are automatically stale.
//!
//! # Why This Works
//!
//! - Merkle root = content hash of all data under prefix
//! - Any write → new merkle root → cache auto-invalidates
//! - Repeated identical searches → memory hit
//! - Bounded by max entries with oldest-eviction
//!
//! # Flow
//!
//! ```text
//! Search query arrives
//!       │
//!       ▼
//! ┌─────────────────────────────┐
//! │  Cache lookup               │
//! │  key = hash(query + prefix) │
//! │  check: cached_merkle_root  │
//! │         == current_root?    │
//! └─────────────────────────────┘
//!       │
//!       ├─→ Hit + root matches → return cached keys
//!       │
//!       └─→ Miss OR stale → run SQL, cache results
//! ```

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::hash_map::DefaultHasher;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use super::Query;

/// Cache key: (prefix, query_hash)
type CacheKey = (String, u64);

/// Cached search result entry
#[derive(Clone, Debug)]
struct CacheEntry {
    /// Merkle root at time of caching
    merkle_root: Vec<u8>,
    /// Cached result keys
    keys: Vec<String>,
}

/// Search result cache with merkle-based invalidation
pub struct SearchCache {
    /// Cache: (prefix, query_hash) → (merkle_root, result_keys)
    cache: DashMap<CacheKey, CacheEntry>,
    /// Insertion order for eviction (oldest first)
    order: Mutex<VecDeque<CacheKey>>,
    /// Maximum number of entries
    max_entries: usize,
    /// Cache hits counter
    hits: AtomicU64,
    /// Cache misses counter
    misses: AtomicU64,
    /// Stale invalidations counter
    stale: AtomicU64,
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct SearchCacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Number of stale entries (merkle root changed)
    pub stale: u64,
    /// Current number of entries
    pub entry_count: usize,
    /// Hit rate (0.0 - 1.0)
    pub hit_rate: f64,
}

impl SearchCache {
    /// Create a new search cache with the given max entries
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: DashMap::new(),
            order: Mutex::new(VecDeque::new()),
            max_entries,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            stale: AtomicU64::new(0),
        }
    }

    /// Try to get cached results for a query
    ///
    /// Returns `Some(keys)` if cache hit and merkle root matches,
    /// `None` if miss or stale.
    pub fn get(&self, prefix: &str, query: &Query, current_merkle_root: &[u8]) -> Option<Vec<String>> {
        let key = self.cache_key(prefix, query);

        if let Some(entry) = self.cache.get(&key) {
            if entry.merkle_root == current_merkle_root {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.keys.clone());
            }
            // Merkle root changed - data is stale
            self.stale.fetch_add(1, Ordering::Relaxed);
            drop(entry); // Release read lock before removing
            self.cache.remove(&key);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache search results
    pub fn insert(&self, prefix: &str, query: &Query, merkle_root: Vec<u8>, keys: Vec<String>) {
        let key = self.cache_key(prefix, query);

        // Evict oldest if at capacity
        if self.cache.len() >= self.max_entries {
            let mut order = self.order.lock();
            while self.cache.len() >= self.max_entries {
                if let Some(old_key) = order.pop_front() {
                    self.cache.remove(&old_key);
                } else {
                    break;
                }
            }
        }

        // Insert new entry
        let is_new = !self.cache.contains_key(&key);
        self.cache.insert(key.clone(), CacheEntry { merkle_root, keys });

        if is_new {
            let mut order = self.order.lock();
            order.push_back(key);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> SearchCacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        SearchCacheStats {
            hits,
            misses,
            stale: self.stale.load(Ordering::Relaxed),
            entry_count: self.cache.len(),
            hit_rate: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
        }
    }

    /// Clear all cached entries
    pub fn clear(&self) {
        self.cache.clear();
        self.order.lock().clear();
    }

    fn cache_key(&self, prefix: &str, query: &Query) -> CacheKey {
        (prefix.to_string(), Self::hash_query(query))
    }

    fn hash_query(query: &Query) -> u64 {
        // Hash the debug representation as a simple approach
        // Could be more sophisticated with a custom Hash impl
        let mut hasher = DefaultHasher::new();
        format!("{:?}", query.root).hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for SearchCache {
    fn default() -> Self {
        // Default to 1000 cached queries
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit() {
        let cache = SearchCache::new(100);
        let query = Query::field_eq("name", "Alice");
        let merkle_root = vec![1, 2, 3, 4];
        let keys = vec!["crdt:users:1".to_string(), "crdt:users:2".to_string()];

        // Insert
        cache.insert("crdt:users:", &query, merkle_root.clone(), keys.clone());

        // Hit with same merkle root
        let result = cache.get("crdt:users:", &query, &merkle_root);
        assert_eq!(result, Some(keys));

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn test_cache_miss() {
        let cache = SearchCache::new(100);
        let query = Query::field_eq("name", "Alice");
        let merkle_root = vec![1, 2, 3, 4];

        // Miss - not cached
        let result = cache.get("crdt:users:", &query, &merkle_root);
        assert_eq!(result, None);

        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_stale_merkle() {
        let cache = SearchCache::new(100);
        let query = Query::field_eq("name", "Alice");
        let old_root = vec![1, 2, 3, 4];
        let new_root = vec![5, 6, 7, 8];
        let keys = vec!["crdt:users:1".to_string()];

        // Insert with old root
        cache.insert("crdt:users:", &query, old_root, keys);

        // Query with new root - should be stale
        let result = cache.get("crdt:users:", &query, &new_root);
        assert_eq!(result, None);

        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.stale, 1);
    }

    #[test]
    fn test_different_queries() {
        let cache = SearchCache::new(100);
        let query1 = Query::field_eq("name", "Alice");
        let query2 = Query::field_eq("name", "Bob");
        let merkle_root = vec![1, 2, 3, 4];

        cache.insert(
            "crdt:users:",
            &query1,
            merkle_root.clone(),
            vec!["alice".to_string()],
        );
        cache.insert(
            "crdt:users:",
            &query2,
            merkle_root.clone(),
            vec!["bob".to_string()],
        );

        // Both should hit
        assert_eq!(
            cache.get("crdt:users:", &query1, &merkle_root),
            Some(vec!["alice".to_string()])
        );
        assert_eq!(
            cache.get("crdt:users:", &query2, &merkle_root),
            Some(vec!["bob".to_string()])
        );
    }

    #[test]
    fn test_different_prefixes() {
        let cache = SearchCache::new(100);
        let query = Query::field_eq("name", "Alice");
        let root1 = vec![1, 2, 3];
        let root2 = vec![4, 5, 6];

        cache.insert("crdt:users:", &query, root1.clone(), vec!["u1".to_string()]);
        cache.insert("crdt:posts:", &query, root2.clone(), vec!["p1".to_string()]);

        // Each prefix has its own cache entry
        assert_eq!(
            cache.get("crdt:users:", &query, &root1),
            Some(vec!["u1".to_string()])
        );
        assert_eq!(
            cache.get("crdt:posts:", &query, &root2),
            Some(vec!["p1".to_string()])
        );
    }

    #[test]
    fn test_hit_rate() {
        let cache = SearchCache::new(100);
        let query = Query::field_eq("name", "Alice");
        let merkle_root = vec![1, 2, 3, 4];
        let keys = vec!["crdt:users:1".to_string()];

        cache.insert("crdt:users:", &query, merkle_root.clone(), keys);

        // 3 hits, 1 miss
        cache.get("crdt:users:", &query, &merkle_root);
        cache.get("crdt:users:", &query, &merkle_root);
        cache.get("crdt:users:", &query, &merkle_root);
        cache.get("crdt:users:", &Query::field_eq("x", "y"), &merkle_root);

        let stats = cache.stats();
        assert_eq!(stats.hits, 3);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate - 0.75).abs() < 0.01);
    }

    #[test]
    fn test_eviction_oldest() {
        let cache = SearchCache::new(3); // Max 3 entries
        let root = vec![1, 2, 3];

        // Insert 3 entries
        cache.insert("p:", &Query::field_eq("n", "1"), root.clone(), vec!["k1".into()]);
        cache.insert("p:", &Query::field_eq("n", "2"), root.clone(), vec!["k2".into()]);
        cache.insert("p:", &Query::field_eq("n", "3"), root.clone(), vec!["k3".into()]);

        assert_eq!(cache.stats().entry_count, 3);

        // Insert 4th - should evict oldest (query 1)
        cache.insert("p:", &Query::field_eq("n", "4"), root.clone(), vec!["k4".into()]);

        assert_eq!(cache.stats().entry_count, 3);

        // Query 1 should be evicted
        assert!(cache.get("p:", &Query::field_eq("n", "1"), &root).is_none());
        // Query 4 should exist
        assert!(cache.get("p:", &Query::field_eq("n", "4"), &root).is_some());
    }
}
