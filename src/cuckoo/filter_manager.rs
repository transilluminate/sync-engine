// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Cuckoo filter manager for L3 existence checks.
//!
//! Uses our expandable-cuckoo-filter for memory-efficient probabilistic
//! membership testing with persistence support. This prevents unnecessary 
//! L3 queries for keys that definitely don't exist.

use expandable_cuckoo_filter::ExpandableCuckooFilter;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{debug, info, warn};

/// Trust state of the cuckoo filter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterTrust {
    /// Filter is empty/warming up - always check L3
    Untrusted,
    /// Filter is warmed and verified - trust "definitely not" responses
    Trusted,
}

/// Cuckoo filter manager for L3 existence checks.
///
/// The filter tracks which keys exist in L3 (SQL). On cache miss:
/// - If filter says "definitely not" AND trusted → return 404 immediately
/// - If filter says "maybe" → check L3
/// - If untrusted → always check L3 (degraded mode)
///
/// The filter must be warmed from L3 on startup and verified via merkle root
/// comparison before being marked trusted.
///
/// ## Persistence
/// 
/// The filter supports export/import for fast startup:
/// - On shutdown: `export()` → save bytes to SQLite
/// - On startup: load bytes → `import()` → `mark_trusted()` → ready instantly
pub struct FilterManager {
    /// Expandable cuckoo filter - auto-grows, supports export/import
    filter: ExpandableCuckooFilter,
    /// Trust state - only trust "not found" if true
    trust_state: AtomicBool,
}

impl FilterManager {
    /// Create a new filter manager (starts untrusted).
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for deterministic seeding (e.g., hostname)
    /// * `initial_capacity` - Expected number of items (filter will grow if exceeded)
    #[must_use]
    pub fn new(node_id: impl Into<String>, initial_capacity: usize) -> Self {
        let node_id = node_id.into();
        info!(
            node_id = %node_id,
            initial_capacity,
            "Creating expandable cuckoo filter manager"
        );

        let filter = ExpandableCuckooFilter::new(node_id, initial_capacity);

        Self {
            filter,
            trust_state: AtomicBool::new(false),
        }
    }

    /// Get current trust state.
    #[must_use]
    #[inline]
    pub fn trust_state(&self) -> FilterTrust {
        if self.trust_state.load(Ordering::Acquire) {
            FilterTrust::Trusted
        } else {
            FilterTrust::Untrusted
        }
    }

    /// Check if filter is trusted (convenience method).
    #[must_use]
    #[inline]
    pub fn is_trusted(&self) -> bool {
        self.trust_state.load(Ordering::Acquire)
    }

    /// Mark filter as trusted (after warmup + verification).
    pub fn mark_trusted(&self) {
        info!(
            entries = self.filter.len(),
            "Marking cuckoo filter as trusted"
        );
        self.trust_state.store(true, Ordering::Release);
    }

    /// Mark filter as untrusted (e.g., after detecting inconsistency).
    pub fn mark_untrusted(&self) {
        warn!("Marking cuckoo filter as untrusted");
        self.trust_state.store(false, Ordering::Release);
    }

    /// Check if a key might exist in L3.
    ///
    /// Returns:
    /// - `Some(true)` → key might exist, check L3
    /// - `Some(false)` → key definitely doesn't exist (trusted mode only)
    /// - `None` → filter is untrusted, must check L3
    #[must_use]
    pub fn might_exist(&self, key: &str) -> Option<bool> {
        if !self.trust_state.load(Ordering::Acquire) {
            // Untrusted - caller must check L3
            return None;
        }

        Some(self.filter.contains(key))
    }

    /// Check if we should query L3 for this key.
    #[must_use]
    #[inline]
    pub fn should_check_l3(&self, key: &str) -> bool {
        match self.might_exist(key) {
            None => true,         // Untrusted, must check
            Some(true) => true,   // Might exist, check
            Some(false) => false, // Definitely not, skip L3
        }
    }

    /// Insert a key into the filter (call when item is persisted to L3).
    pub fn insert(&self, key: &str) {
        self.filter.insert(key);
    }

    /// Remove a key from the filter (call when item is deleted from L3).
    pub fn remove(&self, key: &str) -> bool {
        self.filter.remove(key)
    }

    /// Bulk insert keys during warmup.
    ///
    /// More efficient than individual inserts for large batches.
    pub fn bulk_insert(&self, keys: &[String]) {
        for key in keys {
            self.filter.insert(key.as_str());
        }
        debug!(
            added = keys.len(),
            total = self.filter.len(),
            "Bulk inserted keys into cuckoo filter"
        );
    }

    /// Export filter state to bytes for persistence.
    ///
    /// Save these bytes to SQLite on shutdown for fast startup.
    #[must_use]
    pub fn export(&self) -> Option<Vec<u8>> {
        self.filter.export().ok()
    }

    /// Import filter state from bytes.
    ///
    /// Load bytes from SQLite on startup, then call `mark_trusted()`.
    pub fn import(&self, data: &[u8]) -> Result<(), String> {
        self.filter.import(data).map_err(|e| e.to_string())
    }

    /// Get current entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.filter.len()
    }

    /// Check if filter is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.filter.is_empty()
    }

    /// Get filter stats: (entry_count, capacity, trust_state).
    #[must_use]
    pub fn stats(&self) -> (usize, usize, FilterTrust) {
        (
            self.filter.len(),
            self.filter.capacity(),
            self.trust_state(),
        )
    }

    /// Estimate memory usage in bytes.
    #[must_use]
    pub fn memory_usage_bytes(&self) -> usize {
        // ~8 bytes per fingerprint slot + overhead
        self.filter.capacity().saturating_mul(8).max(1024)
    }

    /// Get the node ID used for deterministic seeding.
    #[must_use]
    pub fn node_id(&self) -> &str {
        self.filter.node_id()
    }

    /// Get the deterministic seed.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.filter.seed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_filter_is_untrusted() {
        let fm = FilterManager::new("test-node", 1000);
        assert_eq!(fm.trust_state(), FilterTrust::Untrusted);
        assert!(fm.is_empty());
        assert_eq!(fm.len(), 0);
    }

    #[test]
    fn test_mark_trusted_and_untrusted() {
        let fm = FilterManager::new("test-node", 1000);
        
        assert_eq!(fm.trust_state(), FilterTrust::Untrusted);
        
        fm.mark_trusted();
        assert_eq!(fm.trust_state(), FilterTrust::Trusted);
        
        fm.mark_untrusted();
        assert_eq!(fm.trust_state(), FilterTrust::Untrusted);
    }

    #[test]
    fn test_insert_and_contains() {
        let fm = FilterManager::new("test-node", 1000);
        fm.mark_trusted();
        
        // Insert key
        fm.insert("key-1");
        assert_eq!(fm.len(), 1);
        
        // Should contain it
        assert_eq!(fm.might_exist("key-1"), Some(true));
        
        // Should not contain other key
        assert_eq!(fm.might_exist("key-2"), Some(false));
    }

    #[test]
    fn test_might_exist_returns_none_when_untrusted() {
        let fm = FilterManager::new("test-node", 1000);
        
        fm.insert("key-1");
        
        // Untrusted, should return None
        assert_eq!(fm.might_exist("key-1"), None);
        assert_eq!(fm.might_exist("key-2"), None);
    }

    #[test]
    fn test_should_check_l3_when_untrusted() {
        let fm = FilterManager::new("test-node", 1000);
        
        // Untrusted = always check L3
        assert!(fm.should_check_l3("any-key"));
        assert!(fm.should_check_l3("another-key"));
    }

    #[test]
    fn test_should_check_l3_when_trusted() {
        let fm = FilterManager::new("test-node", 1000);
        fm.mark_trusted();
        
        fm.insert("exists");
        
        // Key that might exist = check L3
        assert!(fm.should_check_l3("exists"));
        
        // Key that definitely doesn't = don't check L3
        assert!(!fm.should_check_l3("nonexistent"));
    }

    #[test]
    fn test_remove() {
        let fm = FilterManager::new("test-node", 1000);
        fm.mark_trusted();
        
        fm.insert("to-remove");
        assert_eq!(fm.might_exist("to-remove"), Some(true));
        
        let removed = fm.remove("to-remove");
        // Note: cuckoo filter remove may return false if fingerprint collision
        // but the key should no longer be in the filter
        assert!(removed || fm.might_exist("to-remove") == Some(false));
    }

    #[test]
    fn test_bulk_insert() {
        let fm = FilterManager::new("test-node", 1000);
        fm.mark_trusted();
        
        let keys: Vec<String> = (0..100).map(|i| format!("bulk-key-{}", i)).collect();
        fm.bulk_insert(&keys);
        
        assert_eq!(fm.len(), 100);
        
        // Spot check some keys
        assert_eq!(fm.might_exist("bulk-key-0"), Some(true));
        assert_eq!(fm.might_exist("bulk-key-50"), Some(true));
        assert_eq!(fm.might_exist("bulk-key-99"), Some(true));
        assert_eq!(fm.might_exist("not-inserted"), Some(false));
    }

    #[test]
    fn test_export_import() {
        let fm1 = FilterManager::new("test-node", 1000);
        
        // Insert some keys
        fm1.insert("key-a");
        fm1.insert("key-b");
        fm1.insert("key-c");
        
        // Export
        let exported = fm1.export();
        assert!(exported.is_some());
        
        // Create new filter and import
        let fm2 = FilterManager::new("test-node", 1000);
        let import_result = fm2.import(&exported.unwrap());
        assert!(import_result.is_ok());
        
        // Should have same entries
        assert_eq!(fm2.len(), 3);
        
        // Mark trusted and verify
        fm2.mark_trusted();
        assert_eq!(fm2.might_exist("key-a"), Some(true));
        assert_eq!(fm2.might_exist("key-b"), Some(true));
        assert_eq!(fm2.might_exist("key-c"), Some(true));
    }

    #[test]
    fn test_stats() {
        let fm = FilterManager::new("test-node", 1000);
        
        let (count, capacity, trust) = fm.stats();
        assert_eq!(count, 0);
        assert!(capacity > 0);
        assert_eq!(trust, FilterTrust::Untrusted);
        
        fm.insert("key");
        fm.mark_trusted();
        
        let (count, _, trust) = fm.stats();
        assert_eq!(count, 1);
        assert_eq!(trust, FilterTrust::Trusted);
    }

    #[test]
    fn test_memory_usage_bytes() {
        let fm = FilterManager::new("test-node", 10000);
        
        let mem = fm.memory_usage_bytes();
        assert!(mem >= 1024, "Should report at least 1KB");
        assert!(mem < 1024 * 1024, "Should be less than 1MB for 10k capacity");
    }

    #[test]
    fn test_node_id_and_seed() {
        let fm = FilterManager::new("my-unique-node", 1000);
        
        assert_eq!(fm.node_id(), "my-unique-node");
        assert!(fm.seed() > 0);
    }

    #[test]
    fn test_deterministic_seed_same_node() {
        let fm1 = FilterManager::new("same-node", 1000);
        let fm2 = FilterManager::new("same-node", 1000);
        
        assert_eq!(fm1.seed(), fm2.seed());
    }

    #[test]
    fn test_different_seeds_different_nodes() {
        let fm1 = FilterManager::new("node-a", 1000);
        let fm2 = FilterManager::new("node-b", 1000);
        
        assert_ne!(fm1.seed(), fm2.seed());
    }

    #[test]
    fn test_filter_grows_beyond_initial_capacity() {
        let fm = FilterManager::new("test-node", 10); // Very small
        
        // Insert more than initial capacity
        for i in 0..100 {
            fm.insert(&format!("key-{}", i));
        }
        
        // Should have grown
        assert_eq!(fm.len(), 100);
        
        // Capacity should be >= entries
        let (count, capacity, _) = fm.stats();
        assert!(capacity >= count);
    }

    #[test]
    fn test_is_empty() {
        let fm = FilterManager::new("test-node", 1000);
        
        assert!(fm.is_empty());
        
        fm.insert("key");
        assert!(!fm.is_empty());
    }

    #[test]
    fn test_filter_trust_display() {
        assert_eq!(format!("{:?}", FilterTrust::Untrusted), "Untrusted");
        assert_eq!(format!("{:?}", FilterTrust::Trusted), "Trusted");
    }

    #[test]
    fn test_multiple_inserts_same_key() {
        let fm = FilterManager::new("test-node", 1000);
        
        fm.insert("same-key");
        fm.insert("same-key");
        fm.insert("same-key");
        
        // Cuckoo filters may or may not deduplicate, 
        // but should at least contain the key
        fm.mark_trusted();
        assert_eq!(fm.might_exist("same-key"), Some(true));
    }

    #[test]
    fn test_false_positive_rate_reasonable() {
        let fm = FilterManager::new("test-node", 10000);
        fm.mark_trusted();
        
        // Insert 1000 keys
        for i in 0..1000 {
            fm.insert(&format!("inserted-{}", i));
        }
        
        // Check 1000 keys that were NOT inserted
        let mut false_positives = 0;
        for i in 0..1000 {
            if fm.might_exist(&format!("not-inserted-{}", i)) == Some(true) {
                false_positives += 1;
            }
        }
        
        // False positive rate should be < 5% for cuckoo filters
        let fp_rate = false_positives as f64 / 1000.0;
        assert!(fp_rate < 0.05, "False positive rate {} is too high", fp_rate);
    }
}
