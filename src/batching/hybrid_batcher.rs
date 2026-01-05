// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Hybrid batching for efficient writes.
//!
//! The [`HybridBatcher`] collects items and flushes them in batches based on
//! configurable thresholds: item count, total bytes, or elapsed time.
//!
//! # Example
//!
//! ```
//! use sync_engine::{HybridBatcher, BatchConfig, SizedItem};
//!
//! #[derive(Clone)]
//! struct Item { data: String }
//! impl SizedItem for Item {
//!     fn size_bytes(&self) -> usize { self.data.len() }
//! }
//!
//! let config = BatchConfig {
//!     flush_ms: 100,
//!     flush_count: 10,
//!     flush_bytes: 1024,
//! };
//!
//! let mut batcher: HybridBatcher<Item> = HybridBatcher::new(config);
//! assert!(batcher.is_empty());
//!
//! batcher.add(Item { data: "hello".into() });
//! assert!(!batcher.is_empty());
//! ```

use std::time::{Duration, Instant};
use tracing::debug;

/// Batch flush trigger reason
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// Time threshold reached
    Time,
    /// Item count threshold reached
    Count,
    /// Byte size threshold reached
    Size,
    /// Manual flush requested
    Manual,
    /// Shutdown flush
    Shutdown,
    /// View queue flush
    ViewQueue,
}

/// Configuration for hybrid batching
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Flush after this many milliseconds (even if batch is small)
    pub flush_ms: u64,
    /// Flush after this many items
    pub flush_count: usize,
    /// Flush after this many bytes
    pub flush_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            flush_ms: 100,
            flush_count: 1000,
            flush_bytes: 1024 * 1024, // 1 MB
        }
    }
}

/// A batch of items ready for flush
#[derive(Debug)]
pub struct FlushBatch<T> {
    pub items: Vec<T>,
    pub total_bytes: usize,
    pub reason: FlushReason,
}

/// A batch of items pending flush
#[derive(Debug)]
pub struct Batch<T> {
    pub items: Vec<T>,
    pub total_bytes: usize,
    pub created_at: Instant,
}

impl<T> Batch<T> {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            total_bytes: 0,
            created_at: Instant::now(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn push(&mut self, item: T, size_bytes: usize) {
        self.items.push(item);
        self.total_bytes += size_bytes;
    }

    pub fn take(&mut self) -> Vec<T> {
        self.total_bytes = 0;
        self.created_at = Instant::now();
        std::mem::take(&mut self.items)
    }
}

impl<T> Default for Batch<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Hybrid batcher that flushes based on time, count, or size thresholds.
/// Whichever threshold is hit first triggers the flush.
pub struct HybridBatcher<T> {
    config: BatchConfig,
    batch: Batch<T>,
}

impl<T> HybridBatcher<T> {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            batch: Batch::new(),
        }
    }

    /// Add an item to the batch, returns flush reason if threshold hit
    pub fn push(&mut self, item: T, size_bytes: usize) -> Option<FlushReason> {
        self.batch.push(item, size_bytes);

        // Check thresholds in priority order
        if self.batch.len() >= self.config.flush_count {
            Some(FlushReason::Count)
        } else if self.batch.total_bytes >= self.config.flush_bytes {
            Some(FlushReason::Size)
        } else {
            None
        }
    }

    /// Check if time threshold exceeded
    #[must_use]
    pub fn should_flush_time(&self) -> bool {
        !self.batch.is_empty() 
            && self.batch.age() >= Duration::from_millis(self.config.flush_ms)
    }

    /// Take the current batch for flushing
    pub fn take_batch(&mut self) -> Vec<T> {
        let count = self.batch.len();
        let bytes = self.batch.total_bytes;
        let items = self.batch.take();
        debug!(count, bytes, "Batch taken for flush");
        items
    }

    /// Check if batch is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Get current batch stats
    #[must_use]
    pub fn stats(&self) -> (usize, usize, Duration) {
        (self.batch.len(), self.batch.total_bytes, self.batch.age())
    }
}

/// Extension methods for SyncItem batching (avoids orphan rules)
impl<T: SizedItem> HybridBatcher<T> {
    /// Add an item using its intrinsic size
    pub fn add(&mut self, item: T) -> Option<FlushReason> {
        let size = item.size_bytes();
        self.push(item, size)
    }
    
    /// Add multiple items at once
    pub fn add_batch(&mut self, items: Vec<T>) {
        for item in items {
            self.add(item);
        }
    }
    
    /// Take the batch if any threshold is ready
    pub fn take_if_ready(&mut self) -> Option<FlushBatch<T>> {
        // Check count/size threshold
        let reason = if self.batch.len() >= self.config.flush_count {
            Some(FlushReason::Count)
        } else if self.batch.total_bytes >= self.config.flush_bytes {
            Some(FlushReason::Size)
        } else if self.should_flush_time() {
            Some(FlushReason::Time)
        } else {
            None
        };
        
        reason.map(|r| {
            // Capture bytes BEFORE take() resets it
            let total_bytes = self.batch.total_bytes;
            FlushBatch {
                items: self.batch.take(),
                total_bytes,
                reason: r,
            }
        })
    }
    
    /// Force flush regardless of thresholds (for manual flush or shutdown)
    pub fn force_flush(&mut self) -> Option<FlushBatch<T>> {
        self.force_flush_with_reason(FlushReason::Manual)
    }
    
    /// Force flush with a specific reason
    pub fn force_flush_with_reason(&mut self, reason: FlushReason) -> Option<FlushBatch<T>> {
        if self.batch.is_empty() {
            return None;
        }
        // Capture bytes BEFORE take() resets it
        let total_bytes = self.batch.total_bytes;
        Some(FlushBatch {
            items: self.batch.take(),
            total_bytes,
            reason,
        })
    }
}

/// Extension methods for batchable items (have ID for contains check)
impl<T: BatchableItem> HybridBatcher<T> {
    /// Check if an item with the given ID is in the pending batch.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.batch.items.iter().any(|item| item.id() == id)
    }
}

/// Trait for items that know their own size
pub trait SizedItem {
    #[must_use]
    fn size_bytes(&self) -> usize;
}

/// Trait for items that have an ID (for contains check)
pub trait BatchableItem: SizedItem {
    fn id(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    // Simple test item
    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    struct TestItem {
        id: String,
        size: usize,
    }

    impl SizedItem for TestItem {
        fn size_bytes(&self) -> usize {
            self.size
        }
    }

    fn item(id: &str, size: usize) -> TestItem {
        TestItem { id: id.to_string(), size }
    }

    #[test]
    fn test_batch_empty_initially() {
        let batcher: HybridBatcher<TestItem> = HybridBatcher::new(BatchConfig::default());
        assert!(batcher.is_empty());
        let (count, bytes, _) = batcher.stats();
        assert_eq!(count, 0);
        assert_eq!(bytes, 0);
    }

    #[test]
    fn test_batch_tracks_items_and_bytes() {
        let mut batcher = HybridBatcher::new(BatchConfig::default());
        
        batcher.add(item("a", 100));
        batcher.add(item("b", 200));
        batcher.add(item("c", 150));
        
        let (count, bytes, _) = batcher.stats();
        assert_eq!(count, 3);
        assert_eq!(bytes, 450);
        assert!(!batcher.is_empty());
    }

    #[test]
    fn test_flush_on_count_threshold() {
        let config = BatchConfig {
            flush_count: 3,
            flush_bytes: 1_000_000,
            flush_ms: 10_000,
        };
        let mut batcher = HybridBatcher::new(config);
        
        // First two don't trigger
        assert!(batcher.add(item("a", 100)).is_none());
        assert!(batcher.add(item("b", 100)).is_none());
        
        // Third triggers count threshold
        let reason = batcher.add(item("c", 100));
        assert_eq!(reason, Some(FlushReason::Count));
    }

    #[test]
    fn test_flush_on_size_threshold() {
        let config = BatchConfig {
            flush_count: 1000,
            flush_bytes: 500,
            flush_ms: 10_000,
        };
        let mut batcher = HybridBatcher::new(config);
        
        // First doesn't trigger
        assert!(batcher.add(item("a", 200)).is_none());
        assert!(batcher.add(item("b", 200)).is_none());
        
        // This pushes over 500 bytes
        let reason = batcher.add(item("c", 200));
        assert_eq!(reason, Some(FlushReason::Size));
    }

    #[test]
    fn test_flush_on_time_threshold() {
        let config = BatchConfig {
            flush_count: 1000,
            flush_bytes: 1_000_000,
            flush_ms: 10, // 10ms
        };
        let mut batcher = HybridBatcher::new(config);
        
        batcher.add(item("a", 100));
        
        // Not ready yet
        assert!(!batcher.should_flush_time());
        
        // Wait for threshold
        sleep(Duration::from_millis(15));
        
        // Now ready
        assert!(batcher.should_flush_time());
    }

    #[test]
    fn test_take_if_ready_returns_batch() {
        let config = BatchConfig {
            flush_count: 2,
            flush_bytes: 1_000_000,
            flush_ms: 10_000,
        };
        let mut batcher = HybridBatcher::new(config);
        
        batcher.add(item("a", 100));
        
        // Not ready yet
        assert!(batcher.take_if_ready().is_none());
        
        batcher.add(item("b", 200));
        
        // Now ready (count threshold)
        let batch = batcher.take_if_ready().unwrap();
        assert_eq!(batch.items.len(), 2);
        assert_eq!(batch.total_bytes, 300);
        assert_eq!(batch.reason, FlushReason::Count);
        
        // Batcher should be empty now
        assert!(batcher.is_empty());
    }

    #[test]
    fn test_force_flush() {
        let mut batcher = HybridBatcher::new(BatchConfig::default());
        
        batcher.add(item("a", 100));
        batcher.add(item("b", 200));
        
        // Force flush regardless of thresholds
        let batch = batcher.force_flush().unwrap();
        assert_eq!(batch.items.len(), 2);
        assert_eq!(batch.total_bytes, 300);
        assert_eq!(batch.reason, FlushReason::Manual);
        
        // Should be empty now
        assert!(batcher.is_empty());
        
        // Force flush on empty returns None
        assert!(batcher.force_flush().is_none());
        
        // Test force_flush_with_reason
        batcher.add(item("c", 100));
        let batch = batcher.force_flush_with_reason(FlushReason::Shutdown).unwrap();
        assert_eq!(batch.reason, FlushReason::Shutdown);
    }

    #[test]
    fn test_take_resets_batch() {
        let mut batcher = HybridBatcher::new(BatchConfig::default());
        
        batcher.add(item("a", 100));
        batcher.add(item("b", 200));
        
        let items = batcher.take_batch();
        assert_eq!(items.len(), 2);
        
        // Stats should be reset
        let (count, bytes, age) = batcher.stats();
        assert_eq!(count, 0);
        assert_eq!(bytes, 0);
        assert!(age < Duration::from_millis(10)); // Timer reset
    }

    #[test]
    fn test_add_batch() {
        let mut batcher = HybridBatcher::new(BatchConfig::default());
        
        let items = vec![item("a", 100), item("b", 200), item("c", 300)];
        batcher.add_batch(items);
        
        let (count, bytes, _) = batcher.stats();
        assert_eq!(count, 3);
        assert_eq!(bytes, 600);
    }

    #[test]
    fn test_count_beats_size_on_simultaneous_threshold() {
        let config = BatchConfig {
            flush_count: 2,
            flush_bytes: 200,
            flush_ms: 10_000,
        };
        let mut batcher = HybridBatcher::new(config);
        
        batcher.add(item("a", 100));
        
        // This hits both count (2) AND size (200) thresholds
        let reason = batcher.add(item("b", 100));
        
        // Count should win (checked first)
        assert_eq!(reason, Some(FlushReason::Count));
    }
}
