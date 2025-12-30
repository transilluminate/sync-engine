//! Eviction policies for tiered cache management.
//!
//! This module contains eviction logic for both L1 (in-memory) and L2 (Redis)
//! caches, using a consistent tan-curve scoring algorithm.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                    Eviction Module                           │
//! ├──────────────────────────────────────────────────────────────┤
//! │  tan_curve.rs   - Core scoring algorithm                     │
//! │  └─ TanCurveMemoryPressure: pressure → multiplier            │
//! │  └─ TanCurvePolicy: recency + frequency + size → score       │
//! │  └─ CacheEntry: generic entry metadata for scoring           │
//! ├──────────────────────────────────────────────────────────────┤
//! │  redis.rs       - Redis-specific eviction                    │
//! │  └─ RedisEvictionManager: proactive eviction before LRU      │
//! │  └─ RedisMemoryProfile: cached INFO MEMORY to avoid RTT      │
//! │  └─ Protected prefixes: merkle:*, idx:*                      │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # L1 Memory Eviction
//!
//! L1 eviction is handled directly in `coordinator/mod.rs` using `TanCurvePolicy`.
//! The coordinator builds `CacheEntry` from `SyncItem` and calls `select_victims()`.
//!
//! # L2 Redis Eviction
//!
//! Redis eviction is proactive - we evict before Redis LRU kicks in to protect
//! infrastructure keys (merkle trees, RediSearch indexes). The `RedisEvictionManager`
//! tracks key metadata and uses `TanCurvePolicy` for consistent scoring.

pub mod tan_curve;
pub mod redis;