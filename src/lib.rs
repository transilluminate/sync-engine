//! # Sync Engine
//!
//! A high-performance, tiered sync engine for distributed data synchronization.
//!
//! ## Architecture
//!
//! The sync engine uses a three-tier architecture optimized for read-heavy workloads:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        Ingest Layer                         │
//! │  • Accepts SyncItems via submit()                          │
//! │  • Backpressure control based on memory usage              │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    L1: In-Memory Cache                      │
//! │  • DashMap for concurrent access                           │
//! │  • Tan-curve eviction under memory pressure                │
//! │  • Cuckoo filter for existence checks                      │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                    (Batch flush via HybridBatcher)
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     L2: Redis Cache                         │
//! │  • Pipelined batch writes for throughput                   │
//! │  • Merkle tree shadow for sync verification                │
//! │  • Cuckoo filter to skip network hops on miss              │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                    (Batch persist to ground truth)
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   L3: MySQL/SQLite Archive                  │
//! │  • Ground truth storage                                    │
//! │  • Merkle tree for sync verification                       │
//! │  • WAL fallback during outages                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use sync_engine::{SyncEngine, SyncEngineConfig, SyncItem};
//! use serde_json::json;
//! use tokio::sync::watch;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = SyncEngineConfig {
//!         redis_url: Some("redis://localhost:6379".into()),
//!         sql_url: Some("mysql://user:pass@localhost/db".into()),
//!         ..Default::default()
//!     };
//!
//!     let (_tx, rx) = watch::channel(config.clone());
//!     let mut engine = SyncEngine::new(config, rx);
//!     
//!     // Start the engine (connects to backends)
//!     engine.start().await.expect("Failed to start");
//!
//!     // Submit items for sync
//!     let item = SyncItem::new(
//!         "uk.nhs.patient.record.12345".into(),
//!         json!({"name": "John Doe", "nhs_number": "1234567890"})
//!     );
//!     engine.submit(item).await.expect("Failed to submit");
//!
//!     // Retrieve items (L1 → L2 → L3 fallback)
//!     if let Some(item) = engine.get("uk.nhs.patient.record.12345").await.unwrap() {
//!         println!("Found: {:?}", item.content);
//!     }
//!
//!     engine.shutdown().await;
//! }
//! ```
//!
//! ## Features
//!
//! - **Tiered Caching**: L1 (memory) → L2 (Redis) → L3 (MySQL) with automatic fallback
//! - **Batch Writes**: Configurable flush by count, size, or time
//! - **Cuckoo Filters**: Skip network hops when data definitely doesn't exist
//! - **Merkle Trees**: Efficient sync verification between Redis and MySQL
//! - **WAL Durability**: Local SQLite WAL during MySQL outages
//! - **Backpressure**: Graceful degradation under memory pressure
//! - **Circuit Breakers**: Prevent cascade failures to backends
//! - **Retry Logic**: Configurable retry policies for transient failures
//!
//! ## Configuration
//!
//! See [`SyncEngineConfig`] for all configuration options.
//!
//! ## Modules
//!
//! - [`coordinator`]: The main [`SyncEngine`] orchestrating all components
//! - [`storage`]: Storage backends (Redis, SQL, Memory)
//! - [`batching`]: Hybrid batcher for efficient writes
//! - [`cuckoo`]: Probabilistic existence filters
//! - [`merkle`]: Merkle tree for sync verification
//! - [`resilience`]: Circuit breakers, retry logic, WAL
//! - [`eviction`]: Tan-curve eviction policy
//! - [`backpressure`]: Memory pressure handling

pub mod config;
pub mod sync_item;
pub mod storage;
pub mod batching;
pub mod resilience;
pub mod eviction;
pub mod priority;
pub mod cuckoo;
pub mod merkle;
pub mod backpressure;
pub mod coordinator;
pub mod metrics;

// Note: We don't expose a `tracing` module to avoid conflict with the tracing crate

pub use config::SyncEngineConfig;
pub use coordinator::{SyncEngine, EngineState, ItemStatus, BatchResult};
pub use backpressure::BackpressureLevel;
pub use sync_item::{SyncItem, CrdtSnapshot};
pub use storage::traits::{CacheStore, ArchiveStore, StorageError};
pub use cuckoo::filter_manager::{FilterManager, FilterTrust};
pub use batching::hybrid_batcher::{HybridBatcher, BatchConfig, FlushReason, Batch, FlushBatch, SizedItem, BatchableItem};
pub use merkle::{PathMerkle, MerkleBatch, MerkleNode, RedisMerkleStore};
pub use resilience::wal::{WriteAheadLog, MysqlHealthChecker, WalStats};
pub use resilience::circuit_breaker::{CircuitBreaker, CircuitConfig, CircuitError, BackendCircuits};
pub use resilience::retry::RetryConfig;
pub use metrics::LatencyTimer;
