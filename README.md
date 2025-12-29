# Sync Engine

A high-performance, tiered synchronization engine for distributed data systems.

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)

## Overview

Sync Engine provides a three-tier caching and persistence architecture optimized for read-heavy workloads with strong consistency guarantees:

```
┌─────────────────────────────────────────────────────────────┐
│                        Ingest Layer                         │
│  • Accepts SyncItems via submit()                          │
│  • Backpressure control based on memory usage              │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                    L1: In-Memory Cache                      │
│  • DashMap for concurrent access                           │
│  • Tan-curve eviction under memory pressure                │
│  • Cuckoo filter for existence checks                      │
└─────────────────────────────────────────────────────────────┘
                             │
                   (Batch flush via HybridBatcher)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                     L2: Redis Cache                         │
│  • Pipelined batch writes for throughput                   │
│  • Merkle tree shadow for sync verification                │
│  • Cuckoo filter to skip network hops on miss              │
└─────────────────────────────────────────────────────────────┘
                             │
                   (Batch persist to ground truth)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   L3: MySQL/SQLite Archive                  │
│  • Ground truth storage                                    │
│  • Merkle tree for sync verification                       │
│  • WAL fallback during outages                             │
└─────────────────────────────────────────────────────────────┘
```

## Features

- **Tiered Caching**: L1 (memory) → L2 (Redis) → L3 (MySQL) with automatic fallback
- **Batch Writes**: Configurable flush by count, size, or time for throughput
- **Cuckoo Filters**: Probabilistic existence checks to skip unnecessary network hops
- **Merkle Trees**: Efficient sync verification between tiers
- **WAL Durability**: Local SQLite WAL ensures no data loss during MySQL outages
- **Backpressure**: Six-tier graceful degradation under memory pressure
- **Circuit Breakers**: Prevent cascade failures to unhealthy backends
- **Retry Logic**: Configurable exponential backoff for transient failures
- **CRDT Support**: Optional CRDT-aware compaction with `crdt-data-types` integration
- **Compression**: Optional transparent zstd compression for L3 storage

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
sync-engine = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

### Feature Flags

```toml
[dependencies]
sync-engine = { version = "0.1", features = ["compression", "crdt"] }
```

| Feature | Description |
|---------|-------------|
| `compression` | Transparent zstd compression for L3 storage (~70% savings) |
| `crdt` | CRDT-aware compaction using `crdt-data-types` crate |
| `otel` | OpenTelemetry trace context propagation |

Basic usage:

```rust
use sync_engine::{SyncEngine, SyncEngineConfig, SyncItem};
use serde_json::json;
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    let config = SyncEngineConfig {
        redis_url: Some("redis://localhost:6379".into()),
        sql_url: Some("mysql://user:pass@localhost/db".into()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    // Start the engine (connects to backends)
    engine.start().await.expect("Failed to start");

    // Submit items for sync
    let item = SyncItem::new(
        "uk.nhs.patient.record.12345".into(),
        json!({"name": "John Doe", "nhs_number": "1234567890"})
    );
    engine.submit(item).await.expect("Failed to submit");

    // Retrieve items (L1 → L2 → L3 fallback)
    if let Some(item) = engine.get("uk.nhs.patient.record.12345").await.unwrap() {
        println!("Found: {:?}", item.content);
    }

    engine.shutdown().await;
}
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `redis_url` | None | Redis connection string |
| `sql_url` | None | MySQL/SQLite connection string |
| `l1_max_bytes` | 256 MB | Maximum L1 cache size |
| `batch_flush_ms` | 100 | Flush batch after N milliseconds |
| `batch_flush_count` | 1000 | Flush batch after N items |
| `batch_flush_bytes` | 1 MB | Flush batch after N bytes |
| `wal_path` | None | SQLite WAL path for durability |
| `backpressure_warn` | 0.7 | Memory pressure warning threshold |
| `backpressure_critical` | 0.9 | Memory pressure critical threshold |

See [`SyncEngineConfig`](src/config.rs) for all options.

## Architecture

### Engine Lifecycle

```
Created → Connecting → DrainingWal → SyncingRedis → WarmingUp → Ready → Running → ShuttingDown
```

### Backpressure Levels

| Level | Threshold | Behavior |
|-------|-----------|----------|
| Normal | < 70% | Accept all operations |
| Warn | 70-80% | Evict aggressively, emit warnings |
| Throttle | 80-90% | Rate limit writes (HTTP 429) |
| Critical | 90-95% | Reject writes, reads only (HTTP 503) |
| Emergency | 95-98% | Read-only mode, prepare shutdown |
| Shutdown | > 98% | Graceful shutdown initiated |

### Retry Strategies

| Strategy | Max Retries | Use Case |
|----------|-------------|----------|
| `startup()` | 5 (~5s) | Initial connection, fail fast on bad config |
| `daemon()` | ∞ | Runtime reconnection, never give up |
| `query()` | 3 (~1s) | Individual operations, fail fast |

## Testing

```bash
# Run unit tests (fast, no Docker required)
cargo test --lib

# Run integration tests (requires Docker)
cargo test --test integration -- --ignored

# Run all tests with coverage
cargo llvm-cov --all-targets -- --include-ignored
```

Current coverage: **77%** with 164 tests (151 lib + 13 doc).

## Development

```bash
# Check for issues
cargo clippy --all-targets

# Format code
cargo fmt

# Generate documentation
cargo doc --open

# Run benchmarks (if available)
cargo bench
```

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0).

This means:
- ✅ You can use, modify, and distribute this software
- ✅ You must keep the source code open
- ✅ Network use (e.g., SaaS) requires sharing source code
- ✅ Derivative works must use the same license

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
