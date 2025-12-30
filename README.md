# Sync Engine

High-performance tiered sync engine with L1/L2/L3 caching and Redis/SQL backends

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)

## Philosophy: Dumb Byte Router

sync-engine stores `Vec<u8>` and routes to L1/L2/L3 based on caller-provided options.
Compression, serialization, and data interpretation are the caller's responsibility.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Ingest Layer                         │
│  • Accepts SyncItems via submit() / submit_with()          │
│  • Backpressure control based on memory usage              │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                    L1: In-Memory Cache                      │
│  • Moka cache for concurrent access                        │
│  • Tan-curve eviction under memory pressure                │
└─────────────────────────────────────────────────────────────┘
                             │
                   (Batch flush via HybridBatcher)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                     L2: Redis Cache                         │
│  • Pipelined batch writes for throughput                   │
│  • Optional per-item TTL                                   │
│  • EXISTS command for fast existence checks                │
└─────────────────────────────────────────────────────────────┘
                             │
                   (Batch persist to ground truth)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   L3: MySQL/SQLite Archive                  │
│  • Ground truth storage (BLOB column)                      │
│  • Cuckoo filter for fast existence checks                 │
│  • WAL fallback during outages                             │
└─────────────────────────────────────────────────────────────┘
```

## Features

- **Tiered Caching**: L1 (memory) → L2 (Redis) → L3 (SQL) with automatic fallback
- **Binary Storage**: Store raw `Vec<u8>` - caller handles serialization/compression
- **Flexible Routing**: `SubmitOptions` controls which tiers receive data
- **TTL Support**: Per-item TTL for Redis cache entries
- **Batch Writes**: Configurable flush by count, size, or time
- **Cuckoo Filters**: Skip SQL queries when data definitely doesn't exist
- **WAL Durability**: Local SQLite WAL during MySQL outages
- **Backpressure**: Graceful degradation under memory pressure
- **Circuit Breakers**: Prevent cascade failures to backends

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
sync-engine = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

Basic usage:

```rust
use sync_engine::{SyncEngine, SyncEngineConfig, SyncItem, SubmitOptions, CacheTtl};
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
    engine.start().await.expect("Failed to start");

    // Submit with default options (Redis + SQL)
    let item = SyncItem::from_json(
        "uk.nhs.patient.12345".into(),
        json!({"name": "John Doe"})
    );
    engine.submit(item).await.expect("Failed to submit");

    // Submit to Redis only with 1-hour TTL
    let cache_item = SyncItem::from_json(
        "cache.session.abc".into(),
        json!({"user": "alice"})
    );
    engine.submit_with(cache_item, SubmitOptions::cache(CacheTtl::Hour))
        .await.expect("Failed to submit");

    // Retrieve (L1 → L2 → L3 fallback)
    if let Some(item) = engine.get("uk.nhs.patient.12345").await.unwrap() {
        println!("Found: {:?}", item.content_as_json());
    }

    engine.shutdown().await;
}
```

## Submit Options

Control where data is stored per-item:

```rust
use sync_engine::{SubmitOptions, CacheTtl};

// Default: Redis + SQL
let default = SubmitOptions::default();

// Redis only with TTL (ephemeral cache)
let cache = SubmitOptions::cache(CacheTtl::Hour);

// SQL only (durable, skip Redis)
let durable = SubmitOptions::durable();

// Custom routing
let custom = SubmitOptions {
    redis: true,
    redis_ttl: Some(CacheTtl::Day),
    sql: false,
};
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

## Testing

Comprehensive test suite with 211 tests covering unit, property-based, integration, and chaos testing:

| Test Suite | Count | Description |
|------------|-------|-------------|
| **Unit Tests** | 150 | Fast, no external deps |
| **Property Tests** | 12 | Proptest fuzzing for invariants |
| **Integration Tests** | 20 | Real Redis/MySQL via testcontainers |
| **Chaos Tests** | 10 | Failure injection, container killing |
| **Doc Tests** | 19 | Example verification |
| **Total** | **211** | ~78% code coverage |

### Running Tests

```bash
# Unit tests (fast, no Docker)
cargo test --lib

# Property-based fuzzing
cargo test --test proptest_fuzz

# Integration tests (requires Docker)
cargo test --test integration -- --ignored

# Chaos tests (requires Docker)
cargo test --test chaos -- --ignored

# All tests
cargo test -- --include-ignored
```

### Chaos Testing Scenarios

The chaos test suite validates resilience under real-world failure conditions:

- **Container killing**: Redis/MySQL death mid-operation
- **Data corruption**: Garbage JSON, truncated data in Redis
- **Lifecycle edge cases**: Double start, shutdown without start, ops after shutdown
- **Concurrent failures**: Many writers during Redis death
- **Memory pressure**: L1 overflow with backpressure
- **Rapid cycles**: Start/stop 5x without resource leaks

## License

[GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0)
