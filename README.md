# Sync Engine

High-performance tiered sync engine with L1/L2/L3 caching and Redis/SQL backends

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)

## Philosophy: Content-Aware Tiered Storage

sync-engine intelligently routes data through L1/L2/L3 tiers based on content type:
- **JSON payloads** → Stored natively for full-text search (RediSearch, JSON_EXTRACT)
- **Binary blobs** → Stored as opaque bytes in dedicated blob columns

The caller submits `SyncItem` with raw bytes; the engine detects content type and
optimizes storage for queryability while maintaining the original data integrity.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Ingest Layer                         │
│  • Accepts SyncItems via submit() / submit_with()           │
│  • Backpressure control based on memory usage               │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                    L1: In-Memory Cache                      │
│  • Moka cache for concurrent access                         │
│  • Tan-curve eviction under memory pressure                 │
└─────────────────────────────────────────────────────────────┘
                             │
                   (Batch flush via HybridBatcher)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                     L2: Redis Cache                         │
│  • RedisJSON for structured data (JSON.SET/JSON.GET)        │
│  • RediSearch for full-text & field queries                 │
│  • Binary fallback for non-JSON content                     │
│  • Optional per-item TTL                                    │
└─────────────────────────────────────────────────────────────┘
                             │
                   (Batch persist to ground truth)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   L3: MySQL/SQLite Archive                  │
│  • JSON in TEXT column (queryable via JSON_EXTRACT)         │
│  • Binary in BLOB column                                    │
│  • Cuckoo filter for fast existence checks                  │
│  • WAL fallback during outages                              │
└─────────────────────────────────────────────────────────────┘
```

## Features

- **Tiered Caching**: L1 (memory) → L2 (Redis) → L3 (SQL) with automatic fallback
- **Content-Aware Storage**: JSON payloads stored searchable, binaries as blobs
- **RedisJSON + RediSearch**: Full-text and field-based queries on Redis data
- **MySQL JSON Queries**: JSON_EXTRACT on payload column for SQL searches
- **Flexible Routing**: `SubmitOptions` controls which tiers receive data
- **Item State**: Tag items with caller-defined state for grouping and batch ops
- **TTL Support**: Per-item TTL for Redis cache entries
- **Batch Writes**: Configurable flush by count, size, or time
- **Cuckoo Filters**: Skip SQL queries when data definitely doesn't exist
- **WAL Durability**: Local SQLite WAL during MySQL outages
- **Backpressure**: Graceful degradation under memory pressure
- **Circuit Breakers**: Prevent cascade failures to backends
- **Change Detection Capture**: (CDC) stream output into Redis streams for replication

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
sync-engine = "0.2.2"
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

## Item State

Tag items with caller-defined state for grouping and batch operations:

```rust
use sync_engine::{SyncItem, SubmitOptions};
use serde_json::json;

// Create item with state (e.g., CRDT deltas vs base state)
let delta = SyncItem::from_json("crdt.user.123".into(), json!({"op": "add"}))
    .with_state("delta");

// Override state via SubmitOptions
engine.submit_with(item, SubmitOptions::default().with_state("pending")).await?;

// Query by state
let deltas = engine.get_by_state("delta", 1000).await?;
let count = engine.count_by_state("pending").await?;

// Update state (e.g., after processing)
engine.set_state("crdt.user.123", "merged").await?;

// Bulk delete by state
engine.delete_by_state("processed").await?;
```

State is indexed in SQL for fast queries and tracked in Redis SETs for O(1) membership checks.

## Prefix Scan

Query items by ID prefix for CRDT delta-first patterns:

```rust
// Store deltas with hierarchical IDs: delta:{object_id}:{op_id}
let op = SyncItem::from_json("delta:user.123:op001".into(), json!({"op": "+1"}))
    .with_state("delta");
engine.submit(op).await?;

// Fetch ALL deltas for a specific object (for read-repair/merge)
let user_deltas = engine.scan_prefix("delta:user.123:", 1000).await?;

// Count pending deltas
let pending = engine.count_prefix("delta:user.123:").await?;

// After merging, cleanup the deltas
engine.delete_prefix("delta:user.123:").await?;
```

Prefix scan queries SQL directly (ground truth) and leverages the primary key index for efficient `LIKE 'prefix%'` queries.

## Full-Text Search

Create search indices on your JSON data for RediSearch-powered queries with SQL fallback:

```rust
use sync_engine::search::{SearchIndex, Query};
use sync_engine::coordinator::SearchTier;

// Define a search index (the engine handles $.payload paths internally)
let index = SearchIndex::new("users", "crdt:users:")
    .text("name")              // Full-text searchable
    .text("email")
    .numeric_sortable("age")   // Numeric with range queries
    .tag("roles");             // Exact tag matching

engine.create_search_index(index).await?;

// Insert documents as usual
let user = SyncItem::from_json("crdt:users:alice", json!({
    "name": "Alice Smith",
    "email": "alice@example.com",
    "age": 28,
    "roles": ["admin", "developer"]
}));
engine.submit(user).await?;

// Query with fluent builder API
let query = Query::field_eq("name", "Alice Smith")
    .and(Query::numeric_range("age", Some(25.0), Some(35.0)));

let results = engine.search("users", &query, 100).await?;

// Tag-based queries
let admins = engine.search("users", &Query::tags("roles", vec!["admin"]), 100).await?;

// Use SearchTier for control over Redis vs SQL
let redis_only = engine.search_with_options("users", &query, SearchTier::RedisOnly, 100).await?;
```

**Search Features:**
- **Text fields**: Full-text search with phrase matching
- **Numeric fields**: Range queries with optional sorting
- **Tag fields**: Exact multi-value matching with OR semantics
- **Compound queries**: `.and()`, `.or()`, `.negate()` for complex filters
- **Search cache**: Merkle-invalidated SQL result caching for hybrid queries

## Configuration

All configuration options with their defaults:

| Option | Default | Description |
|--------|---------|-------------|
| **Connection** |||
| `redis_url` | `None` | Redis connection string (e.g., `"redis://localhost:6379"`) |
| `redis_prefix` | `None` | Key prefix for namespacing (e.g., `"sync:"`) |
| `sql_url` | `None` | MySQL/SQLite connection string |
| **Memory & Limits** |||
| `l1_max_bytes` | `256 MB` | Maximum L1 cache size |
| `max_payload_bytes` | `16 MB` | Max single item size (prevents cache exhaustion) |
| **Batching** |||
| `batch_flush_ms` | `100` | Flush batch after N milliseconds |
| `batch_flush_count` | `1000` | Flush batch after N items |
| `batch_flush_bytes` | `1 MB` | Flush batch after N bytes |
| **Backpressure** |||
| `backpressure_warn` | `0.7` | Memory pressure warning threshold (0.0-1.0) |
| `backpressure_critical` | `0.9` | Memory pressure critical threshold (0.0-1.0) |
| **WAL (Write-Ahead Log)** |||
| `wal_path` | `None` | SQLite WAL path for durability during outages |
| `wal_max_items` | `None` | Max WAL items before backpressure |
| `wal_drain_batch_size` | `100` | Items per WAL drain batch |
| **Cuckoo Filter** |||
| `cuckoo_warmup_batch_size` | `10000` | Batch size when warming filter from SQL |
| `cf_snapshot_interval_secs` | `30` | Snapshot filter to WAL every N seconds |
| `cf_snapshot_insert_threshold` | `10000` | Snapshot filter after N inserts |
| **Redis Eviction** |||
| `redis_eviction_enabled` | `true` | Enable proactive eviction before Redis LRU |
| `redis_eviction_start` | `0.75` | Start evicting at this pressure (0.0-1.0) |
| `redis_eviction_target` | `0.60` | Target pressure after eviction (0.0-1.0) |
| **Merkle Tree** |||
| `merkle_calc_enabled` | `true` | Enable merkle updates (if sharing a SQL instance, disable on most nodes in cluster) |
| `merkle_calc_jitter_ms` | `0` | Random delay to reduce cluster contention |
| **CDC Stream** |||
| `enable_cdc_stream` | `false` | Enable Change Data Capture to Redis Stream |
| `cdc_stream_maxlen` | `100000` | Max stream entries (MAXLEN ~, relies on Merkle repair) |

## Testing

Comprehensive test suite with 324 tests covering unit, property-based, integration, and chaos testing:

| Test Suite | Count | Description |
|------------|-------|-------------|
| **Unit Tests** | 241 ✅ | Fast, no external deps |
| **Doc Tests** | 31 ✅ | Example verification |
| **Property Tests** | 12 ✅ | Proptest fuzzing for invariants |
| **Integration Tests** | 30 ✅ | Real Redis Stack/MySQL via testcontainers |
| **Chaos Tests** | 10 ✅ | Failure injection, container killing |
| **Total** | **324** ✅ | ~76.6% code coverage |

### Running Tests

```bash
# Unit tests (fast, no Docker)
cargo test --lib

# Doc tests (not so fast, no Docker)
cargo test --doc

# Property-based fuzzing
cargo test --test proptest_fuzz

# Integration tests (requires Docker)
cargo test --test integration -- --ignored

# Chaos tests (requires Docker)
cargo test --test chaos -- --ignored

# All tests
cargo test -- --include-ignored

# Coverage (slow, requires Docker for integration and chaos)
cargo llvm-cov --all-features --lib --tests -- --include-ignored
```

## Examples & CLI Tools

### Basic Usage Example

A complete working example that demonstrates all core functionality:

```bash
# Start the docker environment
docker compose up -d

# Run the example
cargo run --example basic_usage
```

This showcases:
- Connecting to Redis (L2) and MySQL (L3)
- Writing JSON entries with timing
- L1 cache hit performance (microseconds!)
- Merkle tree sync verification
- Cuckoo filter trust verification
- OTEL-compatible metrics export

### CLI Scripts

Utility scripts for inspecting and managing data in the docker environment:

| Script | Purpose |
|--------|---------|
| `./scripts/clear.sh` | Flush Redis + drop/recreate MySQL table |
| `./scripts/redisearch-index.sh` | Build RediSearch full-text index |
| `./scripts/redisearch-query.sh` | Query RediSearch with pretty output |
| `./scripts/redis-search.sh` | Manual Redis search (pattern or JSON) |
| `./scripts/sql-search.sh` | MySQL search with JSON_EXTRACT |
| `./scripts/show-records.sh` | Display full records from Redis + MySQL |

**Examples:**

```bash
# Clear everything and start fresh
./scripts/clear.sh

# Create data with `cargo run --example basic_usage` from above

# Build RediSearch index after populating data
./scripts/redisearch-index.sh

# Query by field, tag, or numeric range
./scripts/redisearch-query.sh "@name:Alice"
./scripts/redisearch-query.sh "@role:{admin}"
./scripts/redisearch-query.sh "@requests:[40000 +inf]"

# Search MySQL with JSON path
./scripts/sql-search.sh '$.name' 'Alice'
./scripts/sql-search.sh --like 'admin'

# Display all records from both backends
./scripts/show-records.sh
./scripts/show-records.sh "user.alice"
```

### Docker Environment

The `docker-compose.yml` provides:

| Service | Port | Description |
|---------|------|-------------|
| Redis Stack | 6379 | Redis + RediSearch + RedisJSON |
| MySQL | 3306 | Ground truth storage |
| RedisInsight | 5540 | Redis GUI |
| Adminer | 8080 | MySQL GUI |

```bash
docker compose up -d    # Start all services
docker compose down     # Stop all services
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

For commercial licensing options, contact: adrian.j.robinson@gmail.com