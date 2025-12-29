# Roadmap

> **Scope**: Reliable sync of JSON data across L1→L2→L3, plus optional CRDT support.
> Enterprise coordination, audit logging, and encryption live in layers above.

## V1.0 ✅ (Delivered)

Core sync engine with three-tier caching and resilience patterns.

### Features
- **Three-tier caching**: L1 (DashMap) → L2 (Redis) → L3 (MySQL/SQLite)
- **Batch writes**: Configurable flush by count, size, or time
- **Backpressure**: Six-tier graceful degradation under memory pressure
- **Circuit breakers**: Prevent cascade failures to unhealthy backends
- **Retry logic**: Configurable exponential backoff (startup/daemon/query presets)
- **WAL durability**: SQLite WAL ensures no data loss during MySQL outages
- **Cuckoo filters**: Probabilistic existence checks with trusted/untrusted states
- **Filter persistence**: Snapshot/restore with merkle root verification
- **Merkle trees**: Sync verification between Redis and MySQL with branch-diff
- **Tan-curve eviction**: Pressure-based cache eviction
- **Proptest fuzz suite** - 12 property tests catch panics from malformed data
- **Hash verification on read** - `get_verified()` detects corruption
- **StorageError::Corruption** - Explicit error variant with expected/actual hashes
- **Corruption metrics** - `sync_engine_corruption_detected_total` counter

### Metrics
- 175 tests (136 lib + 12 proptest + 20 integration + 8 doc)
- 77% code coverage
- 0 clippy warnings

---

## V1.1 ✅ (Delivered)

API ergonomics for common operations.

### Features
- [x] **`contains(id)` → bool** - Fast existence check via Cuckoo filter
- [x] **`status(id)` → ItemStatus** - Sync state: Synced/Pending/Missing
- [x] **`submit_many(items)` → BatchResult** - Atomic multi-item upsert
- [x] **`delete_many(ids)` → BatchResult** - Atomic multi-item delete  
- [x] **`get_many(ids)` → Vec<Option<Item>>** - Parallel fetch
- [x] **`get_or_insert_with(id, factory)`** - Cache-aside pattern
- [x] **`len()` / `is_empty()`** - L1 cache size queries

```rust
pub enum ItemStatus {
    Synced { in_l1: bool, in_l2: bool, in_l3: bool },
    Pending,
    Missing,
}

pub struct BatchResult {
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
}
```

### Metrics
- 149 lib + 14 doc tests
- 0 clippy warnings

---

## V1.2 ✅ (Delivered)

Compression and CRDT-aware features.

### Feature Flags
```toml
[features]
default = []
crdt = ["crdt-data-types"]      # CRDT-aware compaction
compression = ["zstd"]           # Transparent zstd compression
```

### Features
- [x] **`compression` feature** - Transparent zstd compression (level 3)
- [x] **Magic-byte detection** - Auto-detect compressed vs plain JSON on read
- [x] **`crdt` feature** - Uses `crdt-data-types` 0.1.3 for CRDT semantics
- [x] **`CompactionConfig`** - Retention period (default 7 days before compacting)
- [x] **`CompactionPolicy`** - KeepAll or MergeCrdt (no delete - preserves rebuild)
- [x] **`crdt_compact::compact_json()`** - Type-dispatched JSON compaction
- [x] **`crdt_compact::compact_bytes()`** - Type-dispatched Cap'n Proto compaction

```rust
use sync_engine::compression::{compress, decompress};
use sync_engine::compaction::{CompactionConfig, crdt_compact};

// Transparent compression with auto-detect on read
let compressed = compress(&json_value)?;
let value = decompress(&bytes)?;  // Works for both compressed and plain

// CRDT compaction (with `crdt` feature)
let compacted = crdt_compact::compact_json("GCounter", &values)?;
```

### Metrics
- 151 lib + 13 doc tests
- 0 clippy warnings

---

## Out of Scope

These concerns belong in coordination layers above sync-engine:

| Concern | Rationale |
|---------|-----------|
| **Encryption** | Let MySQL TDE / Redis encryption handle this |
| **Multi-region sync** | Coordination layer responsibility |
| **Audit logging** | Separate module with GDPR compliance |
| **Access control** | Application layer concern |
| **Rate limiting** | API gateway / coordination layer |
| **Priority sync** | L1 DashMap already provides instant response; FIFO batching is sufficient |
| **Auto-healing** | Caller knows CRDT types; we just store opaque JSON |

---

## Feature Priority Matrix

| Priority | Feature | Effort | Value | Version |
|----------|---------|--------|-------|---------|
| ✅ Done | `contains()` / `status()` | 2h | API ergonomics | V1.1 |
| ✅ Done | Batch get/submit/delete | 2h | Performance | V1.1 |
| ✅ Done | `get_or_insert_with()` | 1h | Cache-aside | V1.1 |
| ✅ Done | Proptest fuzz suite | 2h | Catches panics | V1.0 |
| ✅ Done | Hash verification | 1h | Detects corruption | V1.0 |
| ✅ Done | Compression (zstd) | 2h | Storage savings | V1.2 |
| ✅ Done | CRDT feature flag | 3h | Clean separation | V1.2 |
| ✅ Done | CRDT compaction | 4h | Storage lifecycle | V1.2 |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on submitting features from this roadmap.
