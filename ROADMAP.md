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

## V1.2 📋 (Planned)

Compression and CRDT-aware features.

### Feature Flags
```toml
[features]
default = []
crdt = ["crdt-data-types"]      # CRDT-aware compaction
compression = ["zstd"]           # Transparent zstd compression (L3 only)
```

### Transparent Compression (L3 only)
Magic-bytes detection with zstd. **Not for Redis** (RediSearch needs plain strings).

```rust
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

impl L3Store {
    pub fn compress(json: &Value) -> Vec<u8> { /* zstd::encode */ }
    pub fn decompress(bytes: &[u8]) -> Result<Value, Error> {
        if bytes.starts_with(&ZSTD_MAGIC) { /* zstd */ }
        else { /* plain JSON */ }
    }
}
```

### CRDT Compaction (with `crdt` feature)
- Retention policies: `retention_full_days`, `retention_compact_days`
- Use `crdt-data-types` merge traits for compaction
- Background compaction job with configurable schedule
- Preserves CRDT semantics during merge

---

## V1.3 📋 (Planned)

Self-healing and performance optimizations.

### CRDT Rebuild from Snapshot
When corruption detected and `crdt` feature enabled:
1. Load CRDT snapshot from L3
2. Re-materialize view using merge semantics
3. Replace corrupted data transparently
4. Emit healing metrics

### Priority Sync Queue
- Sync "hot" items first based on access patterns
- Multi-tier priority levels (critical/high/normal/low)
- Configurable priority scoring function

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

---

## Feature Priority Matrix

| Priority | Feature | Effort | Value | Version |
|----------|---------|--------|-------|---------|
| ✅ Done | `contains()` / `status()` | 2h | API ergonomics | V1.1 |
| ✅ Done | Batch get/submit/delete | 2h | Performance | V1.1 |
| ✅ Done | `get_or_insert_with()` | 1h | Cache-aside | V1.1 |
| ✅ Done | Proptest fuzz suite | 2h | Catches panics | V1.0 |
| ✅ Done | Hash verification | 1h | Detects corruption | V1.0 |
| 🟡 Medium | Compression (L3) | 2h | Storage savings | V1.2 |
| 🟡 Medium | CRDT feature flag | 3h | Clean separation | V1.2 |
| 🟡 Medium | CRDT compaction | 4h | Storage lifecycle | V1.2 |
| 🟢 Nice | CRDT self-healing | 4h | Resilience | V1.3 |
| 🟢 Nice | Priority sync | 3h | Performance | V1.3 |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on submitting features from this roadmap.
