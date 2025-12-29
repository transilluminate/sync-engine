# TODO

Technical debt and future architectural improvements.

---

## 🔴 High Priority

### Simplify to DUMB STORAGE

**Philosophy:** sync-engine stores bytes, routes to tiers. Nothing more.

**Core API:**
```rust
pub struct SubmitOptions {
    /// Store in L2 Redis
    pub redis: bool,              // default: true
    /// Redis TTL (None = no expiry)
    pub redis_ttl: Option<Duration>,
    /// Compress before Redis store (disable for RediSearch indexing)
    pub redis_compress: bool,     // default: false (searchable)
    /// Store in L3 SQL
    pub sql: bool,                // default: true
    /// Compress before SQL store
    pub sql_compress: bool,       // default: true (storage savings)
}

// The entire public API:
engine.submit(id, bytes).await?;                      // default options
engine.submit_with(id, bytes, options).await?;        // explicit routing
engine.get(id).await?;                                // L1 → L2 → L3 (auto-decompress)
engine.get_many(ids).await?;
engine.contains(id).await?;                           // L1 → Redis EXISTS → L3 Cuckoo
engine.delete(id).await?;
engine.delete_many(ids).await?;
```

**Default behaviors:**
| Target | Default | Rationale |
|--------|---------|-----------|
| `redis` | `true` | Fast reads |
| `redis_compress` | `false` | RediSearch needs plain data |
| `sql` | `true` | Durability |
| `sql_compress` | `true` | Storage is cheap, compress always |

**On read:** Magic-byte detection auto-decompresses transparently.

**Tasks:**
- [ ] Change `SyncItem.content` from `Value` to `Vec<u8>` (binary-first)
- [ ] Add `SubmitOptions` struct with all flags
- [ ] Add `submit_with()` method
- [ ] Add Redis TTL support (SET ... EX)
- [ ] Add per-target compression (redis_compress, sql_compress)
- [ ] Leverage existing magic-byte auto-decompress on read
- [ ] Remove L2 Cuckoo filter (TTL makes it untrustworthy)
- [ ] Update `contains()`: L1 → Redis EXISTS → L3 Cuckoo
- [ ] Keep Merkle shadow for L2↔L3 sync verification

**NOT our job (separate crates):**
- Search/query → `search-engine` crate (RediSearch, SQL queries)
- CRDT compaction → `crdt-data-types` (just math, no I/O)
- Orchestration → caller combines search + compact + store

---

## 🟡 Medium Priority

### Remove L2 Cuckoo Filter

TTL expiry breaks Cuckoo invariants. Redis EXISTS is fast enough with Unix sockets.

- [ ] Remove `l2_filter` field
- [ ] Remove L2 filter warmup logic  
- [ ] Update `contains()` to use Redis EXISTS
- [ ] Simplify filter persistence (L3 only)

### Content Type Hint (Optional)

```rust
pub enum ContentType {
    Json,    // Indexable by RediSearch
    Binary,  // Opaque (Cap'n Proto, compressed, etc.)
}
```

May not need this if Redis handles binary transparently. Investigate.

---

## 🟢 Low Priority / Nice-to-Have

### Unix Socket Config

- [ ] Add `redis_socket_path: Option<PathBuf>` to config
- [ ] Note: complicates Docker networking, defer for now

---

## ✅ Done

- V1.0: Core sync engine
- V1.1: API ergonomics (batch ops, contains, status)
- V1.2: Compression + CRDT compaction feature flags

---

## 🚫 Out of Scope (Separate Crates)

| Concern | Where It Lives |
|---------|----------------|
| CRDT merge/compact | `crdt-data-types` crate |
| Search/queries | `search-engine` crate (future) |
| Schema evolution | `FT.ALTER` in search-engine |
| Compaction orchestration | Caller/scheduler |
| Materialized views | Caller computes, we store |
