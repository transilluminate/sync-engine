# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- V1.1 API ergonomics:
  - `contains(id)` - Fast probabilistic existence check via Cuckoo filters
  - `status(id) -> ItemStatus` - Sync state (Synced/Pending/Missing)
  - `get_many(&[ids])` - Parallel batch fetch from L1/L2/L3
  - `submit_many(items) -> BatchResult` - Batch upsert
  - `delete_many(&[ids]) -> BatchResult` - Batch delete
  - `get_or_insert_with(id, factory)` - Cache-aside pattern
  - `len()` / `is_empty()` - L1 cache size queries
- New types: `ItemStatus`, `BatchResult`, `BatchableItem` trait
- Proptest fuzz suite (12 property-based tests)
- Hash verification on read via `get_verified()`
- `StorageError::Corruption` variant with expected/actual hashes
- Corruption detection metrics

### Changed
- License changed from MIT to AGPL-3.0
- Test count increased to 163 (149 lib + 14 doc)

## [0.1.0] - 2024-12-29

### Added
- Initial release of sync-engine
- Three-tier caching architecture (L1 memory → L2 Redis → L3 MySQL/SQLite)
- Hybrid batching with configurable flush thresholds (count, size, time)
- Cuckoo filters for probabilistic existence checks (skip network hops)
- Merkle trees for sync verification between Redis and MySQL
- WAL (Write-Ahead Log) for durability during MySQL outages
- Six-tier backpressure cascade for graceful degradation
- Circuit breakers for Redis and MySQL backends
- Configurable retry logic with exponential backoff
- Tan-curve eviction policy under memory pressure
- Filter persistence for fast startup (snapshot/restore)
- Comprehensive test suite (154 tests, 77% coverage)
- Full inline documentation with doctests

### Security
- No sensitive data logged
- Connection strings handled securely

## [0.1.0] - 2024-12-29

### Added
- Initial implementation
