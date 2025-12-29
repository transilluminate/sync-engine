# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
