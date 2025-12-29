//! Transparent compression for L3 storage.
//!
//! Uses zstd compression with magic-bytes detection for automatic
//! decompression of legacy uncompressed data.
//!
//! # Feature Flag
//!
//! This module requires the `compression` feature:
//!
//! ```toml
//! [dependencies]
//! sync-engine = { version = "0.1", features = ["compression"] }
//! ```
//!
//! # Why L3 Only?
//!
//! Redis (L2) is **not** compressed because:
//! - RediSearch requires plain strings for full-text indexing
//! - Redis already has internal memory optimizations
//! - L2 is a cache layer; compression overhead isn't worth it
//!
//! MySQL/SQLite (L3) benefits from compression because:
//! - JSON/CRDT data compresses extremely well (80-90% reduction)
//! - Disk I/O is the bottleneck, not CPU
//! - Long-term archival storage
//!
//! # Example
//!
//! ```rust,ignore
//! use sync_engine::compression::{compress, decompress};
//! use serde_json::json;
//!
//! let data = json!({"operations": [1, 2, 3, 4, 5]});
//! let compressed = compress(&data)?;
//! let decompressed = decompress(&compressed)?;
//! assert_eq!(data, decompressed);
//! ```

#[cfg(feature = "compression")]
use serde_json::Value;

/// Zstd magic bytes (little-endian): 0xFD2FB528
#[cfg(feature = "compression")]
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Default compression level (3 is a good balance of speed/ratio)
#[cfg(feature = "compression")]
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;

/// Compression error types
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    /// Failed to compress data
    #[error("compression failed: {0}")]
    CompressFailed(String),

    /// Failed to decompress data
    #[error("decompression failed: {0}")]
    DecompressFailed(String),

    /// Failed to parse JSON
    #[error("JSON parse failed: {0}")]
    JsonParseFailed(#[from] serde_json::Error),
}

/// Check if data is zstd-compressed by checking magic bytes.
#[cfg(feature = "compression")]
#[inline]
#[must_use]
pub fn is_compressed(data: &[u8]) -> bool {
    data.len() >= 4 && data[..4] == ZSTD_MAGIC
}

/// Compress JSON value to zstd bytes.
///
/// Returns compressed bytes with zstd magic header.
#[cfg(feature = "compression")]
pub fn compress(value: &Value) -> Result<Vec<u8>, CompressionError> {
    compress_with_level(value, DEFAULT_COMPRESSION_LEVEL)
}

/// Compress JSON value with custom compression level (1-22).
///
/// Higher levels = better compression but slower.
/// - Level 1-3: Fast, good for real-time
/// - Level 10-15: Balanced
/// - Level 19-22: Maximum compression, slow
#[cfg(feature = "compression")]
pub fn compress_with_level(value: &Value, level: i32) -> Result<Vec<u8>, CompressionError> {
    let json_bytes = serde_json::to_vec(value)?;
    
    zstd::encode_all(json_bytes.as_slice(), level)
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))
}

/// Decompress data to JSON value.
///
/// Automatically detects if data is compressed via magic bytes.
/// Uncompressed JSON is parsed directly for backwards compatibility.
#[cfg(feature = "compression")]
pub fn decompress(data: &[u8]) -> Result<Value, CompressionError> {
    if is_compressed(data) {
        // Zstd compressed
        let decompressed = zstd::decode_all(data)
            .map_err(|e| CompressionError::DecompressFailed(e.to_string()))?;
        
        serde_json::from_slice(&decompressed)
            .map_err(CompressionError::from)
    } else {
        // Plain JSON (legacy data)
        serde_json::from_slice(data)
            .map_err(CompressionError::from)
    }
}

/// Compress bytes directly (for raw content).
#[cfg(feature = "compression")]
pub fn compress_bytes(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    compress_bytes_with_level(data, DEFAULT_COMPRESSION_LEVEL)
}

/// Compress bytes with custom level.
#[cfg(feature = "compression")]
pub fn compress_bytes_with_level(data: &[u8], level: i32) -> Result<Vec<u8>, CompressionError> {
    zstd::encode_all(data, level)
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))
}

/// Decompress bytes directly.
///
/// Returns original bytes if not compressed.
#[cfg(feature = "compression")]
pub fn decompress_bytes(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    if is_compressed(data) {
        zstd::decode_all(data)
            .map_err(|e| CompressionError::DecompressFailed(e.to_string()))
    } else {
        Ok(data.to_vec())
    }
}

/// Compression statistics for a single operation.
#[derive(Debug, Clone, Copy)]
pub struct CompressionStats {
    /// Original size in bytes
    pub original_bytes: usize,
    /// Compressed size in bytes
    pub compressed_bytes: usize,
    /// Compression ratio (original / compressed)
    pub ratio: f64,
    /// Space saved as percentage (0.0 - 1.0)
    pub savings: f64,
}

impl CompressionStats {
    /// Calculate stats from original and compressed sizes.
    #[must_use]
    pub fn new(original_bytes: usize, compressed_bytes: usize) -> Self {
        let ratio = if compressed_bytes > 0 {
            original_bytes as f64 / compressed_bytes as f64
        } else {
            0.0
        };
        let savings = if original_bytes > 0 {
            1.0 - (compressed_bytes as f64 / original_bytes as f64)
        } else {
            0.0
        };
        Self {
            original_bytes,
            compressed_bytes,
            ratio,
            savings,
        }
    }
}

/// Compress and return stats.
#[cfg(feature = "compression")]
pub fn compress_with_stats(value: &Value) -> Result<(Vec<u8>, CompressionStats), CompressionError> {
    let json_bytes = serde_json::to_vec(value)?;
    let original_size = json_bytes.len();
    
    let compressed = zstd::encode_all(json_bytes.as_slice(), DEFAULT_COMPRESSION_LEVEL)
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;
    
    let stats = CompressionStats::new(original_size, compressed.len());
    Ok((compressed, stats))
}

// ============================================================================
// Stub implementations when compression feature is disabled
// ============================================================================

/// Check if data is compressed (always false without feature).
#[cfg(not(feature = "compression"))]
#[inline]
#[must_use]
pub fn is_compressed(_data: &[u8]) -> bool {
    false
}

#[cfg(test)]
#[cfg(feature = "compression")]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let data = json!({
            "patient_id": "uk.nhs.patient.12345",
            "operations": [
                {"op": "set", "path": "/name", "value": "John Doe"},
                {"op": "set", "path": "/dob", "value": "1990-01-15"},
            ],
            "metadata": {
                "created_at": "2025-01-01T00:00:00Z",
                "version": 42
            }
        });

        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_is_compressed_detection() {
        let data = json!({"test": "data"});
        let compressed = compress(&data).unwrap();

        assert!(is_compressed(&compressed));
        assert!(!is_compressed(b"{\"test\": \"data\"}"));
        assert!(!is_compressed(b""));
        assert!(!is_compressed(b"abc"));
    }

    #[test]
    fn test_decompress_plain_json() {
        // Simulate legacy uncompressed data
        let plain_json = b"{\"legacy\": true, \"value\": 123}";
        let result = decompress(plain_json).unwrap();

        assert_eq!(result["legacy"], true);
        assert_eq!(result["value"], 123);
    }

    #[test]
    fn test_compression_ratio() {
        // CRDTs compress extremely well due to repetitive structure
        let crdt_ops = json!({
            "operations": (0..100).map(|i| {
                json!({
                    "op": "set",
                    "path": format!("/field_{}", i),
                    "value": i,
                    "timestamp": "2025-01-01T00:00:00Z",
                    "actor": "node-1"
                })
            }).collect::<Vec<_>>()
        });

        let (_compressed, stats) = compress_with_stats(&crdt_ops).unwrap();

        println!(
            "Compression: {} -> {} bytes ({:.1}% savings, {:.1}x ratio)",
            stats.original_bytes,
            stats.compressed_bytes,
            stats.savings * 100.0,
            stats.ratio
        );

        // CRDTs should compress well (at least 50% savings)
        assert!(stats.savings > 0.5, "Expected >50% savings, got {:.1}%", stats.savings * 100.0);
    }

    #[test]
    fn test_compression_levels() {
        let data = json!({"data": "x".repeat(1000)});

        let fast = compress_with_level(&data, 1).unwrap();
        let balanced = compress_with_level(&data, 10).unwrap();
        let max = compress_with_level(&data, 19).unwrap();

        // Higher levels should generally compress better (not always guaranteed)
        println!("Level 1: {} bytes", fast.len());
        println!("Level 10: {} bytes", balanced.len());
        println!("Level 19: {} bytes", max.len());

        // All should decompress correctly
        assert_eq!(decompress(&fast).unwrap(), data);
        assert_eq!(decompress(&balanced).unwrap(), data);
        assert_eq!(decompress(&max).unwrap(), data);
    }

    #[test]
    fn test_compress_bytes_roundtrip() {
        let original = b"Hello, World! This is some test data.";
        let compressed = compress_bytes(original).unwrap();
        let decompressed = decompress_bytes(&compressed).unwrap();

        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_decompress_bytes_uncompressed() {
        let original = b"Plain text data";
        let result = decompress_bytes(original).unwrap();
        assert_eq!(original.as_slice(), result.as_slice());
    }

    #[test]
    fn test_compression_stats() {
        let data = json!({"key": "value".repeat(100)});
        let (_, stats) = compress_with_stats(&data).unwrap();

        assert!(stats.original_bytes > 0);
        assert!(stats.compressed_bytes > 0);
        assert!(stats.ratio > 1.0); // Should compress
        assert!(stats.savings > 0.0);
        assert!(stats.savings < 1.0);
    }
}
