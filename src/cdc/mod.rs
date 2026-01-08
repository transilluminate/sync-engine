// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Change Data Capture (CDC) Stream support.
//!
//! Emits mutations to a Redis Stream for external replication agents.
//! The stream key is `{redis_prefix}:cdc` - each node writes to its own stream.
//! Replication agents tail remote peers' CDC streams to replicate changes.
//!
//! # Stream Format
//!
//! ## PUT operation
//! ```text
//! XADD {prefix}:cdc MAXLEN ~ 100000 *
//!   op    "PUT"
//!   key   "uk.nhs.patient.12345"
//!   hash  "a1b2c3..."                # content_hash for dedup
//!   data  <zstd(content)>            # compressed payload
//!   meta  '{"content_type":"json","version":3,"updated_at":1735776000000}'
//! ```
//!
//! ## DELETE operation
//! ```text
//! XADD {prefix}:cdc MAXLEN ~ 100000 *
//!   op    "DEL"
//!   key   "uk.nhs.patient.12345"
//! ```
//!
//! # Compression Strategy
//!
//! Data is zstd-compressed before writing to the stream, unless it already
//! has zstd magic bytes (to avoid double-compression).

use serde::{Serialize, Deserialize};
use std::io::Read;

/// zstd magic bytes: 0x28 0xB5 0x2F 0xFD
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// CDC stream key suffix (appended to redis_prefix).
/// Convention: redis_prefix includes trailing colon (e.g., "redsqrl:").
/// Result: "redsqrl:cdc" or just "cdc" if no prefix.
pub const CDC_STREAM_SUFFIX: &str = "cdc";

/// CDC operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOp {
    Put,
    Delete,
}

impl CdcOp {
    /// Returns the string representation for the stream field
    pub fn as_str(&self) -> &'static str {
        match self {
            CdcOp::Put => "PUT",
            CdcOp::Delete => "DEL",
        }
    }
}

/// Metadata for CDC PUT entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcMeta {
    pub content_type: String,
    pub version: u64,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_parent: Option<String>,
}

/// A CDC entry ready to be written to the stream
#[derive(Debug, Clone)]
pub struct CdcEntry {
    /// Operation type
    pub op: CdcOp,
    /// Object ID (key)
    pub key: String,
    /// Content hash for dedup (SHA256 of content) - only for PUT
    pub hash: Option<String>,
    /// Compressed data - only for PUT
    pub data: Option<Vec<u8>>,
    /// Metadata JSON - only for PUT
    pub meta: Option<String>,
}

impl CdcEntry {
    /// Create a PUT entry
    pub fn put(
        key: String,
        hash: String,
        content: &[u8],
        content_type: &str,
        version: u64,
        updated_at: i64,
        trace_parent: Option<String>,
    ) -> Self {
        let data = maybe_compress(content);
        let meta = CdcMeta {
            content_type: content_type.to_string(),
            version,
            updated_at,
            trace_parent,
        };
        let meta_json = serde_json::to_string(&meta).unwrap_or_else(|_| "{}".to_string());

        Self {
            op: CdcOp::Put,
            key,
            hash: Some(hash),
            data: Some(data),
            meta: Some(meta_json),
        }
    }

    /// Create a DELETE entry
    pub fn delete(key: String) -> Self {
        Self {
            op: CdcOp::Delete,
            key,
            hash: None,
            data: None,
            meta: None,
        }
    }

    /// Convert to Redis XADD field-value pairs
    /// Returns Vec of (field, value) where value is either String or bytes
    pub fn to_redis_fields(&self) -> Vec<(&'static str, CdcFieldValue)> {
        let mut fields = vec![
            ("op", CdcFieldValue::Str(self.op.as_str())),
            ("key", CdcFieldValue::String(self.key.clone())),
        ];

        if let Some(ref hash) = self.hash {
            fields.push(("hash", CdcFieldValue::String(hash.clone())));
        }
        if let Some(ref data) = self.data {
            fields.push(("data", CdcFieldValue::Bytes(data.clone())));
        }
        if let Some(ref meta) = self.meta {
            fields.push(("meta", CdcFieldValue::String(meta.clone())));
        }

        fields
    }
}

/// Field value types for CDC entries
#[derive(Debug, Clone)]
pub enum CdcFieldValue {
    Str(&'static str),
    String(String),
    Bytes(Vec<u8>),
}

impl CdcFieldValue {
    /// Convert to bytes for Redis
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            CdcFieldValue::Str(s) => s.as_bytes(),
            CdcFieldValue::String(s) => s.as_bytes(),
            CdcFieldValue::Bytes(b) => b,
        }
    }
}

/// Compress data with zstd, unless it's already zstd-compressed.
///
/// Checks for zstd magic bytes at the start of the data to avoid
/// double-compression. Returns original data if compression fails.
pub fn maybe_compress(data: &[u8]) -> Vec<u8> {
    // Skip if already zstd-compressed
    if data.len() >= 4 && data[..4] == ZSTD_MAGIC {
        return data.to_vec();
    }

    // Skip if too small to benefit from compression
    if data.len() < 64 {
        return data.to_vec();
    }

    // Compress with level 3 (good balance of speed/ratio)
    match zstd::encode_all(data, 3) {
        Ok(compressed) => {
            // Only use compressed if it's actually smaller
            if compressed.len() < data.len() {
                compressed
            } else {
                data.to_vec()
            }
        }
        Err(_) => data.to_vec(),
    }
}

/// Decompress zstd data if it has the magic header, otherwise return as-is.
///
/// This is provided for consumers to use when reading from the stream.
pub fn maybe_decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    if data.len() >= 4 && data[..4] == ZSTD_MAGIC {
        let mut decoder = zstd::Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    } else {
        Ok(data.to_vec())
    }
}

/// Check if data appears to be zstd-compressed
pub fn is_zstd_compressed(data: &[u8]) -> bool {
    data.len() >= 4 && data[..4] == ZSTD_MAGIC
}

/// Build the full CDC stream key from an optional prefix
pub fn cdc_stream_key(prefix: Option<&str>) -> String {
    match prefix {
        Some(p) => format!("{}{}", p, CDC_STREAM_SUFFIX),
        None => CDC_STREAM_SUFFIX.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_maybe_compress_small_data() {
        let small = b"hello";
        let result = maybe_compress(small);
        // Small data should not be compressed
        assert_eq!(result, small.to_vec());
    }

    #[test]
    fn test_maybe_compress_already_zstd() {
        // Fake zstd header + some data
        let mut fake_zstd = ZSTD_MAGIC.to_vec();
        fake_zstd.extend_from_slice(b"already compressed data");
        
        let result = maybe_compress(&fake_zstd);
        // Should pass through unchanged
        assert_eq!(result, fake_zstd);
    }

    #[test]
    fn test_maybe_compress_json() {
        // JSON compresses well
        let json = r#"{"name":"John Doe","email":"john@example.com","data":"some repeated data repeated data repeated data repeated data"}"#;
        let result = maybe_compress(json.as_bytes());
        
        // Should be compressed (smaller)
        assert!(result.len() < json.len());
        // Should have zstd magic
        assert!(is_zstd_compressed(&result));
    }

    #[test]
    fn test_roundtrip_compression() {
        let original = r#"{"key":"value","nested":{"a":1,"b":2},"array":[1,2,3,4,5]}"#.repeat(10);
        let compressed = maybe_compress(original.as_bytes());
        let decompressed = maybe_decompress(&compressed).unwrap();
        
        assert_eq!(decompressed, original.as_bytes());
    }

    #[test]
    fn test_cdc_entry_put() {
        let entry = CdcEntry::put(
            "test.key".to_string(),
            "abc123".to_string(),
            b"test content that is long enough to compress well when repeated",
            "json",
            1,
            1735776000000,
            None,
        );

        assert_eq!(entry.op, CdcOp::Put);
        assert_eq!(entry.key, "test.key");
        assert_eq!(entry.hash, Some("abc123".to_string()));
        assert!(entry.data.is_some());
        assert!(entry.meta.is_some());
        
        let meta: CdcMeta = serde_json::from_str(entry.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta.content_type, "json");
        assert_eq!(meta.version, 1);
    }

    #[test]
    fn test_cdc_entry_delete() {
        let entry = CdcEntry::delete("test.key".to_string());

        assert_eq!(entry.op, CdcOp::Delete);
        assert_eq!(entry.key, "test.key");
        assert!(entry.hash.is_none());
        assert!(entry.data.is_none());
        assert!(entry.meta.is_none());
    }

    #[test]
    fn test_cdc_stream_key() {
        // No prefix -> just "cdc"
        assert_eq!(cdc_stream_key(None), "cdc");
        // Prefix with trailing colon -> "myapp:cdc"
        assert_eq!(cdc_stream_key(Some("myapp:")), "myapp:cdc");
        // Empty prefix -> just "cdc"
        assert_eq!(cdc_stream_key(Some("")), "cdc");
    }

    #[test]
    fn test_to_redis_fields_put() {
        let entry = CdcEntry::put(
            "test.key".to_string(),
            "hash123".to_string(),
            b"data",
            "binary",
            2,
            1000,
            Some("00-trace-span-01".to_string()),
        );

        let fields = entry.to_redis_fields();
        assert_eq!(fields.len(), 5); // op, key, hash, data, meta
        
        assert_eq!(fields[0].0, "op");
        assert_eq!(fields[1].0, "key");
        assert_eq!(fields[2].0, "hash");
        assert_eq!(fields[3].0, "data");
        assert_eq!(fields[4].0, "meta");
    }

    #[test]
    fn test_to_redis_fields_delete() {
        let entry = CdcEntry::delete("test.key".to_string());

        let fields = entry.to_redis_fields();
        assert_eq!(fields.len(), 2); // op, key only
        
        assert_eq!(fields[0].0, "op");
        assert_eq!(fields[1].0, "key");
    }
}
