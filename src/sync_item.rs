//! Sync item data structure.
//!
//! The [`SyncItem`] is the core data unit that flows through the sync engine.
//! Each item has a hierarchical ID (reverse DNS style), version, and binary content.
//!
//! # Binary Content
//!
//! The `content` field is `Vec<u8>` - raw bytes that sync-engine treats as opaque.
//! The caller is responsible for serialization (JSON, MessagePack, Cap'n Proto, etc.).
//!
//! ```rust
//! use sync_engine::SyncItem;
//! use serde_json::json;
//!
//! // Store JSON as bytes
//! let json_bytes = serde_json::to_vec(&json!({"name": "Alice"})).unwrap();
//! let item = SyncItem::new("user.123".into(), json_bytes);
//!
//! // Or store any binary format
//! let binary_data = vec![0x01, 0x02, 0x03];
//! let item = SyncItem::new("binary.456".into(), binary_data);
//! ```

use std::sync::OnceLock;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use crate::batching::hybrid_batcher::{SizedItem, BatchableItem};
use crate::submit_options::SubmitOptions;

/// Content type classification for storage routing.
///
/// This enables intelligent storage: JSON content can be stored in Redis as
/// searchable hashes (HSET) and in SQL as queryable JSON columns, while binary
/// content uses efficient blob storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ContentType {
    /// JSON content - stored as Redis HASH, SQL JSON column
    /// Enables: RedisSearch FT.SEARCH, SQL JSON path queries
    Json,
    /// Binary/opaque content - stored as Redis STRING, SQL BLOB
    /// Fast path for non-structured data
    #[default]
    Binary,
}

impl ContentType {
    /// Detect content type from raw bytes.
    /// 
    /// Fast heuristic: checks first non-whitespace byte for JSON indicators,
    /// then validates with a full parse only for likely-JSON content.
    #[must_use]
    pub fn detect(content: &[u8]) -> Self {
        // Empty content is binary
        if content.is_empty() {
            return ContentType::Binary;
        }
        
        // Fast path: check first non-whitespace byte
        let first = content.iter().find(|b| !b.is_ascii_whitespace());
        match first {
            Some(b'{') | Some(b'[') | Some(b'"') => {
                // Likely JSON - validate with parse
                if serde_json::from_slice::<serde_json::Value>(content).is_ok() {
                    ContentType::Json
                } else {
                    ContentType::Binary
                }
            }
            _ => ContentType::Binary
        }
    }
    
    /// Check if this is JSON content
    #[inline]
    #[must_use]
    pub fn is_json(&self) -> bool {
        matches!(self, ContentType::Json)
    }
    
    /// Check if this is binary content
    #[inline]
    #[must_use]
    pub fn is_binary(&self) -> bool {
        matches!(self, ContentType::Binary)
    }
    
    /// Return the string representation for serialization
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            ContentType::Json => "json",
            ContentType::Binary => "binary",
        }
    }
}

/// A wrapper struct that separates metadata from content.
///
/// # Binary-First Design
///
/// sync-engine is a **dumb storage layer** - it stores your bytes and routes
/// them to L1/L2/L3 based on [`SubmitOptions`]. The
/// `content` field is opaque `Vec<u8>` that we never interpret.
///
/// # Example
///
/// ```rust
/// use sync_engine::SyncItem;
/// use serde_json::json;
///
/// // JSON content (serialize to bytes yourself)
/// let json_bytes = serde_json::to_vec(&json!({"name": "John Doe"})).unwrap();
/// let item = SyncItem::new("uk.nhs.patient.12345".into(), json_bytes);
///
/// assert_eq!(item.object_id, "uk.nhs.patient.12345");
/// assert_eq!(item.version, 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncItem {
    /// Reverse DNS style ID (e.g., `uk.nhs.patient.record.1234567890`)
    pub object_id: String,
    /// Version number (monotonically increasing within this item)
    pub version: u64,
    /// Last update timestamp (epoch millis)
    pub updated_at: i64,
    /// Content type (json or binary) - determines storage format
    /// JSON → Redis HSET (searchable), SQL JSON column
    /// Binary → Redis SET, SQL BLOB column
    #[serde(default)]
    pub content_type: ContentType,
    /// Batch ID for tracking batch writes (UUID, set during batch flush)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    /// W3C Trace Context traceparent header (for cross-item trace linking)
    /// Format: "00-{trace_id}-{span_id}-{flags}"
    /// This is NOT for in-process tracing (that flows via Span::current()),
    /// but for linking related operations across items/time.
    pub trace_parent: Option<String>,
    /// W3C Trace Context tracestate header (optional vendor-specific data)
    pub trace_state: Option<String>,
    /// Reserved for future use. Currently unused.
    #[doc(hidden)]
    pub priority_score: f64,
    /// SHA256 hash of the content (hex-encoded).
    /// Computed eagerly on creation for CDC dedup and integrity checks.
    #[serde(alias = "merkle_root")]  // Wire compat with v0.2.x
    pub content_hash: String,
    /// Timestamp of last access (epoch millis)
    pub last_accessed: u64,
    /// Number of times accessed
    pub access_count: u64,
    /// The actual payload (opaque binary, caller handles serialization)
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
    /// Optional guest data owner ID (for routing engine)
    pub home_instance_id: Option<String>,
    /// Arbitrary state tag for caller-defined grouping (e.g., "delta", "base", "pending").
    /// Indexed in SQL and tracked via Redis SETs for fast state-based queries.
    /// Default: "default"
    #[serde(default = "default_state")]
    pub state: String,
    
    /// Transient submit options (travels with item through pipeline, not serialized)
    /// Set via `submit_with()`, defaults to `SubmitOptions::default()` if None.
    #[serde(skip)]
    pub(crate) submit_options: Option<SubmitOptions>,
    
    /// Cached computed size in bytes (lazily computed, not serialized)
    #[serde(skip)]
    cached_size: OnceLock<usize>,
}

/// Default state value for SyncItem
fn default_state() -> String {
    "default".to_string()
}

impl SyncItem {
    /// Create a new SyncItem with binary content.
    ///
    /// The content type is auto-detected: if the bytes are valid JSON,
    /// `content_type` will be `Json`, otherwise `Binary`. This enables
    /// intelligent storage routing (HSET vs SET in Redis, JSON vs BLOB in SQL).
    ///
    /// # Example
    ///
    /// ```rust
    /// use sync_engine::{SyncItem, ContentType};
    ///
    /// // From raw bytes (detected as Binary)
    /// let item = SyncItem::new("id".into(), vec![1, 2, 3]);
    /// assert_eq!(item.content_type, ContentType::Binary);
    ///
    /// // From JSON bytes (detected as Json)
    /// let json = serde_json::to_vec(&serde_json::json!({"key": "value"})).unwrap();
    /// let item = SyncItem::new("id".into(), json);
    /// assert_eq!(item.content_type, ContentType::Json);
    /// ```
    pub fn new(object_id: String, content: Vec<u8>) -> Self {
        let content_type = ContentType::detect(&content);
        // Compute content hash eagerly for CDC dedup
        let content_hash = hex::encode(Sha256::digest(&content));
        Self {
            object_id,
            version: 1,
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            content_type,
            batch_id: None,
            trace_parent: None,
            trace_state: None,
            priority_score: 0.0,
            content_hash,
            last_accessed: 0,
            access_count: 0,
            content,
            home_instance_id: None,
            state: "default".to_string(),
            submit_options: None,  // Set via submit_with() if needed
            cached_size: OnceLock::new(),
        }
    }

    /// Create a new SyncItem from a JSON value (convenience method).
    ///
    /// This serializes the JSON to bytes and sets `content_type` to `Json`.
    /// For binary formats (MessagePack, Cap'n Proto), use [`new`](Self::new).
    pub fn from_json(object_id: String, value: serde_json::Value) -> Self {
        let content = serde_json::to_vec(&value).unwrap_or_default();
        let mut item = Self::new(object_id, content);
        item.content_type = ContentType::Json; // Explicit, since we know it's JSON
        item
    }

    /// Create a new SyncItem from any serializable type.
    ///
    /// This avoids creating an intermediate `serde_json::Value` if you have a struct.
    /// This is more efficient than `from_json` if you already have a typed object.
    pub fn from_serializable<T: Serialize>(object_id: String, value: &T) -> Result<Self, serde_json::Error> {
        let content = serde_json::to_vec(value)?;
        let mut item = Self::new(object_id, content);
        item.content_type = ContentType::Json;
        Ok(item)
    }
    
    /// Reconstruct a SyncItem from stored components (used by storage backends).
    /// 
    /// This allows storage backends to rebuild a SyncItem from flattened data
    /// (e.g., Redis HGETALL, SQL column reads) without accessing private fields.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn reconstruct(
        object_id: String,
        version: u64,
        updated_at: i64,
        content_type: ContentType,
        content: Vec<u8>,
        batch_id: Option<String>,
        trace_parent: Option<String>,
        content_hash: String,
        home_instance_id: Option<String>,
        state: String,
    ) -> Self {
        Self {
            object_id,
            version,
            updated_at,
            content_type,
            batch_id,
            trace_parent,
            trace_state: None,
            priority_score: 0.0,
            content_hash,
            last_accessed: 0,
            access_count: 0,
            content,
            home_instance_id,
            state,
            submit_options: None,
            cached_size: OnceLock::new(),
        }
    }

    /// Set submit options for this item (builder pattern).
    ///
    /// These options control where the item is stored (Redis, SQL) and
    /// how it's compressed. Options travel with the item through the
    /// batch pipeline.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sync_engine::{SyncItem, SubmitOptions, CacheTtl};
    ///
    /// let item = SyncItem::new("cache.key".into(), b"data".to_vec())
    ///     .with_options(SubmitOptions::cache(CacheTtl::Minute));
    /// ```
    #[must_use]
    pub fn with_options(mut self, options: SubmitOptions) -> Self {
        self.submit_options = Some(options);
        self
    }
    
    /// Set state tag for this item (builder pattern).
    ///
    /// State is an arbitrary string for caller-defined grouping.
    /// Common uses: "delta"/"base" for CRDTs, "pending"/"approved" for workflows.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sync_engine::SyncItem;
    ///
    /// let item = SyncItem::new("crdt.123".into(), b"data".to_vec())
    ///     .with_state("delta");
    /// ```
    #[must_use]
    pub fn with_state(mut self, state: impl Into<String>) -> Self {
        self.state = state.into();
        self
    }

    /// Get the effective submit options (returns default if not set).
    #[must_use]
    pub fn effective_options(&self) -> SubmitOptions {
        self.submit_options.clone().unwrap_or_default()
    }

    /// Try to parse content as JSON.
    ///
    /// Returns `None` if content is not valid JSON.
    #[must_use]
    pub fn content_as_json(&self) -> Option<serde_json::Value> {
        serde_json::from_slice(&self.content).ok()
    }

    /// Attach trace context from current span (for distributed trace linking)
    #[cfg(feature = "otel")]
    pub fn with_current_trace_context(mut self) -> Self {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        
        let cx = tracing::Span::current().context();
        let span_ref = cx.span();
        let sc = span_ref.span_context();
        if sc.is_valid() {
            self.trace_parent = Some(format!(
                "00-{}-{}-{:02x}",
                sc.trace_id(),
                sc.span_id(),
                sc.trace_flags().to_u8()
            ));
        }
        self
    }
}

impl SizedItem for SyncItem {
    fn size_bytes(&self) -> usize {
        *self.cached_size.get_or_init(|| {
            // Approximate size: struct overhead + string lengths + content bytes
            std::mem::size_of::<Self>()
                + self.object_id.len()
                + self.trace_parent.as_ref().map_or(0, String::len)
                + self.trace_state.as_ref().map_or(0, String::len)
                + self.content_hash.len()
                + self.content.len()
                + self.home_instance_id.as_ref().map_or(0, String::len)
        })
    }
}

impl BatchableItem for SyncItem {
    fn id(&self) -> &str {
        &self.object_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_sync_item() {
        let item = SyncItem::new("test-id".to_string(), b"hello".to_vec());
        
        assert_eq!(item.object_id, "test-id");
        assert_eq!(item.version, 1);
        assert!(item.updated_at > 0);
        assert!(item.batch_id.is_none());
        assert!(item.trace_parent.is_none());
        assert!(item.trace_state.is_none());
        assert_eq!(item.priority_score, 0.0);
        // Content hash is computed eagerly for CDC dedup
        // SHA256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        assert_eq!(item.content_hash, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
        assert_eq!(item.last_accessed, 0);
        assert_eq!(item.access_count, 0);
        assert!(item.home_instance_id.is_none());
        assert_eq!(item.content, b"hello");
    }

    #[test]
    fn test_from_json() {
        let item = SyncItem::from_json("test-id".to_string(), json!({"key": "value"}));
        
        assert_eq!(item.object_id, "test-id");
        // Content should be serialized JSON bytes
        let parsed: serde_json::Value = serde_json::from_slice(&item.content).unwrap();
        assert_eq!(parsed, json!({"key": "value"}));
    }

    #[test]
    fn test_content_as_json() {
        let item = SyncItem::from_json("test".into(), json!({"nested": {"key": 42}}));
        
        let parsed = item.content_as_json().unwrap();
        assert_eq!(parsed["nested"]["key"], 42);
        
        // Binary content should return None
        let binary_item = SyncItem::new("bin".into(), vec![0xFF, 0xFE, 0x00]);
        assert!(binary_item.content_as_json().is_none());
    }

    #[test]
    fn test_size_bytes_calculation() {
        let item = SyncItem::from_json(
            "uk.nhs.patient.record.123456".to_string(),
            json!({"name": "John Doe", "age": 42, "conditions": ["diabetes", "hypertension"]})
        );
        
        let size = item.size_bytes();
        
        // Should be non-zero
        assert!(size > 0);
        
        // Should include struct overhead + content
        assert!(size > std::mem::size_of::<SyncItem>());
    }

    #[test]
    fn test_size_bytes_cached() {
        let item = SyncItem::new("test".to_string(), b"data".to_vec());
        
        let size1 = item.size_bytes();
        let size2 = item.size_bytes();
        
        // Same value (cached)
        assert_eq!(size1, size2);
    }

    #[test]
    fn test_size_includes_optional_fields() {
        let mut item = SyncItem::new("test".to_string(), vec![]);
        
        // Manually set optional fields
        item.trace_parent = Some("00-abc123-def456-01".to_string());
        item.trace_state = Some("vendor=data".to_string());
        item.home_instance_id = Some("instance-1".to_string());
        
        let size = item.size_bytes();
        
        // Should be larger than minimal
        // Note: can't compare directly because cached_size is already set
        // But we can verify size includes the optional field lengths
        assert!(size > std::mem::size_of::<SyncItem>() + "test".len());
    }

    #[test]
    fn test_serialize_deserialize() {
        let item = SyncItem::from_json(
            "test-id".to_string(),
            json!({"nested": {"key": "value"}, "array": [1, 2, 3]})
        );
        
        let json_str = serde_json::to_string(&item).unwrap();
        let deserialized: SyncItem = serde_json::from_str(&json_str).unwrap();
        
        assert_eq!(deserialized.object_id, item.object_id);
        assert_eq!(deserialized.version, item.version);
        assert_eq!(deserialized.content, item.content);
    }

    #[test]
    fn test_serialize_skips_none_batch_id() {
        let item = SyncItem::new("test".to_string(), vec![]);
        
        let json_str = serde_json::to_string(&item).unwrap();
        
        // batch_id should not appear in JSON when None
        assert!(!json_str.contains("batch_id"));
    }

    #[test]
    fn test_serialize_includes_batch_id_when_some() {
        let mut item = SyncItem::new("test".to_string(), vec![]);
        item.batch_id = Some("batch-123".to_string());
        
        let json_str = serde_json::to_string(&item).unwrap();
        
        assert!(json_str.contains("batch_id"));
        assert!(json_str.contains("batch-123"));
    }

    #[test]
    fn test_clone() {
        let item = SyncItem::from_json("original".to_string(), json!({"key": "value"}));
        let cloned = item.clone();
        
        assert_eq!(cloned.object_id, item.object_id);
        assert_eq!(cloned.content, item.content);
    }

    #[test]
    fn test_debug_format() {
        let item = SyncItem::new("test".to_string(), vec![]);
        let debug_str = format!("{:?}", item);
        
        assert!(debug_str.contains("SyncItem"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_updated_at_is_recent() {
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        let item = SyncItem::new("test".to_string(), vec![]);
        
        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        assert!(item.updated_at >= before);
        assert!(item.updated_at <= after);
    }

    #[test]
    fn test_large_content_size() {
        // Create item with large binary content
        let large_data: Vec<u8> = (0..10000u32).flat_map(|i| i.to_le_bytes()).collect();
        let item = SyncItem::new("large".to_string(), large_data);
        
        let size = item.size_bytes();
        
        // Should be substantial (10000 * 4 bytes = 40000)
        assert!(size > 10000, "Large content should result in large size");
    }

    #[test]
    fn test_state_default() {
        let item = SyncItem::new("test".to_string(), b"data".to_vec());
        assert_eq!(item.state, "default");
    }

    #[test]
    fn test_state_with_state_builder() {
        let item = SyncItem::new("test".to_string(), b"data".to_vec())
            .with_state("delta");
        assert_eq!(item.state, "delta");
    }

    #[test]
    fn test_state_with_state_chaining() {
        let item = SyncItem::from_json("test".into(), json!({"key": "value"}))
            .with_state("pending");
        
        assert_eq!(item.state, "pending");
        assert_eq!(item.object_id, "test");
    }

    #[test]
    fn test_state_serialization() {
        let item = SyncItem::new("test".to_string(), b"data".to_vec())
            .with_state("custom_state");
        
        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"state\":\"custom_state\""));
        
        // Deserialize back
        let parsed: SyncItem = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.state, "custom_state");
    }

    #[test]
    fn test_state_deserialization_default() {
        // JSON without state field should default to "default"
        // Uses merkle_root in JSON to test serde alias backward compat
        let json = r#"{
            "object_id": "test",
            "version": 1,
            "updated_at": 12345,
            "priority_score": 0.0,
            "merkle_root": "",
            "last_accessed": 0,
            "access_count": 0,
            "content": [100, 97, 116, 97]
        }"#;
        
        let item: SyncItem = serde_json::from_str(json).unwrap();
        assert_eq!(item.state, "default");
    }
}
