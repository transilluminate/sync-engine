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
use crate::batching::hybrid_batcher::{SizedItem, BatchableItem};
use crate::submit_options::SubmitOptions;

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
    /// Batch ID for tracking batch writes (UUID, set during batch flush)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    /// W3C Trace Context traceparent header (for cross-item trace linking)
    /// Format: "00-{trace_id}-{span_id}-{flags}"
    /// This is NOT for in-process tracing (that flows via Span::current()),
    /// but for linking CRDT merge operations across items/time.
    pub trace_parent: Option<String>,
    /// W3C Trace Context tracestate header (optional vendor-specific data)
    pub trace_state: Option<String>,
    /// Reserved for future use. Currently unused.
    #[doc(hidden)]
    pub priority_score: f64,
    /// Hash of the content for quick integrity checks
    pub merkle_root: String,
    /// Timestamp of last access (epoch millis)
    pub last_accessed: u64,
    /// Number of times accessed
    pub access_count: u64,
    /// The actual payload (opaque binary, caller handles serialization)
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
    /// Optional guest data owner ID (for routing engine)
    pub home_instance_id: Option<String>,
    
    /// Transient submit options (travels with item through pipeline, not serialized)
    /// Set via `submit_with()`, defaults to `SubmitOptions::default()` if None.
    #[serde(skip)]
    pub(crate) submit_options: Option<SubmitOptions>,
    
    /// Cached computed size in bytes (lazily computed, not serialized)
    #[serde(skip)]
    cached_size: OnceLock<usize>,
}

impl SyncItem {
    /// Create a new SyncItem with binary content.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sync_engine::SyncItem;
    ///
    /// // From raw bytes
    /// let item = SyncItem::new("id".into(), vec![1, 2, 3]);
    ///
    /// // From JSON (serialize yourself)
    /// let json = serde_json::to_vec(&serde_json::json!({"key": "value"})).unwrap();
    /// let item = SyncItem::new("id".into(), json);
    /// ```
    pub fn new(object_id: String, content: Vec<u8>) -> Self {
        Self {
            object_id,
            version: 1,
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            batch_id: None,
            trace_parent: None,
            trace_state: None,
            priority_score: 0.0,
            merkle_root: String::new(),
            last_accessed: 0,
            access_count: 0,
            content,
            home_instance_id: None,
            submit_options: None,  // Set via submit_with() if needed
            cached_size: OnceLock::new(),
        }
    }

    /// Create a new SyncItem from a JSON value (convenience method).
    ///
    /// This serializes the JSON to bytes for you. For better performance
    /// with binary formats (MessagePack, Cap'n Proto), use [`new`](Self::new).
    pub fn from_json(object_id: String, value: serde_json::Value) -> Self {
        let content = serde_json::to_vec(&value).unwrap_or_default();
        Self::new(object_id, content)
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

    /// Attach trace context from current span (for CRDT merge DAG linking)
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

/// A wrapper around a compacted CRDT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdtSnapshot {
    // TODO: Define structure based on crdt-data-types
    pub id: String,
    pub payload: Vec<u8>,
}

impl SizedItem for SyncItem {
    fn size_bytes(&self) -> usize {
        *self.cached_size.get_or_init(|| {
            // Approximate size: struct overhead + string lengths + content bytes
            std::mem::size_of::<Self>()
                + self.object_id.len()
                + self.trace_parent.as_ref().map_or(0, String::len)
                + self.trace_state.as_ref().map_or(0, String::len)
                + self.merkle_root.len()
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
        assert!(item.merkle_root.is_empty());
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
    fn test_crdt_snapshot() {
        let snapshot = CrdtSnapshot {
            id: "snap-1".to_string(),
            payload: vec![1, 2, 3, 4, 5],
        };
        
        assert_eq!(snapshot.id, "snap-1");
        assert_eq!(snapshot.payload.len(), 5);
        
        // Test serialization
        let json_str = serde_json::to_string(&snapshot).unwrap();
        let deserialized: CrdtSnapshot = serde_json::from_str(&json_str).unwrap();
        
        assert_eq!(deserialized.id, snapshot.id);
        assert_eq!(deserialized.payload, snapshot.payload);
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
}
