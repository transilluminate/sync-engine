//! Property-based tests (fuzzing) for sync engine resilience.
//!
//! Uses proptest to generate random/malformed inputs and verify the engine
//! never panics, only returns clean errors.
//!
//! Run with: `cargo test --test proptest_fuzz`

use proptest::prelude::*;
use serde_json::{json, Value};

use sync_engine::{SyncItem, SizedItem};

// =============================================================================
// Strategies for generating test data
// =============================================================================

/// Generate a valid SyncItem with random content
fn valid_sync_item_strategy() -> impl Strategy<Value = SyncItem> {
    (
        "[a-z]{1,10}(\\.[a-z]{1,10}){0,5}",  // object_id like "uk.nhs.patient"
        prop::collection::vec(any::<u8>(), 0..1000),  // random content bytes
    )
        .prop_map(|(id, bytes)| {
            let content = json!({
                "data": base64_encode(&bytes),
                "size": bytes.len(),
            });
            SyncItem::new(id, content)
        })
}

/// Generate arbitrary JSON values (including invalid structures)
fn arbitrary_json_strategy() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| Value::Number(n.into())),
        ".*".prop_map(Value::String),
    ];

    leaf.prop_recursive(
        4,   // depth
        64,  // max nodes
        10,  // items per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..10)
                    .prop_map(Value::Array),
                prop::collection::hash_map(".*", inner, 0..10)
                    .prop_map(|m| Value::Object(m.into_iter().collect())),
            ]
        },
    )
}

fn base64_encode(bytes: &[u8]) -> String {
    // Simple hex encoding for test data
    hex::encode(bytes)
}

// =============================================================================
// Deserialization Fuzz Tests
// =============================================================================

proptest! {
    /// SyncItem deserialization should never panic on arbitrary bytes
    #[test]
    fn fuzz_sync_item_from_random_bytes(bytes in prop::collection::vec(any::<u8>(), 0..10000)) {
        // Should never panic, only return Err
        let result: Result<SyncItem, _> = serde_json::from_slice(&bytes);
        // We don't care if it fails, just that it doesn't panic
        let _ = result;
    }

    /// SyncItem deserialization should handle arbitrary JSON gracefully
    #[test]
    fn fuzz_sync_item_from_arbitrary_json(json in arbitrary_json_strategy()) {
        let serialized = serde_json::to_vec(&json).unwrap();
        let result: Result<SyncItem, _> = serde_json::from_slice(&serialized);
        // Either parses (if JSON happens to match SyncItem shape) or fails cleanly
        let _ = result;
    }

    /// Corrupted serialized SyncItem should fail gracefully
    #[test]
    fn fuzz_corrupted_sync_item(
        item in valid_sync_item_strategy(),
        corruption in prop::collection::vec(any::<u8>(), 1..50),
        position in 0usize..10000,
    ) {
        let serialized = serde_json::to_vec(&item).unwrap();
        
        if serialized.is_empty() {
            return Ok(());
        }
        
        let mut corrupted = serialized.clone();
        let pos = position % corrupted.len();
        
        // Inject corruption
        for (i, b) in corruption.iter().enumerate() {
            let idx = (pos + i) % corrupted.len();
            corrupted[idx] ^= b; // XOR to corrupt
        }
        
        // Should never panic
        let result: Result<SyncItem, _> = serde_json::from_slice(&corrupted);
        let _ = result;
    }
}

// =============================================================================
// Merkle Hash Consistency Tests
// =============================================================================

proptest! {
    /// Merkle hashing should be deterministic
    #[test]
    fn prop_merkle_hash_deterministic(content in ".*") {
        use sha2::{Sha256, Digest};
        
        let hash1 = Sha256::digest(content.as_bytes());
        let hash2 = Sha256::digest(content.as_bytes());
        
        prop_assert_eq!(hash1, hash2, "Same content should produce same hash");
    }

    /// Different content should (almost always) produce different hashes
    #[test]
    fn prop_merkle_hash_collision_resistant(
        content1 in ".{1,100}",
        content2 in ".{1,100}",
    ) {
        use sha2::{Sha256, Digest};
        
        if content1 != content2 {
            let hash1 = Sha256::digest(content1.as_bytes());
            let hash2 = Sha256::digest(content2.as_bytes());
            
            // Collisions are astronomically unlikely with SHA256
            prop_assert_ne!(hash1.as_slice(), hash2.as_slice(), 
                "Different content should produce different hashes");
        }
    }
}

// =============================================================================
// SyncItem Invariant Tests
// =============================================================================

proptest! {
    /// SyncItem size calculation should never panic
    #[test]
    fn prop_sync_item_size_never_panics(
        id in ".*",
        content in arbitrary_json_strategy(),
    ) {
        let item = SyncItem::new(id, content);
        let size = item.size_bytes();
        prop_assert!(size > 0, "Size should always be positive");
    }

    /// SyncItem serialization roundtrip should preserve data
    #[test]
    fn prop_sync_item_roundtrip(item in valid_sync_item_strategy()) {
        let serialized = serde_json::to_vec(&item).unwrap();
        let deserialized: SyncItem = serde_json::from_slice(&serialized).unwrap();
        
        prop_assert_eq!(item.object_id, deserialized.object_id);
        prop_assert_eq!(item.version, deserialized.version);
        prop_assert_eq!(item.content, deserialized.content);
    }

    /// Object IDs with special characters should be handled
    #[test]
    fn prop_object_id_special_chars(
        id in "[\\x00-\\xFF]{0,255}",
    ) {
        let item = SyncItem::new(id.clone(), json!({"test": true}));
        prop_assert_eq!(&item.object_id, &id);
        
        // Should serialize/deserialize without panic
        if let Ok(serialized) = serde_json::to_vec(&item) {
            let _ = serde_json::from_slice::<SyncItem>(&serialized);
        }
    }
}

// =============================================================================
// Batch Operation Invariant Tests  
// =============================================================================

proptest! {
    /// Batch of items should maintain individual integrity
    #[test]
    fn prop_batch_integrity(
        items in prop::collection::vec(valid_sync_item_strategy(), 0..100),
    ) {
        let total_size: usize = items.iter().map(|i| i.size_bytes()).sum();
        prop_assert!(total_size >= items.len() * std::mem::size_of::<SyncItem>());
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

proptest! {
    /// Empty strings and null bytes should be handled
    #[test]
    fn prop_empty_and_null_handling(
        null_count in 0usize..100,
    ) {
        let id = "\0".repeat(null_count);
        let item = SyncItem::new(id.clone(), json!(null));
        
        prop_assert_eq!(item.object_id.len(), null_count);
        prop_assert!(item.size_bytes() > 0);
    }

    /// Very large content should not cause issues
    #[test]
    fn prop_large_content(
        size in 0usize..100_000,
    ) {
        let large_string = "x".repeat(size);
        let item = SyncItem::new("test.large".into(), json!({"data": large_string}));
        
        let computed_size = item.size_bytes();
        prop_assert!(computed_size >= size, "Size should include content");
    }

    /// Deeply nested JSON should be handled
    #[test]
    fn prop_deeply_nested_json(depth in 0usize..50) {
        let mut value = json!({"leaf": "data"});
        for i in 0..depth {
            value = json!({ format!("level_{}", i): value });
        }
        
        let item = SyncItem::new("test.nested".into(), value);
        prop_assert!(item.size_bytes() > 0);
        
        // Serialization should work
        let result = serde_json::to_vec(&item);
        prop_assert!(result.is_ok());
    }
}
