// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Path-based Merkle tree for hierarchical sync verification.
//!
//! Uses the natural tree structure from reverse DNS object IDs:
//! `uk.nhs.patient.record.123` becomes a path in the tree.
//!
//! Stored in Redis as a "shadow" alongside data - data can be evicted,
//! but Merkle nodes persist (they're tiny: 32 bytes per node).

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tracing::instrument;

/// A node in the path-based Merkle tree.
#[derive(Debug, Clone)]
pub struct MerkleNode {
    /// Hash of this node (computed from children or leaf value)
    pub hash: [u8; 32],
    /// Child segment -> child hash (empty for leaves)
    pub children: BTreeMap<String, [u8; 32]>,
    /// True if this is a leaf (actual item), false if interior node
    pub is_leaf: bool,
}

impl MerkleNode {
    /// Create a leaf node from a version hash.
    pub fn leaf(version_hash: [u8; 32]) -> Self {
        Self {
            hash: version_hash,
            children: BTreeMap::new(),
            is_leaf: true,
        }
    }

    /// Create an interior node, computing hash from children.
    pub fn interior(children: BTreeMap<String, [u8; 32]>) -> Self {
        let hash = Self::compute_hash(&children);
        Self {
            hash,
            children,
            is_leaf: false,
        }
    }

    /// Compute hash from sorted children (deterministic).
    fn compute_hash(children: &BTreeMap<String, [u8; 32]>) -> [u8; 32] {
        let mut hasher = Sha256::new();
        // BTreeMap is already sorted by key
        for (segment, child_hash) in children {
            hasher.update(segment.as_bytes());
            hasher.update(b":");
            hasher.update(child_hash);
            hasher.update(b";");
        }
        hasher.finalize().into()
    }

    /// Recompute hash after children changed.
    pub fn recompute_hash(&mut self) {
        if !self.is_leaf {
            self.hash = Self::compute_hash(&self.children);
        }
    }

    /// Add or update a child, returns true if hash changed.
    pub fn set_child(&mut self, segment: String, child_hash: [u8; 32]) -> bool {
        let old_hash = self.hash;
        self.children.insert(segment, child_hash);
        self.recompute_hash();
        self.hash != old_hash
    }

    /// Remove a child, returns true if hash changed.
    pub fn remove_child(&mut self, segment: &str) -> bool {
        let old_hash = self.hash;
        self.children.remove(segment);
        self.recompute_hash();
        self.hash != old_hash
    }
}

/// Manages path-based Merkle tree operations.
///
/// This is the local computation layer - storage is handled by RedisMerkleStore.
#[derive(Debug)]
pub struct PathMerkle;

impl PathMerkle {
    /// Split an object_id into path segments.
    ///
    /// `uk.nhs.patient.record.123` -> `["uk", "nhs", "patient", "record", "123"]`
    #[inline]
    pub fn split_path(object_id: &str) -> Vec<&str> {
        object_id.split('.').collect()
    }

    /// Get the parent prefix for a path.
    ///
    /// `uk.nhs.patient.record.123` -> `uk.nhs.patient.record`
    /// `uk` -> `""` (root)
    pub fn parent_prefix(path: &str) -> &str {
        match path.rfind('.') {
            Some(idx) => &path[..idx],
            None => "",
        }
    }

    /// Get all ancestor prefixes for a path (excluding root, including self).
    ///
    /// `uk.nhs.patient` -> `["uk", "uk.nhs", "uk.nhs.patient"]`
    pub fn ancestor_prefixes(path: &str) -> Vec<String> {
        let segments: Vec<&str> = path.split('.').collect();
        let mut prefixes = Vec::with_capacity(segments.len());
        let mut current = String::new();
        
        for (i, segment) in segments.iter().enumerate() {
            if i > 0 {
                current.push('.');
            }
            current.push_str(segment);
            prefixes.push(current.clone());
        }
        
        prefixes
    }

    /// Compute the leaf hash for a sync item.
    ///
    /// Hash = SHA256(object_id || version || timestamp || payload_hash)
    #[instrument(skip(payload_hash))]
    pub fn leaf_hash(
        object_id: &str,
        version: u64,
        timestamp: i64,
        payload_hash: &[u8; 32],
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(object_id.as_bytes());
        hasher.update(b"|");
        hasher.update(version.to_le_bytes());
        hasher.update(b"|");
        hasher.update(timestamp.to_le_bytes());
        hasher.update(b"|");
        hasher.update(payload_hash);
        hasher.finalize().into()
    }

    /// Compute hash of a payload (for leaf_hash input).
    pub fn payload_hash(payload: &[u8]) -> [u8; 32] {
        Sha256::digest(payload).into()
    }
}

/// Batch of Merkle updates to apply atomically.
#[derive(Debug, Default)]
pub struct MerkleBatch {
    /// Leaf updates: object_id -> new hash (or None to delete)
    pub leaves: BTreeMap<String, Option<[u8; 32]>>,
}

impl MerkleBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a leaf update.
    pub fn insert(&mut self, object_id: String, hash: [u8; 32]) {
        self.leaves.insert(object_id, Some(hash));
    }

    /// Mark a leaf for deletion.
    pub fn delete(&mut self, object_id: String) {
        self.leaves.insert(object_id, None);
    }

    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// Get all affected prefixes (for bubble-up updates).
    ///
    /// Returns prefixes in bottom-up order (leaves first, root last).
    pub fn affected_prefixes(&self) -> Vec<String> {
        use std::collections::BTreeSet;
        
        let mut all_prefixes = BTreeSet::new();
        
        for object_id in self.leaves.keys() {
            for prefix in PathMerkle::ancestor_prefixes(object_id) {
                all_prefixes.insert(prefix);
            }
        }
        
        // Convert to vec and reverse (we want bottom-up for processing)
        let mut prefixes: Vec<_> = all_prefixes.into_iter().collect();
        // Sort by depth (more dots = deeper = process first)
        prefixes.sort_by(|a, b| {
            let depth_a = a.matches('.').count();
            let depth_b = b.matches('.').count();
            depth_b.cmp(&depth_a) // Reverse: deeper first
        });
        
        prefixes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_path() {
        assert_eq!(
            PathMerkle::split_path("uk.nhs.patient.record.123"),
            vec!["uk", "nhs", "patient", "record", "123"]
        );
    }

    #[test]
    fn test_parent_prefix() {
        assert_eq!(PathMerkle::parent_prefix("uk.nhs.patient"), "uk.nhs");
        assert_eq!(PathMerkle::parent_prefix("uk"), "");
    }

    #[test]
    fn test_ancestor_prefixes() {
        assert_eq!(
            PathMerkle::ancestor_prefixes("uk.nhs.patient"),
            vec!["uk", "uk.nhs", "uk.nhs.patient"]
        );
    }

    #[test]
    fn test_merkle_node_hash_deterministic() {
        let mut children = BTreeMap::new();
        children.insert("a".to_string(), [1u8; 32]);
        children.insert("b".to_string(), [2u8; 32]);
        
        let node1 = MerkleNode::interior(children.clone());
        let node2 = MerkleNode::interior(children);
        
        assert_eq!(node1.hash, node2.hash);
    }

    #[test]
    fn test_merkle_batch_affected_prefixes() {
        let mut batch = MerkleBatch::new();
        batch.insert("uk.nhs.patient.123".to_string(), [1u8; 32]);
        batch.insert("uk.nhs.doctor.456".to_string(), [2u8; 32]);
        
        let prefixes = batch.affected_prefixes();
        
        // Should be sorted deepest first
        assert!(prefixes.iter().position(|p| p == "uk.nhs.patient")
            < prefixes.iter().position(|p| p == "uk.nhs"));
        assert!(prefixes.iter().position(|p| p == "uk.nhs")
            < prefixes.iter().position(|p| p == "uk"));
    }
}
