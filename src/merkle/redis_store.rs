//! Redis storage for Merkle tree nodes.
//!
//! Merkle nodes are stored separately from data and are NEVER evicted.
//! They're tiny (32 bytes + overhead) and critical for sync verification.

use super::path_tree::{MerkleBatch, MerkleNode};
use crate::StorageError;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::collections::BTreeMap;
use tracing::{debug, instrument};

/// Redis key prefixes for Merkle storage.
const MERKLE_HASH_PREFIX: &str = "merkle:hash:";
const MERKLE_CHILDREN_PREFIX: &str = "merkle:children:";

/// Redis-backed Merkle tree storage.
///
/// Uses two key patterns:
/// - `merkle:hash:{prefix}` -> 32-byte hash (string, hex-encoded)
/// - `merkle:children:{prefix}` -> sorted set of `segment:hash` pairs
#[derive(Clone)]
pub struct RedisMerkleStore {
    conn: ConnectionManager,
}

impl RedisMerkleStore {
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    /// Get the hash for a prefix (interior node or leaf).
    #[instrument(skip(self))]
    pub async fn get_hash(&self, prefix: &str) -> Result<Option<[u8; 32]>, StorageError> {
        let key = format!("{}{}", MERKLE_HASH_PREFIX, prefix);
        let mut conn = self.conn.clone();
        
        let result: Option<String> = conn.get(&key).await.map_err(|e| {
            StorageError::Backend(format!("Failed to get merkle hash: {}", e))
        })?;
        
        match result {
            Some(hex_str) => {
                let bytes = hex::decode(&hex_str).map_err(|e| {
                    StorageError::Backend(format!("Invalid merkle hash hex: {}", e))
                })?;
                if bytes.len() != 32 {
                    return Err(StorageError::Backend(format!(
                        "Invalid merkle hash length: {}",
                        bytes.len()
                    )));
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&bytes);
                Ok(Some(hash))
            }
            None => Ok(None),
        }
    }

    /// Get children of an interior node.
    #[instrument(skip(self))]
    pub async fn get_children(
        &self,
        prefix: &str,
    ) -> Result<BTreeMap<String, [u8; 32]>, StorageError> {
        let key = format!("{}{}", MERKLE_CHILDREN_PREFIX, prefix);
        let mut conn = self.conn.clone();
        
        // ZRANGE returns members as strings
        let members: Vec<String> = conn.zrange(&key, 0, -1).await.map_err(|e| {
            StorageError::Backend(format!("Failed to get merkle children: {}", e))
        })?;
        
        let mut children: BTreeMap<String, [u8; 32]> = BTreeMap::new();
        for member in &members {
            // member format: "segment:hexhash"
            let member_str: &str = member.as_str();
            if let Some((segment, hash_hex)) = member_str.split_once(':') {
                let bytes = hex::decode(hash_hex).map_err(|e| {
                    StorageError::Backend(format!("Invalid child hash hex: {}", e))
                })?;
                if bytes.len() == 32 {
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(&bytes);
                    children.insert(segment.to_string(), hash);
                }
            }
        }
        
        Ok(children)
    }

    /// Get a full node (hash + children).
    pub async fn get_node(&self, prefix: &str) -> Result<Option<MerkleNode>, StorageError> {
        let hash = self.get_hash(prefix).await?;
        
        match hash {
            Some(h) => {
                let children: BTreeMap<String, [u8; 32]> = self.get_children(prefix).await?;
                Ok(Some(if children.is_empty() {
                    MerkleNode::leaf(h)
                } else {
                    MerkleNode {
                        hash: h,
                        children,
                        is_leaf: false,
                    }
                }))
            }
            None => Ok(None),
        }
    }

    /// Apply a batch of Merkle updates atomically.
    ///
    /// This handles the full bubble-up: updates leaves, then recomputes
    /// all affected interior nodes bottom-up.
    #[instrument(skip(self, batch), fields(batch_size = batch.len()))]
    pub async fn apply_batch(&self, batch: &MerkleBatch) -> Result<(), StorageError> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Step 1: Apply leaf updates
        for (object_id, maybe_hash) in &batch.leaves {
            let hash_key = format!("{}{}", MERKLE_HASH_PREFIX, object_id);
            
            match maybe_hash {
                Some(hash) => {
                    let hex_str = hex::encode(hash);
                    pipe.set(&hash_key, &hex_str);
                    debug!(object_id = %object_id, "Setting leaf hash");
                }
                None => {
                    pipe.del(&hash_key);
                    debug!(object_id = %object_id, "Deleting leaf hash");
                }
            }
        }

        // Execute leaf updates first
        pipe.query_async::<()>(&mut conn).await.map_err(|e| {
            StorageError::Backend(format!("Failed to apply merkle leaf updates: {}", e))
        })?;

        // Step 2: Bubble up - recompute interior nodes bottom-up
        let affected_prefixes = batch.affected_prefixes();
        
        for prefix in affected_prefixes {
            self.recompute_interior_node(&prefix).await?;
        }

        Ok(())
    }

    /// Recompute an interior node's hash from its children.
    #[instrument(skip(self))]
    async fn recompute_interior_node(&self, prefix: &str) -> Result<(), StorageError> {
        let mut conn = self.conn.clone();
        
        // Build the prefix for finding direct children
        let prefix_with_dot = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}.", prefix)
        };
        
        // Use SCAN instead of KEYS to avoid blocking Redis
        let scan_pattern = if prefix.is_empty() {
            format!("{}*", MERKLE_HASH_PREFIX)
        } else {
            format!("{}{}.*", MERKLE_HASH_PREFIX, prefix)
        };
        
        let mut keys: Vec<String> = Vec::new();
        let mut cursor = 0u64;
        
        loop {
            let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&scan_pattern)
                .arg("COUNT")
                .arg(100)  // Fetch 100 keys at a time
                .query_async(&mut conn)
                .await
                .map_err(|e| StorageError::Backend(format!("Failed to scan merkle keys: {}", e)))?;
            
            keys.extend(batch);
            cursor = new_cursor;
            
            if cursor == 0 {
                break;
            }
        }
        
        let mut children: BTreeMap<String, [u8; 32]> = BTreeMap::new();
        
        for key in &keys {
            // Extract the path from the key
            let path: &str = key.strip_prefix(MERKLE_HASH_PREFIX).unwrap_or(key.as_str());
            
            // Check if this is a direct child
            let suffix: &str = if prefix.is_empty() {
                path
            } else {
                match path.strip_prefix(&prefix_with_dot) {
                    Some(s) => s,
                    None => continue,
                }
            };
            
            // Direct child has no dots in suffix (take first segment only)
            if let Some(segment) = suffix.split('.').next() {
                // Only if segment IS the whole suffix (no more dots)
                if segment == suffix || !suffix.contains('.') {
                    // This is a direct child - but we need the full child path
                    let child_path = if prefix.is_empty() {
                        segment.to_string()
                    } else {
                        format!("{}.{}", prefix, segment)
                    };
                    
                    if let Some(hash) = self.get_hash(&child_path).await? {
                        children.insert(segment.to_string(), hash);
                    }
                }
            }
        }

        if children.is_empty() {
            // No children, this might be a leaf or deleted node
            return Ok(());
        }

        // Compute new hash
        let node = MerkleNode::interior(children.clone());
        let hash_hex = hex::encode(node.hash);
        
        // Update hash and children set
        let hash_key = format!("{}{}", MERKLE_HASH_PREFIX, prefix);
        let children_key = format!("{}{}", MERKLE_CHILDREN_PREFIX, prefix);
        
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.set(&hash_key, &hash_hex);
        
        // Clear and rebuild children set
        pipe.del(&children_key);
        for (segment, hash) in &children {
            let member = format!("{}:{}", segment, hex::encode(hash));
            pipe.zadd(&children_key, &member, 0i64);
        }
        
        pipe.query_async::<()>(&mut conn).await.map_err(|e| {
            StorageError::Backend(format!("Failed to update interior node: {}", e))
        })?;

        debug!(prefix = %prefix, children_count = children.len(), "Recomputed interior node");
        
        Ok(())
    }

    /// Get the root hash (empty prefix = root of tree).
    pub async fn root_hash(&self) -> Result<Option<[u8; 32]>, StorageError> {
        // The root is the hash of all top-level segments
        self.recompute_interior_node("").await?;
        
        // Root hash is stored at ""
        let key = MERKLE_HASH_PREFIX.to_string();
        let mut conn = self.conn.clone();
        
        let result: Option<String> = conn.get(&key).await.map_err(|e| {
            StorageError::Backend(format!("Failed to get root hash: {}", e))
        })?;
        
        match result {
            Some(hex_str) => {
                let bytes = hex::decode(&hex_str).map_err(|e| {
                    StorageError::Backend(format!("Invalid root hash hex: {}", e))
                })?;
                if bytes.len() != 32 {
                    return Err(StorageError::Backend(format!(
                        "Invalid root hash length: {}",
                        bytes.len()
                    )));
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&bytes);
                Ok(Some(hash))
            }
            None => Ok(None),
        }
    }

    /// Compare hashes and find differing branches.
    ///
    /// Returns prefixes where our hash differs from theirs.
    #[instrument(skip(self, their_children))]
    pub async fn diff_children(
        &self,
        prefix: &str,
        their_children: &BTreeMap<String, [u8; 32]>,
    ) -> Result<Vec<String>, StorageError> {
        let our_children: BTreeMap<String, [u8; 32]> = self.get_children(prefix).await?;
        let mut diffs = Vec::new();
        
        let prefix_with_dot = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}.", prefix)
        };

        // Find segments where hashes differ or we have but they don't
        for (segment, our_hash) in &our_children {
            match their_children.get(segment) {
                Some(their_hash) if their_hash != our_hash => {
                    diffs.push(format!("{}{}", prefix_with_dot, segment));
                }
                None => {
                    // We have it, they don't
                    diffs.push(format!("{}{}", prefix_with_dot, segment));
                }
                _ => {} // Hashes match
            }
        }

        // Find segments they have but we don't
        for segment in their_children.keys() {
            if !our_children.contains_key(segment) {
                diffs.push(format!("{}{}", prefix_with_dot, segment));
            }
        }

        Ok(diffs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_prefixes() {
        assert_eq!(
            format!("{}{}", MERKLE_HASH_PREFIX, "uk.nhs.patient"),
            "merkle:hash:uk.nhs.patient"
        );
        assert_eq!(
            format!("{}{}", MERKLE_CHILDREN_PREFIX, "uk.nhs"),
            "merkle:children:uk.nhs"
        );
    }
}
