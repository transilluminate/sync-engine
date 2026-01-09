// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Redis cache for SQL Merkle tree (shadow copy).
//!
//! This is a **read cache** of the SQL Merkle tree, NOT a Merkle tree of Redis data.
//! SQL is ground truth - this just provides fast reads for cold path sync.
//!
//! # Design
//!
//! - SQL Merkle store does all tree computation (hashing, bubble-up)
//! - After each SQL batch, we copy the affected nodes to Redis
//! - Cold path queries read from this cache instead of SQL
//! - If cache is empty/stale, we fall back to SQL
//!
//! # Keys
//!
//! - `{prefix}merkle:hash:{path}` → 32-byte hash (hex-encoded)
//! - `{prefix}merkle:children:{path}` → sorted set of `segment:hash` pairs

use super::path_tree::MerkleNode;
use crate::StorageError;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::collections::BTreeMap;
use tracing::{debug, instrument};

/// Redis key prefixes for Merkle cache.
const MERKLE_HASH_PREFIX: &str = "merkle:hash:";
const MERKLE_CHILDREN_PREFIX: &str = "merkle:children:";

/// Redis cache for SQL Merkle tree.
///
/// This is a **dumb cache** - no tree computation happens here.
/// All tree logic (hashing, bubble-up) is handled by SqlMerkleStore.
#[derive(Clone)]
pub struct MerkleCacheStore {
    conn: ConnectionManager,
    /// Key prefix for namespacing (e.g., "node-a:" → "node-a:merkle:hash:...")
    prefix: String,
}

impl MerkleCacheStore {
    /// Create a new cache store without a prefix.
    pub fn new(conn: ConnectionManager) -> Self {
        Self::with_prefix(conn, None)
    }
    
    /// Create a new cache store with an optional prefix.
    pub fn with_prefix(conn: ConnectionManager, prefix: Option<&str>) -> Self {
        Self { 
            conn,
            prefix: prefix.unwrap_or("").to_string(),
        }
    }
    
    /// Build the full key with prefix.
    #[inline]
    fn prefixed_key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}{}", self.prefix, suffix)
        }
    }
    
    /// Get the prefix used for all merkle keys.
    pub fn key_prefix(&self) -> &str {
        &self.prefix
    }

    // =========================================================================
    // Read Operations (for cold path sync)
    // =========================================================================

    /// Get the hash for a path (root = "").
    #[instrument(skip(self))]
    pub async fn get_hash(&self, path: &str) -> Result<Option<[u8; 32]>, StorageError> {
        let key = self.prefixed_key(&format!("{}{}", MERKLE_HASH_PREFIX, path));
        let mut conn = self.conn.clone();
        
        let result: Option<String> = conn.get(&key).await.map_err(|e| {
            StorageError::Backend(format!("Failed to get merkle hash from cache: {}", e))
        })?;
        
        match result {
            Some(hex_str) => {
                let bytes = hex::decode(&hex_str).map_err(|e| {
                    StorageError::Backend(format!("Invalid merkle hash hex in cache: {}", e))
                })?;
                if bytes.len() != 32 {
                    return Err(StorageError::Backend(format!(
                        "Invalid merkle hash length in cache: {}",
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

    /// Get the root hash (path = "").
    pub async fn root_hash(&self) -> Result<Option<[u8; 32]>, StorageError> {
        self.get_hash("").await
    }

    /// Get children of an interior node.
    #[instrument(skip(self))]
    pub async fn get_children(
        &self,
        path: &str,
    ) -> Result<BTreeMap<String, [u8; 32]>, StorageError> {
        let key = self.prefixed_key(&format!("{}{}", MERKLE_CHILDREN_PREFIX, path));
        let mut conn = self.conn.clone();
        
        // ZRANGE returns members as strings
        let members: Vec<String> = conn.zrange(&key, 0, -1).await.map_err(|e| {
            StorageError::Backend(format!("Failed to get merkle children from cache: {}", e))
        })?;
        
        let mut children: BTreeMap<String, [u8; 32]> = BTreeMap::new();
        for member in &members {
            // member format: "segment:hexhash"
            if let Some((segment, hash_hex)) = member.split_once(':') {
                if let Ok(bytes) = hex::decode(hash_hex) {
                    if bytes.len() == 32 {
                        let mut hash = [0u8; 32];
                        hash.copy_from_slice(&bytes);
                        children.insert(segment.to_string(), hash);
                    }
                }
            }
        }
        
        Ok(children)
    }

    /// Get a full node (hash + children).
    pub async fn get_node(&self, path: &str) -> Result<Option<MerkleNode>, StorageError> {
        let hash = self.get_hash(path).await?;
        
        match hash {
            Some(h) => {
                let children = self.get_children(path).await?;
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

    // =========================================================================
    // Write Operations (copy from SQL)
    // =========================================================================

    /// Copy a node from SQL to this cache.
    ///
    /// Called after SQL merkle batch completes. Copies both hash and children.
    pub async fn cache_node(
        &self,
        path: &str,
        hash: [u8; 32],
        children: &BTreeMap<String, [u8; 32]>,
    ) -> Result<(), StorageError> {
        let hash_key = self.prefixed_key(&format!("{}{}", MERKLE_HASH_PREFIX, path));
        let children_key = self.prefixed_key(&format!("{}{}", MERKLE_CHILDREN_PREFIX, path));
        
        let hash_hex = hex::encode(hash);
        let mut conn = self.conn.clone();
        
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        // Set hash
        pipe.set(&hash_key, &hash_hex);
        
        // Clear and rebuild children set (if any)
        pipe.del(&children_key);
        for (segment, child_hash) in children {
            let member = format!("{}:{}", segment, hex::encode(child_hash));
            pipe.zadd(&children_key, &member, 0i64);
        }
        
        pipe.query_async::<()>(&mut conn).await.map_err(|e| {
            StorageError::Backend(format!("Failed to cache merkle node: {}", e))
        })?;
        
        Ok(())
    }

    /// Delete a node from cache.
    pub async fn delete_node(&self, path: &str) -> Result<(), StorageError> {
        let hash_key = self.prefixed_key(&format!("{}{}", MERKLE_HASH_PREFIX, path));
        let children_key = self.prefixed_key(&format!("{}{}", MERKLE_CHILDREN_PREFIX, path));
        
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.del(&hash_key);
        pipe.del(&children_key);
        
        pipe.query_async::<()>(&mut conn).await.map_err(|e| {
            StorageError::Backend(format!("Failed to delete cached merkle node: {}", e))
        })?;
        
        Ok(())
    }

    /// Sync entire SQL merkle tree to cache.
    ///
    /// Useful on startup or after cache invalidation.
    /// Reads all nodes from SQL and copies to Redis.
    #[instrument(skip(self, sql_store))]
    pub async fn sync_from_sql(
        &self,
        sql_store: &super::SqlMerkleStore,
    ) -> Result<usize, StorageError> {
        // Get all nodes from SQL
        let nodes = sql_store.get_all_nodes().await?;
        let count = nodes.len();
        
        if count == 0 {
            debug!("No SQL merkle nodes to cache");
            return Ok(0);
        }
        
        // Copy each node to Redis
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        for (path, hash, children) in &nodes {
            let hash_key = self.prefixed_key(&format!("{}{}", MERKLE_HASH_PREFIX, path));
            let children_key = self.prefixed_key(&format!("{}{}", MERKLE_CHILDREN_PREFIX, path));
            
            pipe.set(&hash_key, hex::encode(hash));
            pipe.del(&children_key);
            
            for (segment, child_hash) in children {
                let member = format!("{}:{}", segment, hex::encode(child_hash));
                pipe.zadd(&children_key, &member, 0i64);
            }
        }
        
        pipe.query_async::<()>(&mut conn).await.map_err(|e| {
            StorageError::Backend(format!("Failed to sync merkle cache from SQL: {}", e))
        })?;
        
        debug!(nodes_cached = count, "Synced SQL merkle tree to cache");
        Ok(count)
    }

    /// Sync only affected paths from SQL to cache.
    ///
    /// More efficient than full sync for incremental updates.
    /// Syncs the leaves, their ancestors, and the root.
    #[instrument(skip(self, sql_store, affected_paths))]
    pub async fn sync_affected_from_sql(
        &self,
        sql_store: &super::SqlMerkleStore,
        affected_paths: &[String],
    ) -> Result<usize, StorageError> {
        use std::collections::HashSet;
        use super::PathMerkle;
        
        if affected_paths.is_empty() {
            return Ok(0);
        }
        
        // Collect all paths that need syncing (leaves + ancestors + root)
        let mut paths_to_sync: HashSet<String> = HashSet::new();
        paths_to_sync.insert(String::new()); // Always sync root
        
        for path in affected_paths {
            paths_to_sync.insert(path.clone());
            // Add all ancestor prefixes
            for ancestor in PathMerkle::ancestor_prefixes(path) {
                paths_to_sync.insert(ancestor);
            }
        }
        
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();
        let mut count = 0;
        
        for path in &paths_to_sync {
            // Get hash and children from SQL
            if let Ok(Some(hash)) = sql_store.get_hash(path).await {
                let hash_key = self.prefixed_key(&format!("{}{}", MERKLE_HASH_PREFIX, path));
                pipe.set(&hash_key, hex::encode(hash));
                count += 1;
                
                // Sync children for interior nodes
                if let Ok(children) = sql_store.get_children(path).await {
                    if !children.is_empty() {
                        let children_key = self.prefixed_key(&format!("{}{}", MERKLE_CHILDREN_PREFIX, path));
                        pipe.del(&children_key);
                        for (segment, child_hash) in &children {
                            let member = format!("{}:{}", segment, hex::encode(child_hash));
                            pipe.zadd(&children_key, &member, 0i64);
                        }
                    }
                }
            }
        }
        
        if count > 0 {
            pipe.query_async::<()>(&mut conn).await.map_err(|e| {
                StorageError::Backend(format!("Failed to sync affected merkle nodes to cache: {}", e))
            })?;
        }
        
        debug!(paths_synced = count, "Synced affected merkle paths to cache");
        Ok(count)
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
        let our_children = self.get_children(prefix).await?;
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
                    diffs.push(format!("{}{}", prefix_with_dot, segment));
                }
                _ => {}
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
