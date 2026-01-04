// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Public Merkle tree accessor API for external sync.
//!
//! This module exposes merkle tree queries for use by external nodes in a 
//! multi-instance deployment. The typical sync flow:
//!
//! ```text
//! Remote Node                           Local Node
//!     │                                      │
//!     ├──── "What's your root?" ────────────►│
//!     │◄──── "0xABCD" ─────────────────────┤
//!     │                                      │
//!     │  (Hmm, mine is 0x1234, different!)   │
//!     │                                      │
//!     ├──── "Children of root?" ────────────►│
//!     │◄──── [{path:"A",hash:"..."}, ...] ───┤
//!     │                                      │
//!     │  (A matches, B differs!)             │
//!     │                                      │
//!     ├──── "Children of B?" ───────────────►│
//!     │◄──── [{path:"BA",hash:"..."}, ...] ──┤
//!     │                                      │
//!     │  (Found it! BA.leaf.123 differs)     │
//!     │                                      │
//!     ├──── "Get sync_item BA.leaf.123" ────►│
//!     │◄──── { full CRDT data } ─────────────┤
//! ```

use std::collections::BTreeMap;
use tracing::{debug, instrument};

use crate::merkle::MerkleNode;
use crate::storage::traits::StorageError;

use super::SyncEngine;

/// Result of comparing merkle trees between two nodes.
#[derive(Debug, Clone)]
pub struct MerkleDiff {
    /// Paths where hashes differ (needs sync)
    pub divergent_paths: Vec<String>,
    /// Paths that exist locally but not remotely (local additions)
    pub local_only: Vec<String>,
    /// Paths that exist remotely but not locally (remote additions)
    pub remote_only: Vec<String>,
}

impl SyncEngine {
    // ═══════════════════════════════════════════════════════════════════════════
    // Public Merkle API: For external sync between nodes
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the current merkle root hash.
    ///
    /// Returns the root hash from SQL (ground truth), or from Redis if they match.
    /// This is the starting point for sync verification.
    ///
    /// # Returns
    /// - `Some([u8; 32])` - The root hash
    /// - `None` - No data has been written yet (empty tree)
    #[instrument(skip(self))]
    pub async fn get_merkle_root(&self) -> Result<Option<[u8; 32]>, StorageError> {
        // SQL is ground truth - always prefer it
        if let Some(ref sql_merkle) = self.sql_merkle {
            return sql_merkle.root_hash().await;
        }
        
        // Fall back to Redis if no SQL
        if let Some(ref redis_merkle) = self.redis_merkle {
            return redis_merkle.get_hash("").await;
        }
        
        Ok(None)
    }
    
    /// Get the hash for a specific merkle path.
    ///
    /// Serves from Redis if `redis_root == sql_root` (fast path), 
    /// otherwise falls back to SQL (slow path).
    ///
    /// # Arguments
    /// * `path` - The merkle path (e.g., "uk.nhs.patient")
    ///
    /// # Returns
    /// - `Some([u8; 32])` - The hash at this path
    /// - `None` - Path doesn't exist in the tree
    #[instrument(skip(self))]
    pub async fn get_merkle_hash(&self, path: &str) -> Result<Option<[u8; 32]>, StorageError> {
        // Check if Redis shadow is in sync with SQL ground truth
        if self.is_merkle_synced().await? {
            // Fast path: serve from Redis
            if let Some(ref redis_merkle) = self.redis_merkle {
                return redis_merkle.get_hash(path).await;
            }
        }
        
        // Slow path: query SQL
        if let Some(ref sql_merkle) = self.sql_merkle {
            return sql_merkle.get_hash(path).await;
        }
        
        Ok(None)
    }
    
    /// Get a full merkle node (hash + children).
    ///
    /// Use this for tree traversal during sync.
    ///
    /// # Arguments
    /// * `path` - The merkle path (use "" for root)
    #[instrument(skip(self))]
    pub async fn get_merkle_node(&self, path: &str) -> Result<Option<MerkleNode>, StorageError> {
        // Check if Redis shadow is in sync
        if self.is_merkle_synced().await? {
            if let Some(ref redis_merkle) = self.redis_merkle {
                return redis_merkle.get_node(path).await;
            }
        }
        
        // Fall back to SQL
        if let Some(ref sql_merkle) = self.sql_merkle {
            return sql_merkle.get_node(path).await;
        }
        
        Ok(None)
    }
    
    /// Get children of a merkle path.
    ///
    /// Returns a map of `segment -> hash` for all direct children.
    /// Use this to traverse the tree level by level.
    ///
    /// # Arguments
    /// * `path` - Parent path (use "" for top-level children)
    ///
    /// # Example
    /// ```text
    /// path="" → {"uk": 0xABC, "us": 0xDEF}
    /// path="uk" → {"nhs": 0x123, "private": 0x456}
    /// ```
    #[instrument(skip(self))]
    pub async fn get_merkle_children(&self, path: &str) -> Result<BTreeMap<String, [u8; 32]>, StorageError> {
        // Check if Redis shadow is in sync
        if self.is_merkle_synced().await? {
            if let Some(ref redis_merkle) = self.redis_merkle {
                return redis_merkle.get_children(path).await;
            }
        }
        
        // Fall back to SQL
        if let Some(ref sql_merkle) = self.sql_merkle {
            return sql_merkle.get_children(path).await;
        }
        
        Ok(BTreeMap::new())
    }
    
    /// Find paths where local and remote merkle trees diverge.
    ///
    /// This is the main sync algorithm - given remote node's hashes,
    /// find the minimal set of paths that need to be synced.
    ///
    /// # Arguments
    /// * `remote_nodes` - Pairs of (path, hash) from the remote node
    ///
    /// # Returns
    /// Paths where hashes differ (need to fetch/push data)
    ///
    /// # Algorithm
    /// For each remote (path, hash) pair:
    /// 1. Get local hash for that path
    /// 2. If hashes match → subtree is synced, skip
    /// 3. If hashes differ → add to divergent list
    /// 4. If local missing → add to remote_only
    /// 5. If remote missing → add to local_only
    #[instrument(skip(self, remote_nodes), fields(remote_count = remote_nodes.len()))]
    pub async fn find_divergent_paths(
        &self,
        remote_nodes: &[(String, [u8; 32])],
    ) -> Result<MerkleDiff, StorageError> {
        let mut divergent_paths = Vec::new();
        let remote_only = Vec::new(); // Paths remote has, we don't
        let local_only = Vec::new();  // Paths we have, remote doesn't
        
        for (path, remote_hash) in remote_nodes {
            let local_hash = self.get_merkle_hash(path).await?;
            
            match local_hash {
                Some(local) if local == *remote_hash => {
                    // Hashes match - this subtree is synced
                    debug!(path = %path, "Merkle path synced");
                }
                Some(local) => {
                    // Hashes differ - need to sync this path
                    debug!(
                        path = %path, 
                        local = %hex::encode(local),
                        remote = %hex::encode(remote_hash),
                        "Merkle path diverged"
                    );
                    divergent_paths.push(path.clone());
                }
                None => {
                    // We don't have this path at all
                    debug!(path = %path, "Path exists on remote only");
                    divergent_paths.push(path.clone());
                }
            }
        }
        
        Ok(MerkleDiff {
            divergent_paths,
            local_only,
            remote_only,
        })
    }
    
    /// Check if Redis merkle tree is in sync with SQL ground truth.
    ///
    /// This is used to determine whether we can serve merkle queries from Redis
    /// (fast) or need to fall back to SQL (slow but authoritative).
    #[instrument(skip(self))]
    pub async fn is_merkle_synced(&self) -> Result<bool, StorageError> {
        let sql_root = if let Some(ref sql_merkle) = self.sql_merkle {
            sql_merkle.root_hash().await?
        } else {
            return Ok(false);
        };
        
        let redis_root = if let Some(ref redis_merkle) = self.redis_merkle {
            redis_merkle.get_hash("").await?
        } else {
            return Ok(false);
        };
        
        Ok(sql_root == redis_root)
    }
    
    /// Drill down the merkle tree to find all divergent leaf paths.
    ///
    /// Starting from a known-divergent path, recursively descend until
    /// we find the actual leaves that need syncing.
    ///
    /// # Arguments
    /// * `start_path` - A path known to have divergent hashes
    /// * `remote_children` - Function to get children from remote node
    ///
    /// # Returns
    /// List of leaf paths that need to be synced
    #[instrument(skip(self, remote_children))]
    pub async fn drill_down_divergence<F, Fut>(
        &self,
        start_path: &str,
        remote_children: F,
    ) -> Result<Vec<String>, StorageError>
    where
        F: Fn(String) -> Fut,
        Fut: std::future::Future<Output = Result<BTreeMap<String, [u8; 32]>, StorageError>>,
    {
        let mut divergent_leaves = Vec::new();
        let mut paths_to_check = vec![start_path.to_string()];
        
        while let Some(path) = paths_to_check.pop() {
            let local_children = self.get_merkle_children(&path).await?;
            let remote = remote_children(path.clone()).await?;
            
            // Find which children differ
            for (segment, remote_hash) in &remote {
                let child_path = if path.is_empty() {
                    segment.clone()
                } else {
                    format!("{}.{}", path, segment)
                };
                
                match local_children.get(segment) {
                    Some(local_hash) if local_hash == remote_hash => {
                        // Child is synced
                    }
                    Some(_) => {
                        // Child differs - check if leaf or drill deeper
                        let node = self.get_merkle_node(&child_path).await?;
                        if node.map(|n| n.is_leaf).unwrap_or(true) {
                            divergent_leaves.push(child_path);
                        } else {
                            paths_to_check.push(child_path);
                        }
                    }
                    None => {
                        // We don't have this child - it's a missing subtree
                        divergent_leaves.push(child_path);
                    }
                }
            }
            
            // Check for children we have that remote doesn't
            for segment in local_children.keys() {
                if !remote.contains_key(segment) {
                    let child_path = if path.is_empty() {
                        segment.clone()
                    } else {
                        format!("{}.{}", path, segment)
                    };
                    // Remote missing this - they need it from us
                    divergent_leaves.push(child_path);
                }
            }
        }
        
        Ok(divergent_leaves)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_merkle_diff_struct() {
        let diff = MerkleDiff {
            divergent_paths: vec!["uk.nhs".to_string()],
            local_only: vec!["uk.private".to_string()],
            remote_only: vec!["us.medicare".to_string()],
        };
        
        assert_eq!(diff.divergent_paths.len(), 1);
        assert_eq!(diff.local_only.len(), 1);
        assert_eq!(diff.remote_only.len(), 1);
    }
}
