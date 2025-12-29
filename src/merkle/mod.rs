//! Path-based Merkle tree for efficient sync verification.
//!
//! # Design
//!
//! Uses reverse DNS object IDs to create a natural tree structure:
//!
//! ```text
//! uk.nhs.patient.record.123
//!
//! Becomes:
//!
//! uk ─────────────────── hash(nhs)
//! └── nhs ───────────── hash(patient)
//!     └── patient ───── hash(record)
//!         └── record ── hash(123, 456, ...)
//!             ├── 123 ─ leaf hash
//!             └── 456 ─ leaf hash
//! ```
//!
//! # Storage Strategy
//!
//! Merkle nodes are stored **separately** from data in Redis:
//!
//! - `data:uk.nhs.patient.record.123` → SyncItem (evictable by tan curve)
//! - `merkle:hash:uk.nhs.patient.record` → 32-byte hash (NEVER evicted)
//! - `merkle:children:uk.nhs.patient.record` → sorted set of child:hash pairs
//!
//! This allows efficient sync verification even when data is evicted from cache.
//!
//! # Sync Protocol
//!
//! 1. Client sends their root hash
//! 2. If different, server sends top-level children with hashes
//! 3. Client identifies differing branches
//! 4. Drill down until leaves are reached
//! 5. Transfer only the differing items
//!
//! This is O(diff_size × tree_depth) instead of O(total_items).

mod path_tree;
mod redis_store;
mod sql_store;

pub use path_tree::{MerkleBatch, MerkleNode, PathMerkle};
pub use redis_store::RedisMerkleStore;
pub use sql_store::SqlMerkleStore;
