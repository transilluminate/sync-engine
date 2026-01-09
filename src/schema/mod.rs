// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Schema-based table partitioning for horizontal scalability.
//!
//! Routes object IDs to separate SQL tables based on prefix matching.
//! This enables partitioning large datasets across dedicated tables
//! while maintaining a unified API surface.
//!
//! # Example
//!
//! ```rust,no_run
//! use sync_engine::schema::SchemaRegistry;
//!
//! let registry = SchemaRegistry::new();
//!
//! // Register prefixes to route to separate tables
//! registry.register("view:users:", "users_items");
//! registry.register("crdt:users:", "users_items");
//! registry.register("view:orders:", "orders_items");
//!
//! // Keys are routed by longest prefix match
//! assert_eq!(registry.table_for_key("view:users:alice"), "users_items");
//! assert_eq!(registry.table_for_key("crdt:users:bob"), "users_items");
//! assert_eq!(registry.table_for_key("view:orders:123"), "orders_items");
//!
//! // Unknown prefixes fall back to default table
//! assert_eq!(registry.table_for_key("unknown:key"), "sync_items");
//! ```
//!
//! # Design
//!
//! - **Longest prefix match**: More specific prefixes take precedence
//! - **Default fallback**: Unmatched keys go to `sync_items`
//! - **SQLite bypass**: Returns `sync_items` for all keys (no benefit from partitioning)
//! - **Thread-safe**: Uses `parking_lot::RwLock` for concurrent access

use parking_lot::RwLock;

/// Default table name for unmatched keys.
pub const DEFAULT_TABLE: &str = "sync_items";

/// Registry mapping key prefixes to table names.
///
/// Thread-safe for concurrent reads with occasional writes.
/// Prefixes are matched using longest-prefix-first semantics.
#[derive(Debug)]
pub struct SchemaRegistry {
    /// Prefix -> table name mappings.
    /// Stored sorted by prefix length (descending) for efficient matching.
    mappings: RwLock<Vec<(String, String)>>,
    
    /// Whether to skip partitioning (e.g., for SQLite).
    bypass: bool,
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            mappings: RwLock::new(Vec::new()),
            bypass: false,
        }
    }
    
    /// Create a registry that bypasses all routing (returns default table).
    /// 
    /// Use this for SQLite where table partitioning has no benefit.
    #[must_use]
    pub fn bypass() -> Self {
        Self {
            mappings: RwLock::new(Vec::new()),
            bypass: true,
        }
    }
    
    /// Register a prefix to route to a specific table.
    ///
    /// Multiple prefixes can map to the same table.
    /// Longer prefixes take precedence over shorter ones.
    ///
    /// # Arguments
    /// * `prefix` - Key prefix to match (e.g., "view:users:")
    /// * `table_name` - Target table name (e.g., "users_items")
    pub fn register(&self, prefix: &str, table_name: &str) {
        let mut mappings = self.mappings.write();
        
        // Check if this prefix already exists
        if let Some(pos) = mappings.iter().position(|(p, _)| p == prefix) {
            // Update existing
            mappings[pos].1 = table_name.to_string();
        } else {
            // Insert new
            mappings.push((prefix.to_string(), table_name.to_string()));
        }
        
        // Sort by prefix length descending (longest first for matching)
        mappings.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    }
    
    /// Unregister a prefix.
    ///
    /// Returns `true` if the prefix was found and removed.
    pub fn unregister(&self, prefix: &str) -> bool {
        let mut mappings = self.mappings.write();
        if let Some(pos) = mappings.iter().position(|(p, _)| p == prefix) {
            mappings.remove(pos);
            true
        } else {
            false
        }
    }
    
    /// Get the table name for a given key.
    ///
    /// Uses longest-prefix-first matching. Returns [`DEFAULT_TABLE`] if no prefix matches
    /// or if bypass mode is enabled.
    #[must_use]
    pub fn table_for_key(&self, key: &str) -> &'static str {
        if self.bypass {
            return DEFAULT_TABLE;
        }
        
        let mappings = self.mappings.read();
        
        // Mappings are sorted by length descending, so first match is longest
        for (prefix, table) in mappings.iter() {
            if key.starts_with(prefix) {
                // Return static str by leaking - tables are registered once at startup
                // and persist for the lifetime of the process
                return Box::leak(table.clone().into_boxed_str());
            }
        }
        
        DEFAULT_TABLE
    }
    
    /// Get all registered prefixes for a table.
    #[must_use]
    pub fn prefixes_for_table(&self, table_name: &str) -> Vec<String> {
        self.mappings
            .read()
            .iter()
            .filter(|(_, t)| t == table_name)
            .map(|(p, _)| p.clone())
            .collect()
    }
    
    /// Get all registered tables (excluding default).
    #[must_use]
    pub fn tables(&self) -> Vec<String> {
        let mappings = self.mappings.read();
        let mut tables: Vec<String> = mappings.iter().map(|(_, t)| t.clone()).collect();
        tables.sort();
        tables.dedup();
        tables
    }
    
    /// Get the number of registered prefixes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.mappings.read().len()
    }
    
    /// Check if the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mappings.read().is_empty()
    }
    
    /// Check if bypass mode is enabled.
    #[must_use]
    pub fn is_bypass(&self) -> bool {
        self.bypass
    }
    
    /// Clear all registered prefixes.
    pub fn clear(&self) {
        self.mappings.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_empty_registry_returns_default() {
        let registry = SchemaRegistry::new();
        assert_eq!(registry.table_for_key("any:key"), DEFAULT_TABLE);
        assert_eq!(registry.table_for_key("view:users:alice"), DEFAULT_TABLE);
    }
    
    #[test]
    fn test_basic_routing() {
        let registry = SchemaRegistry::new();
        registry.register("view:users:", "users_items");
        
        assert_eq!(registry.table_for_key("view:users:alice"), "users_items");
        assert_eq!(registry.table_for_key("view:users:bob"), "users_items");
        assert_eq!(registry.table_for_key("view:orders:123"), DEFAULT_TABLE);
    }
    
    #[test]
    fn test_multiple_prefixes_same_table() {
        let registry = SchemaRegistry::new();
        registry.register("view:users:", "users_items");
        registry.register("crdt:users:", "users_items");
        
        assert_eq!(registry.table_for_key("view:users:alice"), "users_items");
        assert_eq!(registry.table_for_key("crdt:users:bob"), "users_items");
    }
    
    #[test]
    fn test_longest_prefix_wins() {
        let registry = SchemaRegistry::new();
        registry.register("crdt:", "crdt_items");
        registry.register("crdt:users:", "users_items");
        
        // More specific prefix should win
        assert_eq!(registry.table_for_key("crdt:users:alice"), "users_items");
        // Less specific prefix for other keys
        assert_eq!(registry.table_for_key("crdt:orders:123"), "crdt_items");
    }
    
    #[test]
    fn test_bypass_mode() {
        let registry = SchemaRegistry::bypass();
        registry.register("view:users:", "users_items");
        
        // Bypass ignores all registrations
        assert_eq!(registry.table_for_key("view:users:alice"), DEFAULT_TABLE);
        assert!(registry.is_bypass());
    }
    
    #[test]
    fn test_unregister() {
        let registry = SchemaRegistry::new();
        registry.register("view:users:", "users_items");
        
        assert_eq!(registry.table_for_key("view:users:alice"), "users_items");
        
        assert!(registry.unregister("view:users:"));
        assert_eq!(registry.table_for_key("view:users:alice"), DEFAULT_TABLE);
        
        // Unregistering non-existent prefix returns false
        assert!(!registry.unregister("view:users:"));
    }
    
    #[test]
    fn test_update_existing_prefix() {
        let registry = SchemaRegistry::new();
        registry.register("view:users:", "users_items");
        assert_eq!(registry.table_for_key("view:users:alice"), "users_items");
        
        // Re-register with different table
        registry.register("view:users:", "users_v2_items");
        assert_eq!(registry.table_for_key("view:users:alice"), "users_v2_items");
        
        // Should only have one entry
        assert_eq!(registry.len(), 1);
    }
    
    #[test]
    fn test_prefixes_for_table() {
        let registry = SchemaRegistry::new();
        registry.register("view:users:", "users_items");
        registry.register("crdt:users:", "users_items");
        registry.register("view:orders:", "orders_items");
        
        let prefixes = registry.prefixes_for_table("users_items");
        assert_eq!(prefixes.len(), 2);
        assert!(prefixes.contains(&"view:users:".to_string()));
        assert!(prefixes.contains(&"crdt:users:".to_string()));
    }
    
    #[test]
    fn test_tables() {
        let registry = SchemaRegistry::new();
        registry.register("view:users:", "users_items");
        registry.register("crdt:users:", "users_items");
        registry.register("view:orders:", "orders_items");
        
        let tables = registry.tables();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"orders_items".to_string()));
        assert!(tables.contains(&"users_items".to_string()));
    }
    
    #[test]
    fn test_clear() {
        let registry = SchemaRegistry::new();
        registry.register("view:users:", "users_items");
        registry.register("view:orders:", "orders_items");
        
        assert_eq!(registry.len(), 2);
        registry.clear();
        assert!(registry.is_empty());
        assert_eq!(registry.table_for_key("view:users:alice"), DEFAULT_TABLE);
    }
    
    #[test]
    fn test_order_independence() {
        // Registration order shouldn't affect matching
        let registry = SchemaRegistry::new();
        registry.register("crdt:users:", "users_items"); // Register specific first
        registry.register("crdt:", "crdt_items");        // Then broad
        
        assert_eq!(registry.table_for_key("crdt:users:alice"), "users_items");
        assert_eq!(registry.table_for_key("crdt:orders:123"), "crdt_items");
        
        // Try opposite order
        let registry2 = SchemaRegistry::new();
        registry2.register("crdt:", "crdt_items");        // Register broad first
        registry2.register("crdt:users:", "users_items"); // Then specific
        
        assert_eq!(registry2.table_for_key("crdt:users:alice"), "users_items");
        assert_eq!(registry2.table_for_key("crdt:orders:123"), "crdt_items");
    }
}
