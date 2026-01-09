// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Schema management API for table partitioning.
//!
//! Provides methods for registering schemas and routing keys to specific tables.

use tracing::info;

use crate::storage::traits::StorageError;

use super::SyncEngine;

impl SyncEngine {
    /// Register a schema prefix to route keys to a specific table.
    ///
    /// Creates the table if it doesn't exist (MySQL only - SQLite ignores partitioning).
    /// Multiple prefixes can map to the same table.
    ///
    /// # Arguments
    /// * `schema_name` - Schema name used to derive table name (e.g., "users" → "users_items")
    /// * `prefix` - Key prefix to route (e.g., "view:users:" or "crdt:users:")
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::SyncEngine;
    /// # async fn example(engine: &mut SyncEngine) {
    /// // Register view and crdt prefixes for the users schema
    /// engine.register_schema("users", "view:users:").await.unwrap();
    /// engine.register_schema("users", "crdt:users:").await.unwrap();
    ///
    /// // Now these keys route to users_items table:
    /// // - view:users:alice
    /// // - crdt:users:bob
    /// // - view:users:_schema:users
    /// # }
    /// ```
    pub async fn register_schema(&self, schema_name: &str, prefix: &str) -> Result<(), StorageError> {
        let table_name = format!("{}_items", schema_name);
        
        // Create table if it doesn't exist (MySQL only)
        if let Some(ref sql_store) = self.sql_store {
            sql_store.ensure_table(&table_name).await?;
        }
        
        // Register prefix in registry
        self.schema_registry.register(prefix, &table_name);
        
        info!(
            schema = %schema_name,
            prefix = %prefix,
            table = %table_name,
            "Schema registered"
        );
        
        Ok(())
    }
    
    /// Register multiple prefixes for a schema in one call.
    ///
    /// This is a convenience method for registering both view and crdt prefixes.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::SyncEngine;
    /// # async fn example(engine: &mut SyncEngine) {
    /// // Register both prefixes at once
    /// engine.register_schema_prefixes("users", &["view:users:", "crdt:users:"]).await.unwrap();
    /// # }
    /// ```
    pub async fn register_schema_prefixes(&self, schema_name: &str, prefixes: &[&str]) -> Result<(), StorageError> {
        for prefix in prefixes {
            self.register_schema(schema_name, prefix).await?;
        }
        Ok(())
    }
    
    /// Unregister a prefix (for testing or cleanup).
    ///
    /// Note: This does NOT drop the table - it only removes the routing.
    pub fn unregister_prefix(&self, prefix: &str) -> bool {
        self.schema_registry.unregister(prefix)
    }
    
    /// Get the table name for a given key.
    ///
    /// Returns "sync_items" for unregistered prefixes.
    #[must_use]
    pub fn table_for_key(&self, key: &str) -> &'static str {
        self.schema_registry.table_for_key(key)
    }
    
    /// Get all registered schema tables.
    #[must_use]
    pub fn registered_tables(&self) -> Vec<String> {
        self.schema_registry.tables()
    }
    
    /// Get all prefixes routing to a specific table.
    #[must_use]
    pub fn prefixes_for_table(&self, table_name: &str) -> Vec<String> {
        self.schema_registry.prefixes_for_table(table_name)
    }
    
    /// Check if schema partitioning is in bypass mode (SQLite).
    #[must_use]
    pub fn is_schema_bypass(&self) -> bool {
        self.schema_registry.is_bypass()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SyncEngineConfig;
    use tokio::sync::watch;

    fn test_config() -> SyncEngineConfig {
        SyncEngineConfig {
            redis_url: None,
            sql_url: None,
            wal_path: None,
            l1_max_bytes: 1024 * 1024,
            ..Default::default()
        }
    }

    #[test]
    fn test_table_for_key_empty_registry() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // No schemas registered - everything goes to default
        assert_eq!(engine.table_for_key("view:users:alice"), "sync_items");
        assert_eq!(engine.table_for_key("crdt:users:bob"), "sync_items");
    }

    #[test]
    fn test_table_for_key_with_registrations() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        // Register prefixes manually (bypassing async ensure_table)
        engine.schema_registry.register("view:users:", "users_items");
        engine.schema_registry.register("crdt:users:", "users_items");
        
        assert_eq!(engine.table_for_key("view:users:alice"), "users_items");
        assert_eq!(engine.table_for_key("crdt:users:bob"), "users_items");
        assert_eq!(engine.table_for_key("unknown:key"), "sync_items");
    }

    #[test]
    fn test_registered_tables() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        engine.schema_registry.register("view:users:", "users_items");
        engine.schema_registry.register("view:orders:", "orders_items");
        
        let tables = engine.registered_tables();
        assert!(tables.contains(&"users_items".to_string()));
        assert!(tables.contains(&"orders_items".to_string()));
    }

    #[test]
    fn test_unregister_prefix() {
        let config = test_config();
        let (_tx, rx) = watch::channel(config.clone());
        let engine = SyncEngine::new(config, rx);
        
        engine.schema_registry.register("view:users:", "users_items");
        assert_eq!(engine.table_for_key("view:users:alice"), "users_items");
        
        assert!(engine.unregister_prefix("view:users:"));
        assert_eq!(engine.table_for_key("view:users:alice"), "sync_items");
    }
}
