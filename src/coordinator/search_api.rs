//! Search API for SyncEngine
//!
//! Provides full-text search capabilities via RediSearch with SQL fallback.
//!
//! # Architecture
//!
//! ```text
//! search(index, query)
//!       │
//!       ├─→ Check SearchCache (merkle-validated)
//!       │        │
//!       │        └─→ Hit? Return cached keys
//!       │
//!       ├─→ FT.SEARCH Redis (fast)
//!       │        │
//!       │        └─→ Results? Return
//!       │
//!       └─→ Durable tier? SQL fallback
//!                │
//!                └─→ Cache results with merkle root
//! ```

use std::time::Instant;
use tracing::{debug, info};

use crate::metrics;
use crate::search::{
    IndexManager, SearchIndex, SearchCache, SearchCacheStats,
    Query, RediSearchTranslator, SqlTranslator,
};
use crate::sync_item::SyncItem;
use crate::storage::traits::StorageError;

use super::SyncEngine;

/// Search tier strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchTier {
    /// Redis only - no SQL fallback (for view: prefix ephemeral data)
    RedisOnly,
    /// Redis with SQL fallback (for crdt: prefix durable data)
    #[default]
    RedisWithSqlFallback,
}

/// Search result with metadata
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Matching items
    pub items: Vec<SyncItem>,
    /// Source of results
    pub source: SearchSource,
    /// Whether results came from cache
    pub cached: bool,
}

/// Where search results came from
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchSource {
    /// Results from RediSearch FT.SEARCH
    Redis,
    /// Results from SQL query
    Sql,
    /// Results from SearchCache
    Cache,
    /// No results found
    Empty,
}

/// Search-related state for SyncEngine
#[derive(Default)]
pub struct SearchState {
    /// Index manager
    pub index_manager: IndexManager,
    /// Search result cache
    pub cache: SearchCache,
}

impl SyncEngine {
    // ═══════════════════════════════════════════════════════════════════════════
    // Search API
    // ═══════════════════════════════════════════════════════════════════════════

    /// Register a search index.
    ///
    /// Creates the index in RediSearch using FT.CREATE. The index will
    /// automatically index all JSON documents with matching key prefix.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, search::SearchIndex};
    /// # async fn example(engine: &SyncEngine) -> Result<(), Box<dyn std::error::Error>> {
    /// // Define index schema
    /// let index = SearchIndex::new("users", "crdt:users:")
    ///     .text_sortable("name")
    ///     .text("email")
    ///     .numeric_sortable("age")
    ///     .tag("roles");
    ///
    /// // Create in RediSearch
    /// engine.create_search_index(index).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_search_index(&self, index: SearchIndex) -> Result<(), StorageError> {
        let l2 = self.l2_store.as_ref().ok_or_else(|| {
            StorageError::Connection("Redis not available for search index".into())
        })?;

        // Build FT.CREATE command with global redis prefix
        let redis_prefix = self.config.read().redis_prefix.clone();
        let args = index.to_ft_create_args_with_prefix(redis_prefix.as_deref());
        debug!(index = %index.name, prefix = %index.prefix, redis_prefix = ?redis_prefix, "Creating search index");

        // Execute FT.CREATE via raw Redis command
        match l2.ft_create(&args).await {
            Ok(()) => {
                metrics::record_search_index_operation("create", true);
                
                // Register in index manager
                if let Some(ref search_state) = self.search_state {
                    search_state.write().index_manager.register(index);
                }

                info!(index = %args[0], "Search index created");
                Ok(())
            }
            Err(e) => {
                metrics::record_search_index_operation("create", false);
                Err(e)
            }
        }
    }

    /// Drop a search index.
    ///
    /// Removes the index from RediSearch. Does not delete the indexed documents.
    pub async fn drop_search_index(&self, name: &str) -> Result<(), StorageError> {
        let l2 = self.l2_store.as_ref().ok_or_else(|| {
            StorageError::Connection("Redis not available".into())
        })?;

        let prefix = self.config.read().redis_prefix.clone().unwrap_or_default();
        let index_name = format!("{}idx:{}", prefix, name);
        
        match l2.ft_dropindex(&index_name).await {
            Ok(()) => {
                metrics::record_search_index_operation("drop", true);
                info!(index = %index_name, "Search index dropped");
                Ok(())
            }
            Err(e) => {
                metrics::record_search_index_operation("drop", false);
                Err(e)
            }
        }
    }

    /// Search for items using RediSearch query syntax.
    ///
    /// Searches the specified index using FT.SEARCH. For durable data (crdt: prefix),
    /// falls back to SQL if Redis returns no results.
    ///
    /// # Arguments
    ///
    /// * `index_name` - Name of the search index (without "idx:" prefix)
    /// * `query` - Query AST built with `Query::` constructors
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sync_engine::{SyncEngine, search::Query};
    /// # async fn example(engine: &SyncEngine) -> Result<(), Box<dyn std::error::Error>> {
    /// // Simple field query
    /// let results = engine.search("users", &Query::field_eq("name", "Alice")).await?;
    ///
    /// // Complex query with AND/OR
    /// let query = Query::field_eq("status", "active")
    ///     .and(Query::numeric_range("age", Some(25.0), Some(40.0)));
    /// let results = engine.search("users", &query).await?;
    ///
    /// for item in results.items {
    ///     println!("Found: {}", item.object_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search(
        &self,
        index_name: &str,
        query: &Query,
    ) -> Result<SearchResult, StorageError> {
        self.search_with_options(index_name, query, SearchTier::default(), 100).await
    }

    /// Search with explicit tier and limit options.
    pub async fn search_with_options(
        &self,
        index_name: &str,
        query: &Query,
        tier: SearchTier,
        limit: usize,
    ) -> Result<SearchResult, StorageError> {
        let start = Instant::now();
        
        // Get index info
        let prefix = if let Some(ref search_state) = self.search_state {
            search_state.read()
                .index_manager
                .get(index_name)
                .map(|idx| idx.prefix.clone())
        } else {
            None
        };

        let prefix = prefix.unwrap_or_else(|| format!("crdt:{}:", index_name));

        // Check cache first (if we have merkle root)
        if let Some(ref search_state) = self.search_state {
            if let Some(merkle_root) = self.get_merkle_root_for_prefix(&prefix).await {
                let cached_keys = search_state.read().cache.get(&prefix, query, &merkle_root);
                if let Some(keys) = cached_keys {
                    debug!(index = %index_name, count = keys.len(), "Search cache hit");
                    metrics::record_search_cache(true);
                    metrics::record_search_latency("cache", start.elapsed());
                    metrics::record_search_results(keys.len());
                    let items = self.fetch_items_by_keys(&keys).await?;
                    return Ok(SearchResult {
                        items,
                        source: SearchSource::Cache,
                        cached: true,
                    });
                }
            }
        }

        // Try RediSearch
        let redis_start = Instant::now();
        let redis_results = self.search_redis(index_name, query, limit).await;

        match redis_results {
            Ok(items) if !items.is_empty() => {
                debug!(index = %index_name, count = items.len(), "RediSearch results");
                metrics::record_search_query("redis", "success");
                metrics::record_search_latency("redis", redis_start.elapsed());
                metrics::record_search_results(items.len());
                Ok(SearchResult {
                    items,
                    source: SearchSource::Redis,
                    cached: false,
                })
            }
            Ok(_) | Err(_) => {
                // Record Redis attempt (empty or error)
                if redis_results.is_err() {
                    metrics::record_search_query("redis", "error");
                } else {
                    metrics::record_search_query("redis", "empty");
                }
                
                // Empty or error - try SQL fallback for durable tier
                if tier == SearchTier::RedisWithSqlFallback {
                    let sql_start = Instant::now();
                    let sql_results = self.search_sql(query, limit).await?;
                    let is_empty = sql_results.is_empty();
                    
                    metrics::record_search_query("sql", "success");
                    metrics::record_search_latency("sql", sql_start.elapsed());
                    metrics::record_search_results(sql_results.len());

                    // Cache results if we have merkle
                    if !is_empty {
                        if let Some(ref search_state) = self.search_state {
                            if let Some(merkle_root) = self.get_merkle_root_for_prefix(&prefix).await {
                                let keys: Vec<String> = sql_results.iter()
                                    .map(|item| item.object_id.clone())
                                    .collect();
                                search_state.write().cache.insert(&prefix, query, merkle_root, keys);
                            }
                        }
                    }

                    Ok(SearchResult {
                        items: sql_results,
                        source: if is_empty { SearchSource::Empty } else { SearchSource::Sql },
                        cached: false,
                    })
                } else {
                    // RedisOnly tier - return empty
                    metrics::record_search_results(0);
                    Ok(SearchResult {
                        items: vec![],
                        source: SearchSource::Empty,
                        cached: false,
                    })
                }
            }
        }
    }

    /// Search using raw RediSearch query string (Redis-only, no SQL fallback).
    ///
    /// Use this when you need the full power of RediSearch syntax
    /// without the Query AST. This is an **advanced API** with caveats:
    ///
    /// - **No SQL fallback**: If Redis is unavailable, this will fail
    /// - **No search cache**: Results are not cached via the merkle system
    /// - **Manual paths**: You must use `$.payload.{field}` paths in your query
    /// - **No translation**: The query string is passed directly to FT.SEARCH
    ///
    /// Prefer `search()` or `search_with_options()` for most use cases.
    ///
    /// # Example
    /// ```ignore
    /// // Raw RediSearch query with explicit payload paths
    /// let results = engine.search_raw(
    ///     "users",
    ///     "@name:(Alice Smith) @age:[25 35]",
    ///     100
    /// ).await?;
    /// ```
    pub async fn search_raw(
        &self,
        index_name: &str,
        query_str: &str,
        limit: usize,
    ) -> Result<Vec<SyncItem>, StorageError> {
        let l2 = self.l2_store.as_ref().ok_or_else(|| {
            StorageError::Connection("Redis not available for raw search".into())
        })?;

        let prefix = self.config.read().redis_prefix.clone().unwrap_or_default();
        let index = format!("{}idx:{}", prefix, index_name);
        
        metrics::record_search_query("redis_raw", "attempt");
        let start = std::time::Instant::now();
        
        let keys = l2.ft_search(&index, query_str, limit).await?;
        let items = self.fetch_items_by_keys(&keys).await?;
        
        metrics::record_search_query("redis_raw", "success");
        metrics::record_search_latency("redis_raw", start.elapsed());
        metrics::record_search_results(items.len());
        
        Ok(items)
    }

    /// Direct SQL search (bypasses Redis, SQL-only).
    ///
    /// Queries the SQL archive directly using JSON_EXTRACT.
    /// This is an **advanced API** with specific use cases:
    ///
    /// - **No Redis**: Bypasses L2 cache entirely
    /// - **No caching**: Results are not cached via the merkle system
    /// - **Ground truth**: Queries the durable SQL archive
    ///
    /// Useful for:
    /// - Analytics queries that need complete data
    /// - When Redis is unavailable or not trusted
    /// - Bulk operations on archived data
    ///
    /// Prefer `search()` or `search_with_options()` for most use cases.
    pub async fn search_sql(
        &self,
        query: &Query,
        limit: usize,
    ) -> Result<Vec<SyncItem>, StorageError> {
        let l3 = self.l3_store.as_ref().ok_or_else(|| {
            StorageError::Connection("SQL not available".into())
        })?;

        metrics::record_search_query("sql_direct", "attempt");
        let start = std::time::Instant::now();

        let sql_query = SqlTranslator::translate(query, "data");
        debug!(clause = %sql_query.clause, "SQL search");

        let results = l3.search(&sql_query.clause, &sql_query.params, limit).await?;
        
        metrics::record_search_query("sql_direct", "success");
        metrics::record_search_latency("sql_direct", start.elapsed());
        metrics::record_search_results(results.len());
        
        Ok(results)
    }

    /// Get search cache statistics.
    pub fn search_cache_stats(&self) -> Option<SearchCacheStats> {
        self.search_state.as_ref().map(|s| s.read().cache.stats())
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Internal helpers
    // ═══════════════════════════════════════════════════════════════════════════

    /// Fetch items by keys, filtering out None results
    async fn fetch_items_by_keys(&self, keys: &[String]) -> Result<Vec<SyncItem>, StorageError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let results = self.get_many(&key_refs).await;
        Ok(results.into_iter().flatten().collect())
    }

    async fn search_redis(
        &self,
        index_name: &str,
        query: &Query,
        limit: usize,
    ) -> Result<Vec<SyncItem>, StorageError> {
        let l2 = self.l2_store.as_ref().ok_or_else(|| {
            StorageError::Connection("Redis not available".into())
        })?;

        let prefix = self.config.read().redis_prefix.clone().unwrap_or_default();
        let index = format!("{}idx:{}", prefix, index_name);
        let query_str = RediSearchTranslator::translate(query);
        debug!(index = %index, query = %query_str, "FT.SEARCH");

        let keys = l2.ft_search(&index, &query_str, limit).await?;
        self.fetch_items_by_keys(&keys).await
    }

    async fn get_merkle_root_for_prefix(&self, prefix: &str) -> Option<Vec<u8>> {
        // Extract the path segment from prefix (e.g., "crdt:users:" -> "crdt:users")
        let path = prefix.trim_end_matches(':');

        if let Some(ref redis_merkle) = self.redis_merkle {
            if let Ok(Some(node)) = redis_merkle.get_node(path).await {
                return Some(node.hash.to_vec());
            }
        }

        None
    }
}
