// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Search Infrastructure
//!
//! Full-text search across items using RediSearch and MySQL.
//!
//! # Architecture
//!
//! ```text
//! QueryBuilder (AST)
//!     ↓
//!     ├─→ RediSearchTranslator → FT.SEARCH syntax
//!     └─→ SqlTranslator → MySQL JSON_EXTRACT queries
//! ```
//!
//! # Index Registration
//!
//! sync-engine stays "dumb" about content but provides index lifecycle management:
//!
//! ```rust,no_run
//! # use sync_engine::{SyncEngine, search::{SearchIndex, Query}};
//! # async fn example(engine: &SyncEngine) {
//! // Register schema at startup using builder pattern
//! let index = SearchIndex::new("users", "crdt:users:")
//!     .text_sortable("name")
//!     .text("email")
//!     .numeric_sortable("age")
//!     .tag("roles");
//!
//! engine.create_search_index(index).await.unwrap();
//!
//! // RediSearch auto-indexes matching JSON documents
//! // Search using Query AST
//! let query = Query::field_eq("name", "Alice");
//! let results = engine.search("users", &query).await.unwrap();
//! # }
//! ```
//!
//! # Query Language (Lucene/RediSearch syntax)
//!
//! ```text
//! @name:Alice               - Field equals
//! @age:[25 40]              - Numeric range
//! @tags:{rust|database}     - Tag membership (OR)
//! @name:Alice AND @age:[25 40]  - Boolean AND
//! @status:active | @status:pending  - Boolean OR
//! -@deleted:true            - Boolean NOT
//! @name:*alice*             - Wildcard contains
//! @name:ali*                - Prefix match
//! @name:%alice%             - Fuzzy match (Levenshtein)
//! ```

mod query_builder;
mod redis_translator;
mod sql_translator;
mod index_manager;
mod search_cache;

pub use query_builder::{Query, QueryBuilder, QueryNode, FieldQuery, FieldOperator, QueryValue};
pub use redis_translator::RediSearchTranslator;
pub use sql_translator::{SqlTranslator, SqlQuery, SqlParam};
pub use index_manager::{
    DistanceMetric, IndexManager, SearchField, SearchFieldType, SearchIndex, VectorAlgorithm,
    VectorParams,
};
pub use search_cache::{SearchCache, SearchCacheStats};
