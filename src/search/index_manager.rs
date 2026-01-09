// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Index Manager
//!
//! Manages RediSearch index lifecycle - creation, deletion, and schema management.
//!
//! # RediSearch Index Creation
//!
//! ```text
//! FT.CREATE idx:users
//!   ON JSON
//!   PREFIX 1 crdt:users:
//!   SCHEMA
//!     $.name AS name TEXT
//!     $.email AS email TEXT SORTABLE
//!     $.age AS age NUMERIC SORTABLE
//!     $.tags AS tags TAG
//! ```

use std::collections::HashMap;

/// Vector search algorithm for RediSearch.
/// HNSW provides faster queries with higher memory usage.
/// FLAT provides exact results with O(n) query time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorAlgorithm {
    /// Hierarchical Navigable Small World - approximate nearest neighbors.
    /// Faster queries but requires more memory. Good for large datasets.
    Hnsw,
    /// Brute-force flat index - exact nearest neighbors.
    /// O(n) query time but lower memory. Good for smaller datasets.
    Flat,
}

impl std::fmt::Display for VectorAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VectorAlgorithm::Hnsw => write!(f, "HNSW"),
            VectorAlgorithm::Flat => write!(f, "FLAT"),
        }
    }
}

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// L2 Euclidean distance - good for dense embeddings
    L2,
    /// Inner product - good for normalized vectors (e.g., cosine pre-normalized)
    InnerProduct,
    /// Cosine similarity - good for text embeddings
    Cosine,
}

impl std::fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistanceMetric::L2 => write!(f, "L2"),
            DistanceMetric::InnerProduct => write!(f, "IP"),
            DistanceMetric::Cosine => write!(f, "COSINE"),
        }
    }
}

/// Parameters for RediSearch vector fields.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorParams {
    /// The algorithm to use (HNSW or FLAT)
    pub algorithm: VectorAlgorithm,
    /// Vector dimensionality (must match your embeddings)
    pub dim: usize,
    /// Distance metric for similarity calculation
    pub distance_metric: DistanceMetric,
    /// HNSW M parameter: number of outgoing edges per node (default 16)
    pub hnsw_m: Option<usize>,
    /// HNSW EF_CONSTRUCTION: search depth during index building (default 200)
    pub hnsw_ef_construction: Option<usize>,
}

impl VectorParams {
    /// Create HNSW vector params with sensible defaults.
    /// M=16, EF_CONSTRUCTION=200 are RediSearch defaults.
    pub fn hnsw(dim: usize, distance_metric: DistanceMetric) -> Self {
        Self {
            algorithm: VectorAlgorithm::Hnsw,
            dim,
            distance_metric,
            hnsw_m: None,
            hnsw_ef_construction: None,
        }
    }

    /// Create FLAT vector params.
    pub fn flat(dim: usize, distance_metric: DistanceMetric) -> Self {
        Self {
            algorithm: VectorAlgorithm::Flat,
            dim,
            distance_metric,
            hnsw_m: None,
            hnsw_ef_construction: None,
        }
    }

    /// Set HNSW M parameter (number of edges per node).
    /// Higher = better recall, more memory. Typical: 12-48.
    pub fn with_m(mut self, m: usize) -> Self {
        self.hnsw_m = Some(m);
        self
    }

    /// Set HNSW EF_CONSTRUCTION parameter (build-time search depth).
    /// Higher = better index quality, slower build. Typical: 100-500.
    pub fn with_ef_construction(mut self, ef: usize) -> Self {
        self.hnsw_ef_construction = Some(ef);
        self
    }

    /// Generate RediSearch schema arguments for this vector field.
    /// Returns: [nargs, TYPE, FLOAT32, DIM, {dim}, DISTANCE_METRIC, {metric}, ...]
    fn to_schema_args(&self) -> Vec<String> {
        let mut args = vec![
            "TYPE".to_string(),
            "FLOAT32".to_string(),
            "DIM".to_string(),
            self.dim.to_string(),
            "DISTANCE_METRIC".to_string(),
            self.distance_metric.to_string(),
        ];

        // Add HNSW-specific params if set
        if self.algorithm == VectorAlgorithm::Hnsw {
            if let Some(m) = self.hnsw_m {
                args.push("M".to_string());
                args.push(m.to_string());
            }
            if let Some(ef) = self.hnsw_ef_construction {
                args.push("EF_CONSTRUCTION".to_string());
                args.push(ef.to_string());
            }
        }

        // Prepend the count (RediSearch requires nargs before params)
        let mut result = vec![args.len().to_string()];
        result.extend(args);
        result
    }
}

/// Search index definition
#[derive(Debug, Clone)]
pub struct SearchIndex {
    /// Index name (will be prefixed with "idx:")
    pub name: String,
    /// Key prefix this index covers (e.g., "crdt:users:")
    pub prefix: String,
    /// Field definitions for the index
    pub fields: Vec<SearchField>,
}

impl SearchIndex {
    /// Create a new search index definition
    pub fn new(name: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            prefix: prefix.into(),
            fields: Vec::new(),
        }
    }

    /// Add a text field
    pub fn text(mut self, name: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Text,
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a text field with custom JSON path
    pub fn text_at(mut self, name: impl Into<String>, json_path: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: Some(json_path.into()),
            field_type: SearchFieldType::Text,
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a sortable text field
    pub fn text_sortable(mut self, name: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Text,
            sortable: true,
            no_index: false,
        });
        self
    }

    /// Add a sortable text field with custom JSON path
    pub fn text_sortable_at(mut self, name: impl Into<String>, json_path: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: Some(json_path.into()),
            field_type: SearchFieldType::Text,
            sortable: true,
            no_index: false,
        });
        self
    }

    /// Add a numeric field
    pub fn numeric(mut self, name: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Numeric,
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a numeric field with custom JSON path
    pub fn numeric_at(mut self, name: impl Into<String>, json_path: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: Some(json_path.into()),
            field_type: SearchFieldType::Numeric,
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a sortable numeric field
    pub fn numeric_sortable(mut self, name: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Numeric,
            sortable: true,
            no_index: false,
        });
        self
    }

    /// Add a sortable numeric field with custom JSON path
    pub fn numeric_sortable_at(mut self, name: impl Into<String>, json_path: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: Some(json_path.into()),
            field_type: SearchFieldType::Numeric,
            sortable: true,
            no_index: false,
        });
        self
    }

    /// Add a tag field
    pub fn tag(mut self, name: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Tag,
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a tag field with custom JSON path
    pub fn tag_at(mut self, name: impl Into<String>, json_path: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: Some(json_path.into()),
            field_type: SearchFieldType::Tag,
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a geo field
    pub fn geo(mut self, name: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Geo,
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a geo field with custom JSON path
    pub fn geo_at(mut self, name: impl Into<String>, json_path: impl Into<String>) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: Some(json_path.into()),
            field_type: SearchFieldType::Geo,
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add an HNSW vector field for similarity search.
    ///
    /// HNSW (Hierarchical Navigable Small World) is an approximate nearest neighbors
    /// algorithm that provides fast queries with good recall.
    ///
    /// # Arguments
    /// * `name` - Field name
    /// * `dim` - Vector dimensionality (must match your embeddings)
    /// * `metric` - Distance metric (L2, InnerProduct, or Cosine)
    ///
    /// # Example
    /// ```ignore
    /// SearchIndex::new("docs", "crdt:docs:")
    ///     .vector_hnsw("embedding", 1536, DistanceMetric::Cosine)
    /// ```
    pub fn vector_hnsw(
        mut self,
        name: impl Into<String>,
        dim: usize,
        metric: DistanceMetric,
    ) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Vector(VectorParams::hnsw(dim, metric)),
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add an HNSW vector field with custom JSON path.
    pub fn vector_hnsw_at(
        mut self,
        name: impl Into<String>,
        json_path: impl Into<String>,
        dim: usize,
        metric: DistanceMetric,
    ) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: Some(json_path.into()),
            field_type: SearchFieldType::Vector(VectorParams::hnsw(dim, metric)),
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add an HNSW vector field with tuned parameters.
    ///
    /// # Arguments
    /// * `name` - Field name
    /// * `params` - Pre-configured VectorParams with custom M and EF_CONSTRUCTION
    ///
    /// # Example
    /// ```ignore
    /// let params = VectorParams::hnsw(1536, DistanceMetric::Cosine)
    ///     .with_m(32)
    ///     .with_ef_construction(400);
    ///
    /// SearchIndex::new("docs", "crdt:docs:")
    ///     .vector_with_params("embedding", params)
    /// ```
    pub fn vector_with_params(mut self, name: impl Into<String>, params: VectorParams) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Vector(params),
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a FLAT vector field for exact similarity search.
    ///
    /// FLAT is a brute-force algorithm with O(n) query time but provides
    /// exact results. Good for smaller datasets (<10k vectors).
    ///
    /// # Arguments
    /// * `name` - Field name
    /// * `dim` - Vector dimensionality (must match your embeddings)
    /// * `metric` - Distance metric (L2, InnerProduct, or Cosine)
    pub fn vector_flat(
        mut self,
        name: impl Into<String>,
        dim: usize,
        metric: DistanceMetric,
    ) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: None,
            field_type: SearchFieldType::Vector(VectorParams::flat(dim, metric)),
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Add a FLAT vector field with custom JSON path.
    pub fn vector_flat_at(
        mut self,
        name: impl Into<String>,
        json_path: impl Into<String>,
        dim: usize,
        metric: DistanceMetric,
    ) -> Self {
        self.fields.push(SearchField {
            name: name.into(),
            json_path: Some(json_path.into()),
            field_type: SearchFieldType::Vector(VectorParams::flat(dim, metric)),
            sortable: false,
            no_index: false,
        });
        self
    }

    /// Generate the FT.CREATE command arguments
    pub fn to_ft_create_args(&self) -> Vec<String> {
        self.to_ft_create_args_with_prefix(None)
    }

    /// Generate FT.CREATE args with optional global redis prefix
    ///
    /// The redis_prefix is prepended to both the index name and the key prefix
    /// to match the actual key structure in Redis.
    pub fn to_ft_create_args_with_prefix(&self, redis_prefix: Option<&str>) -> Vec<String> {
        let prefix = redis_prefix.unwrap_or("");
        
        let mut args = vec![
            format!("{}idx:{}", prefix, self.name),
            "ON".to_string(),
            "JSON".to_string(),
            "PREFIX".to_string(),
            "1".to_string(),
            format!("{}{}", prefix, self.prefix),
            "SCHEMA".to_string(),
        ];

        for field in &self.fields {
            args.extend(field.to_schema_args());
        }

        args
    }
}

/// Search field definition
#[derive(Debug, Clone)]
pub struct SearchField {
    /// Field name (used in queries)
    pub name: String,
    /// JSON path (defaults to $.{name})
    pub json_path: Option<String>,
    /// Field type
    pub field_type: SearchFieldType,
    /// Whether the field is sortable
    pub sortable: bool,
    /// Whether to exclude from indexing (useful for SORTABLE-only fields)
    pub no_index: bool,
}

impl SearchField {
    fn to_schema_args(&self) -> Vec<String> {
        // Default path: user data is stored under $.payload in our JSON wrapper
        let json_path = self
            .json_path
            .clone()
            .unwrap_or_else(|| format!("$.payload.{}", self.name));

        let mut args = vec![
            json_path,
            "AS".to_string(),
            self.name.clone(),
        ];

        // Vector fields have special syntax: VECTOR {ALGO} {nargs} {params...}
        match &self.field_type {
            SearchFieldType::Vector(params) => {
                args.push("VECTOR".to_string());
                args.push(params.algorithm.to_string());
                args.extend(params.to_schema_args());
            }
            _ => {
                args.push(self.field_type.to_string());
            }
        }

        if self.sortable {
            args.push("SORTABLE".to_string());
        }

        if self.no_index {
            args.push("NOINDEX".to_string());
        }

        args
    }
}

/// Search field types supported by RediSearch
#[derive(Debug, Clone, PartialEq)]
pub enum SearchFieldType {
    /// Full-text searchable field
    Text,
    /// Numeric field (supports range queries)
    Numeric,
    /// Tag field (exact match, supports OR)
    Tag,
    /// Geographic field (latitude, longitude)
    Geo,
    /// Vector field for similarity search with algorithm-specific params
    Vector(VectorParams),
}

impl std::fmt::Display for SearchFieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchFieldType::Text => write!(f, "TEXT"),
            SearchFieldType::Numeric => write!(f, "NUMERIC"),
            SearchFieldType::Tag => write!(f, "TAG"),
            SearchFieldType::Geo => write!(f, "GEO"),
            SearchFieldType::Vector(params) => write!(f, "VECTOR {}", params.algorithm),
        }
    }
}

/// Index manager for RediSearch
pub struct IndexManager {
    /// Registered indexes by name
    indexes: HashMap<String, SearchIndex>,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    /// Register an index definition
    pub fn register(&mut self, index: SearchIndex) {
        self.indexes.insert(index.name.clone(), index);
    }

    /// Get a registered index by name
    pub fn get(&self, name: &str) -> Option<&SearchIndex> {
        self.indexes.get(name)
    }

    /// Get all registered indexes
    pub fn all(&self) -> impl Iterator<Item = &SearchIndex> {
        self.indexes.values()
    }

    /// Generate FT.CREATE arguments for an index
    pub fn ft_create_args(&self, name: &str) -> Option<Vec<String>> {
        self.indexes.get(name).map(|idx| idx.to_ft_create_args())
    }

    /// Find index by prefix match
    pub fn find_by_prefix(&self, key: &str) -> Option<&SearchIndex> {
        self.indexes.values().find(|idx| key.starts_with(&idx.prefix))
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_index() {
        let index = SearchIndex::new("users", "crdt:users:")
            .text("name")
            .text("email")
            .numeric("age");

        let args = index.to_ft_create_args();
        assert_eq!(args[0], "idx:users");
        assert_eq!(args[1], "ON");
        assert_eq!(args[2], "JSON");
        assert_eq!(args[3], "PREFIX");
        assert_eq!(args[4], "1");
        assert_eq!(args[5], "crdt:users:");
        assert_eq!(args[6], "SCHEMA");
        // Default paths are $.payload.{name} since user data is stored under payload
        assert!(args.contains(&"$.payload.name".to_string()));
        assert!(args.contains(&"name".to_string()));
        assert!(args.contains(&"TEXT".to_string()));
    }

    #[test]
    fn test_sortable_fields() {
        let index = SearchIndex::new("users", "crdt:users:")
            .text_sortable("name")
            .numeric_sortable("age");

        let args = index.to_ft_create_args();
        // Check SORTABLE appears
        let sortable_count = args.iter().filter(|a| *a == "SORTABLE").count();
        assert_eq!(sortable_count, 2);
    }

    #[test]
    fn test_tag_field() {
        let index = SearchIndex::new("items", "crdt:items:").tag("tags");

        let args = index.to_ft_create_args();
        assert!(args.contains(&"TAG".to_string()));
    }

    #[test]
    fn test_custom_json_path() {
        let index =
            SearchIndex::new("users", "crdt:users:").text_at("username", "$.profile.name");

        let args = index.to_ft_create_args();
        assert!(args.contains(&"$.profile.name".to_string()));
        assert!(args.contains(&"username".to_string()));
    }

    #[test]
    fn test_index_manager_register() {
        let mut manager = IndexManager::new();

        let index = SearchIndex::new("users", "crdt:users:")
            .text("name")
            .numeric("age");

        manager.register(index);

        assert!(manager.get("users").is_some());
        assert!(manager.get("unknown").is_none());
    }

    #[test]
    fn test_index_manager_find_by_prefix() {
        let mut manager = IndexManager::new();

        manager.register(SearchIndex::new("users", "crdt:users:"));
        manager.register(SearchIndex::new("posts", "crdt:posts:"));

        let found = manager.find_by_prefix("crdt:users:abc123");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "users");

        let found = manager.find_by_prefix("crdt:posts:xyz");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "posts");

        let not_found = manager.find_by_prefix("crdt:comments:1");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_ft_create_full_command() {
        let index = SearchIndex::new("users", "crdt:users:")
            .text_sortable("name")
            .text("email")
            .numeric_sortable("age")
            .tag("roles");

        let args = index.to_ft_create_args();

        // Verify the structure
        // FT.CREATE idx:users ON JSON PREFIX 1 crdt:users: SCHEMA
        //   $.payload.name AS name TEXT SORTABLE
        //   $.payload.email AS email TEXT
        //   $.payload.age AS age NUMERIC SORTABLE
        //   $.payload.roles AS roles TAG
        // Note: default paths use $.payload.{field} since user data is wrapped in payload

        assert_eq!(args[0], "idx:users");
        assert_eq!(args[6], "SCHEMA");

        // Build the full command string for verification
        let cmd = format!("FT.CREATE {}", args.join(" "));
        assert!(cmd.contains("idx:users"));
        assert!(cmd.contains("ON JSON"));
        assert!(cmd.contains("PREFIX 1 crdt:users:"));
        assert!(cmd.contains("$.payload.name AS name TEXT SORTABLE"));
        assert!(cmd.contains("$.payload.email AS email TEXT"));
        assert!(cmd.contains("$.payload.age AS age NUMERIC SORTABLE"));
        assert!(cmd.contains("$.payload.roles AS roles TAG"));
    }

    #[test]
    fn test_vector_hnsw_basic() {
        let index = SearchIndex::new("docs", "crdt:docs:")
            .text("title")
            .vector_hnsw("embedding", 1536, DistanceMetric::Cosine);

        let args = index.to_ft_create_args();
        let cmd = format!("FT.CREATE {}", args.join(" "));

        // Verify HNSW vector field structure
        assert!(cmd.contains("$.payload.embedding AS embedding VECTOR HNSW"));
        assert!(cmd.contains("TYPE FLOAT32"));
        assert!(cmd.contains("DIM 1536"));
        assert!(cmd.contains("DISTANCE_METRIC COSINE"));
    }

    #[test]
    fn test_vector_hnsw_with_params() {
        let params = VectorParams::hnsw(768, DistanceMetric::L2)
            .with_m(32)
            .with_ef_construction(400);

        let index =
            SearchIndex::new("embeddings", "crdt:embeddings:").vector_with_params("vec", params);

        let args = index.to_ft_create_args();
        let cmd = format!("FT.CREATE {}", args.join(" "));

        assert!(cmd.contains("VECTOR HNSW"));
        assert!(cmd.contains("DIM 768"));
        assert!(cmd.contains("DISTANCE_METRIC L2"));
        assert!(cmd.contains("M 32"));
        assert!(cmd.contains("EF_CONSTRUCTION 400"));
    }

    #[test]
    fn test_vector_flat() {
        let index = SearchIndex::new("small", "crdt:small:")
            .vector_flat("embedding", 384, DistanceMetric::InnerProduct);

        let args = index.to_ft_create_args();
        let cmd = format!("FT.CREATE {}", args.join(" "));

        assert!(cmd.contains("VECTOR FLAT"));
        assert!(cmd.contains("DIM 384"));
        assert!(cmd.contains("DISTANCE_METRIC IP"));
    }

    #[test]
    fn test_vector_custom_path() {
        let index = SearchIndex::new("docs", "crdt:docs:").vector_hnsw_at(
            "embedding",
            "$.metadata.vector",
            512,
            DistanceMetric::Cosine,
        );

        let args = index.to_ft_create_args();
        let cmd = format!("FT.CREATE {}", args.join(" "));

        assert!(cmd.contains("$.metadata.vector AS embedding VECTOR HNSW"));
    }

    #[test]
    fn test_vector_params_nargs() {
        // Verify nargs count is correct
        let params = VectorParams::hnsw(1536, DistanceMetric::Cosine);
        let args = params.to_schema_args();

        // nargs should be first: 6 base params (TYPE FLOAT32 DIM 1536 DISTANCE_METRIC COSINE)
        assert_eq!(args[0], "6");
        assert_eq!(args[1], "TYPE");
        assert_eq!(args[2], "FLOAT32");
        assert_eq!(args[3], "DIM");
        assert_eq!(args[4], "1536");
        assert_eq!(args[5], "DISTANCE_METRIC");
        assert_eq!(args[6], "COSINE");
    }

    #[test]
    fn test_vector_params_nargs_with_hnsw_options() {
        let params = VectorParams::hnsw(1536, DistanceMetric::Cosine)
            .with_m(24)
            .with_ef_construction(300);
        let args = params.to_schema_args();

        // nargs should be 10: 6 base + M 24 + EF_CONSTRUCTION 300
        assert_eq!(args[0], "10");
        assert!(args.contains(&"M".to_string()));
        assert!(args.contains(&"24".to_string()));
        assert!(args.contains(&"EF_CONSTRUCTION".to_string()));
        assert!(args.contains(&"300".to_string()));
    }

    #[test]
    fn test_mixed_index_with_vectors() {
        let index = SearchIndex::new("documents", "crdt:documents:")
            .text_sortable("title")
            .text("content")
            .tag("category")
            .numeric("created_at")
            .vector_hnsw("embedding", 1536, DistanceMetric::Cosine);

        let args = index.to_ft_create_args();
        let cmd = format!("FT.CREATE {}", args.join(" "));

        // Verify all field types are present
        assert!(cmd.contains("AS title TEXT SORTABLE"));
        assert!(cmd.contains("AS content TEXT"));
        assert!(cmd.contains("AS category TAG"));
        assert!(cmd.contains("AS created_at NUMERIC"));
        assert!(cmd.contains("AS embedding VECTOR HNSW"));
    }

    #[test]
    fn test_distance_metrics_display() {
        assert_eq!(format!("{}", DistanceMetric::L2), "L2");
        assert_eq!(format!("{}", DistanceMetric::InnerProduct), "IP");
        assert_eq!(format!("{}", DistanceMetric::Cosine), "COSINE");
    }

    #[test]
    fn test_vector_algorithm_display() {
        assert_eq!(format!("{}", VectorAlgorithm::Hnsw), "HNSW");
        assert_eq!(format!("{}", VectorAlgorithm::Flat), "FLAT");
    }
}
