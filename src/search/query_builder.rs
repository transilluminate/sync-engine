// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Query Builder - AST for search queries
//!
//! Provides a type-safe way to build search queries that can be translated
//! to both RediSearch FT.SEARCH syntax and MySQL JSON_EXTRACT queries.
//!
//! # Example
//!
//! ```rust
//! use sync_engine::search::{Query, QueryBuilder};
//!
//! // Simple field query
//! let query = Query::field_eq("name", "Alice");
//!
//! // Complex query with builder
//! let query = QueryBuilder::new()
//!     .field_eq("name", "Alice")
//!     .numeric_range("age", Some(25.0), Some(40.0))
//!     .build_and();
//!
//! // Boolean combinations
//! let query = Query::field_eq("status", "active")
//!     .or(Query::field_eq("status", "pending"));
//! ```

use serde::{Deserialize, Serialize};

/// Search query AST
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    /// Root query node
    pub root: QueryNode,
}

impl Query {
    /// Create a new query from a root node
    pub fn new(root: QueryNode) -> Self {
        Self { root }
    }

    /// Create a field equals query: @field:value
    pub fn field_eq(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Equals,
            value: QueryValue::Text(value.into()),
        }))
    }

    /// Create a tag query: @tags:{value1|value2}
    pub fn tags(field: impl Into<String>, values: Vec<String>) -> Self {
        Self::new(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::In,
            value: QueryValue::Tags(values),
        }))
    }

    /// Create a numeric range query: @age:[min max]
    pub fn numeric_range(field: impl Into<String>, min: Option<f64>, max: Option<f64>) -> Self {
        Self::new(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Range,
            value: QueryValue::NumericRange { min, max },
        }))
    }

    /// Create a full-text search query (contains)
    pub fn text_search(field: impl Into<String>, text: impl Into<String>) -> Self {
        Self::new(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Contains,
            value: QueryValue::Text(text.into()),
        }))
    }

    /// Create a prefix match query: @field:prefix*
    pub fn prefix(field: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self::new(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Prefix,
            value: QueryValue::Text(prefix.into()),
        }))
    }

    /// Create a fuzzy match query: @field:%value%
    pub fn fuzzy(field: impl Into<String>, text: impl Into<String>) -> Self {
        Self::new(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Fuzzy,
            value: QueryValue::Text(text.into()),
        }))
    }

    /// Create a vector similarity (KNN) search query.
    ///
    /// Returns the k nearest neighbors by vector similarity.
    ///
    /// # Arguments
    /// * `field` - The vector field name (must be indexed with vector_hnsw/vector_flat)
    /// * `vector` - Query embedding vector (must match field dimensionality)
    /// * `k` - Number of nearest neighbors to return
    ///
    /// # Example
    /// ```ignore
    /// let embedding = vec![0.1, 0.2, 0.3, /* ... 1536 dims for OpenAI */];
    /// let query = Query::vector("embedding", embedding, 10);
    /// let results = engine.search("documents", &query).await?;
    /// ```
    pub fn vector(field: impl Into<String>, vector: Vec<f32>, k: usize) -> Self {
        Self::new(QueryNode::Vector(VectorQuery {
            field: field.into(),
            vector,
            k,
        }))
    }

    /// Create a filtered vector search query.
    ///
    /// Combines a filter query with vector KNN search. The filter is applied
    /// first, then KNN is performed on the filtered results.
    ///
    /// # Example
    /// ```ignore
    /// let embedding = get_embedding("semantic search query");
    /// let query = Query::vector_filtered(
    ///     Query::tags("category", vec!["tech".into()]),
    ///     "embedding",
    ///     embedding,
    ///     10
    /// );
    /// ```
    pub fn vector_filtered(
        filter: Query,
        field: impl Into<String>,
        vector: Vec<f32>,
        k: usize,
    ) -> Self {
        // Combine filter with vector search using AND
        // The translator will handle the special KNN syntax
        Self::new(QueryNode::And(vec![
            filter.root,
            QueryNode::Vector(VectorQuery {
                field: field.into(),
                vector,
                k,
            }),
        ]))
    }

    /// Combine with AND
    pub fn and(self, other: Query) -> Self {
        Self::new(QueryNode::And(vec![self.root, other.root]))
    }

    /// Combine with OR
    pub fn or(self, other: Query) -> Self {
        Self::new(QueryNode::Or(vec![self.root, other.root]))
    }

    /// Negate query
    pub fn negate(self) -> Self {
        Self::new(QueryNode::Not(Box::new(self.root)))
    }
}

/// Query AST node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryNode {
    /// Field query: @field:value
    Field(FieldQuery),
    /// Boolean AND: (query1 query2)
    And(Vec<QueryNode>),
    /// Boolean OR: (query1 | query2)
    Or(Vec<QueryNode>),
    /// Boolean NOT: -query
    Not(Box<QueryNode>),
    /// Vector KNN search: [KNN k @field $blob]
    Vector(VectorQuery),
}

/// Vector similarity search query (KNN)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorQuery {
    /// Vector field name (e.g., "embedding")
    pub field: String,
    /// Query vector (will be serialized as FLOAT32 blob)
    pub vector: Vec<f32>,
    /// Number of nearest neighbors to return
    pub k: usize,
}

/// Field query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldQuery {
    /// Field name (e.g., "name", "age", "tags")
    pub field: String,
    /// Comparison operator
    pub operator: FieldOperator,
    /// Query value
    pub value: QueryValue,
}

/// Field comparison operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldOperator {
    /// Exact match: @field:value
    Equals,
    /// Contains text: @field:*value*
    Contains,
    /// Numeric/date range: @field:[min max]
    Range,
    /// Tag membership: @tags:{value1|value2}
    In,
    /// Prefix match: @field:prefix*
    Prefix,
    /// Fuzzy match: @field:%value%
    Fuzzy,
}

/// Query value type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryValue {
    /// Text value
    Text(String),
    /// Numeric value
    Numeric(f64),
    /// Numeric range [min, max]
    NumericRange { min: Option<f64>, max: Option<f64> },
    /// Tag values (OR semantics)
    Tags(Vec<String>),
    /// Boolean value
    Boolean(bool),
}

/// Builder for complex queries
#[derive(Default)]
pub struct QueryBuilder {
    nodes: Vec<QueryNode>,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Add a field equals constraint
    pub fn field_eq(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.nodes.push(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Equals,
            value: QueryValue::Text(value.into()),
        }));
        self
    }

    /// Add a numeric equals constraint
    pub fn numeric_eq(mut self, field: impl Into<String>, value: f64) -> Self {
        self.nodes.push(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Equals,
            value: QueryValue::Numeric(value),
        }));
        self
    }

    /// Add a numeric range constraint
    pub fn numeric_range(mut self, field: impl Into<String>, min: Option<f64>, max: Option<f64>) -> Self {
        self.nodes.push(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Range,
            value: QueryValue::NumericRange { min, max },
        }));
        self
    }

    /// Add a tag constraint
    pub fn tags(mut self, field: impl Into<String>, values: Vec<String>) -> Self {
        self.nodes.push(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::In,
            value: QueryValue::Tags(values),
        }));
        self
    }

    /// Add a contains constraint
    pub fn contains(mut self, field: impl Into<String>, text: impl Into<String>) -> Self {
        self.nodes.push(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Contains,
            value: QueryValue::Text(text.into()),
        }));
        self
    }

    /// Add a prefix constraint
    pub fn prefix(mut self, field: impl Into<String>, prefix: impl Into<String>) -> Self {
        self.nodes.push(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Prefix,
            value: QueryValue::Text(prefix.into()),
        }));
        self
    }

    /// Add a fuzzy constraint
    pub fn fuzzy(mut self, field: impl Into<String>, text: impl Into<String>) -> Self {
        self.nodes.push(QueryNode::Field(FieldQuery {
            field: field.into(),
            operator: FieldOperator::Fuzzy,
            value: QueryValue::Text(text.into()),
        }));
        self
    }

    /// Build query with AND semantics (all constraints must match)
    pub fn build_and(self) -> Query {
        if self.nodes.is_empty() {
            // Empty query matches everything (RediSearch: *)
            Query::new(QueryNode::Field(FieldQuery {
                field: "*".to_string(),
                operator: FieldOperator::Equals,
                value: QueryValue::Text("*".to_string()),
            }))
        } else if self.nodes.len() == 1 {
            Query::new(self.nodes.into_iter().next().unwrap())
        } else {
            Query::new(QueryNode::And(self.nodes))
        }
    }

    /// Build query with OR semantics (any constraint can match)
    pub fn build_or(self) -> Query {
        if self.nodes.is_empty() {
            // Empty query matches everything
            Query::new(QueryNode::Field(FieldQuery {
                field: "*".to_string(),
                operator: FieldOperator::Equals,
                value: QueryValue::Text("*".to_string()),
            }))
        } else if self.nodes.len() == 1 {
            Query::new(self.nodes.into_iter().next().unwrap())
        } else {
            Query::new(QueryNode::Or(self.nodes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_field_query() {
        let query = Query::field_eq("name", "Alice");
        assert_eq!(
            query.root,
            QueryNode::Field(FieldQuery {
                field: "name".to_string(),
                operator: FieldOperator::Equals,
                value: QueryValue::Text("Alice".to_string()),
            })
        );
    }

    #[test]
    fn test_and_query() {
        let query = Query::field_eq("name", "Alice")
            .and(Query::numeric_range("age", Some(25.0), Some(40.0)));

        match query.root {
            QueryNode::And(nodes) => {
                assert_eq!(nodes.len(), 2);
            }
            _ => panic!("Expected And node"),
        }
    }

    #[test]
    fn test_or_query() {
        let query = Query::field_eq("status", "active")
            .or(Query::field_eq("status", "pending"));

        match query.root {
            QueryNode::Or(nodes) => {
                assert_eq!(nodes.len(), 2);
            }
            _ => panic!("Expected Or node"),
        }
    }

    #[test]
    fn test_not_query() {
        let query = Query::field_eq("deleted", "true").negate();

        match query.root {
            QueryNode::Not(_) => {}
            _ => panic!("Expected Not node"),
        }
    }

    #[test]
    fn test_tag_query() {
        let query = Query::tags("tags", vec!["rust".to_string(), "database".to_string()]);

        match query.root {
            QueryNode::Field(FieldQuery { field, operator, value }) => {
                assert_eq!(field, "tags");
                assert_eq!(operator, FieldOperator::In);
                assert_eq!(value, QueryValue::Tags(vec!["rust".to_string(), "database".to_string()]));
            }
            _ => panic!("Expected Field node"),
        }
    }

    #[test]
    fn test_query_builder_and() {
        let query = QueryBuilder::new()
            .field_eq("name", "Alice")
            .numeric_range("age", Some(25.0), Some(40.0))
            .tags("tags", vec!["rust".to_string()])
            .build_and();

        match query.root {
            QueryNode::And(nodes) => {
                assert_eq!(nodes.len(), 3);
            }
            _ => panic!("Expected And node"),
        }
    }

    #[test]
    fn test_query_builder_or() {
        let query = QueryBuilder::new()
            .field_eq("status", "active")
            .field_eq("status", "pending")
            .build_or();

        match query.root {
            QueryNode::Or(nodes) => {
                assert_eq!(nodes.len(), 2);
            }
            _ => panic!("Expected Or node"),
        }
    }

    #[test]
    fn test_complex_query() {
        // (name:Alice AND age:[25 40]) OR (name:Bob AND age:[30 50])
        let alice_query = Query::field_eq("name", "Alice")
            .and(Query::numeric_range("age", Some(25.0), Some(40.0)));

        let bob_query = Query::field_eq("name", "Bob")
            .and(Query::numeric_range("age", Some(30.0), Some(50.0)));

        let query = alice_query.or(bob_query);

        match query.root {
            QueryNode::Or(nodes) => {
                assert_eq!(nodes.len(), 2);
                // Each should be an And node
                for node in nodes {
                    match node {
                        QueryNode::And(inner_nodes) => {
                            assert_eq!(inner_nodes.len(), 2);
                        }
                        _ => panic!("Expected And node"),
                    }
                }
            }
            _ => panic!("Expected Or node"),
        }
    }

    #[test]
    fn test_prefix_query() {
        let query = Query::prefix("email", "admin@");
        match query.root {
            QueryNode::Field(FieldQuery { operator, .. }) => {
                assert_eq!(operator, FieldOperator::Prefix);
            }
            _ => panic!("Expected Field node"),
        }
    }

    #[test]
    fn test_fuzzy_query() {
        let query = Query::fuzzy("name", "alice");
        match query.root {
            QueryNode::Field(FieldQuery { operator, .. }) => {
                assert_eq!(operator, FieldOperator::Fuzzy);
            }
            _ => panic!("Expected Field node"),
        }
    }

    #[test]
    fn test_empty_builder_and() {
        let query = QueryBuilder::new().build_and();
        // Should produce a match-all query
        match query.root {
            QueryNode::Field(FieldQuery { field, .. }) => {
                assert_eq!(field, "*");
            }
            _ => panic!("Expected Field node"),
        }
    }
}
