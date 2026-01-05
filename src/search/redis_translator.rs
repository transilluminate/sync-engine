// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! RediSearch Translator
//!
//! Translates Query AST to RediSearch FT.SEARCH syntax.
//!
//! # RediSearch Query Syntax
//!
//! ```text
//! @field:value              - Exact match
//! @field:*value*            - Contains
//! @field:[min max]          - Numeric range
//! @tags:{value1|value2}     - Tag membership
//! @field:prefix*            - Prefix match
//! @field:%value%            - Fuzzy match (Levenshtein distance 1)
//! query1 query2             - AND (implicit)
//! query1 | query2           - OR
//! -query                    - NOT
//! (query1 query2)           - Grouping
//! (filter)=>[KNN k @field $blob]  - Vector KNN search
//! ```

use super::query_builder::{FieldOperator, FieldQuery, Query, QueryNode, QueryValue, VectorQuery};

/// Result of translating a query that may contain vector search.
/// Contains the query string and any binary parameters needed for vector search.
#[derive(Debug, Clone)]
pub struct TranslatedQuery {
    /// The FT.SEARCH query string
    pub query: String,
    /// Binary parameters for vector search (param_name -> blob)
    /// These must be passed to FT.SEARCH as PARAMS
    pub params: Vec<(String, Vec<u8>)>,
}

impl TranslatedQuery {
    /// Create a simple query with no parameters
    pub fn simple(query: String) -> Self {
        Self {
            query,
            params: Vec::new(),
        }
    }

    /// Check if this query requires binary parameters
    pub fn has_params(&self) -> bool {
        !self.params.is_empty()
    }
}

/// RediSearch query translator
pub struct RediSearchTranslator;

impl RediSearchTranslator {
    /// Translate Query AST to RediSearch FT.SEARCH syntax.
    /// For queries without vectors, use this simple method.
    pub fn translate(query: &Query) -> String {
        Self::translate_with_params(query).query
    }

    /// Translate Query AST to RediSearch FT.SEARCH syntax with parameters.
    /// Use this for queries that may contain vector search.
    pub fn translate_with_params(query: &Query) -> TranslatedQuery {
        Self::translate_node_with_params(&query.root)
    }

    fn translate_node_with_params(node: &QueryNode) -> TranslatedQuery {
        match node {
            QueryNode::Field(field_query) => {
                TranslatedQuery::simple(Self::translate_field(field_query))
            }
            QueryNode::And(nodes) => {
                // Check if any node is a vector query
                let (vector_nodes, filter_nodes): (Vec<_>, Vec<_>) = nodes
                    .iter()
                    .partition(|n| matches!(n, QueryNode::Vector(_)));

                if let Some(QueryNode::Vector(vq)) = vector_nodes.first() {
                    // We have a vector query with optional filter
                    let filter = if filter_nodes.is_empty() {
                        "*".to_string()
                    } else {
                        let parts: Vec<String> = filter_nodes
                            .iter()
                            .map(|n| Self::translate_node_with_params(n).query)
                            .collect();
                        if parts.len() == 1 {
                            parts[0].clone()
                        } else {
                            format!("({})", parts.join(" "))
                        }
                    };
                    Self::translate_vector_query(vq, &filter)
                } else {
                    // No vector query, just AND
                    let parts: Vec<String> = nodes
                        .iter()
                        .map(|n| Self::translate_node_with_params(n).query)
                        .collect();
                    let query = if parts.len() == 1 {
                        parts[0].clone()
                    } else {
                        format!("({})", parts.join(" "))
                    };
                    TranslatedQuery::simple(query)
                }
            }
            QueryNode::Or(nodes) => {
                let parts: Vec<String> = nodes
                    .iter()
                    .map(|n| Self::translate_node_with_params(n).query)
                    .collect();
                let query = if parts.len() == 1 {
                    parts[0].clone()
                } else {
                    format!("({})", parts.join(" | "))
                };
                TranslatedQuery::simple(query)
            }
            QueryNode::Not(inner) => {
                let inner_result = Self::translate_node_with_params(inner);
                TranslatedQuery {
                    query: format!("-({})", inner_result.query),
                    params: inner_result.params,
                }
            }
            QueryNode::Vector(vq) => {
                // Standalone vector query with wildcard filter
                Self::translate_vector_query(vq, "*")
            }
        }
    }

    /// Translate a vector query with the given filter.
    /// RediSearch KNN syntax: (filter)=>[KNN k @field $blob]
    fn translate_vector_query(vq: &VectorQuery, filter: &str) -> TranslatedQuery {
        let param_name = "vec_blob";
        
        // Convert Vec<f32> to little-endian bytes (FLOAT32 format)
        let blob: Vec<u8> = vq.vector
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();

        let query = format!(
            "({})=>[KNN {} @{} ${} AS vector_score]",
            filter,
            vq.k,
            Self::escape_field_name(&vq.field),
            param_name
        );

        TranslatedQuery {
            query,
            params: vec![(param_name.to_string(), blob)],
        }
    }

    #[allow(dead_code)]
    fn translate_node(node: &QueryNode) -> String {
        Self::translate_node_with_params(node).query
    }

    fn translate_field(field: &FieldQuery) -> String {
        let field_name = Self::escape_field_name(&field.field);

        match (&field.operator, &field.value) {
            (FieldOperator::Equals, QueryValue::Text(text)) => {
                // For multi-word text, use phrase matching with parentheses
                // e.g., @name:(Alice Smith) which requires all terms to match
                let escaped = Self::escape_special_chars(text);
                if text.contains(' ') {
                    format!("@{}:({})", field_name, escaped)
                } else {
                    format!("@{}:{}", field_name, escaped)
                }
            }
            (FieldOperator::Equals, QueryValue::Numeric(num)) => {
                format!("@{}:[{} {}]", field_name, num, num)
            }
            (FieldOperator::Equals, QueryValue::Boolean(b)) => {
                format!("@{}:{}", field_name, if *b { "true" } else { "false" })
            }
            (FieldOperator::Contains, QueryValue::Text(text)) => {
                format!("@{}:*{}*", field_name, Self::escape_special_chars(text))
            }
            (FieldOperator::Range, QueryValue::NumericRange { min, max }) => {
                let min_str = min.map(|v| v.to_string()).unwrap_or_else(|| "-inf".to_string());
                let max_str = max.map(|v| v.to_string()).unwrap_or_else(|| "+inf".to_string());
                format!("@{}:[{} {}]", field_name, min_str, max_str)
            }
            (FieldOperator::In, QueryValue::Tags(tags)) => {
                let tag_str = tags
                    .iter()
                    .map(|t| Self::escape_value(t))
                    .collect::<Vec<_>>()
                    .join("|");
                format!("@{}:{{{}}}", field_name, tag_str)
            }
            (FieldOperator::Prefix, QueryValue::Text(text)) => {
                format!("@{}:{}*", field_name, Self::escape_value(text))
            }
            (FieldOperator::Fuzzy, QueryValue::Text(text)) => {
                format!("@{}:%{}%", field_name, Self::escape_value(text))
            }
            _ => {
                // Fallback for unsupported combinations
                format!("@{}:{:?}", field_name, field.value)
            }
        }
    }

    fn escape_field_name(field: &str) -> String {
        // Field names with special chars need backtick escaping
        if field.contains(|c: char| !c.is_alphanumeric() && c != '_') {
            format!("`{}`", field)
        } else {
            field.to_string()
        }
    }

    /// Escape special RediSearch characters but preserve spaces (for phrase matching).
    fn escape_special_chars(value: &str) -> String {
        let mut escaped = String::new();
        for c in value.chars() {
            match c {
                // RediSearch special chars that need escaping (not spaces - used in phrases)
                '@' | ':' | '|' | '(' | ')' | '[' | ']' | '{' | '}' | '*' | '%' | '-' | '+' => {
                    escaped.push('\\');
                    escaped.push(c);
                }
                _ => escaped.push(c),
            }
        }
        escaped
    }

    /// Escape all special chars including spaces (for single-term matching).
    fn escape_value(value: &str) -> String {
        let mut escaped = String::new();
        for c in value.chars() {
            match c {
                // RediSearch special chars that need escaping
                '@' | ':' | '|' | '(' | ')' | '[' | ']' | '{' | '}' | '*' | '%' | '-' | '+' | ' ' => {
                    escaped.push('\\');
                    escaped.push(c);
                }
                _ => escaped.push(c),
            }
        }
        escaped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_field_query() {
        let query = Query::field_eq("name", "Alice");
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@name:Alice");
    }

    #[test]
    fn test_field_with_spaces() {
        // Multi-word text queries use phrase matching with parentheses
        let query = Query::field_eq("name", "Alice Smith");
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@name:(Alice Smith)");
    }

    #[test]
    fn test_numeric_range() {
        let query = Query::numeric_range("age", Some(25.0), Some(40.0));
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@age:[25 40]");
    }

    #[test]
    fn test_numeric_range_unbounded_min() {
        let query = Query::numeric_range("age", None, Some(40.0));
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@age:[-inf 40]");
    }

    #[test]
    fn test_numeric_range_unbounded_max() {
        let query = Query::numeric_range("score", Some(100.0), None);
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@score:[100 +inf]");
    }

    #[test]
    fn test_tag_query() {
        let query = Query::tags("tags", vec!["rust".to_string(), "database".to_string()]);
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@tags:{rust|database}");
    }

    #[test]
    fn test_and_query() {
        let query = Query::field_eq("name", "Alice")
            .and(Query::numeric_range("age", Some(25.0), Some(40.0)));
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "(@name:Alice @age:[25 40])");
    }

    #[test]
    fn test_or_query() {
        let query = Query::field_eq("status", "active")
            .or(Query::field_eq("status", "pending"));
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "(@status:active | @status:pending)");
    }

    #[test]
    fn test_not_query() {
        let query = Query::field_eq("deleted", "true").negate();
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "-(@deleted:true)");
    }

    #[test]
    fn test_contains_query() {
        let query = Query::text_search("description", "database");
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@description:*database*");
    }

    #[test]
    fn test_prefix_query() {
        let query = Query::prefix("email", "admin");
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@email:admin*");
    }

    #[test]
    fn test_fuzzy_query() {
        let query = Query::fuzzy("name", "alice");
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@name:%alice%");
    }

    #[test]
    fn test_complex_query() {
        // (name:Alice AND age:[25 40]) OR (name:Bob AND tags:{rust|database})
        let alice_query = Query::field_eq("name", "Alice")
            .and(Query::numeric_range("age", Some(25.0), Some(40.0)));

        let bob_query = Query::field_eq("name", "Bob")
            .and(Query::tags("tags", vec!["rust".to_string(), "database".to_string()]));

        let query = alice_query.or(bob_query);
        let redis_query = RediSearchTranslator::translate(&query);

        assert_eq!(
            redis_query,
            "((@name:Alice @age:[25 40]) | (@name:Bob @tags:{rust|database}))"
        );
    }

    #[test]
    fn test_escape_special_chars() {
        let query = Query::field_eq("email", "user@example.com");
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@email:user\\@example.com");
    }

    #[test]
    fn test_escape_colon() {
        let query = Query::field_eq("time", "12:30");
        let redis_query = RediSearchTranslator::translate(&query);
        assert_eq!(redis_query, "@time:12\\:30");
    }

    #[test]
    fn test_vector_query_basic() {
        let embedding = vec![0.1, 0.2, 0.3, 0.4];
        let query = Query::vector("embedding", embedding.clone(), 10);
        let result = RediSearchTranslator::translate_with_params(&query);

        // Query should have KNN syntax with wildcard filter
        assert_eq!(result.query, "(*)=>[KNN 10 @embedding $vec_blob AS vector_score]");

        // Should have one parameter
        assert_eq!(result.params.len(), 1);
        assert_eq!(result.params[0].0, "vec_blob");

        // Verify blob is little-endian f32 bytes
        let blob = &result.params[0].1;
        assert_eq!(blob.len(), 4 * 4); // 4 floats * 4 bytes each
    }

    #[test]
    fn test_vector_query_with_filter() {
        let embedding = vec![0.1, 0.2, 0.3, 0.4];
        let query = Query::vector_filtered(
            Query::tags("category", vec!["tech".into()]),
            "embedding",
            embedding,
            5,
        );
        let result = RediSearchTranslator::translate_with_params(&query);

        // Should have filter before KNN
        assert_eq!(result.query, "(@category:{tech})=>[KNN 5 @embedding $vec_blob AS vector_score]");
        assert!(result.has_params());
    }

    #[test]
    fn test_vector_query_complex_filter() {
        let embedding = vec![1.0, 2.0];
        let filter = Query::field_eq("status", "active")
            .and(Query::numeric_range("age", Some(18.0), None));

        let query = Query::vector_filtered(filter, "vec", embedding, 20);
        let result = RediSearchTranslator::translate_with_params(&query);

        // Filter should be AND-combined
        assert!(result.query.contains("@status:active"));
        assert!(result.query.contains("@age:[18 +inf]"));
        assert!(result.query.contains("=>[KNN 20 @vec $vec_blob AS vector_score]"));
    }

    #[test]
    fn test_vector_blob_encoding() {
        // Verify exact byte encoding
        let embedding = vec![1.0f32];
        let query = Query::vector("field", embedding, 1);
        let result = RediSearchTranslator::translate_with_params(&query);

        let blob = &result.params[0].1;
        // 1.0f32 in little-endian is 0x3f800000
        assert_eq!(blob, &[0x00, 0x00, 0x80, 0x3f]);
    }

    #[test]
    fn test_non_vector_query_has_no_params() {
        let query = Query::field_eq("name", "test");
        let result = RediSearchTranslator::translate_with_params(&query);

        assert!(!result.has_params());
        assert!(result.params.is_empty());
    }
}
