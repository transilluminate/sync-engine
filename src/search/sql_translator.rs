//! SQL Translator
//!
//! Translates Query AST to SQL WHERE clauses for JSON column queries.
//! Uses MySQL JSON_EXTRACT syntax for querying JSON documents.
//!
//! # SQL Syntax Generated
//!
//! ```sql
//! JSON_EXTRACT(data, '$.field') = 'value'              -- Exact match
//! JSON_EXTRACT(data, '$.field') LIKE '%value%'         -- Contains
//! JSON_EXTRACT(data, '$.age') BETWEEN 25 AND 40        -- Range
//! JSON_EXTRACT(data, '$.field') LIKE 'prefix%'         -- Prefix
//! JSON_CONTAINS(data->'$.tags', '"value"')             -- Tag membership
//! ```

use super::query_builder::{FieldOperator, FieldQuery, Query, QueryNode, QueryValue};

/// SQL query translator for JSON column queries
pub struct SqlTranslator;

/// SQL query result with parameterized placeholders
#[derive(Debug, Clone)]
pub struct SqlQuery {
    /// The WHERE clause (without "WHERE" keyword)
    pub clause: String,
    /// The parameter values in order
    pub params: Vec<SqlParam>,
}

/// SQL parameter value
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParam {
    Text(String),
    Numeric(f64),
    Boolean(bool),
}

impl SqlTranslator {
    /// Translate Query AST to parameterized SQL WHERE clause
    ///
    /// Uses `?` placeholders for parameters (MySQL style)
    pub fn translate(query: &Query, json_column: &str) -> SqlQuery {
        let mut params = Vec::new();
        let clause = Self::translate_node(&query.root, json_column, &mut params);
        SqlQuery { clause, params }
    }

    /// Translate Query AST to SQL WHERE clause with inline values
    ///
    /// Warning: Only use for debugging, not for actual queries (SQL injection risk)
    pub fn translate_inline(query: &Query, json_column: &str) -> String {
        let mut params = Vec::new();
        let clause = Self::translate_node(&query.root, json_column, &mut params);

        // Replace placeholders with actual values
        let mut result = clause;
        for param in params {
            let value = match param {
                SqlParam::Text(s) => format!("'{}'", s.replace('\'', "''")),
                SqlParam::Numeric(n) => n.to_string(),
                SqlParam::Boolean(b) => if b { "TRUE" } else { "FALSE" }.to_string(),
            };
            result = result.replacen('?', &value, 1);
        }
        result
    }

    fn translate_node(node: &QueryNode, json_col: &str, params: &mut Vec<SqlParam>) -> String {
        match node {
            QueryNode::Field(field_query) => Self::translate_field(field_query, json_col, params),
            QueryNode::And(nodes) => {
                let parts: Vec<String> = nodes
                    .iter()
                    .map(|n| Self::translate_node(n, json_col, params))
                    .collect();
                if parts.len() == 1 {
                    parts[0].clone()
                } else {
                    format!("({})", parts.join(" AND "))
                }
            }
            QueryNode::Or(nodes) => {
                let parts: Vec<String> = nodes
                    .iter()
                    .map(|n| Self::translate_node(n, json_col, params))
                    .collect();
                if parts.len() == 1 {
                    parts[0].clone()
                } else {
                    format!("({})", parts.join(" OR "))
                }
            }
            QueryNode::Not(inner) => {
                format!("NOT ({})", Self::translate_node(inner, json_col, params))
            }
        }
    }

    fn translate_field(
        field: &FieldQuery,
        json_col: &str,
        params: &mut Vec<SqlParam>,
    ) -> String {
        let json_path = Self::json_path(&field.field);

        match (&field.operator, &field.value) {
            (FieldOperator::Equals, QueryValue::Text(text)) => {
                params.push(SqlParam::Text(text.clone()));
                format!("JSON_UNQUOTE(JSON_EXTRACT({}, '{}')) = ?", json_col, json_path)
            }
            (FieldOperator::Equals, QueryValue::Numeric(num)) => {
                params.push(SqlParam::Numeric(*num));
                format!("JSON_EXTRACT({}, '{}') = ?", json_col, json_path)
            }
            (FieldOperator::Equals, QueryValue::Boolean(b)) => {
                params.push(SqlParam::Boolean(*b));
                format!("JSON_EXTRACT({}, '{}') = ?", json_col, json_path)
            }
            (FieldOperator::Contains, QueryValue::Text(text)) => {
                params.push(SqlParam::Text(format!("%{}%", text)));
                format!(
                    "JSON_UNQUOTE(JSON_EXTRACT({}, '{}')) LIKE ?",
                    json_col, json_path
                )
            }
            (FieldOperator::Range, QueryValue::NumericRange { min, max }) => {
                match (min, max) {
                    (Some(min_val), Some(max_val)) => {
                        params.push(SqlParam::Numeric(*min_val));
                        params.push(SqlParam::Numeric(*max_val));
                        format!(
                            "JSON_EXTRACT({}, '{}') BETWEEN ? AND ?",
                            json_col, json_path
                        )
                    }
                    (Some(min_val), None) => {
                        params.push(SqlParam::Numeric(*min_val));
                        format!("JSON_EXTRACT({}, '{}') >= ?", json_col, json_path)
                    }
                    (None, Some(max_val)) => {
                        params.push(SqlParam::Numeric(*max_val));
                        format!("JSON_EXTRACT({}, '{}') <= ?", json_col, json_path)
                    }
                    (None, None) => "1=1".to_string(), // Always true - no bounds
                }
            }
            (FieldOperator::In, QueryValue::Tags(tags)) => {
                // Use JSON_CONTAINS for tag membership
                // Check if any of the tags are in the array
                let conditions: Vec<String> = tags
                    .iter()
                    .map(|tag| {
                        params.push(SqlParam::Text(format!("\"{}\"", tag)));
                        format!("JSON_CONTAINS({}->'{}', ?)", json_col, json_path)
                    })
                    .collect();

                if conditions.len() == 1 {
                    conditions[0].clone()
                } else {
                    format!("({})", conditions.join(" OR "))
                }
            }
            (FieldOperator::Prefix, QueryValue::Text(text)) => {
                params.push(SqlParam::Text(format!("{}%", text)));
                format!(
                    "JSON_UNQUOTE(JSON_EXTRACT({}, '{}')) LIKE ?",
                    json_col, json_path
                )
            }
            (FieldOperator::Fuzzy, QueryValue::Text(text)) => {
                // SQL doesn't have native fuzzy matching
                // Fallback to contains for now
                params.push(SqlParam::Text(format!("%{}%", text)));
                format!(
                    "JSON_UNQUOTE(JSON_EXTRACT({}, '{}')) LIKE ?",
                    json_col, json_path
                )
            }
            _ => {
                // Fallback - always false for unsupported combinations
                "1=0".to_string()
            }
        }
    }

    fn json_path(field: &str) -> String {
        // Support dot notation for nested fields
        // e.g., "user.name" -> "$.user.name"
        if field.starts_with('$') {
            field.to_string()
        } else {
            format!("$.{}", field)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_field_query() {
        let query = Query::field_eq("name", "Alice");
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(sql.clause, "JSON_UNQUOTE(JSON_EXTRACT(data, '$.name')) = ?");
        assert_eq!(sql.params, vec![SqlParam::Text("Alice".to_string())]);
    }

    #[test]
    fn test_inline_simple_field() {
        let query = Query::field_eq("name", "Alice");
        let sql = SqlTranslator::translate_inline(&query, "data");
        assert_eq!(sql, "JSON_UNQUOTE(JSON_EXTRACT(data, '$.name')) = 'Alice'");
    }

    #[test]
    fn test_numeric_range() {
        let query = Query::numeric_range("age", Some(25.0), Some(40.0));
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(sql.clause, "JSON_EXTRACT(data, '$.age') BETWEEN ? AND ?");
        assert_eq!(
            sql.params,
            vec![SqlParam::Numeric(25.0), SqlParam::Numeric(40.0)]
        );
    }

    #[test]
    fn test_numeric_range_unbounded_min() {
        let query = Query::numeric_range("age", None, Some(40.0));
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(sql.clause, "JSON_EXTRACT(data, '$.age') <= ?");
        assert_eq!(sql.params, vec![SqlParam::Numeric(40.0)]);
    }

    #[test]
    fn test_numeric_range_unbounded_max() {
        let query = Query::numeric_range("score", Some(100.0), None);
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(sql.clause, "JSON_EXTRACT(data, '$.score') >= ?");
        assert_eq!(sql.params, vec![SqlParam::Numeric(100.0)]);
    }

    #[test]
    fn test_tag_query() {
        let query = Query::tags("tags", vec!["rust".to_string(), "database".to_string()]);
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(
            sql.clause,
            "(JSON_CONTAINS(data->'$.tags', ?) OR JSON_CONTAINS(data->'$.tags', ?))"
        );
        assert_eq!(
            sql.params,
            vec![
                SqlParam::Text("\"rust\"".to_string()),
                SqlParam::Text("\"database\"".to_string())
            ]
        );
    }

    #[test]
    fn test_and_query() {
        let query =
            Query::field_eq("name", "Alice").and(Query::numeric_range("age", Some(25.0), Some(40.0)));
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(
            sql.clause,
            "(JSON_UNQUOTE(JSON_EXTRACT(data, '$.name')) = ? AND JSON_EXTRACT(data, '$.age') BETWEEN ? AND ?)"
        );
        assert_eq!(
            sql.params,
            vec![
                SqlParam::Text("Alice".to_string()),
                SqlParam::Numeric(25.0),
                SqlParam::Numeric(40.0)
            ]
        );
    }

    #[test]
    fn test_or_query() {
        let query =
            Query::field_eq("status", "active").or(Query::field_eq("status", "pending"));
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(
            sql.clause,
            "(JSON_UNQUOTE(JSON_EXTRACT(data, '$.status')) = ? OR JSON_UNQUOTE(JSON_EXTRACT(data, '$.status')) = ?)"
        );
    }

    #[test]
    fn test_not_query() {
        let query = Query::field_eq("deleted", "true").negate();
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(
            sql.clause,
            "NOT (JSON_UNQUOTE(JSON_EXTRACT(data, '$.deleted')) = ?)"
        );
    }

    #[test]
    fn test_contains_query() {
        let query = Query::text_search("description", "database");
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(
            sql.clause,
            "JSON_UNQUOTE(JSON_EXTRACT(data, '$.description')) LIKE ?"
        );
        assert_eq!(sql.params, vec![SqlParam::Text("%database%".to_string())]);
    }

    #[test]
    fn test_prefix_query() {
        let query = Query::prefix("email", "admin");
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(
            sql.clause,
            "JSON_UNQUOTE(JSON_EXTRACT(data, '$.email')) LIKE ?"
        );
        assert_eq!(sql.params, vec![SqlParam::Text("admin%".to_string())]);
    }

    #[test]
    fn test_nested_field() {
        let query = Query::field_eq("user.profile.name", "Alice");
        let sql = SqlTranslator::translate(&query, "data");
        assert_eq!(
            sql.clause,
            "JSON_UNQUOTE(JSON_EXTRACT(data, '$.user.profile.name')) = ?"
        );
    }

    #[test]
    fn test_complex_query() {
        let alice_query =
            Query::field_eq("name", "Alice").and(Query::numeric_range("age", Some(25.0), Some(40.0)));

        let bob_query = Query::field_eq("name", "Bob")
            .and(Query::tags("tags", vec!["rust".to_string()]));

        let query = alice_query.or(bob_query);
        let sql = SqlTranslator::translate(&query, "data");

        // Should be ((name=Alice AND age BETWEEN) OR (name=Bob AND tags CONTAINS))
        assert!(sql.clause.starts_with("(("));
        assert!(sql.clause.contains(" AND "));
        assert!(sql.clause.contains(" OR "));
    }
}
