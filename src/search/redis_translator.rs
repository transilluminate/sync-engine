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
//! ```

use super::query_builder::{FieldOperator, FieldQuery, Query, QueryNode, QueryValue};

/// RediSearch query translator
pub struct RediSearchTranslator;

impl RediSearchTranslator {
    /// Translate Query AST to RediSearch FT.SEARCH syntax
    pub fn translate(query: &Query) -> String {
        Self::translate_node(&query.root)
    }

    fn translate_node(node: &QueryNode) -> String {
        match node {
            QueryNode::Field(field_query) => Self::translate_field(field_query),
            QueryNode::And(nodes) => {
                let parts: Vec<String> = nodes.iter().map(Self::translate_node).collect();
                if parts.len() == 1 {
                    parts[0].clone()
                } else {
                    format!("({})", parts.join(" "))
                }
            }
            QueryNode::Or(nodes) => {
                let parts: Vec<String> = nodes.iter().map(Self::translate_node).collect();
                if parts.len() == 1 {
                    parts[0].clone()
                } else {
                    format!("({})", parts.join(" | "))
                }
            }
            QueryNode::Not(inner) => {
                format!("-({})", Self::translate_node(inner))
            }
        }
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
}
