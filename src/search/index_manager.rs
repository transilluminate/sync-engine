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
            self.field_type.to_string(),
        ];

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchFieldType {
    /// Full-text searchable field
    Text,
    /// Numeric field (supports range queries)
    Numeric,
    /// Tag field (exact match, supports OR)
    Tag,
    /// Geographic field (latitude, longitude)
    Geo,
    /// Vector field (for similarity search)
    Vector,
}

impl std::fmt::Display for SearchFieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchFieldType::Text => write!(f, "TEXT"),
            SearchFieldType::Numeric => write!(f, "NUMERIC"),
            SearchFieldType::Tag => write!(f, "TAG"),
            SearchFieldType::Geo => write!(f, "GEO"),
            SearchFieldType::Vector => write!(f, "VECTOR"),
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
}
