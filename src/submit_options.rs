//! Submit options for caller-controlled storage routing.
//!
//! sync-engine is a **dumb storage layer** - it stores bytes and routes
//! them to L1/L2/L3 based on caller-provided options. The caller decides
//! where data goes. Compression is the caller's responsibility.
//!
//! # Example
//!
//! ```rust,ignore
//! use sync_engine::{SubmitOptions, CacheTtl};
//!
//! // Default: store in both Redis and SQL
//! let default_opts = SubmitOptions::default();
//!
//! // Durable storage (SQL only, skip Redis cache)
//! let durable_opts = SubmitOptions::durable();
//!
//! // Ephemeral cache (Redis only with TTL)
//! let cache_opts = SubmitOptions::cache(CacheTtl::Hour);
//! ```

use std::time::Duration;

/// Standard cache TTL values that encourage batch grouping.
///
/// Using standard TTLs means items naturally batch together for efficient
/// pipelined writes. Custom durations are supported but should be used sparingly.
///
/// # Batching Behavior
///
/// Items with the same `CacheTtl` variant are batched together:
/// - 1000 items with `CacheTtl::Short` → 1 Redis pipeline
/// - 500 items with `CacheTtl::Short` + 500 with `CacheTtl::Hour` → 2 pipelines
///
/// # Example
///
/// ```rust
/// use sync_engine::{SubmitOptions, CacheTtl};
///
/// // Prefer standard TTLs for batching efficiency
/// let opts = SubmitOptions::cache(CacheTtl::Hour);
///
/// // Custom TTL when you really need a specific duration
/// let opts = SubmitOptions::cache(CacheTtl::custom_secs(90));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheTtl {
    /// 1 minute - very short-lived cache
    Minute,
    /// 5 minutes - short cache
    Short,
    /// 15 minutes - medium cache
    Medium,
    /// 1 hour - standard cache (most common)
    Hour,
    /// 24 hours - long cache
    Day,
    /// 7 days - very long cache
    Week,
    /// Custom duration in seconds (use sparingly to preserve batching)
    Custom(u64),
}

impl CacheTtl {
    /// Create a custom TTL from seconds.
    ///
    /// **Prefer standard TTLs** (`Minute`, `Hour`, etc.) for better batching.
    /// Custom TTLs create separate batches, reducing pipeline efficiency.
    #[must_use]
    pub fn custom_secs(secs: u64) -> Self {
        Self::Custom(secs)
    }

    /// Convert to Duration.
    #[must_use]
    pub fn to_duration(self) -> Duration {
        match self {
            CacheTtl::Minute => Duration::from_secs(60),
            CacheTtl::Short => Duration::from_secs(5 * 60),
            CacheTtl::Medium => Duration::from_secs(15 * 60),
            CacheTtl::Hour => Duration::from_secs(60 * 60),
            CacheTtl::Day => Duration::from_secs(24 * 60 * 60),
            CacheTtl::Week => Duration::from_secs(7 * 24 * 60 * 60),
            CacheTtl::Custom(secs) => Duration::from_secs(secs),
        }
    }

    /// Get the TTL in seconds (for OptionsKey grouping).
    #[must_use]
    pub fn as_secs(self) -> u64 {
        self.to_duration().as_secs()
    }
}

impl From<Duration> for CacheTtl {
    /// Convert Duration to CacheTtl, snapping to standard values when close.
    fn from(d: Duration) -> Self {
        let secs = d.as_secs();
        match secs {
            0..=90 => CacheTtl::Minute,           // 0-1.5min → 1min
            91..=450 => CacheTtl::Short,          // 1.5-7.5min → 5min
            451..=2700 => CacheTtl::Medium,       // 7.5-45min → 15min
            2701..=5400 => CacheTtl::Hour,        // 45min-1.5hr → 1hr
            5401..=129600 => CacheTtl::Day,       // 1.5hr-36hr → 24hr
            129601..=864000 => CacheTtl::Week,    // 36hr-10days → 7days
            _ => CacheTtl::Custom(secs),          // Beyond 10 days → custom
        }
    }
}

/// Options for controlling where data is stored.
///
/// sync-engine is a **dumb byte router** - it stores `Vec<u8>` and routes
/// to L1 (always), L2 (Redis), and L3 (SQL) based on these options.
///
/// **Compression is the caller's responsibility.** Compress before submit
/// if desired. This allows callers to choose their trade-offs:
/// - Compressed data = smaller storage, but no SQL JSON search
/// - Uncompressed JSON = SQL JSON functions work, larger storage
#[derive(Debug, Clone)]
pub struct SubmitOptions {
    /// Store in L2 Redis.
    ///
    /// Default: `true`
    pub redis: bool,

    /// TTL for Redis key. `None` means no expiry.
    ///
    /// Use [`CacheTtl`] enum values for efficient batching.
    ///
    /// Default: `None`
    pub redis_ttl: Option<CacheTtl>,

    /// Store in L3 SQL (MySQL/SQLite).
    ///
    /// Default: `true`
    pub sql: bool,
    
    /// Override state tag for this item.
    ///
    /// If `Some`, overrides the item's existing state.
    /// If `None`, uses the item's current state (default: "default").
    ///
    /// State is indexed for fast queries: SQL index + Redis SETs.
    pub state: Option<String>,
}

impl Default for SubmitOptions {
    fn default() -> Self {
        Self {
            redis: true,
            redis_ttl: None,
            sql: true,
            state: None,
        }
    }
}

impl SubmitOptions {
    /// Create options for Redis-only ephemeral cache with TTL.
    ///
    /// Uses [`CacheTtl`] enum for efficient batching. Items with the same
    /// TTL variant are batched together in a single Redis pipeline.
    ///
    /// - Redis: yes, not compressed (searchable)
    /// - SQL: no
    ///
    /// # Example
    ///
    /// ```rust
    /// use sync_engine::{SubmitOptions, CacheTtl};
    ///
    /// // Standard 1-hour cache (batches efficiently)
    /// let opts = SubmitOptions::cache(CacheTtl::Hour);
    ///
    /// // 5-minute short cache
    /// let opts = SubmitOptions::cache(CacheTtl::Short);
    /// ```
    #[must_use]
    pub fn cache(ttl: CacheTtl) -> Self {
        Self {
            redis: true,
            redis_ttl: Some(ttl),
            sql: false,
            state: None,
        }
    }

    /// Create options for SQL-only durable storage.
    ///
    /// - Redis: no
    /// - SQL: yes
    #[must_use]
    pub fn durable() -> Self {
        Self {
            redis: false,
            redis_ttl: None,
            sql: true,
            state: None,
        }
    }
    
    /// Set the state for items submitted with these options (builder pattern).
    ///
    /// # Example
    ///
    /// ```rust
    /// use sync_engine::SubmitOptions;
    ///
    /// let opts = SubmitOptions::default().with_state("delta");
    /// ```
    #[must_use]
    pub fn with_state(mut self, state: impl Into<String>) -> Self {
        self.state = Some(state.into());
        self
    }

    /// Returns true if data should be stored anywhere.
    #[must_use]
    pub fn stores_anywhere(&self) -> bool {
        self.redis || self.sql
    }
}

/// Key for grouping items with compatible options in batches.
///
/// Items with the same `OptionsKey` can be batched together for
/// efficient pipelined writes. Uses [`CacheTtl`] enum directly for
/// natural grouping by standard TTL values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OptionsKey {
    /// Store in Redis
    pub redis: bool,
    /// TTL enum value (None = no expiry)
    pub redis_ttl: Option<CacheTtl>,
    /// Store in SQL
    pub sql: bool,
}

impl From<&SubmitOptions> for OptionsKey {
    fn from(opts: &SubmitOptions) -> Self {
        Self {
            redis: opts.redis,
            redis_ttl: opts.redis_ttl,
            sql: opts.sql,
        }
    }
}

impl From<SubmitOptions> for OptionsKey {
    fn from(opts: SubmitOptions) -> Self {
        Self::from(&opts)
    }
}

impl OptionsKey {
    /// Convert back to SubmitOptions (for use in flush logic).
    #[must_use]
    pub fn to_options(&self) -> SubmitOptions {
        SubmitOptions {
            redis: self.redis,
            redis_ttl: self.redis_ttl,
            sql: self.sql,
            state: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = SubmitOptions::default();
        assert!(opts.redis);
        assert!(opts.redis_ttl.is_none());
        assert!(opts.sql);
    }

    #[test]
    fn test_cache_options() {
        let opts = SubmitOptions::cache(CacheTtl::Hour);
        assert!(opts.redis);
        assert_eq!(opts.redis_ttl, Some(CacheTtl::Hour));
        assert!(!opts.sql);
    }

    #[test]
    fn test_durable_options() {
        let opts = SubmitOptions::durable();
        assert!(!opts.redis);
        assert!(opts.sql);
    }

    #[test]
    fn test_stores_anywhere() {
        assert!(SubmitOptions::default().stores_anywhere());
        assert!(SubmitOptions::cache(CacheTtl::Minute).stores_anywhere());
        assert!(SubmitOptions::durable().stores_anywhere());
        
        let nowhere = SubmitOptions {
            redis: false,
            sql: false,
            ..Default::default()
        };
        assert!(!nowhere.stores_anywhere());
    }

    #[test]
    fn test_options_key_grouping() {
        let opts1 = SubmitOptions::default();
        let opts2 = SubmitOptions::default();
        
        let key1 = OptionsKey::from(&opts1);
        let key2 = OptionsKey::from(&opts2);
        
        // Same options should have same key
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_options_key_same_ttl_enum() {
        // Items with same CacheTtl variant batch together
        let opts1 = SubmitOptions::cache(CacheTtl::Hour);
        let opts2 = SubmitOptions::cache(CacheTtl::Hour);
        
        let key1 = OptionsKey::from(&opts1);
        let key2 = OptionsKey::from(&opts2);
        
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_options_key_different_ttl_enum() {
        // Different CacheTtl variants = different batches
        let opts1 = SubmitOptions::cache(CacheTtl::Hour);
        let opts2 = SubmitOptions::cache(CacheTtl::Day);
        
        let key1 = OptionsKey::from(&opts1);
        let key2 = OptionsKey::from(&opts2);
        
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_ttl_to_duration() {
        assert_eq!(CacheTtl::Minute.to_duration(), Duration::from_secs(60));
        assert_eq!(CacheTtl::Short.to_duration(), Duration::from_secs(300));
        assert_eq!(CacheTtl::Hour.to_duration(), Duration::from_secs(3600));
        assert_eq!(CacheTtl::Day.to_duration(), Duration::from_secs(86400));
        assert_eq!(CacheTtl::Custom(123).to_duration(), Duration::from_secs(123));
    }

    #[test]
    fn test_cache_ttl_from_duration_snapping() {
        // Close to 1 minute → snaps to Minute
        assert_eq!(CacheTtl::from(Duration::from_secs(45)), CacheTtl::Minute);
        assert_eq!(CacheTtl::from(Duration::from_secs(90)), CacheTtl::Minute);
        
        // Close to 5 minutes → snaps to Short
        assert_eq!(CacheTtl::from(Duration::from_secs(180)), CacheTtl::Short);
        
        // Close to 1 hour → snaps to Hour
        assert_eq!(CacheTtl::from(Duration::from_secs(3600)), CacheTtl::Hour);
    }

    #[test]
    fn test_options_key_roundtrip() {
        let original = SubmitOptions::cache(CacheTtl::Hour);
        let key = OptionsKey::from(&original);
        let recovered = key.to_options();
        
        assert_eq!(original.redis, recovered.redis);
        assert_eq!(original.redis_ttl, recovered.redis_ttl);
        assert_eq!(original.sql, recovered.sql);
    }

    #[test]
    fn test_options_key_hashable() {
        use std::collections::HashMap;
        
        let mut map: HashMap<OptionsKey, Vec<String>> = HashMap::new();
        
        let key = OptionsKey::from(&SubmitOptions::default());
        map.entry(key).or_default().push("item1".into());
        
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_state_default_none() {
        let opts = SubmitOptions::default();
        assert!(opts.state.is_none());
    }

    #[test]
    fn test_state_with_state_builder() {
        let opts = SubmitOptions::default().with_state("delta");
        assert_eq!(opts.state, Some("delta".to_string()));
    }

    #[test]
    fn test_state_cache_with_state() {
        let opts = SubmitOptions::cache(CacheTtl::Hour).with_state("pending");
        assert!(opts.redis);
        assert!(!opts.sql);
        assert_eq!(opts.state, Some("pending".to_string()));
    }

    #[test]
    fn test_state_durable_with_state() {
        let opts = SubmitOptions::durable().with_state("archived");
        assert!(!opts.redis);
        assert!(opts.sql);
        assert_eq!(opts.state, Some("archived".to_string()));
    }

    #[test]
    fn test_state_to_options_preserves_none() {
        let opts = SubmitOptions::cache(CacheTtl::Hour);
        let key = OptionsKey::from(&opts);
        let recovered = key.to_options();
        assert!(recovered.state.is_none());
    }
}
