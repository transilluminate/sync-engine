//! Proactive Redis eviction manager.
//!
//! Provides intelligent eviction before Redis LRU kicks in, protecting
//! infrastructure keys (merkle trees, indexes) while evicting data keys
//! based on access patterns and memory pressure.
//!
//! Uses the same tan-curve scoring algorithm as L1 memory eviction.
//!
//! # Strategy
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │  Layer 1: Proactive Tan-Curve (sync-engine managed)        │
//! │  └─ Score = f(last_access, access_count, size, pressure)   │
//! │  └─ Protected: merkle:*, idx:*, RediSearch indexes         │
//! │  └─ Evict lowest-scoring data keys first                   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Layer 2: Redis LRU (emergency fallback)                   │
//! │  └─ Only kicks in if proactive eviction miscalculates      │
//! │  └─ maxmemory-policy: allkeys-lru                          │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tracing::{debug, info};

use super::tan_curve::{CacheEntry, TanCurvePolicy};

/// Cached Redis memory profile to avoid round-trips.
#[derive(Debug, Clone)]
pub struct RedisMemoryProfile {
    /// Used memory in bytes (from INFO MEMORY)
    pub used_bytes: u64,
    /// Max memory in bytes (from CONFIG GET maxmemory)
    pub max_bytes: u64,
    /// When we last refreshed from Redis
    pub last_updated: Instant,
    /// Estimated bytes written since last refresh
    pub pending_writes_estimate: u64,
}

impl Default for RedisMemoryProfile {
    fn default() -> Self {
        Self {
            used_bytes: 0,
            max_bytes: 100 * 1024 * 1024, // 100MB default
            last_updated: Instant::now(),
            pending_writes_estimate: 0,
        }
    }
}

impl RedisMemoryProfile {
    /// Current memory pressure (0.0 to 1.0+)
    pub fn pressure(&self) -> f64 {
        if self.max_bytes == 0 {
            return 0.0;
        }
        (self.used_bytes + self.pending_writes_estimate) as f64 / self.max_bytes as f64
    }
    
    /// Whether we need to refresh from Redis
    pub fn needs_refresh(&self, max_staleness: Duration, max_drift_bytes: u64) -> bool {
        self.last_updated.elapsed() > max_staleness
            || self.pending_writes_estimate > max_drift_bytes
    }
    
    /// Add pending writes estimate (call after batch write)
    pub fn add_pending_writes(&mut self, bytes: u64) {
        self.pending_writes_estimate += bytes;
    }
    
    /// Refresh from Redis INFO output
    pub fn refresh_from_info(&mut self, used_bytes: u64, max_bytes: u64) {
        self.used_bytes = used_bytes;
        self.max_bytes = max_bytes;
        self.pending_writes_estimate = 0;
        self.last_updated = Instant::now();
    }
}

/// Metadata for a tracked Redis key (for eviction scoring).
#[derive(Debug, Clone)]
pub struct RedisKeyMeta {
    /// When the key was last accessed
    pub last_access: Instant,
    /// Number of times accessed
    pub access_count: u64,
    /// Approximate size in bytes
    pub size_bytes: usize,
    /// Whether this key is protected from eviction
    pub protected: bool,
}

impl RedisKeyMeta {
    pub fn new(size_bytes: usize, protected: bool) -> Self {
        Self {
            last_access: Instant::now(),
            access_count: 1,
            size_bytes,
            protected,
        }
    }
    
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count += 1;
    }
    
    /// Convert to CacheEntry for scoring with TanCurvePolicy
    pub fn to_cache_entry(&self, id: String) -> CacheEntry {
        CacheEntry {
            id,
            size_bytes: self.size_bytes,
            created_at: self.last_access, // Approximate
            last_access: self.last_access,
            access_count: self.access_count,
            is_dirty: self.protected, // Protected keys act like "dirty" (don't evict)
        }
    }
}

/// Configuration for Redis eviction.
#[derive(Debug, Clone)]
pub struct RedisEvictionConfig {
    /// Pressure threshold to start proactive eviction (0.0-1.0)
    pub eviction_start_pressure: f64,
    /// Target pressure after eviction (0.0-1.0)
    pub eviction_target_pressure: f64,
    /// Max staleness before refreshing memory profile
    pub max_profile_staleness: Duration,
    /// Max drift in bytes before refreshing memory profile
    pub max_profile_drift_bytes: u64,
    /// Batch size for eviction (keys to delete per round)
    pub eviction_batch_size: usize,
    /// Key prefixes to protect from eviction
    pub protected_prefixes: Vec<String>,
}

impl Default for RedisEvictionConfig {
    fn default() -> Self {
        Self {
            eviction_start_pressure: 0.75,  // Start evicting at 75%
            eviction_target_pressure: 0.60, // Evict down to 60%
            max_profile_staleness: Duration::from_secs(5),
            max_profile_drift_bytes: 1024 * 1024, // 1MB
            eviction_batch_size: 100,
            protected_prefixes: vec![
                "merkle:".to_string(),
                "idx:".to_string(),
            ],
        }
    }
}

/// Proactive Redis eviction manager.
/// 
/// Reuses `TanCurvePolicy` from the memory eviction module for consistent
/// scoring across L1 (memory) and L2 (Redis) caches.
pub struct RedisEvictionManager {
    config: RedisEvictionConfig,
    policy: TanCurvePolicy,
    memory_profile: Arc<RwLock<RedisMemoryProfile>>,
    key_metadata: Arc<RwLock<HashMap<String, RedisKeyMeta>>>,
    prefix: Option<String>,
}

impl RedisEvictionManager {
    /// Create a new eviction manager.
    pub fn new(config: RedisEvictionConfig, prefix: Option<String>) -> Self {
        Self {
            config,
            policy: TanCurvePolicy::default(),
            memory_profile: Arc::new(RwLock::new(RedisMemoryProfile::default())),
            key_metadata: Arc::new(RwLock::new(HashMap::new())),
            prefix,
        }
    }
    
    /// Get current memory pressure (0.0 to 1.0+).
    pub fn pressure(&self) -> f64 {
        self.memory_profile.read().pressure()
    }
    
    /// Check if we need to refresh the memory profile.
    pub fn needs_profile_refresh(&self) -> bool {
        let profile = self.memory_profile.read();
        profile.needs_refresh(
            self.config.max_profile_staleness,
            self.config.max_profile_drift_bytes,
        )
    }
    
    /// Update memory profile from Redis INFO output.
    pub fn refresh_profile(&self, used_bytes: u64, max_bytes: u64) {
        let mut profile = self.memory_profile.write();
        profile.refresh_from_info(used_bytes, max_bytes);
        debug!(
            used_mb = used_bytes / 1024 / 1024,
            max_mb = max_bytes / 1024 / 1024,
            pressure = format!("{:.1}%", profile.pressure() * 100.0),
            "Redis memory profile refreshed"
        );
    }
    
    /// Record a batch write (updates pending estimate).
    pub fn record_batch_write(&self, bytes: u64) {
        self.memory_profile.write().add_pending_writes(bytes);
    }
    
    /// Record a key write (for eviction scoring).
    pub fn record_key_write(&self, key: &str, size_bytes: usize) {
        let protected = self.is_protected(key);
        let mut metadata = self.key_metadata.write();
        metadata.insert(key.to_string(), RedisKeyMeta::new(size_bytes, protected));
    }
    
    /// Record a key access (touch for LRU scoring).
    pub fn record_key_access(&self, key: &str) {
        let mut metadata = self.key_metadata.write();
        if let Some(meta) = metadata.get_mut(key) {
            meta.touch();
        }
    }
    
    /// Remove key from tracking (after eviction or deletion).
    pub fn remove_key(&self, key: &str) {
        self.key_metadata.write().remove(key);
    }
    
    /// Check if eviction is needed.
    pub fn needs_eviction(&self) -> bool {
        self.pressure() >= self.config.eviction_start_pressure
    }
    
    /// Get keys to evict, sorted by eviction score (lowest first).
    /// Returns up to `eviction_batch_size` keys.
    pub fn get_eviction_candidates(&self) -> Vec<String> {
        let pressure = self.pressure();
        if pressure < self.config.eviction_start_pressure {
            return vec![];
        }
        
        let metadata = self.key_metadata.read();
        
        // Convert to CacheEntry for scoring with TanCurvePolicy
        let entries: Vec<CacheEntry> = metadata
            .iter()
            .map(|(key, meta)| meta.to_cache_entry(key.clone()))
            .collect();
        
        if entries.is_empty() {
            return vec![];
        }
        
        // Use TanCurvePolicy for consistent scoring
        let victims = self.policy.select_victims(
            &entries, 
            self.config.eviction_batch_size, 
            pressure
        );
        
        if !victims.is_empty() {
            info!(
                candidates = victims.len(),
                pressure = format!("{:.1}%", pressure * 100.0),
                "Selected Redis eviction candidates"
            );
        }
        
        victims
    }
    
    /// Check if a key is protected from eviction.
    fn is_protected(&self, key: &str) -> bool {
        // Strip prefix if present
        let key_without_prefix = if let Some(ref prefix) = self.prefix {
            key.strip_prefix(prefix).unwrap_or(key)
        } else {
            key
        };
        
        // Check against protected prefixes
        self.config.protected_prefixes.iter().any(|p| key_without_prefix.starts_with(p))
    }
    
    /// Get statistics for monitoring.
    pub fn stats(&self) -> RedisEvictionStats {
        let profile = self.memory_profile.read();
        let metadata = self.key_metadata.read();
        
        let protected_count = metadata.values().filter(|m| m.protected).count();
        let data_count = metadata.len() - protected_count;
        
        RedisEvictionStats {
            used_bytes: profile.used_bytes,
            max_bytes: profile.max_bytes,
            pending_writes_estimate: profile.pending_writes_estimate,
            pressure: profile.pressure(),
            tracked_keys: metadata.len(),
            protected_keys: protected_count,
            data_keys: data_count,
            profile_age_secs: profile.last_updated.elapsed().as_secs_f64(),
        }
    }
}

/// Statistics for monitoring Redis eviction state.
#[derive(Debug, Clone)]
pub struct RedisEvictionStats {
    pub used_bytes: u64,
    pub max_bytes: u64,
    pub pending_writes_estimate: u64,
    pub pressure: f64,
    pub tracked_keys: usize,
    pub protected_keys: usize,
    pub data_keys: usize,
    pub profile_age_secs: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_memory_profile_pressure() {
        let mut profile = RedisMemoryProfile {
            used_bytes: 50 * 1024 * 1024,  // 50MB
            max_bytes: 100 * 1024 * 1024,  // 100MB
            ..Default::default()
        };
        
        assert!((profile.pressure() - 0.5).abs() < 0.01);
        
        profile.add_pending_writes(25 * 1024 * 1024); // +25MB
        assert!((profile.pressure() - 0.75).abs() < 0.01);
    }
    
    #[test]
    fn test_profile_needs_refresh() {
        let mut profile = RedisMemoryProfile::default();
        
        // Fresh profile doesn't need refresh
        assert!(!profile.needs_refresh(Duration::from_secs(5), 1024 * 1024));
        
        // After drift, needs refresh
        profile.add_pending_writes(2 * 1024 * 1024);
        assert!(profile.needs_refresh(Duration::from_secs(5), 1024 * 1024));
    }
    
    #[test]
    fn test_protected_keys() {
        let config = RedisEvictionConfig::default();
        let manager = RedisEvictionManager::new(config, Some("sync:".to_string()));
        
        // Merkle keys are protected (with global prefix stripped first)
        assert!(manager.is_protected("sync:merkle:hash:user.alice")); // → "merkle:hash:user.alice"
        assert!(manager.is_protected("sync:merkle:children:user"));   // → "merkle:children:user"
        
        // Index keys are protected  
        assert!(manager.is_protected("sync:idx:users"));              // → "idx:users"
        
        // Data keys are NOT protected (they're the eviction candidates!)
        assert!(!manager.is_protected("sync:user.alice"));            // → "user.alice"
        assert!(!manager.is_protected("sync:config.app"));            // → "config.app"
        
        // Keys without global prefix still work (defensive)
        assert!(manager.is_protected("merkle:hash:test"));            // → "merkle:hash:test"
    }
    
    #[test]
    fn test_no_eviction_under_threshold() {
        let config = RedisEvictionConfig::default();
        let manager = RedisEvictionManager::new(config, None);
        
        // Low pressure
        manager.refresh_profile(30 * 1024 * 1024, 100 * 1024 * 1024); // 30%
        manager.record_key_write("data:key", 10_000);
        
        let candidates = manager.get_eviction_candidates();
        assert!(candidates.is_empty(), "Should not evict under threshold");
    }
    
    #[test]
    fn test_eviction_uses_tan_curve_policy() {
        let config = RedisEvictionConfig {
            eviction_start_pressure: 0.5,
            eviction_batch_size: 10,
            ..Default::default()
        };
        let manager = RedisEvictionManager::new(config, None);
        
        // Set up high pressure
        manager.refresh_profile(80 * 1024 * 1024, 100 * 1024 * 1024); // 80%
        
        // Add data keys (not protected)
        manager.record_key_write("data:old", 10_000);
        manager.record_key_write("data:new", 10_000);
        
        // Make "old" key old by manipulating metadata
        {
            let mut meta = manager.key_metadata.write();
            if let Some(m) = meta.get_mut("data:old") {
                m.last_access = Instant::now() - Duration::from_secs(3600);
            }
        }
        
        // Get candidates - should prefer older key
        let candidates = manager.get_eviction_candidates();
        
        // Both should be candidates (not protected)
        assert!(!candidates.is_empty());
        // Older key should be first
        if candidates.len() >= 2 {
            assert_eq!(candidates[0], "data:old");
        }
    }
}
