// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

use std::time::Instant;
use std::f64::consts::PI;

/// Cache entry metadata for eviction scoring
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub id: String,
    pub size_bytes: usize,
    pub created_at: Instant,
    pub last_access: Instant,
    pub access_count: u64,
    pub is_dirty: bool,
}

impl CacheEntry {
    pub fn new(id: String, size_bytes: usize) -> Self {
        let now = Instant::now();
        Self {
            id,
            size_bytes,
            created_at: now,
            last_access: now,
            access_count: 0,
            is_dirty: true,
        }
    }

    pub fn record_access(&mut self) {
        self.last_access = Instant::now();
        self.access_count = self.access_count.saturating_add(1);
    }

    pub fn idle_secs(&self) -> f64 {
        self.last_access.elapsed().as_secs_f64()
    }
}

/// Tan curve memory pressure calculator.
/// Maps memory usage to a smooth multiplier using tan curve.
pub struct TanCurveMemoryPressure {
    /// Memory % where pressure starts (default: 75%)
    pub center_percent: f64,
    /// Memory % where pressure is critical (default: 95%)
    pub critical_percent: f64,
}

impl Default for TanCurveMemoryPressure {
    fn default() -> Self {
        Self {
            center_percent: 0.75,
            critical_percent: 0.95,
        }
    }
}

impl TanCurveMemoryPressure {
    /// Calculate score multiplier based on memory pressure.
    /// Returns 1.0 at low pressure, smoothly decreasing to 0.0 at critical.
    pub fn multiplier(&self, used_percent: f64) -> f64 {
        if used_percent <= self.center_percent {
            return 1.0;
        }
        if used_percent >= self.critical_percent {
            return 0.0;
        }

        let range = self.critical_percent - self.center_percent;
        let normalized = (used_percent - self.center_percent) / range;
        let angle = (normalized - 0.5) * PI / 1.2;
        let tan_value = angle.tan();
        (1.0 - (tan_value + 2.0) / 4.0).clamp(0.0, 1.0)
    }
}

/// Tan curve eviction policy combining recency, frequency, and size.
pub struct TanCurvePolicy {
    /// Half-life for recency decay (seconds)
    pub recency_half_life: f64,
    /// Max access count for normalization
    pub max_access_count: u64,
    /// Baseline size in bytes for size scoring
    pub baseline_size_bytes: usize,
    /// Weights for each component (recency, frequency, size)
    pub weights: (f64, f64, f64),
    /// Memory pressure calculator
    pub memory_pressure: TanCurveMemoryPressure,
}

impl Default for TanCurvePolicy {
    fn default() -> Self {
        Self {
            recency_half_life: 3600.0, // 1 hour
            max_access_count: 1000,
            baseline_size_bytes: 1024 * 1024, // 1 MB
            weights: (0.4, 0.4, 0.2), // recency, frequency, size
            memory_pressure: TanCurveMemoryPressure::default(),
        }
    }
}

impl TanCurvePolicy {
    /// Calculate eviction score (0.0 = evict first, 1.0 = keep)
    pub fn calculate_score(&self, entry: &CacheEntry, memory_used_percent: f64) -> f64 {
        let recency = (-entry.idle_secs() / self.recency_half_life).exp();
        
        let frequency = if entry.access_count == 0 {
            0.0
        } else {
            let count = entry.access_count.min(self.max_access_count) as f64;
            (1.0 + count).ln() / (1.0 + self.max_access_count as f64).ln()
        };
        
        let size_mb = entry.size_bytes as f64 / self.baseline_size_bytes as f64;
        let size_score = 1.0 / (1.0 + size_mb);

        let base_score = recency * self.weights.0 
            + frequency * self.weights.1 
            + size_score * self.weights.2;

        let multiplier = self.memory_pressure.multiplier(memory_used_percent);
        base_score * multiplier
    }

    /// Select victims for eviction (returns IDs sorted by score, lowest first)
    pub fn select_victims(&self, entries: &[CacheEntry], count: usize, memory_used_percent: f64) -> Vec<String> {
        let mut scored: Vec<_> = entries
            .iter()
            .filter(|e| !e.is_dirty) // Only evict clean entries
            .map(|e| (e.id.clone(), self.calculate_score(e, memory_used_percent)))
            .collect();

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().take(count).map(|(id, _)| id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn make_entry(id: &str, idle_secs: f64, access_count: u64, size_bytes: usize, is_dirty: bool) -> CacheEntry {
        let now = Instant::now();
        CacheEntry {
            id: id.to_string(),
            size_bytes,
            created_at: now - Duration::from_secs_f64(idle_secs + 100.0),
            last_access: now - Duration::from_secs_f64(idle_secs),
            access_count,
            is_dirty,
        }
    }

    #[test]
    fn test_memory_pressure_multiplier_low_pressure() {
        let pressure = TanCurveMemoryPressure::default();
        assert_eq!(pressure.multiplier(0.0), 1.0);
        assert_eq!(pressure.multiplier(0.5), 1.0);
        assert_eq!(pressure.multiplier(0.74), 1.0);
    }

    #[test]
    fn test_memory_pressure_multiplier_high_pressure() {
        let pressure = TanCurveMemoryPressure::default();
        // At 95%+ pressure, multiplier should be 0 (force evict)
        assert_eq!(pressure.multiplier(0.95), 0.0);
        assert_eq!(pressure.multiplier(1.0), 0.0);
    }

    #[test]
    fn test_memory_pressure_multiplier_transition() {
        let pressure = TanCurveMemoryPressure::default();
        // Between 75% and 95%, multiplier decreases
        let m1 = pressure.multiplier(0.80);
        let m2 = pressure.multiplier(0.85);
        let m3 = pressure.multiplier(0.90);
        
        assert!(m1 > m2, "multiplier should decrease as pressure increases");
        assert!(m2 > m3, "multiplier should decrease as pressure increases");
        assert!(m1 < 1.0, "multiplier should be less than 1.0 above center");
        assert!(m3 > 0.0, "multiplier should be above 0 below critical");
    }

    #[test]
    fn test_eviction_score_prefers_old_unused_items() {
        let policy = TanCurvePolicy::default();
        
        // Old, never-accessed item
        let old_unused = make_entry("old_unused", 3600.0, 0, 1024, false);
        // Recently accessed, frequently used item
        let hot = make_entry("hot", 1.0, 100, 1024, false);
        
        let score_old = policy.calculate_score(&old_unused, 0.5);
        let score_hot = policy.calculate_score(&hot, 0.5);
        
        assert!(score_old < score_hot, "old unused item should have lower score (evict first)");
    }

    #[test]
    fn test_eviction_score_prefers_large_items() {
        let policy = TanCurvePolicy::default();
        
        // Small item
        let small = make_entry("small", 100.0, 10, 1024, false);
        // Large item (same age/access pattern)
        let large = make_entry("large", 100.0, 10, 10 * 1024 * 1024, false);
        
        let score_small = policy.calculate_score(&small, 0.5);
        let score_large = policy.calculate_score(&large, 0.5);
        
        assert!(score_large < score_small, "large item should have lower score (evict first)");
    }

    #[test]
    fn test_select_victims_skips_dirty() {
        let policy = TanCurvePolicy::default();
        
        let entries = vec![
            make_entry("clean1", 100.0, 0, 1024, false),
            make_entry("dirty1", 100.0, 0, 1024, true),  // Should be skipped
            make_entry("clean2", 200.0, 0, 1024, false),
            make_entry("dirty2", 300.0, 0, 1024, true),  // Should be skipped
        ];
        
        let victims = policy.select_victims(&entries, 10, 0.5);
        
        assert_eq!(victims.len(), 2, "should only return clean entries");
        assert!(!victims.contains(&"dirty1".to_string()));
        assert!(!victims.contains(&"dirty2".to_string()));
    }

    #[test]
    fn test_select_victims_respects_count_limit() {
        let policy = TanCurvePolicy::default();
        
        let entries: Vec<_> = (0..100)
            .map(|i| make_entry(&format!("item{}", i), i as f64, 0, 1024, false))
            .collect();
        
        let victims = policy.select_victims(&entries, 5, 0.5);
        
        assert_eq!(victims.len(), 5, "should return exactly requested count");
    }

    #[test]
    fn test_high_pressure_forces_eviction() {
        let policy = TanCurvePolicy::default();
        
        // Hot item that would normally have high score
        let hot = make_entry("hot", 1.0, 1000, 1024, false);
        
        let score_low_pressure = policy.calculate_score(&hot, 0.5);
        let score_high_pressure = policy.calculate_score(&hot, 0.95);
        
        assert!(score_high_pressure < score_low_pressure, 
            "high pressure should reduce scores to force eviction");
        assert_eq!(score_high_pressure, 0.0, 
            "at critical pressure, all scores should be 0");
    }
}
