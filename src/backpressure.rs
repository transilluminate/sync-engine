// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Backpressure handling for graceful degradation under load.
//!
//! The sync engine uses a six-tier backpressure cascade to handle
//! memory pressure gracefully without dropping data.
//!
//! # Example
//!
//! ```
//! use sync_engine::BackpressureLevel;
//!
//! // Normal operation
//! let level = BackpressureLevel::from_pressure(0.5);
//! assert_eq!(level, BackpressureLevel::Normal);
//! assert!(level.should_accept_writes());
//!
//! // Under pressure - throttle writes
//! let level = BackpressureLevel::from_pressure(0.85);
//! assert_eq!(level, BackpressureLevel::Throttle);
//! assert!(level.should_accept_writes()); // Still accepts, but slower
//!
//! // Critical - reject new writes
//! let level = BackpressureLevel::from_pressure(0.92);
//! assert_eq!(level, BackpressureLevel::Critical);
//! assert!(!level.should_accept_writes());
//! ```

/// Backpressure level based on memory/queue pressure.
/// 
/// Six-tier cascade for graceful degradation:
/// - **Normal** (< 70%): Accept all operations
/// - **Warn** (70-80%): Evict aggressively, emit warnings  
/// - **Throttle** (80-90%): Rate limit writes (HTTP 429)
/// - **Critical** (90-95%): Reject writes, reads only (HTTP 503)
/// - **Emergency** (95-98%): Read-only mode, prepare shutdown
/// - **Shutdown** (> 98%): Graceful shutdown initiated
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BackpressureLevel {
    Normal = 0,
    Warn = 1,
    Throttle = 2,
    Critical = 3,
    Emergency = 4,
    Shutdown = 5,
}

impl BackpressureLevel {
    /// Calculate backpressure level from pressure ratio (0.0 → 1.0)
    #[must_use]
    pub fn from_pressure(pressure: f64) -> Self {
        match pressure {
            p if p < 0.70 => Self::Normal,
            p if p < 0.80 => Self::Warn,
            p if p < 0.90 => Self::Throttle,
            p if p < 0.95 => Self::Critical,
            p if p < 0.98 => Self::Emergency,
            _ => Self::Shutdown,
        }
    }

    /// Check if writes should be accepted at this level
    #[must_use]
    pub fn should_accept_writes(&self) -> bool {
        matches!(self, Self::Normal | Self::Warn | Self::Throttle)
    }

    /// Check if reads should be accepted at this level
    #[must_use]
    pub fn should_accept_reads(&self) -> bool {
        !matches!(self, Self::Shutdown)
    }

    /// Get eviction multiplier (higher pressure = more aggressive)
    #[must_use]
    pub fn eviction_multiplier(&self) -> f64 {
        match self {
            Self::Normal => 1.0,
            Self::Warn => 1.5,
            Self::Throttle => 2.0,
            Self::Critical => 3.0,
            Self::Emergency => 5.0,
            Self::Shutdown => 10.0,
        }
    }

    /// Suggested HTTP status code for this level
    #[must_use]
    pub fn http_status_code(&self) -> Option<u16> {
        match self {
            Self::Normal | Self::Warn => None,
            Self::Throttle => Some(429),
            Self::Critical | Self::Emergency | Self::Shutdown => Some(503),
        }
    }

    /// Suggested Retry-After header value (seconds)
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            Self::Throttle => Some(1),
            Self::Critical => Some(5),
            Self::Emergency | Self::Shutdown => Some(30),
            _ => None,
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            Self::Normal => "Normal operation",
            Self::Warn => "Warning - high memory usage",
            Self::Throttle => "Throttling - rate limiting active",
            Self::Critical => "Critical - writes rejected",
            Self::Emergency => "Emergency - read-only mode",
            Self::Shutdown => "Shutdown - graceful shutdown initiated",
        }
    }
}

impl std::fmt::Display for BackpressureLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pressure_level_thresholds() {
        assert_eq!(BackpressureLevel::from_pressure(0.0), BackpressureLevel::Normal);
        assert_eq!(BackpressureLevel::from_pressure(0.69), BackpressureLevel::Normal);
        assert_eq!(BackpressureLevel::from_pressure(0.70), BackpressureLevel::Warn);
        assert_eq!(BackpressureLevel::from_pressure(0.79), BackpressureLevel::Warn);
        assert_eq!(BackpressureLevel::from_pressure(0.80), BackpressureLevel::Throttle);
        assert_eq!(BackpressureLevel::from_pressure(0.89), BackpressureLevel::Throttle);
        assert_eq!(BackpressureLevel::from_pressure(0.90), BackpressureLevel::Critical);
        assert_eq!(BackpressureLevel::from_pressure(0.94), BackpressureLevel::Critical);
        assert_eq!(BackpressureLevel::from_pressure(0.95), BackpressureLevel::Emergency);
        assert_eq!(BackpressureLevel::from_pressure(0.97), BackpressureLevel::Emergency);
        assert_eq!(BackpressureLevel::from_pressure(0.98), BackpressureLevel::Shutdown);
        assert_eq!(BackpressureLevel::from_pressure(1.0), BackpressureLevel::Shutdown);
    }

    #[test]
    fn test_should_accept_writes() {
        assert!(BackpressureLevel::Normal.should_accept_writes());
        assert!(BackpressureLevel::Warn.should_accept_writes());
        assert!(BackpressureLevel::Throttle.should_accept_writes());
        assert!(!BackpressureLevel::Critical.should_accept_writes());
        assert!(!BackpressureLevel::Emergency.should_accept_writes());
        assert!(!BackpressureLevel::Shutdown.should_accept_writes());
    }

    #[test]
    fn test_should_accept_reads() {
        assert!(BackpressureLevel::Normal.should_accept_reads());
        assert!(BackpressureLevel::Warn.should_accept_reads());
        assert!(BackpressureLevel::Throttle.should_accept_reads());
        assert!(BackpressureLevel::Critical.should_accept_reads());
        assert!(BackpressureLevel::Emergency.should_accept_reads());
        assert!(!BackpressureLevel::Shutdown.should_accept_reads());
    }

    #[test]
    fn test_eviction_multiplier_increases_with_pressure() {
        let levels = [
            BackpressureLevel::Normal,
            BackpressureLevel::Warn,
            BackpressureLevel::Throttle,
            BackpressureLevel::Critical,
            BackpressureLevel::Emergency,
            BackpressureLevel::Shutdown,
        ];

        for i in 1..levels.len() {
            assert!(
                levels[i].eviction_multiplier() >= levels[i - 1].eviction_multiplier(),
                "eviction multiplier should increase with pressure"
            );
        }
    }

    #[test]
    fn test_http_status_codes() {
        assert_eq!(BackpressureLevel::Normal.http_status_code(), None);
        assert_eq!(BackpressureLevel::Warn.http_status_code(), None);
        assert_eq!(BackpressureLevel::Throttle.http_status_code(), Some(429));
        assert_eq!(BackpressureLevel::Critical.http_status_code(), Some(503));
        assert_eq!(BackpressureLevel::Emergency.http_status_code(), Some(503));
        assert_eq!(BackpressureLevel::Shutdown.http_status_code(), Some(503));
    }

    #[test]
    fn test_retry_after_increases_with_severity() {
        assert_eq!(BackpressureLevel::Normal.retry_after_secs(), None);
        assert_eq!(BackpressureLevel::Throttle.retry_after_secs(), Some(1));
        assert_eq!(BackpressureLevel::Critical.retry_after_secs(), Some(5));
        assert_eq!(BackpressureLevel::Emergency.retry_after_secs(), Some(30));
    }

    #[test]
    fn test_level_ordering() {
        assert!(BackpressureLevel::Normal < BackpressureLevel::Warn);
        assert!(BackpressureLevel::Warn < BackpressureLevel::Throttle);
        assert!(BackpressureLevel::Throttle < BackpressureLevel::Critical);
        assert!(BackpressureLevel::Critical < BackpressureLevel::Emergency);
        assert!(BackpressureLevel::Emergency < BackpressureLevel::Shutdown);
    }
}
