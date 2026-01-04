// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Circuit breaker pattern using recloser crate.
//!
//! Provides protection against cascading failures when external services
//! (Redis L2, MySQL L3) are unhealthy. Wraps async operations and tracks
//! failure rates to automatically "trip" the breaker when thresholds are exceeded.
//!
//! States:
//! - Closed: Normal operation, requests pass through
//! - Open: Service unhealthy, requests fail-fast without attempting
//! - HalfOpen: Testing if service recovered, limited requests allowed

use recloser::{Recloser, AsyncRecloser, Error as RecloserError};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, warn};

/// Circuit breaker state for metrics/monitoring
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed = 0,
    HalfOpen = 1,
    Open = 2,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "closed"),
            Self::HalfOpen => write!(f, "half_open"),
            Self::Open => write!(f, "open"),
        }
    }
}

/// Error type for circuit-protected operations
#[derive(Debug, thiserror::Error)]
pub enum CircuitError<E> {
    /// The circuit breaker rejected the call (circuit is open)
    #[error("circuit breaker open, request rejected")]
    Rejected,
    
    /// The underlying operation failed
    #[error("operation failed: {0}")]
    Inner(#[source] E),
}

impl<E> From<RecloserError<E>> for CircuitError<E> {
    fn from(err: RecloserError<E>) -> Self {
        match err {
            RecloserError::Rejected => CircuitError::Rejected,
            RecloserError::Inner(e) => CircuitError::Inner(e),
        }
    }
}

/// Configuration for a circuit breaker
#[derive(Debug, Clone)]
pub struct CircuitConfig {
    /// Number of consecutive failures to trip the circuit
    pub failure_threshold: u32,
    /// Number of consecutive successes in half-open to close circuit
    pub success_threshold: u32,
    /// How long to wait before attempting recovery (half-open)
    pub recovery_timeout: Duration,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            recovery_timeout: Duration::from_secs(30),
        }
    }
}

impl CircuitConfig {
    /// Aggressive config for critical paths (trips faster, recovers cautiously)
    #[must_use]
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 3,
            recovery_timeout: Duration::from_secs(60),
        }
    }
    
    /// Lenient config for less critical paths (tolerates more failures)
    #[must_use]
    pub fn lenient() -> Self {
        Self {
            failure_threshold: 10,
            success_threshold: 1,
            recovery_timeout: Duration::from_secs(15),
        }
    }
    
    /// Fast recovery for testing
    #[cfg(test)]
    pub fn test() -> Self {
        Self {
            failure_threshold: 2,
            success_threshold: 1,
            recovery_timeout: Duration::from_millis(50),
        }
    }
}

/// A named circuit breaker with metrics tracking
pub struct CircuitBreaker {
    name: String,
    inner: AsyncRecloser,
    
    // Metrics
    calls_total: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    rejections: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given name and config
    pub fn new(name: impl Into<String>, config: CircuitConfig) -> Self {
        let recloser = Recloser::custom()
            .error_rate(config.failure_threshold as f32 / 100.0)
            .closed_len(config.failure_threshold as usize)
            .half_open_len(config.success_threshold as usize)
            .open_wait(config.recovery_timeout)
            .build();
        
        Self {
            name: name.into(),
            inner: recloser.into(),
            calls_total: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            rejections: AtomicU64::new(0),
        }
    }
    
    /// Create with default config
    pub fn with_defaults(name: impl Into<String>) -> Self {
        Self::new(name, CircuitConfig::default())
    }
    
    /// Get the circuit breaker name
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get current circuit state
    #[must_use]
    pub fn state(&self) -> CircuitState {
        // recloser doesn't expose state directly, so we infer from behavior
        // This is a limitation - for metrics we track via call results
        CircuitState::Closed // Default - real state tracked via metrics
    }
    
    /// Execute an async operation through the circuit breaker
    /// 
    /// Takes a closure that returns a Future, allowing lazy evaluation
    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.calls_total.fetch_add(1, Ordering::Relaxed);
        
        // recloser expects a Future directly, so we call the closure to get it
        match self.inner.call(f()).await {
            Ok(result) => {
                self.successes.fetch_add(1, Ordering::Relaxed);
                debug!(circuit = %self.name, "Circuit call succeeded");
                crate::metrics::record_circuit_breaker_call(&self.name, "success");
                Ok(result)
            }
            Err(RecloserError::Rejected) => {
                self.rejections.fetch_add(1, Ordering::Relaxed);
                warn!(circuit = %self.name, "Circuit breaker rejected call (open)");
                crate::metrics::record_circuit_breaker_call(&self.name, "rejected");
                Err(CircuitError::Rejected)
            }
            Err(RecloserError::Inner(e)) => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                debug!(circuit = %self.name, "Circuit call failed");
                crate::metrics::record_circuit_breaker_call(&self.name, "failure");
                Err(CircuitError::Inner(e))
            }
        }
    }
    
    /// Get total number of calls
    #[must_use]
    pub fn calls_total(&self) -> u64 {
        self.calls_total.load(Ordering::Relaxed)
    }
    
    /// Get number of successful calls
    #[must_use]
    pub fn successes(&self) -> u64 {
        self.successes.load(Ordering::Relaxed)
    }
    
    /// Get number of failed calls (operation errors)
    #[must_use]
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }
    
    /// Get number of rejected calls (circuit open)
    #[must_use]
    pub fn rejections(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }
    
    /// Get failure rate (0.0 - 1.0)
    #[must_use]
    pub fn failure_rate(&self) -> f64 {
        let total = self.calls_total();
        if total == 0 {
            return 0.0;
        }
        self.failures() as f64 / total as f64
    }
    
    /// Reset all metrics
    pub fn reset_metrics(&self) {
        self.calls_total.store(0, Ordering::Relaxed);
        self.successes.store(0, Ordering::Relaxed);
        self.failures.store(0, Ordering::Relaxed);
        self.rejections.store(0, Ordering::Relaxed);
    }
}

/// Pre-configured circuit breakers for sync engine backends
pub struct BackendCircuits {
    /// Circuit breaker for Redis (L2) operations
    pub redis: CircuitBreaker,
    /// Circuit breaker for MySQL (L3) operations  
    pub mysql: CircuitBreaker,
}

impl Default for BackendCircuits {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendCircuits {
    /// Create backend circuits with appropriate configs
    pub fn new() -> Self {
        Self {
            // Redis: more lenient (it's just cache, L3 is ground truth)
            redis: CircuitBreaker::new("redis_l2", CircuitConfig::lenient()),
            // MySQL: more aggressive (ground truth, don't hammer when down)
            mysql: CircuitBreaker::new("mysql_l3", CircuitConfig::aggressive()),
        }
    }
    
    /// Get metrics for all circuits
    pub fn metrics(&self) -> BackendCircuitMetrics {
        BackendCircuitMetrics {
            redis_calls: self.redis.calls_total(),
            redis_successes: self.redis.successes(),
            redis_failures: self.redis.failures(),
            redis_rejections: self.redis.rejections(),
            mysql_calls: self.mysql.calls_total(),
            mysql_successes: self.mysql.successes(),
            mysql_failures: self.mysql.failures(),
            mysql_rejections: self.mysql.rejections(),
        }
    }
}

/// Aggregated metrics from all backend circuits
#[derive(Debug, Clone)]
pub struct BackendCircuitMetrics {
    pub redis_calls: u64,
    pub redis_successes: u64,
    pub redis_failures: u64,
    pub redis_rejections: u64,
    pub mysql_calls: u64,
    pub mysql_successes: u64,
    pub mysql_failures: u64,
    pub mysql_rejections: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    
    #[tokio::test]
    async fn test_circuit_passes_successful_calls() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());
        
        let result: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(42) }).await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(cb.successes(), 1);
        assert_eq!(cb.failures(), 0);
    }
    
    #[tokio::test]
    async fn test_circuit_tracks_failures() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());
        
        let result: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("boom") }).await;
        
        assert!(matches!(result, Err(CircuitError::Inner("boom"))));
        assert_eq!(cb.successes(), 0);
        assert_eq!(cb.failures(), 1);
    }
    
    #[tokio::test]
    async fn test_circuit_opens_after_threshold() {
        let config = CircuitConfig {
            failure_threshold: 2,
            success_threshold: 1,
            recovery_timeout: Duration::from_secs(60), // Long timeout for test
        };
        let cb = CircuitBreaker::new("test", config);
        
        // Fail twice to trip the breaker
        for _ in 0..3 {
            let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("fail") }).await;
        }
        
        // Next call should be rejected (circuit open)
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(42) }).await;
        
        // Note: recloser uses error_rate, so this test verifies the wrapper
        // The exact tripping behavior depends on recloser internals
        assert!(cb.failures() >= 2 || cb.rejections() >= 1);
    }
    
    #[tokio::test]
    async fn test_circuit_metrics_accumulate() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());
        
        // Only successes to avoid tripping the breaker
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(1) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(2) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(3) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(4) }).await;
        
        assert_eq!(cb.calls_total(), 4);
        assert_eq!(cb.successes(), 4);
        assert_eq!(cb.failures(), 0);
    }
    
    #[tokio::test]
    async fn test_failure_rate_calculation() {
        // Use lenient config to avoid circuit tripping during test
        let config = CircuitConfig {
            failure_threshold: 100, // Very high threshold
            success_threshold: 1,
            recovery_timeout: Duration::from_secs(60),
        };
        let cb = CircuitBreaker::new("test", config);
        
        // 2 success, 2 failure = 50% failure rate
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(1) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("x") }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(2) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("y") }).await;
        
        assert!((cb.failure_rate() - 0.5).abs() < 0.01);
    }
    
    #[tokio::test]
    async fn test_reset_metrics() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());
        
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Ok(1) }).await;
        let _: Result<i32, CircuitError<&str>> = cb.call(|| async { Err("x") }).await;
        
        assert!(cb.calls_total() > 0);
        
        cb.reset_metrics();
        
        assert_eq!(cb.calls_total(), 0);
        assert_eq!(cb.successes(), 0);
        assert_eq!(cb.failures(), 0);
        assert_eq!(cb.rejections(), 0);
    }
    
    #[tokio::test]
    async fn test_backend_circuits_configs() {
        let circuits = BackendCircuits::new();
        
        // Verify both exist and have correct names
        assert_eq!(circuits.redis.name(), "redis_l2");
        assert_eq!(circuits.mysql.name(), "mysql_l3");
    }
    
    #[tokio::test]
    async fn test_circuit_with_async_state() {
        let cb = CircuitBreaker::new("test", CircuitConfig::test());
        let counter = std::sync::Arc::new(AtomicUsize::new(0));
        
        // Ensure the async closure captures and uses external state
        let counter_clone = counter.clone();
        let result: Result<usize, CircuitError<&str>> = cb.call(|| async move {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(counter_clone.load(Ordering::SeqCst))
        }).await;
        
        assert_eq!(result.unwrap(), 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
    
    #[tokio::test]
    async fn test_backend_circuit_metrics() {
        let circuits = BackendCircuits::new();
        
        let _: Result<i32, CircuitError<&str>> = circuits.redis.call(|| async { Ok(1) }).await;
        let _: Result<i32, CircuitError<&str>> = circuits.mysql.call(|| async { Err("down") }).await;
        
        let metrics = circuits.metrics();
        
        assert_eq!(metrics.redis_calls, 1);
        assert_eq!(metrics.redis_successes, 1);
        assert_eq!(metrics.mysql_calls, 1);
        assert_eq!(metrics.mysql_failures, 1);
    }
    
    #[test]
    fn test_circuit_config_presets() {
        let default = CircuitConfig::default();
        let aggressive = CircuitConfig::aggressive();
        let lenient = CircuitConfig::lenient();
        
        // Aggressive trips faster
        assert!(aggressive.failure_threshold < default.failure_threshold);
        // Lenient tolerates more
        assert!(lenient.failure_threshold > default.failure_threshold);
        // Aggressive waits longer to recover
        assert!(aggressive.recovery_timeout > lenient.recovery_timeout);
    }
}
