// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Retry logic with exponential backoff.
//!
//! Provides configurable retry behavior for transient failures.
//! Different presets are available for different use cases.
//!
//! # Example
//!
//! ```
//! use sync_engine::RetryConfig;
//! use std::time::Duration;
//!
//! // Startup: fail fast on bad config
//! let startup = RetryConfig::startup();
//! assert_eq!(startup.max_retries, Some(5));
//!
//! // Daemon: never give up on reconnection
//! let daemon = RetryConfig::daemon();
//! assert_eq!(daemon.max_retries, None); // Infinite
//!
//! // Query: quick retry, then fail
//! let query = RetryConfig::query();
//! assert_eq!(query.max_retries, Some(3));
//! ```

use std::time::Duration;
use tokio::time::sleep;
use tracing::{warn, info};
use std::future::Future;

/// Configuration for connection/operation retry behavior.
///
/// Use the preset constructors for common patterns:
/// - [`RetryConfig::startup()`] - Fast-fail for initial connections
/// - [`RetryConfig::daemon()`] - Infinite retry for runtime reconnection
/// - [`RetryConfig::query()`] - Quick retry for individual operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub factor: f64,
    pub max_retries: Option<usize>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::daemon()
    }
}

impl RetryConfig {
    /// Fast-fail retry for initial startup connection.
    /// Attempts 5 times with exponential backoff, failing after ~5 seconds.
    /// Use this during daemon startup to detect configuration errors quickly.
    #[must_use]
    pub fn startup() -> Self {
        Self {
            max_retries: Some(5),
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(2),
            factor: 2.0,
        }
    }

    /// Infinite retry for long-running daemon (never give up!).
    /// Retries forever with exponential backoff capped at 5 minutes.
    /// Use this for runtime reconnection after initial startup succeeds.
    #[must_use]
    pub fn daemon() -> Self {
        Self {
            max_retries: None, // Infinite
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300), // Cap at 5 minutes
            factor: 2.0,
        }
    }

    /// Quick retry for individual queries (don't block forever).
    /// 3 attempts with fast backoff - if it fails, let caller handle it.
    #[must_use]
    pub fn query() -> Self {
        Self {
            max_retries: Some(3),
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            factor: 2.0,
        }
    }

    /// Fast retry for tests (minimal delays)
    #[cfg(test)]
    pub fn test() -> Self {
        Self {
            max_retries: Some(3),
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            factor: 2.0,
        }
    }
}

pub async fn retry<F, Fut, T, E>(
    operation_name: &str,
    config: &RetryConfig,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = config.initial_delay;
    let mut attempts = 0;

    loop {
        match operation().await {
            Ok(val) => {
                if attempts > 0 {
                    info!("Operation '{}' succeeded after {} retries", operation_name, attempts);
                }
                return Ok(val);
            }
            Err(err) => {
                attempts += 1;
                
                if let Some(max) = config.max_retries {
                    if attempts >= max {
                        return Err(err);
                    }
                }

                if config.max_retries.is_none() {
                    // Daemon mode - infinite retries
                    warn!(
                        "Operation '{}' failed (attempt {}, will retry forever): {}. Next retry in {:?}...",
                        operation_name, attempts, err, delay
                    );
                } else {
                    warn!(
                        "Operation '{}' failed (attempt {}/{}): {}. Retrying in {:?}...",
                        operation_name, attempts, config.max_retries.unwrap(), err, delay
                    );
                }

                sleep(delay).await;
                delay = (delay.mul_f64(config.factor)).min(config.max_delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct TestError(String);

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[tokio::test]
    async fn test_retry_succeeds_first_try() {
        let result: Result<i32, TestError> = retry(
            "test_op",
            &RetryConfig::test(),
            || async { Ok(42) },
        ).await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<i32, TestError> = retry(
            "test_op",
            &RetryConfig::test(),
            || {
                let a = attempts_clone.clone();
                async move {
                    let count = a.fetch_add(1, Ordering::SeqCst) + 1;
                    if count < 3 {
                        Err(TestError(format!("fail {}", count)))
                    } else {
                        Ok(42)
                    }
                }
            },
        ).await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausts_retries() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let config = RetryConfig {
            max_retries: Some(3),
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            factor: 2.0,
        };

        let result: Result<i32, TestError> = retry(
            "test_op",
            &config,
            || {
                let a = attempts_clone.clone();
                async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    Err(TestError("always fail".to_string()))
                }
            },
        ).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().0.contains("always fail"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_retry_config_presets() {
        // Startup config - limited retries, fast-fail
        let startup = RetryConfig::startup();
        assert!(startup.max_retries.is_some());
        assert_eq!(startup.max_retries.unwrap(), 5);

        // Daemon config - infinite retries
        let daemon = RetryConfig::daemon();
        assert!(daemon.max_retries.is_none());

        // Query config - few retries
        let query = RetryConfig::query();
        assert!(query.max_retries.is_some());
        assert_eq!(query.max_retries.unwrap(), 3);
    }

    #[test]
    fn test_delay_exponential_backoff() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            factor: 2.0,
            max_retries: Some(5),
        };

        let mut delay = config.initial_delay;
        
        // First delay: 100ms
        assert_eq!(delay, Duration::from_millis(100));
        
        // Second: 200ms
        delay = (delay.mul_f64(config.factor)).min(config.max_delay);
        assert_eq!(delay, Duration::from_millis(200));
        
        // Third: 400ms
        delay = (delay.mul_f64(config.factor)).min(config.max_delay);
        assert_eq!(delay, Duration::from_millis(400));
    }

    #[test]
    fn test_delay_caps_at_max() {
        let config = RetryConfig {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            factor: 10.0, // Aggressive factor
            max_retries: Some(5),
        };

        let mut delay = config.initial_delay;
        delay = (delay.mul_f64(config.factor)).min(config.max_delay);
        
        // Should cap at max_delay
        assert_eq!(delay, Duration::from_secs(5));
    }
}
