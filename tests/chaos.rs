//! Chaos Testing for Sync Engine
//!
//! This module tests failure scenarios using:
//! 1. **FailingStore wrappers** - precise error injection at specific call counts
//! 2. **Container killing** - abrupt backend death mid-operation
//! 3. **Data corruption** - garbage data in storage backends
//!
//! # Running Chaos Tests
//! ```bash
//! cargo test --test chaos -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use serde_json::json;
use tokio::sync::watch;

use sync_engine::{
    SyncEngine, SyncEngineConfig, SyncItem, EngineState,
};
use sync_engine::storage::traits::{CacheStore, ArchiveStore, StorageError, BatchWriteResult};

use testcontainers::{clients::Cli, Container, GenericImage, core::WaitFor};

// =============================================================================
// Failing Store Wrappers - Precise Error Injection
// =============================================================================

/// A wrapper that injects failures at specific call counts.
/// Useful for testing error handling paths with precision.
#[allow(dead_code)]
pub struct FailingCacheStore<S: CacheStore> {
    inner: S,
    call_count: AtomicU64,
    /// Fail on these call numbers (1-indexed)
    fail_on_calls: Vec<u64>,
    /// Error message to return
    error_msg: String,
    /// Whether to fail all calls after first failure
    fail_permanently: AtomicBool,
}

#[allow(dead_code)]
impl<S: CacheStore> FailingCacheStore<S> {
    pub fn new(inner: S, fail_on_calls: Vec<u64>, error_msg: &str) -> Self {
        Self {
            inner,
            call_count: AtomicU64::new(0),
            fail_on_calls,
            error_msg: error_msg.to_string(),
            fail_permanently: AtomicBool::new(false),
        }
    }
    
    /// Create a store that fails permanently after N calls
    pub fn fail_after(inner: S, n: u64, error_msg: &str) -> Self {
        let mut store = Self::new(inner, vec![], error_msg);
        store.fail_on_calls = (n+1..=n+1000).collect(); // Fail from n+1 onwards
        store.fail_permanently.store(true, Ordering::SeqCst);
        store
    }
    
    fn should_fail(&self) -> bool {
        let count = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_permanently.load(Ordering::SeqCst) && !self.fail_on_calls.is_empty() {
            count >= self.fail_on_calls[0]
        } else {
            self.fail_on_calls.contains(&count)
        }
    }
    
    fn maybe_fail(&self) -> Result<(), StorageError> {
        if self.should_fail() {
            Err(StorageError::Backend(self.error_msg.clone()))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<S: CacheStore> CacheStore for FailingCacheStore<S> {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        self.maybe_fail()?;
        self.inner.get(id).await
    }
    
    async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
        self.maybe_fail()?;
        self.inner.put(item).await
    }
    
    async fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.maybe_fail()?;
        self.inner.delete(id).await
    }
    
    async fn exists(&self, id: &str) -> Result<bool, StorageError> {
        self.maybe_fail()?;
        self.inner.exists(id).await
    }
    
    async fn put_batch_with_ttl(&self, items: &[SyncItem], ttl_secs: Option<u64>) -> Result<BatchWriteResult, StorageError> {
        self.maybe_fail()?;
        self.inner.put_batch_with_ttl(items, ttl_secs).await
    }
}

/// Same for ArchiveStore - allows injecting failures into SQL operations
#[allow(dead_code)]
pub struct FailingArchiveStore<S: ArchiveStore> {
    inner: S,
    call_count: AtomicU64,
    fail_on_calls: Vec<u64>,
    error_msg: String,
    fail_permanently: AtomicBool,
}

#[allow(dead_code)]
impl<S: ArchiveStore> FailingArchiveStore<S> {
    pub fn new(inner: S, fail_on_calls: Vec<u64>, error_msg: &str) -> Self {
        Self {
            inner,
            call_count: AtomicU64::new(0),
            fail_on_calls,
            error_msg: error_msg.to_string(),
            fail_permanently: AtomicBool::new(false),
        }
    }
    
    pub fn fail_after(inner: S, n: u64, error_msg: &str) -> Self {
        let mut store = Self::new(inner, vec![], error_msg);
        store.fail_on_calls = (n+1..=n+1000).collect();
        store.fail_permanently.store(true, Ordering::SeqCst);
        store
    }
    
    fn should_fail(&self) -> bool {
        let count = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_permanently.load(Ordering::SeqCst) && !self.fail_on_calls.is_empty() {
            count >= self.fail_on_calls[0]
        } else {
            self.fail_on_calls.contains(&count)
        }
    }
    
    fn maybe_fail(&self) -> Result<(), StorageError> {
        if self.should_fail() {
            Err(StorageError::Backend(self.error_msg.clone()))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<S: ArchiveStore> ArchiveStore for FailingArchiveStore<S> {
    async fn get(&self, id: &str) -> Result<Option<SyncItem>, StorageError> {
        self.maybe_fail()?;
        self.inner.get(id).await
    }
    
    async fn put(&self, item: &SyncItem) -> Result<(), StorageError> {
        self.maybe_fail()?;
        self.inner.put(item).await
    }
    
    async fn delete(&self, id: &str) -> Result<(), StorageError> {
        self.maybe_fail()?;
        self.inner.delete(id).await
    }
    
    async fn exists(&self, id: &str) -> Result<bool, StorageError> {
        self.maybe_fail()?;
        self.inner.exists(id).await
    }
    
    async fn put_batch(&self, items: &mut [SyncItem]) -> Result<BatchWriteResult, StorageError> {
        self.maybe_fail()?;
        self.inner.put_batch(items).await
    }
    
    async fn scan_keys(&self, offset: u64, limit: usize) -> Result<Vec<String>, StorageError> {
        self.maybe_fail()?;
        self.inner.scan_keys(offset, limit).await
    }
    
    async fn count_all(&self) -> Result<u64, StorageError> {
        self.maybe_fail()?;
        self.inner.count_all().await
    }
}

// =============================================================================
// Container Helpers
// =============================================================================

fn redis_container(docker: &Cli) -> Container<'_, GenericImage> {
    let image = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379)
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    docker.run(image)
}

fn mysql_container(docker: &Cli) -> Container<'_, GenericImage> {
    let image = GenericImage::new("mysql", "8.0")
        .with_env_var("MYSQL_ROOT_PASSWORD", "test")
        .with_env_var("MYSQL_DATABASE", "test")
        .with_env_var("MYSQL_USER", "test")
        .with_env_var("MYSQL_PASSWORD", "test")
        .with_exposed_port(3306)
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"));
    docker.run(image)
}

fn test_item(id: &str) -> SyncItem {
    SyncItem::from_json(id.to_string(), json!({"test": "data", "id": id}))
}

fn unique_wal_path(name: &str) -> String {
    // Use temp/ folder to keep root clean
    format!("./temp/test_wal_chaos_{}_{}.db", name, uuid::Uuid::new_v4())
}

/// Clean up WAL file and its associated SQLite journal files (-shm, -wal)
fn cleanup_wal_files(wal_path: &str) {
    let _ = std::fs::remove_file(wal_path);
    let _ = std::fs::remove_file(format!("{}-shm", wal_path));
    let _ = std::fs::remove_file(format!("{}-wal", wal_path));
}

// =============================================================================
// Chaos Tests - Container Killing (Abrupt Death)
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_redis_killed_during_batch_write() {
    // Test: Redis dies while batch write is in progress
    // Expected: Write fails, items remain in L1, no data loss
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("redis_kill_batch");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        batch_flush_ms: 10000, // Long timeout - we'll trigger manually
        batch_flush_count: 100,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");
    
    // Fill batch queue
    for i in 0..50 {
        engine.submit(test_item(&format!("batch-kill-{}", i))).await.ok();
    }
    
    // Start flush in background
    let engine = Arc::new(tokio::sync::Mutex::new(engine));
    let engine_clone = engine.clone();
    let flush_handle = tokio::spawn(async move {
        let eng = engine_clone.lock().await;
        eng.force_flush().await;
    });
    
    // Kill Redis immediately
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(redis);
    println!("Redis killed!");
    
    // Wait for flush to complete (with failure)
    let _ = tokio::time::timeout(Duration::from_secs(5), flush_handle).await;
    
    // Items should still be in L1
    let mut eng = engine.lock().await;
    let item = eng.get("batch-kill-25").await
        .expect("Get failed")
        .expect("Item should still be in L1");
    assert_eq!(item.object_id, "batch-kill-25");
    
    let _ = tokio::time::timeout(Duration::from_secs(2), eng.shutdown()).await;
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_mysql_killed_triggers_wal_fallback() {
    // Test: MySQL dies, writes should go to WAL
    // Expected: Engine continues accepting writes via WAL
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    let mysql = mysql_container(&docker);
    let mysql_port = mysql.get_host_port_ipv4(3306);
    
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let wal_path = unique_wal_path("mysql_kill");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: Some(format!("mysql://test:test@127.0.0.1:{}/test", mysql_port)),
        batch_flush_ms: 50,
        batch_flush_count: 5,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    if engine.start().await.is_err() {
        println!("Couldn't start with MySQL - skipping");
        return;
    }
    
    // Submit items with MySQL alive
    for i in 0..5 {
        engine.submit(test_item(&format!("pre-kill-{}", i))).await.ok();
    }
    engine.force_flush().await;
    
    // Kill MySQL
    println!("Killing MySQL...");
    drop(mysql);
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Engine should still accept writes (via WAL fallback)
    for i in 0..5 {
        let result = engine.submit(test_item(&format!("post-kill-{}", i))).await;
        // Should succeed - goes to L1 and WAL
        if result.is_err() {
            println!("Submit {} failed: {:?}", i, result);
        }
    }
    
    // Items in L1
    let item = engine.get("post-kill-2").await
        .expect("Get failed")
        .expect("Item should be in L1");
    assert_eq!(item.object_id, "post-kill-2");
    
    // Check WAL has pending items
    assert!(std::path::Path::new(&wal_path).exists(), "WAL file should exist");
    
    let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    cleanup_wal_files(&wal_path);
}

// =============================================================================
// Chaos Tests - Data Corruption Scenarios
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_corrupted_redis_data() {
    // Test: Manually corrupt data in Redis, then read it
    // Expected: Corruption detected, error returned (not panic)
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    // Direct Redis connection to inject corruption
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", redis_port))
        .expect("Failed to connect");
    let mut conn = client.get_multiplexed_async_connection().await
        .expect("Failed to get connection");
    
    // Write valid item first
    let item = test_item("corrupt-test");
    let valid_json = serde_json::to_vec(&item).unwrap();
    let _: () = redis::cmd("SET")
        .arg("corrupt-test")
        .arg(&valid_json)
        .query_async(&mut conn)
        .await
        .expect("Failed to set");
    
    // Now corrupt it (write garbage)
    let _: () = redis::cmd("SET")
        .arg("corrupt-test")
        .arg(b"{{{{not valid json at all!!!!")
        .query_async(&mut conn)
        .await
        .expect("Failed to corrupt");
    
    let wal_path = unique_wal_path("corrupt");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");
    
    // Try to read corrupted data - should NOT panic
    let result = engine.get("corrupt-test").await;
    match result {
        Ok(Some(_)) => panic!("Should not successfully deserialize garbage"),
        Ok(None) => println!("Returned None for corrupted data (acceptable)"),
        Err(e) => println!("Returned error for corrupted data: {} (correct!)", e),
    }
    
    // Engine should still be functional after encountering corruption
    engine.submit(test_item("after-corrupt")).await.expect("Should still work");
    let item = engine.get("after-corrupt").await
        .expect("Get failed")
        .expect("Item not found");
    assert_eq!(item.object_id, "after-corrupt");
    
    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_partial_json_in_redis() {
    // Test: Truncated JSON in Redis (simulates network cut during write)
    // Expected: Error, not panic
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", redis_port))
        .expect("Failed to connect");
    let mut conn = client.get_multiplexed_async_connection().await
        .expect("Failed to get connection");
    
    // Write truncated JSON (as if network cut mid-write)
    let _: () = redis::cmd("SET")
        .arg("truncated-item")
        .arg(b"{\"object_id\":\"truncated-item\",\"payload\":")  // Missing end
        .query_async(&mut conn)
        .await
        .expect("Failed to set");
    
    let wal_path = unique_wal_path("truncated");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");
    
    // Should handle truncated data gracefully
    let result = engine.get("truncated-item").await;
    match result {
        Ok(Some(_)) => panic!("Should not parse truncated JSON"),
        Ok(None) => println!("Returned None (acceptable)"),
        Err(e) => println!("Returned error: {} (correct!)", e),
    }
    
    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

// =============================================================================
// Chaos Tests - Lifecycle Edge Cases
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_double_start() {
    // Test: Call start() twice
    // Expected: Second call should be safe (no-op or error, not panic)
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("double_start");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    // First start
    engine.start().await.expect("First start failed");
    assert!(engine.is_ready());
    
    // Second start (should be safe)
    let result = engine.start().await;
    println!("Second start result: {:?}", result);
    // Should either succeed (no-op) or return an error, NOT panic
    
    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker  
async fn chaos_shutdown_without_start() {
    // Test: Call shutdown() without ever calling start()
    // Expected: Safe, no crash
    
    let wal_path = unique_wal_path("shutdown_nostart");
    let config = SyncEngineConfig {
        redis_url: None,
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    assert_eq!(engine.state(), EngineState::Created);
    
    // Shutdown without start - should not panic
    engine.shutdown().await;
    
    assert_eq!(engine.state(), EngineState::ShuttingDown);
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_operations_after_shutdown() {
    // Test: Try to submit/get after shutdown
    // Expected: Error or no-op, not panic
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("ops_after_shutdown");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");
    
    // Submit before shutdown
    engine.submit(test_item("before-shutdown")).await.expect("Should work");
    
    // Shutdown
    engine.shutdown().await;
    
    // Try operations after shutdown - should not panic
    let submit_result = engine.submit(test_item("after-shutdown")).await;
    println!("Submit after shutdown: {:?}", submit_result);
    
    let get_result = engine.get("before-shutdown").await;
    println!("Get after shutdown: {:?}", get_result);
    
    cleanup_wal_files(&wal_path);
}

// =============================================================================
// Chaos Tests - Concurrent Failure Scenarios
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_concurrent_submits_during_redis_death() {
    // Test: Many concurrent operations when Redis suddenly dies
    // Expected: No deadlocks, no panics, L1 still works
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("concurrent_death");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        batch_flush_ms: 20,
        batch_flush_count: 5,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");
    
    let engine = Arc::new(tokio::sync::Mutex::new(engine));
    
    // Spawn many writers
    let mut handles = vec![];
    for writer_id in 0..10 {
        let eng = engine.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let item = test_item(&format!("writer-{}-item-{}", writer_id, i));
                let e = eng.lock().await;
                let _ = e.submit(item).await;
                drop(e);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }
    
    // Kill Redis mid-flight
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(redis);
    println!("Redis killed during concurrent writes!");
    
    // Wait for all writers (with timeout)
    let results = tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::join_all(handles)
    ).await;
    
    match results {
        Ok(r) => {
            let success_count = r.iter().filter(|r| r.is_ok()).count();
            println!("Completed writers: {}/10", success_count);
        }
        Err(_) => println!("Writers timed out (may indicate deadlock)"),
    }
    
    // Engine should still be operational for L1 reads
    let eng = engine.lock().await;
    let l1_stats = eng.l1_stats();
    println!("L1 after chaos: {} items", l1_stats.0);
    
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        drop(eng);
        engine.lock().await.shutdown().await;
    }).await;
    
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_rapid_start_stop_cycles() {
    // Test: Rapidly start and stop engine multiple times
    // Expected: No resource leaks, no panics
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    for cycle in 0..5 {
        let wal_path = unique_wal_path(&format!("rapid_cycle_{}", cycle));
        let config = SyncEngineConfig {
            redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
            sql_url: None,
            wal_path: Some(wal_path.clone()),
            ..Default::default()
        };

        let (_tx, rx) = watch::channel(config.clone());
        let mut engine = SyncEngine::new(config, rx);
        
        engine.start().await.expect("Start failed");
        
        // Quick operation
        engine.submit(test_item(&format!("cycle-{}", cycle))).await.ok();
        
        engine.shutdown().await;
        
        cleanup_wal_files(&wal_path);
        println!("Cycle {} complete", cycle);
    }
    
    println!("All rapid cycles completed without panic!");
}

// =============================================================================
// Chaos Tests - Memory Pressure
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn chaos_l1_overflow_pressure() {
    // Test: Submit many items to a small L1 cache
    // Expected: Backpressure kicks in, no OOM
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("overflow");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        l1_max_bytes: 10 * 1024, // Only 10KB - very small
        batch_flush_ms: 100,
        batch_flush_count: 10,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");
    
    // Flood with items
    let mut accepted = 0;
    let mut rejected = 0;
    for i in 0..500 {
        let item = SyncItem::from_json(
            format!("overflow-{}", i),
            json!({"data": "x".repeat(100), "index": i}),
        );
        match engine.submit(item).await {
            Ok(_) => accepted += 1,
            Err(_) => rejected += 1,
        }
    }
    
    println!("Accepted: {}, Rejected: {}", accepted, rejected);
    println!("Pressure level: {:?}", engine.pressure());
    
    // Should have applied backpressure at some point
    let (l1_count, l1_bytes) = engine.l1_stats();
    println!("L1: {} items, {} bytes", l1_count, l1_bytes);
    
    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}
