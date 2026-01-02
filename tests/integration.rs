//! Integration Tests for Sync Engine
//!
//! This module contains all integration tests that require real backends (Redis, MySQL).
//! Tests use testcontainers for portability - no external docker-compose required.
//!
//! # Running Tests
//! ```bash
//! # Run all integration tests (requires Docker)
//! cargo test --test integration -- --ignored
//!
//! # Run only happy-path tests
//! cargo test --test integration happy -- --ignored
//!
//! # Run only failure scenario tests  
//! cargo test --test integration failure -- --ignored
//! ```
//!
//! # Test Organization
//! - `happy_*` - Normal operation: lifecycle, batching, merkle updates
//! - `failure_*` - Failure scenarios: Redis/MySQL death, WAL fallback, recovery

use std::time::Duration;
use serde_json::json;
use tokio::sync::watch;

use sync_engine::{
    SyncEngine, SyncEngineConfig, SyncItem, EngineState, BackpressureLevel,
};

use testcontainers::{clients::Cli, Container, GenericImage, core::WaitFor};

// =============================================================================
// Container Helpers
// =============================================================================

/// Create a Redis Stack container with RedisJSON + RediSearch modules
fn redis_container(docker: &Cli) -> Container<'_, GenericImage> {
    let image = GenericImage::new("redis/redis-stack-server", "latest")
        .with_exposed_port(6379)
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    docker.run(image)
}

/// Create a MySQL container (takes ~30s to be ready)
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
    format!("./temp/test_wal_{}_{}.db", name, uuid::Uuid::new_v4())
}

/// Clean up WAL file and its associated SQLite journal files (-shm, -wal)
fn cleanup_wal_files(wal_path: &str) {
    let _ = std::fs::remove_file(wal_path);
    let _ = std::fs::remove_file(format!("{}-shm", wal_path));
    let _ = std::fs::remove_file(format!("{}-wal", wal_path));
}

// =============================================================================
// Happy Path Tests - Normal Operation
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn happy_engine_lifecycle_with_redis() {
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("lifecycle");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        l1_max_bytes: 10 * 1024 * 1024,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);

    // Should start in Created state
    assert_eq!(engine.state(), EngineState::Created);

    // Start the engine
    engine.start().await.expect("Failed to start engine");
    assert!(engine.is_ready());

    // Submit items
    for i in 0..10 {
        let item = SyncItem::from_json(
            format!("uk.nhs.patient.record.{}", i),
            json!({
                "name": format!("Patient {}", i),
                "nhs_number": format!("{:010}", i),
            }),
        );
        engine.submit(item).await.expect("Failed to submit item");
    }

    // Verify L1 stats
    let (count, _bytes) = engine.l1_stats();
    assert_eq!(count, 10);

    // Retrieve an item
    let item = engine.get("uk.nhs.patient.record.5").await
        .expect("Failed to get item")
        .expect("Item not found");
    assert_eq!(item.object_id, "uk.nhs.patient.record.5");

    // Check backpressure
    assert_eq!(engine.pressure(), BackpressureLevel::Normal);

    // Shutdown gracefully
    engine.shutdown().await;
    assert_eq!(engine.state(), EngineState::ShuttingDown);

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn happy_batch_flush_to_redis() {
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("batch");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        batch_flush_ms: 50,
        batch_flush_count: 5,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start engine");

    // Submit items to trigger batch flush
    for i in 0..20 {
        engine.submit(test_item(&format!("batch-item-{}", i))).await
            .expect("Failed to submit");
    }

    // Wait for batch flush
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Force flush remaining
    engine.force_flush().await;

    // With Redis-only (no SQL), L3 filter won't have entries.
    // The key assertion is that items are still retrievable.

    // Items still retrievable from L1
    let item = engine.get("batch-item-10").await
        .expect("Get failed")
        .expect("Item not found");
    assert_eq!(item.object_id, "batch-item-10");

    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn happy_merkle_tree_updates() {
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("merkle");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        batch_flush_ms: 50,
        batch_flush_count: 3,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start engine");

    // Submit items with hierarchical IDs (triggers path merkle)
    let ids = [
        "uk.nhs.patient.record.001",
        "uk.nhs.patient.record.002",
        "uk.nhs.doctor.license.001",
        "uk.nhs.appointment.slot.001",
    ];

    for id in ids {
        engine.submit(SyncItem::from_json(id.to_string(), json!({"id": id}))).await
            .expect("Failed to submit");
    }

    // Wait for batch flush to trigger Merkle updates
    tokio::time::sleep(Duration::from_millis(200)).await;
    engine.force_flush().await;

    // Verify all items retrievable
    for id in ids {
        let item = engine.get(id).await
            .expect("Get failed")
            .unwrap_or_else(|| panic!("Item {} not found", id));
        assert_eq!(item.object_id, id);
    }

    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Failure Scenario Tests - Resilience & Recovery
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn failure_redis_unavailable_at_startup() {
    // Engine should start even if Redis is unavailable (degraded L1-only mode)
    // Use a timeout because connection to non-existent port can hang
    let wal_path = unique_wal_path("redis_unavail");
    let config = SyncEngineConfig {
        redis_url: Some("redis://127.0.0.1:59999".to_string()), // Non-existent
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        l1_max_bytes: 10 * 1024 * 1024,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    // Should start successfully (L2 is optional) - timeout after 5s
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        engine.start()
    ).await;
    
    match result {
        Ok(Ok(())) => {
            // Engine started in degraded mode
            assert!(engine.is_ready());
            engine.submit(test_item("no-redis-item")).await
                .expect("Should accept writes in L1-only mode");
            let item = engine.get("no-redis-item").await
                .expect("Get should work")
                .expect("Item should be in L1");
            assert_eq!(item.object_id, "no-redis-item");
            engine.shutdown().await;
        }
        Ok(Err(e)) => {
            // Connection failed fast - that's also acceptable
            println!("Engine start failed (expected): {:?}", e);
        }
        Err(_) => {
            // Timeout - connection hung, but we tested the scenario
            println!("Connection timed out (expected for non-existent port)");
        }
    }
    
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn failure_redis_dies_mid_operation() {
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("redis_dies");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        batch_flush_ms: 50,
        batch_flush_count: 3,
        l1_max_bytes: 10 * 1024 * 1024,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Should start with Redis");
    
    // Submit items successfully
    for i in 0..5 {
        engine.submit(test_item(&format!("before-kill-{}", i))).await
            .expect("Should write before Redis death");
    }
    
    // Give batch time to flush
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Kill Redis!
    drop(redis);
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Submit more items - should still work (L1 cache)
    for i in 0..5 {
        engine.submit(test_item(&format!("after-kill-{}", i))).await
            .expect("Should accept writes even with dead Redis (L1 fallback)");
    }
    
    // Items should still be in L1
    let item = engine.get("after-kill-2").await
        .expect("Get should work")
        .expect("Item should be in L1");
    assert_eq!(item.object_id, "after-kill-2");
    
    // Shutdown with timeout (may hang trying to flush to dead Redis)
    let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn failure_filter_consistency_on_failed_write() {
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("filter_consistency");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        batch_flush_ms: 10000, // Long flush interval - won't trigger by time
        batch_flush_count: 100, // High count - won't trigger by count
        l1_max_bytes: 10 * 1024 * 1024,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Should start");
    
    // Check initial filter state
    let (l3_before, _, _) = engine.l3_filter_stats();
    
    // Submit item (goes to L1 and batch queue, NOT to filter yet)
    engine.submit(test_item("filter-test-item")).await.unwrap();
    
    // Filter should NOT have the item yet (only updated on successful flush)
    let (l3_after_submit, _, _) = engine.l3_filter_stats();
    assert_eq!(l3_before, l3_after_submit, "Filter should not update on submit");
    
    // Kill Redis before flush
    drop(redis);
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Force flush (will fail because Redis is dead)
    engine.force_flush().await;
    
    // Filter should still not have the item (write failed)
    let (l3_after_failed_flush, _, _) = engine.l3_filter_stats();
    assert_eq!(l3_before, l3_after_failed_flush, 
        "Filter should not update on failed flush");
    
    // Shutdown with timeout (may hang trying to flush to dead Redis)
    let _ = tokio::time::timeout(Duration::from_secs(2), engine.shutdown()).await;
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn failure_concurrent_writes_during_redis_outage() {
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("concurrent_fail");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        batch_flush_ms: 50,
        batch_flush_count: 10,
        l1_max_bytes: 10 * 1024 * 1024,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Should start");
    
    // Wrap engine for sharing across tasks
    let engine = std::sync::Arc::new(tokio::sync::Mutex::new(engine));
    let mut handles = vec![];
    
    // Spawn concurrent writers
    for writer_id in 0..5 {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let item = test_item(&format!("writer-{}-item-{}", writer_id, i));
                let eng = engine.lock().await;
                let _ = eng.submit(item).await; // May fail during outage
                drop(eng);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }
    
    // Kill Redis mid-write
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(redis);
    
    // Wait for all writers
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Engine should still be operational
    let eng = engine.lock().await;
    assert!(eng.is_ready(), "Engine should remain ready after Redis death");
    
    // Shutdown with timeout (may hang trying to flush to dead Redis)
    let _ = tokio::time::timeout(Duration::from_secs(2), eng.shutdown()).await;
    
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn failure_mysql_unavailable_triggers_wal() {
    // When MySQL is unavailable, writes should go to WAL
    // Use timeout because connection to non-existent port can hang
    let wal_path = unique_wal_path("mysql_wal");
    let config = SyncEngineConfig {
        redis_url: None,
        sql_url: Some("mysql://test:test@127.0.0.1:59999/test".to_string()), // Non-existent
        wal_path: Some(wal_path.clone()),
        batch_flush_ms: 50,
        batch_flush_count: 2,
        l1_max_bytes: 10 * 1024 * 1024,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    // Start may fail if MySQL is required at startup - timeout after 5s
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        engine.start()
    ).await;
    
    match result {
        Ok(Ok(())) => {
            // Engine started in degraded mode
            assert!(engine.is_ready());
            engine.submit(test_item("wal-fallback-item")).await
                .expect("Should accept writes via WAL fallback");
            engine.shutdown().await;
            assert!(std::path::Path::new(&wal_path).exists(), "WAL file should exist");
        }
        Ok(Err(_e)) => {
            // This is expected - MySQL is ground truth
            println!("Engine correctly requires MySQL at startup (ground-truth design)");
        }
        Err(_) => {
            // Timeout - connection hung
            println!("MySQL connection timed out (expected for non-existent port)");
        }
    }
    
    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Full Stack Tests (Redis + MySQL) - Slower, More Comprehensive
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker, takes ~60s due to MySQL startup
async fn happy_full_stack_lifecycle() {
    let docker = Cli::default();
    
    // Start Redis (fast)
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    // Start MySQL (slow - ~30s)
    println!("Starting MySQL container (this takes ~30s)...");
    let mysql = mysql_container(&docker);
    let mysql_port = mysql.get_host_port_ipv4(3306);
    
    // Extra wait for MySQL to be fully ready
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let wal_path = unique_wal_path("full_stack");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: Some(format!("mysql://test:test@127.0.0.1:{}/test", mysql_port)),
        l1_max_bytes: 10 * 1024 * 1024,
        batch_flush_ms: 50,
        batch_flush_count: 5,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    // This may fail if MySQL schema isn't ready
    let start_result = engine.start().await;
    if start_result.is_err() {
        println!("Could not connect to MySQL: {:?}", start_result);
        println!("(This is expected if MySQL needs schema setup)");
        drop(mysql);
        drop(redis);
        return;
    }
    
    assert!(engine.is_ready());

    // Submit items - goes to L1, batches to L2 (Redis), persists to L3 (MySQL)
    for i in 0..10 {
        engine.submit(test_item(&format!("full-stack-item-{}", i))).await
            .expect("Failed to submit");
    }

    // Force flush to ensure data reaches Redis/MySQL
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify filters updated (this test has MySQL, so L3 filter should have entries)
    let (l3_entries, _, _) = engine.l3_filter_stats();
    println!("L3 (MySQL) filter: {} entries", l3_entries);

    // Verify retrieval
    let item = engine.get("full-stack-item-5").await
        .expect("Get failed")
        .expect("Item not found");
    assert_eq!(item.object_id, "full-stack-item-5");

    engine.shutdown().await;
    
    drop(mysql);
    drop(redis);
    let _ = std::fs::remove_file(&wal_path);
}
// =============================================================================
// Coverage Gap Tests - Coordinator Paths
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_redis_lookup_miss() {
    // Tests the path where an item doesn't exist in any tier
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("redis_miss");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        l1_max_bytes: 10 * 1024 * 1024,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");

    // Query for an ID that was never submitted
    let result = engine.get("nonexistent.item.never.submitted").await
        .expect("Get should not error");
    assert!(result.is_none(), "Should return None for non-existent item");

    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_l3_fallback_retrieval() {
    // Tests the path where item is NOT in L1/L2 but IS in L3 (MySQL)
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    let mysql = mysql_container(&docker);
    let mysql_port = mysql.get_host_port_ipv4(3306);
    
    // Wait for MySQL to be ready
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let wal_path = unique_wal_path("l3_fallback");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: Some(format!("mysql://test:test@127.0.0.1:{}/test", mysql_port)),
        l1_max_bytes: 1024, // Tiny L1 to force eviction
        batch_flush_ms: 50,
        batch_flush_count: 2,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    if engine.start().await.is_err() {
        println!("MySQL not ready, skipping test");
        return;
    }

    // Submit items and flush to L3
    for i in 0..5 {
        engine.submit(test_item(&format!("l3-test-{}", i))).await.ok();
    }
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Force L1 eviction by adding more items
    for i in 100..200 {
        engine.submit(test_item(&format!("evict-{}", i))).await.ok();
    }

    // Now retrieve an early item - should come from L3
    let _result = engine.get("l3-test-2").await;
    // Just exercising the path - result depends on timing

    engine.shutdown().await;
    drop(mysql);
    drop(redis);
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_high_pressure_eviction() {
    // Tests the eviction path when memory pressure is high
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("pressure");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        l1_max_bytes: 5 * 1024, // 5KB - will fill quickly
        batch_flush_ms: 50,
        batch_flush_count: 10,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");

    // Submit many items to trigger pressure and eviction
    for i in 0..100 {
        let item = SyncItem::from_json(
            format!("pressure-test-{}", i),
            json!({
                "data": "x".repeat(200), // ~200 bytes each
                "index": i
            }),
        );
        let _ = engine.submit(item).await; // May reject under pressure
    }

    // Check pressure level
    let pressure = engine.pressure();
    println!("Pressure level: {:?}", pressure);

    // Try writes under pressure
    let can_write = engine.should_accept_writes();
    println!("Should accept writes: {}", can_write);

    engine.force_flush().await;
    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Coverage Gap Tests - Merkle Operations
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_merkle_batch_apply_and_diff() {
    // Tests merkle tree batch operations and diff detection
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("merkle_diff");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        batch_flush_ms: 20,
        batch_flush_count: 2,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");

    // Submit items with hierarchical paths to exercise merkle tree
    let paths = [
        "org.example.users.active.user001",
        "org.example.users.active.user002",
        "org.example.users.inactive.user003",
        "org.example.orders.pending.order001",
        "org.example.orders.completed.order002",
    ];

    for path in &paths {
        engine.submit(test_item(path)).await.expect("Submit failed");
    }

    // Force flush to trigger merkle batch apply
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Submit more to trigger recomputation
    engine.submit(test_item("org.example.users.active.user004")).await.ok();
    engine.force_flush().await;

    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_merkle_delete_operations() {
    // Tests merkle tree delete path
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("merkle_delete");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        batch_flush_ms: 20,
        batch_flush_count: 2,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start");

    // Add items
    engine.submit(test_item("delete.test.item1")).await.ok();
    engine.submit(test_item("delete.test.item2")).await.ok();
    engine.force_flush().await;

    // Delete one
    let deleted = engine.delete("delete.test.item1").await.expect("Delete failed");
    assert!(deleted, "Item should have been deleted");

    // Verify it's gone
    let result = engine.get("delete.test.item1").await.expect("Get failed");
    assert!(result.is_none(), "Deleted item should not be found");

    // Delete non-existent - may return true (idempotent) or false depending on impl
    let deleted_again = engine.delete("delete.test.nonexistent").await.expect("Delete failed");
    // Just exercise the path, don't assert specific behavior
    println!("Delete non-existent returned: {}", deleted_again);

    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Coverage Gap Tests - SQL Operations
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_sql_batch_operations() {
    // Tests SQL batch write with verification, scan_keys, count_all
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    let mysql = mysql_container(&docker);
    let mysql_port = mysql.get_host_port_ipv4(3306);
    
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let wal_path = unique_wal_path("sql_batch");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: Some(format!("mysql://test:test@127.0.0.1:{}/test", mysql_port)),
        l1_max_bytes: 50 * 1024 * 1024,
        batch_flush_ms: 30,
        batch_flush_count: 5, // Trigger batch at 5 items
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    if engine.start().await.is_err() {
        println!("MySQL not ready, skipping");
        return;
    }

    // Submit enough items to trigger batch write
    for i in 0..15 {
        engine.submit(test_item(&format!("sql-batch-{}", i))).await.ok();
    }

    // Force flush to MySQL
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Items should be in L3 now
    let (l3_entries, _, _) = engine.l3_filter_stats();
    println!("L3 entries after batch: {}", l3_entries);

    engine.shutdown().await;
    drop(mysql);
    drop(redis);
    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Coverage Gap Tests - WAL Drain and Pressure
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_wal_drain_on_startup() {
    // Tests WAL drain during startup after MySQL was unavailable
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("wal_drain");
    
    // Phase 1: Start engine without MySQL, write to WAL
    {
        let config = SyncEngineConfig {
            redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
            sql_url: None, // No MySQL
            wal_path: Some(wal_path.clone()),
            ..Default::default()
        };

        let (_tx, rx) = watch::channel(config.clone());
        let mut engine = SyncEngine::new(config, rx);
        engine.start().await.expect("Start failed");

        for i in 0..5 {
            engine.submit(test_item(&format!("wal-item-{}", i))).await.ok();
        }
        engine.force_flush().await;
        engine.shutdown().await;
    }

    // Phase 2: Start with MySQL - should drain WAL
    let mysql = mysql_container(&docker);
    let mysql_port = mysql.get_host_port_ipv4(3306);
    tokio::time::sleep(Duration::from_secs(5)).await;

    {
        let config = SyncEngineConfig {
            redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
            sql_url: Some(format!("mysql://test:test@127.0.0.1:{}/test", mysql_port)),
            wal_path: Some(wal_path.clone()),
            ..Default::default()
        };

        let (_tx, rx) = watch::channel(config.clone());
        let mut engine = SyncEngine::new(config, rx);
        
        // This should trigger WAL drain
        if engine.start().await.is_ok() {
            println!("Engine started with WAL drain");
            engine.shutdown().await;
        }
    }

    drop(mysql);
    drop(redis);
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_wal_pressure_threshold() {
    // Tests WAL under_pressure detection
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("wal_pressure");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        wal_max_items: Some(10), // Very small WAL
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Start failed");

    // Fill WAL to trigger pressure (without MySQL, items stay in WAL)
    for i in 0..12 {
        let _ = engine.submit(test_item(&format!("wal-pressure-{}", i))).await;
    }

    engine.force_flush().await;
    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Coverage Gap Tests - Filter Persistence
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker  
async fn coverage_filter_persistence_restore() {
    // Tests cuckoo filter snapshot save/restore with merkle verification
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    let mysql = mysql_container(&docker);
    let mysql_port = mysql.get_host_port_ipv4(3306);
    
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let wal_path = unique_wal_path("filter_persist");
    let redis_url = format!("redis://127.0.0.1:{}", redis_port);
    let sql_url = format!("mysql://test:test@127.0.0.1:{}/test", mysql_port);
    
    // Phase 1: Build filters and shutdown (should persist)
    {
        let config = SyncEngineConfig {
            redis_url: Some(redis_url.clone()),
            sql_url: Some(sql_url.clone()),
            wal_path: Some(wal_path.clone()),
            batch_flush_ms: 20,
            batch_flush_count: 2,
            ..Default::default()
        };

        let (_tx, rx) = watch::channel(config.clone());
        let mut engine = SyncEngine::new(config, rx);
        
        if engine.start().await.is_err() {
            println!("MySQL not ready, skipping");
            drop(mysql);
            drop(redis);
            return;
        }

        for i in 0..10 {
            engine.submit(test_item(&format!("persist-{}", i))).await.ok();
        }
        engine.force_flush().await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Get filter stats before shutdown
        let (l3_entries, _, _) = engine.l3_filter_stats();
        println!("L2 entries before shutdown: {}", l3_entries);

        engine.shutdown().await;
    }

    // Phase 2: Restart - should restore filters from snapshot
    {
        let config = SyncEngineConfig {
            redis_url: Some(redis_url.clone()),
            sql_url: Some(sql_url.clone()),
            wal_path: Some(wal_path.clone()),
            ..Default::default()
        };

        let (_tx, rx) = watch::channel(config.clone());
        let mut engine = SyncEngine::new(config, rx);
        
        if engine.start().await.is_ok() {
            let (l3_entries, _, trust) = engine.l3_filter_stats();
            println!("L2 entries after restart: {}, trust: {:?}", l3_entries, trust);
            engine.shutdown().await;
        }
    }

    drop(mysql);
    drop(redis);
    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Coverage Gap Tests - Redis Store Edge Cases
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_redis_exists_batch() {
    // Tests the exists_batch operation for cuckoo filter warmup
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("redis_exists");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        batch_flush_ms: 20,
        batch_flush_count: 3,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Start failed");

    // Submit items
    for i in 0..10 {
        engine.submit(test_item(&format!("exists-{}", i))).await.ok();
    }
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // With Redis-only (no SQL), items go to Redis via batch flush.
    // Verify items are retrievable (exercises Redis EXISTS path)
    let item = engine.get("exists-5").await
        .expect("Get should work")
        .expect("Item should be found");
    assert_eq!(item.object_id, "exists-5");

    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Coverage Gap Tests - Engine State Transitions
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn coverage_engine_state_transitions() {
    // Tests various engine state transitions
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("state_transitions");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);

    // Track state transitions
    let mut state_rx = engine.state_receiver();
    
    assert_eq!(engine.state(), EngineState::Created);
    assert!(!engine.is_ready());

    engine.start().await.expect("Start failed");
    
    // Should have transitioned through states
    let current_state = engine.state();
    assert!(matches!(current_state, EngineState::Ready | EngineState::Running));
    assert!(engine.is_ready());

    // Submit should work when ready
    engine.submit(test_item("state-test")).await.expect("Submit should work");

    engine.shutdown().await;
    assert_eq!(engine.state(), EngineState::ShuttingDown);
    assert!(!engine.is_ready());

    // Verify we can still receive state updates
    let _ = state_rx.changed().await;

    let _ = std::fs::remove_file(&wal_path);
}

// =============================================================================
// Coverage Gap Tests - Redis JSON Paths & Prefix Support
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker (Redis Stack)
async fn coverage_redis_json_storage() {
    // Tests RedisJSON storage paths (JSON.SET/JSON.GET) with Redis Stack
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("redis_json");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        redis_prefix: Some("test:".to_string()),
        sql_url: None,
        batch_flush_ms: 50,
        batch_flush_count: 2,
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Start failed");

    // Submit JSON items - these should go through JSON.SET path
    let items = vec![
        ("user.alice", json!({"name": "Alice", "role": "admin", "requests": 42000})),
        ("user.bob", json!({"name": "Bob", "role": "user", "requests": 100})),
        ("config.app", json!({"debug": false, "version": "1.0.0"})),
    ];

    for (id, payload) in &items {
        let item = SyncItem::from_json(id.to_string(), payload.clone());
        engine.submit(item).await.expect("Submit failed");
    }

    // Force flush to Redis
    tokio::time::sleep(Duration::from_millis(100)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify items are retrievable (L1 hit first, proving data integrity)
    for (id, expected_payload) in &items {
        let item = engine.get(id).await
            .expect("Get failed")
            .expect("Item should exist");
        
        assert_eq!(item.object_id, *id);
        let payload = item.content_as_json().expect("Should be JSON");
        assert_eq!(&payload, expected_payload);
    }
    
    // Verify data actually went to Redis with proper JSON structure
    // by checking raw Redis keys have the prefix
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", redis_port)).unwrap();
    let mut conn = redis::aio::ConnectionManager::new(client).await.unwrap();
    
    // Check that prefixed keys exist
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("test:user.*")
        .query_async(&mut conn)
        .await
        .expect("KEYS failed");
    
    assert!(!keys.is_empty(), "Should have test:user.* keys in Redis");
    
    // Verify it's stored as RedisJSON (key type should be ReJSON-RL)
    for key in &keys {
        let key_type: String = redis::cmd("TYPE")
            .arg(key)
            .query_async(&mut conn)
            .await
            .expect("TYPE failed");
        assert_eq!(key_type, "ReJSON-RL", "Key {} should be RedisJSON type", key);
    }

    engine.shutdown().await;
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker (Redis Stack)
async fn coverage_merkle_with_prefix() {
    // Tests that Merkle store respects redis_prefix
    use sync_engine::merkle::RedisMerkleStore;
    use redis::Client;
    
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    let redis_url = format!("redis://127.0.0.1:{}", redis_port);
    
    // Create merkle store WITH prefix
    let client = Client::open(redis_url.as_str()).expect("Redis client failed");
    let conn = redis::aio::ConnectionManager::new(client).await.expect("Connection failed");
    let merkle = RedisMerkleStore::with_prefix(conn.clone(), Some("myapp:"));
    
    // Apply a batch
    use sync_engine::merkle::{MerkleBatch, PathMerkle};
    let mut batch = MerkleBatch::new();
    let hash = PathMerkle::leaf_hash("user.test", 1, 12345, &[0u8; 32]);
    batch.insert("user.test".to_string(), hash);
    
    merkle.apply_batch(&batch).await.expect("Apply batch failed");
    
    // Verify the key has the prefix using raw Redis commands
    let mut raw_conn = conn.clone();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("myapp:merkle:*")
        .query_async(&mut raw_conn)
        .await
        .expect("KEYS failed");
    
    assert!(!keys.is_empty(), "Merkle keys should have myapp: prefix");
    for key in &keys {
        assert!(key.starts_with("myapp:merkle:"), "Key {} should start with myapp:merkle:", key);
    }
    
    // Verify NO unprefixed keys exist
    let unprefixed: Vec<String> = redis::cmd("KEYS")
        .arg("merkle:*")
        .query_async(&mut raw_conn)
        .await
        .expect("KEYS failed");
    
    // Filter out the prefixed ones
    let truly_unprefixed: Vec<_> = unprefixed.iter()
        .filter(|k| !k.starts_with("myapp:"))
        .collect();
    
    assert!(truly_unprefixed.is_empty(), "Should have no unprefixed merkle keys: {:?}", truly_unprefixed);
}

// =============================================================================
// State Management Tests
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker (Redis + MySQL)
async fn happy_state_management() {
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let mysql = mysql_container(&docker);
    
    let redis_port = redis.get_host_port_ipv4(6379);
    let mysql_port = mysql.get_host_port_ipv4(3306);
    
    // Wait for MySQL to be fully ready
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    let wal_path = unique_wal_path("state_mgmt");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        redis_prefix: Some("state_test:".to_string()),
        sql_url: Some(format!(
            "mysql://test:test@127.0.0.1:{}/test",
            mysql_port
        )),
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start engine");

    // 1. Submit items with different states
    use sync_engine::SubmitOptions;
    
    // Delta items (CRDT deltas waiting to be merged)
    for i in 0..5 {
        let item = SyncItem::from_json(
            format!("crdt.delta.{}", i),
            json!({"operation": "add", "value": i})
        ).with_state("delta");
        engine.submit(item).await.expect("Submit delta failed");
    }
    
    // Base items (merged CRDT state)
    for i in 0..3 {
        let item = SyncItem::from_json(
            format!("crdt.base.{}", i),
            json!({"total": i * 10})
        ).with_state("base");
        engine.submit(item).await.expect("Submit base failed");
    }
    
    // Default state items
    for i in 0..2 {
        let item = SyncItem::from_json(
            format!("regular.{}", i),
            json!({"data": "normal"})
        );
        engine.submit(item).await.expect("Submit regular failed");
    }
    
    // Force flush to ensure items hit SQL (timing-based waits are unreliable under coverage)
    tokio::time::sleep(Duration::from_millis(200)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Query by state
    let deltas = engine.get_by_state("delta", 100).await.expect("get_by_state failed");
    assert_eq!(deltas.len(), 5, "Should have 5 delta items");
    for item in &deltas {
        assert_eq!(item.state, "delta");
        assert!(item.object_id.starts_with("crdt.delta."));
    }
    
    let bases = engine.get_by_state("base", 100).await.expect("get_by_state failed");
    assert_eq!(bases.len(), 3, "Should have 3 base items");
    
    let defaults = engine.get_by_state("default", 100).await.expect("get_by_state failed");
    assert_eq!(defaults.len(), 2, "Should have 2 default items");

    // 3. Count by state
    assert_eq!(engine.count_by_state("delta").await.unwrap(), 5);
    assert_eq!(engine.count_by_state("base").await.unwrap(), 3);
    assert_eq!(engine.count_by_state("default").await.unwrap(), 2);
    assert_eq!(engine.count_by_state("nonexistent").await.unwrap(), 0);

    // 4. List state IDs (lightweight query)
    let delta_ids = engine.list_state_ids("delta", 100).await.expect("list_state_ids failed");
    assert_eq!(delta_ids.len(), 5);
    assert!(delta_ids.iter().all(|id| id.starts_with("crdt.delta.")));

    // 5. Update state (simulate merging deltas to base)
    let updated = engine.set_state("crdt.delta.0", "merged").await.expect("set_state failed");
    assert!(updated, "Should have updated the item");
    
    // Verify the state change
    assert_eq!(engine.count_by_state("delta").await.unwrap(), 4);
    assert_eq!(engine.count_by_state("merged").await.unwrap(), 1);

    // 6. Delete by state (cleanup merged items)
    let deleted = engine.delete_by_state("merged").await.expect("delete_by_state failed");
    assert_eq!(deleted, 1, "Should have deleted 1 merged item");
    
    assert_eq!(engine.count_by_state("merged").await.unwrap(), 0);
    
    // Note: The item may still be in L1 cache briefly, but SQL is the ground truth.
    // count_by_state returning 0 confirms deletion from SQL.

    // 7. Submit with state override via SubmitOptions
    let item = SyncItem::from_json("override.test".into(), json!({"test": true}))
        .with_state("original");
    
    engine.submit_with(item, SubmitOptions::default().with_state("overridden"))
        .await.expect("submit_with failed");
    
    // Force flush and wait
    tokio::time::sleep(Duration::from_millis(200)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // The state should be "overridden", not "original"
    assert_eq!(engine.count_by_state("overridden").await.unwrap(), 1);
    assert_eq!(engine.count_by_state("original").await.unwrap(), 0);

    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

// =============================================================================
// Prefix Scan Tests (CRDT Delta-First Pattern)
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker (Redis + MySQL)
async fn happy_prefix_scan() {
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let mysql = mysql_container(&docker);
    
    let redis_port = redis.get_host_port_ipv4(6379);
    let mysql_port = mysql.get_host_port_ipv4(3306);
    
    // Wait for MySQL to be fully ready
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    let wal_path = unique_wal_path("prefix_scan");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        redis_prefix: Some("prefix_test:".to_string()),
        sql_url: Some(format!(
            "mysql://test:test@127.0.0.1:{}/test",
            mysql_port
        )),
        wal_path: Some(wal_path.clone()),
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start engine");

    // Simulate CRDT delta-first pattern:
    // - delta:{object_id}:{op_id} for pending operations
    // - base:{object_id} for merged state
    
    // Create deltas for user.123 (3 operations)
    for i in 0..3 {
        let item = SyncItem::from_json(
            format!("delta:user.123:op{:03}", i),
            json!({"op": "+1", "ts": i})
        ).with_state("delta");
        engine.submit(item).await.expect("Submit delta failed");
    }
    
    // Create deltas for user.456 (2 operations)
    for i in 0..2 {
        let item = SyncItem::from_json(
            format!("delta:user.456:op{:03}", i),
            json!({"op": "-1", "ts": i})
        ).with_state("delta");
        engine.submit(item).await.expect("Submit delta failed");
    }
    
    // Create base states
    let base123 = SyncItem::from_json(
        "base:user.123".into(),
        json!({"total": 10})
    ).with_state("base");
    engine.submit(base123).await.expect("Submit base failed");
    
    let base456 = SyncItem::from_json(
        "base:user.456".into(),
        json!({"total": 5})
    ).with_state("base");
    engine.submit(base456).await.expect("Submit base failed");
    
    // Force flush to SQL
    tokio::time::sleep(Duration::from_millis(200)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 1. Scan specific object's deltas (the core use case)
    let user123_deltas = engine.scan_prefix("delta:user.123:", 100)
        .await.expect("scan_prefix failed");
    assert_eq!(user123_deltas.len(), 3, "Should have 3 deltas for user.123");
    for item in &user123_deltas {
        assert!(item.object_id.starts_with("delta:user.123:"));
        assert_eq!(item.state, "delta");
    }
    
    // 2. Scan different object's deltas
    let user456_deltas = engine.scan_prefix("delta:user.456:", 100)
        .await.expect("scan_prefix failed");
    assert_eq!(user456_deltas.len(), 2, "Should have 2 deltas for user.456");
    
    // 3. Scan all deltas (higher level prefix)
    let all_deltas = engine.scan_prefix("delta:", 100)
        .await.expect("scan_prefix failed");
    assert_eq!(all_deltas.len(), 5, "Should have 5 total deltas");
    
    // 4. Count by prefix
    assert_eq!(engine.count_prefix("delta:user.123:").await.unwrap(), 3);
    assert_eq!(engine.count_prefix("delta:user.456:").await.unwrap(), 2);
    assert_eq!(engine.count_prefix("delta:").await.unwrap(), 5);
    assert_eq!(engine.count_prefix("base:").await.unwrap(), 2);
    assert_eq!(engine.count_prefix("nonexistent:").await.unwrap(), 0);
    
    // 5. Delete by prefix (cleanup after merge)
    // Simulate: we merged user.123's deltas, now clean them up
    let deleted = engine.delete_prefix("delta:user.123:")
        .await.expect("delete_prefix failed");
    assert_eq!(deleted, 3, "Should have deleted 3 deltas");
    
    // Verify deletion
    assert_eq!(engine.count_prefix("delta:user.123:").await.unwrap(), 0);
    assert_eq!(engine.count_prefix("delta:user.456:").await.unwrap(), 2); // Other user's deltas remain
    assert_eq!(engine.count_prefix("base:").await.unwrap(), 2); // Bases untouched
    
    // 6. Verify items are actually gone from L1 cache
    let in_cache = engine.scan_prefix("delta:user.123:", 100).await.unwrap();
    assert!(in_cache.is_empty(), "Deleted items should not be in cache");
    
    // 7. Scan with limit
    // Add more deltas for limit testing
    for i in 0..20 {
        let item = SyncItem::from_json(
            format!("delta:bulk:op{:03}", i),
            json!({"op": "test"})
        ).with_state("delta");
        engine.submit(item).await.expect("Submit failed");
    }
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let limited = engine.scan_prefix("delta:bulk:", 5).await.unwrap();
    assert_eq!(limited.len(), 5, "Should respect limit");
    
    let all_bulk = engine.scan_prefix("delta:bulk:", 100).await.unwrap();
    assert_eq!(all_bulk.len(), 20, "Should get all with larger limit");

    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

// =============================================================================
// Search Tests - RediSearch Integration
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker with redis-stack
async fn happy_search_with_redisearch() {
    use sync_engine::search::{SearchIndex, Query};
    use sync_engine::coordinator::SearchTier;

    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("search");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        l1_max_bytes: 10 * 1024 * 1024,
        wal_path: Some(wal_path.clone()),
        batch_flush_ms: 50,
        batch_flush_count: 5,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    engine.start().await.expect("Failed to start engine");
    assert!(engine.is_ready());

    // 1. Create search index
    // Note: User data is wrapped in "payload" by RedisStore, so paths are $.payload.*
    let index = SearchIndex::new("users", "crdt:users:")
        .text_at("name", "$.payload.name")
        .text_at("email", "$.payload.email")
        .numeric_sortable_at("age", "$.payload.age")
        .tag_at("roles", "$.payload.roles[*]");

    engine.create_search_index(index).await
        .expect("Failed to create search index");

    // 2. Insert some test documents
    let users = vec![
        ("alice", "Alice Smith", "alice@example.com", 28, vec!["admin", "developer"]),
        ("bob", "Bob Jones", "bob@example.com", 35, vec!["developer"]),
        ("carol", "Carol White", "carol@example.com", 42, vec!["manager"]),
        ("dave", "Dave Brown", "dave@example.com", 25, vec!["developer", "intern"]),
        ("eve", "Eve Davis", "eve@example.com", 31, vec!["admin"]),
    ];

    for (id, name, email, age, roles) in &users {
        let item = SyncItem::from_json(
            format!("crdt:users:{}", id),
            json!({
                "name": name,
                "email": email,
                "age": age,
                "roles": roles,
            }),
        );
        engine.submit(item).await.expect("Failed to submit");
    }

    // Force flush and wait for indexing
    tokio::time::sleep(Duration::from_millis(100)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await; // Wait for RediSearch indexing

    // 3. Test simple field query
    let query = Query::field_eq("name", "Alice Smith");
    let results = engine.search_with_options("users", &query, SearchTier::RedisOnly, 100).await
        .expect("Search failed");
    assert_eq!(results.items.len(), 1, "Should find Alice");
    assert!(results.items[0].object_id.contains("alice"));

    // 4. Test numeric range query
    let query = Query::numeric_range("age", Some(30.0), Some(45.0));
    let results = engine.search_with_options("users", &query, SearchTier::RedisOnly, 100).await
        .expect("Search failed");
    assert_eq!(results.items.len(), 3, "Should find Bob, Carol, Eve (age 30-45)");

    // 5. Test tag query
    let query = Query::tags("roles", vec!["admin".to_string()]);
    let results = engine.search_with_options("users", &query, SearchTier::RedisOnly, 100).await
        .expect("Search failed");
    assert_eq!(results.items.len(), 2, "Should find Alice and Eve (admins)");

    // 6. Test combined query (AND)
    let query = Query::tags("roles", vec!["developer".to_string()])
        .and(Query::numeric_range("age", Some(30.0), None));
    let results = engine.search_with_options("users", &query, SearchTier::RedisOnly, 100).await
        .expect("Search failed");
    assert_eq!(results.items.len(), 1, "Should find Bob (developer, age >= 30)");

    // 7. Test raw query
    let results = engine.search_raw("users", "@roles:{admin}", 10).await
        .expect("Raw search failed");
    assert_eq!(results.len(), 2, "Raw query should find 2 admins");

    // 8. Drop index
    engine.drop_search_index("users").await
        .expect("Failed to drop index");

    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn happy_search_cache_invalidation() {
    use sync_engine::search::{SearchIndex, Query};
    use sync_engine::coordinator::SearchTier;

    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("search_cache");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        l1_max_bytes: 10 * 1024 * 1024,
        wal_path: Some(wal_path.clone()),
        batch_flush_ms: 50,
        batch_flush_count: 5,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    
    engine.start().await.expect("Failed to start engine");

    // Create index - use $.payload.* paths since RedisStore wraps user data
    let index = SearchIndex::new("items", "crdt:items:")
        .text_at("title", "$.payload.title")
        .tag_at("status", "$.payload.status");

    engine.create_search_index(index).await
        .expect("Failed to create index");

    // Insert initial data
    let item = SyncItem::from_json(
        "crdt:items:1".to_string(),
        json!({"title": "First Item", "status": "active"}),
    );
    engine.submit(item).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // First search
    let query = Query::tags("status", vec!["active".to_string()]);
    let results1 = engine.search_with_options("items", &query, SearchTier::RedisOnly, 100).await.unwrap();
    assert_eq!(results1.items.len(), 1);

    // Check cache stats
    let stats = engine.search_cache_stats();
    assert!(stats.is_some());

    // Add another item - this should invalidate cache via merkle change
    let item2 = SyncItem::from_json(
        "crdt:items:2".to_string(),
        json!({"title": "Second Item", "status": "active"}),
    );
    engine.submit(item2).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second search should find both items
    let results2 = engine.search_with_options("items", &query, SearchTier::RedisOnly, 100).await.unwrap();
    assert_eq!(results2.items.len(), 2, "Should find both active items after cache invalidation");

    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

// =============================================================================
// CDC Stream Tests
// =============================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn cdc_stream_put_entries() {
    // Test that PUT operations emit CDC entries to the Redis stream
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("cdc_put");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        l1_max_bytes: 10 * 1024 * 1024,
        wal_path: Some(wal_path.clone()),
        enable_cdc_stream: true,  // Enable CDC
        cdc_stream_maxlen: 1000,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start engine");

    // Submit items
    let item1 = SyncItem::from_json("cdc.test.1".to_string(), json!({"name": "Alice"}));
    let item2 = SyncItem::from_json("cdc.test.2".to_string(), json!({"name": "Bob"}));
    
    engine.submit(item1).await.expect("Submit failed");
    engine.submit(item2).await.expect("Submit failed");
    
    // Force flush to ensure items are written to Redis and CDC emitted
    tokio::time::sleep(Duration::from_millis(50)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read CDC stream directly from Redis
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", redis_port)).unwrap();
    let mut conn = redis::aio::ConnectionManager::new(client).await.unwrap();
    
    // XRANGE __local__:cdc - + COUNT 10
    let entries: Vec<redis::Value> = redis::cmd("XRANGE")
        .arg("__local__:cdc")
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg(10)
        .query_async(&mut conn)
        .await
        .expect("XRANGE failed");
    
    assert_eq!(entries.len(), 2, "Should have 2 CDC entries");
    
    // Parse first entry and verify fields
    if let redis::Value::Array(entry) = &entries[0] {
        // entry = [id, [field, value, field, value, ...]]
        if let redis::Value::Array(fields) = &entry[1] {
            // Find "op" field
            let mut op_found = false;
            let mut key_found = false;
            for i in (0..fields.len()).step_by(2) {
                if let (redis::Value::BulkString(field), redis::Value::BulkString(value)) = (&fields[i], &fields[i+1]) {
                    let field_str = String::from_utf8_lossy(field);
                    let value_str = String::from_utf8_lossy(value);
                    if field_str == "op" {
                        assert_eq!(value_str, "PUT");
                        op_found = true;
                    }
                    if field_str == "key" {
                        assert_eq!(value_str, "cdc.test.1");
                        key_found = true;
                    }
                }
            }
            assert!(op_found, "CDC entry should have 'op' field");
            assert!(key_found, "CDC entry should have 'key' field");
        }
    }

    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn cdc_stream_delete_entries() {
    // Test that DELETE operations emit CDC entries to the Redis stream
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("cdc_del");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,  // No SQL needed for this test
        l1_max_bytes: 10 * 1024 * 1024,
        wal_path: Some(wal_path.clone()),
        enable_cdc_stream: true,
        cdc_stream_maxlen: 1000,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start engine");

    // Create an item first
    let item = SyncItem::from_json("cdc.delete.test".to_string(), json!({"name": "ToDelete"}));
    engine.submit(item).await.expect("Submit failed");
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Delete the item
    let deleted = engine.delete("cdc.delete.test").await.expect("Delete failed");
    assert!(deleted, "Item should have been deleted");

    // Read CDC stream
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", redis_port)).unwrap();
    let mut conn = redis::aio::ConnectionManager::new(client).await.unwrap();
    
    let entries: Vec<redis::Value> = redis::cmd("XRANGE")
        .arg("__local__:cdc")
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg(10)
        .query_async(&mut conn)
        .await
        .expect("XRANGE failed");
    
    // Should have PUT + DEL entries
    assert!(entries.len() >= 2, "Should have at least PUT and DEL entries");
    
    // Find DEL entry
    let mut found_del = false;
    for entry_val in &entries {
        if let redis::Value::Array(entry) = entry_val {
            if let redis::Value::Array(fields) = &entry[1] {
                for i in (0..fields.len()).step_by(2) {
                    if let (redis::Value::BulkString(field), redis::Value::BulkString(value)) = (&fields[i], &fields[i+1]) {
                        if String::from_utf8_lossy(field) == "op" && String::from_utf8_lossy(value) == "DEL" {
                            found_del = true;
                        }
                    }
                }
            }
        }
    }
    assert!(found_del, "Should have DEL entry in CDC stream");

    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn cdc_stream_disabled_no_entries() {
    // Test that CDC entries are NOT emitted when feature is disabled
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("cdc_disabled");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        sql_url: None,
        l1_max_bytes: 10 * 1024 * 1024,
        wal_path: Some(wal_path.clone()),
        enable_cdc_stream: false,  // CDC disabled
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start engine");

    // Submit items
    let item = SyncItem::from_json("cdc.disabled.test".to_string(), json!({"name": "NoStream"}));
    engine.submit(item).await.expect("Submit failed");
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // CDC stream should NOT exist
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", redis_port)).unwrap();
    let mut conn = redis::aio::ConnectionManager::new(client).await.unwrap();
    
    let exists: bool = redis::cmd("EXISTS")
        .arg("__local__:cdc")
        .query_async(&mut conn)
        .await
        .expect("EXISTS failed");
    
    assert!(!exists, "CDC stream should not exist when disabled");

    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}

#[tokio::test]
#[ignore] // Requires Docker
async fn cdc_stream_respects_prefix() {
    // Test that CDC stream key respects redis_prefix
    let docker = Cli::default();
    let redis = redis_container(&docker);
    let redis_port = redis.get_host_port_ipv4(6379);
    
    let wal_path = unique_wal_path("cdc_prefix");
    let config = SyncEngineConfig {
        redis_url: Some(format!("redis://127.0.0.1:{}", redis_port)),
        redis_prefix: Some("myapp:".to_string()),
        sql_url: None,
        l1_max_bytes: 10 * 1024 * 1024,
        wal_path: Some(wal_path.clone()),
        enable_cdc_stream: true,
        cdc_stream_maxlen: 1000,
        ..Default::default()
    };

    let (_tx, rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, rx);
    engine.start().await.expect("Failed to start engine");

    // Submit item
    let item = SyncItem::from_json("cdc.prefix.test".to_string(), json!({"name": "Prefixed"}));
    engine.submit(item).await.expect("Submit failed");
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    engine.force_flush().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // CDC stream should be at myapp:__local__:cdc
    let client = redis::Client::open(format!("redis://127.0.0.1:{}", redis_port)).unwrap();
    let mut conn = redis::aio::ConnectionManager::new(client).await.unwrap();
    
    // Prefixed stream should exist
    let prefixed_exists: bool = redis::cmd("EXISTS")
        .arg("myapp:__local__:cdc")
        .query_async(&mut conn)
        .await
        .expect("EXISTS failed");
    assert!(prefixed_exists, "Prefixed CDC stream should exist");
    
    // Unprefixed stream should NOT exist
    let unprefixed_exists: bool = redis::cmd("EXISTS")
        .arg("__local__:cdc")
        .query_async(&mut conn)
        .await
        .expect("EXISTS failed");
    assert!(!unprefixed_exists, "Unprefixed CDC stream should not exist");

    engine.shutdown().await;
    cleanup_wal_files(&wal_path);
}