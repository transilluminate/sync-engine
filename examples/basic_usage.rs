// Copyright (c) 2025-2026 Adrian Robinson. Licensed under the AGPL-3.0.
// See LICENSE file in the project root for full license text.

//! Basic sync-engine usage example.
//!
//! Demonstrates:
//! 1. Connecting to Redis (L2) and MySQL (L3) via docker-compose
//! 2. Writing 5 simple JSON entries
//! 3. Checking existence
//! 4. Fetching entries back
//! 5. Displaying metrics (OTEL-compatible)
//! 6. Clean shutdown
//!
//! # Prerequisites
//!
//! Start the docker-compose environment:
//! ```bash
//! docker compose up -d
//! ```
//!
//! # Run
//!
//! ```bash
//! cargo run --example basic_usage
//! ```

use serde_json::json;
use sync_engine::{SyncEngine, SyncEngineConfig, SyncItem, EngineState};
use tokio::sync::watch;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install metrics recorder (captures all metrics for OTEL export)
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("failed to install metrics recorder");

    // Simple logging (no filter for simplicity)
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║           sync-engine: Basic Usage Example                    ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");

    // ─────────────────────────────────────────────────────────────────────────
    // 1. Configure and start the engine
    // ─────────────────────────────────────────────────────────────────────────
    println!("📦 Configuring sync-engine...");
    
    let config = SyncEngineConfig {
        // Connect to docker-compose Redis
        redis_url: Some("redis://localhost:6379".into()),
        // Connect to docker-compose MySQL  
        sql_url: Some("mysql://test:test@localhost:3306/test".into()),
        // Namespace prefix for Redis keys (plays nice with other data)
        redis_prefix: Some("sync:".into()),
        // L1 memory limit (64MB for demo)
        l1_max_bytes: 64 * 1024 * 1024,
        // Batch settings (flush quickly for demo)
        batch_flush_count: 10,
        batch_flush_ms: 500,
        // CDC stream for replication
        enable_cdc_stream: true,
        cdc_stream_maxlen: 10_000,
        ..Default::default()
    };

    let (_config_tx, config_rx) = watch::channel(config.clone());
    let mut engine = SyncEngine::new(config, config_rx);

    println!("   State: {:?}", engine.state());
    
    println!("\n🚀 Starting engine (connecting to backends)...");
    engine.start().await?;
    
    assert_eq!(engine.state(), EngineState::Ready);
    println!("   ✅ Engine ready! State: {:?}", engine.state());

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Create and submit 5 simple entries (with timing)
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📝 Creating 5 sample entries...");
    println!("   ⏱️  Timing each .await() to showcase L1 cache speed");
    
    let entries = vec![
        ("user.alice", json!({"name": "Alice", "role": "admin"})),
        ("user.bob", json!({"name": "Bob", "role": "user"})),
        ("user.carol", json!({"name": "Carol", "role": "user"})),
        ("config.app", json!({"theme": "dark", "version": "2.0"})),
        ("stats.daily", json!({"requests": 42000, "latency_p99": 12})),
    ];

    let mut submit_times = Vec::new();
    for (id, data) in &entries {
        let item = SyncItem::from_json(id.to_string(), data.clone());
        let start = std::time::Instant::now();
        engine.submit(item).await?;
        let elapsed = start.elapsed();
        submit_times.push(elapsed);
        println!("   └─ Submitted: {} → {} ({:?})", id, data, elapsed);
    }
    
    let avg_submit: std::time::Duration = submit_times.iter().sum::<std::time::Duration>() / submit_times.len() as u32;
    println!("   ⚡ Submit avg: {:?} (L1 memory write, non-blocking)", avg_submit);

    // Force flush to ensure data reaches L2/L3
    println!("\n⏳ Flushing to backends... (waiting 100ms for async writes)");
    engine.force_flush().await;
    // Small delay to let async writes complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    println!("   ✅ Flush complete!");

    // ─────────────────────────────────────────────────────────────────────────
    // 3. Check existence (fast probabilistic + authoritative)
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n🔍 Checking existence...");
    
    // Fast check: definitely missing? (authoritative negative from Cuckoo)
    let def_missing = engine.definitely_missing("user.alice");
    println!("   └─ definitely_missing('user.alice'): {}", def_missing);
    
    // Fast check: might exist? (L1 + Cuckoo probabilistic)
    let might = engine.might_exist("user.alice");
    println!("   └─ might_exist('user.alice'): {}", might);
    
    // Authoritative check (L1 → L2 → L3)
    let exists = engine.contains("user.alice").await;
    println!("   └─ contains('user.alice'): {}", exists);
    
    // Check non-existent key
    let missing = engine.contains("user.nobody").await;
    println!("   └─ contains('user.nobody'): {}", missing);

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Fetch entries back and display
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📖 Fetching entries back (with timing)...");
    println!("   ⏱️  These should hit L1 cache (microseconds!)");
    
    let mut get_times = Vec::new();
    for (id, _) in &entries {
        let start = std::time::Instant::now();
        let result = engine.get(id).await?;
        let elapsed = start.elapsed();
        get_times.push(elapsed);
        
        match result {
            Some(item) => {
                let json = item.content_as_json()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "<binary>".into());
                println!("   └─ {} (v{}) → {} ({:?})", item.object_id, item.version, json, elapsed);
            }
            None => {
                println!("   └─ {} → NOT FOUND ({:?})", id, elapsed);
            }
        }
    }
    
    let avg_get: std::time::Duration = get_times.iter().sum::<std::time::Duration>() / get_times.len() as u32;
    println!("   ⚡ Get avg: {:?} (L1 cache hit)", avg_get);

    // ─────────────────────────────────────────────────────────────────────────
    // 5. Display metrics and merkle roots
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📊 Engine Metrics:");
    
    // L1 cache stats
    let (l1_entries, l1_bytes) = engine.l1_stats();
    println!("   ┌─ L1 Cache (Memory)");
    println!("   │  └─ Entries: {}", l1_entries);
    println!("   │  └─ Size: {} bytes", l1_bytes);
    println!("   │  └─ Pressure: {:.1}%", engine.memory_pressure() * 100.0);
    
    // L3 filter stats (before verification)
    let (filter_entries, filter_capacity, trust) = engine.l3_filter_stats();
    println!("   ├─ L3 Cuckoo Filter (before verify)");
    println!("   │  └─ Entries: {}/{}", filter_entries, filter_capacity);
    println!("   │  └─ Trust: {:?}", trust);
    
    // Merkle roots (sync verification)
    let (redis_root, sql_root) = engine.merkle_roots().await;
    println!("   ├─ Merkle Trees");
    println!("   │  └─ Redis (L2): {}", redis_root.as_deref().unwrap_or("(empty)"));
    println!("   │  └─ SQL (L3):   {}", sql_root.as_deref().unwrap_or("(empty)"));
    if redis_root == sql_root {
        println!("   │  └─ ✅ Roots match! Data is in sync.");
    } else {
        println!("   │  └─ ⚠️  Roots differ (expected during warmup)");
    }
    
    // Verify and trust the filter
    println!("\n🔐 Verifying L3 filter against SQL merkle...");
    let trusted = engine.verify_filter().await;
    let (_, _, trust_after) = engine.l3_filter_stats();
    println!("   └─ Verified: {} → Trust: {:?}", trusted, trust_after);
    
    // Item status (shows where data lives)
    println!("\n📍 Item Status:");
    for (id, _) in &entries {
        let status = engine.status(id).await;
        println!("   └─ {}: {:?}", id, status);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. Dump raw metrics (OTEL-compatible)
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n📈 Raw Metrics (OTEL export format):");
    // Update gauge metrics with current state before snapshotting
    engine.update_gauge_metrics();
    dump_metrics(&snapshotter);

    // ─────────────────────────────────────────────────────────────────────────
    // 7. Clean shutdown
    // ─────────────────────────────────────────────────────────────────────────
    println!("\n🛑 Shutting down...");
    engine.shutdown().await;
    println!("   ✅ Shutdown complete! State: {:?}", engine.state());

    // Clean up WAL database file (data remains in Redis/MySQL for inspection)
    println!("\n🧹 Cleaning up WAL file...");
    if let Err(e) = std::fs::remove_file("./sync_engine_wal.db") {
        if e.kind() != std::io::ErrorKind::NotFound {
            println!("   └─ Warning: could not remove WAL: {}", e);
        }
    } else {
        println!("   └─ Removed: ./sync_engine_wal.db");
    }
    // Also remove WAL journal files if they exist
    let _ = std::fs::remove_file("./sync_engine_wal.db-shm");
    let _ = std::fs::remove_file("./sync_engine_wal.db-wal");
    println!("   ✅ WAL cleanup complete!");
    
    println!("\n💡 Data remains in Redis/MySQL - inspect with:");
    println!("   └─ Redis:  redis-cli JSON.GET sync:user.alice '$.payload'");
    println!("   └─ MySQL:  SELECT id, payload FROM sync_items;");
    println!("   └─ Query:  SELECT * FROM sync_items WHERE JSON_EXTRACT(payload, '$.name') = 'Alice';");
    println!("   └─ UIs:    http://localhost:5540 (RedisInsight)");
    println!("              http://localhost:8080 (Adminer)");

    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║                    Example complete!                          ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");

    Ok(())
}
/// Dump all captured metrics in OTEL-compatible format
fn dump_metrics(snapshotter: &Snapshotter) {
    let snapshot = snapshotter.snapshot();
    
    // Collect and sort metrics by name for cleaner output
    let mut counters: Vec<_> = vec![];
    let mut gauges: Vec<_> = vec![];
    let mut histograms: Vec<_> = vec![];
    
    for (composite_key, _, _, value) in snapshot.into_vec() {
        let (kind, key) = composite_key.into_parts();
        let name = key.name();
        let labels: Vec<_> = key.labels().map(|l| format!("{}={}", l.key(), l.value())).collect();
        let label_str = if labels.is_empty() { String::new() } else { format!("{{{}}}", labels.join(",")) };
        let _ = kind; // unused but helps understand the structure
        
        match value {
            DebugValue::Counter(v) => counters.push((name.to_string(), label_str, v)),
            DebugValue::Gauge(v) => gauges.push((name.to_string(), label_str, v.into_inner())),
            DebugValue::Histogram(samples) => {
                let count = samples.len();
                let sum: f64 = samples.iter().map(|v| v.into_inner()).sum();
                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                let min = samples.iter().map(|v| v.into_inner()).fold(f64::INFINITY, f64::min);
                let max = samples.iter().map(|v| v.into_inner()).fold(f64::NEG_INFINITY, f64::max);
                histograms.push((name.to_string(), label_str, count, sum, avg, min, max));
            }
        }
    }
    
    // Sort each category
    counters.sort_by(|a, b| a.0.cmp(&b.0));
    gauges.sort_by(|a, b| a.0.cmp(&b.0));
    histograms.sort_by(|a, b| a.0.cmp(&b.0));
    
    // Print counters
    if !counters.is_empty() {
        println!("   ┌─ Counters (cumulative)");
        for (name, labels, value) in &counters {
            println!("   │  └─ {}{} = {}", name, labels, value);
        }
    }
    
    // Print gauges
    if !gauges.is_empty() {
        println!("   ├─ Gauges (current value)");
        for (name, labels, value) in &gauges {
            println!("   │  └─ {}{} = {:.2}", name, labels, value);
        }
    }
    
    // Print histograms
    if !histograms.is_empty() {
        println!("   └─ Histograms (distributions)");
        for (name, labels, count, sum, avg, min, max) in &histograms {
            if *min == f64::INFINITY {
                println!("   │  └─ {}{} = (no samples)", name, labels);
            } else {
                println!("   │  └─ {}{}", name, labels);
                println!("   │     count={} sum={:.4} avg={:.4} min={:.4} max={:.4}", 
                    count, sum, avg, min, max);
            }
        }
    }
    
    if counters.is_empty() && gauges.is_empty() && histograms.is_empty() {
        println!("   └─ (no metrics recorded)");
    }
}