#!/bin/bash
# Clear test docker environment (Redis + MySQL)

set -e

echo ""
echo "🧹 Clearing test docker environment..."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Clear Redis
# ─────────────────────────────────────────────────────────────────────────────
echo "📦 Redis (localhost:6379)"
KEYS_BEFORE=$(redis-cli DBSIZE | awk '{print $2}')
echo "   └─ Keys before: $KEYS_BEFORE"

redis-cli FLUSHDB > /dev/null
echo "   └─ ✅ FLUSHDB complete!"

KEYS_AFTER=$(redis-cli DBSIZE | awk '{print $2}')
echo "   └─ Keys after: $KEYS_AFTER"

# ─────────────────────────────────────────────────────────────────────────────
# Clear MySQL
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "📦 MySQL (localhost:3306/test)"

ROWS_BEFORE=$(docker exec mysql mysql -utest -ptest -N -e "SELECT COUNT(*) FROM sync_items" test 2>/dev/null || echo "0")
echo "   └─ Rows before: $ROWS_BEFORE"

docker exec mysql mysql -utest -ptest -e "DROP TABLE IF EXISTS sync_items" test 2>/dev/null
echo "   └─ ✅ Table dropped!"

docker exec mysql mysql -utest -ptest -e "
CREATE TABLE IF NOT EXISTS sync_items (
    id VARCHAR(255) PRIMARY KEY,
    version BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    payload_hash VARCHAR(64),
    payload LONGTEXT,
    payload_blob MEDIUMBLOB,
    audit TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)" test 2>/dev/null
echo "   └─ ✅ Table recreated!"

ROWS_AFTER=$(docker exec mysql mysql -utest -ptest -N -e "SELECT COUNT(*) FROM sync_items" test 2>/dev/null)
echo "   └─ Rows after: $ROWS_AFTER"

echo ""
echo "✨ Environment cleared! Ready for fresh data."
echo ""
