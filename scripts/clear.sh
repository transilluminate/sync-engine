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
KEYS_BEFORE=$(redis-cli DBSIZE)
echo "   └─ Keys before: $KEYS_BEFORE"

redis-cli FLUSHDB > /dev/null
echo "   └─ ✅ FLUSHDB complete!"

KEYS_AFTER=$(redis-cli DBSIZE)
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

ROWS_AFTER=$(docker exec mysql mysql -utest -ptest -N -e "SELECT COUNT(*) FROM sync_items" test 2>/dev/null || echo "0")
echo "   └─ Rows after: $ROWS_AFTER"

echo ""
echo "✨ Environment cleared! Ready for fresh data."
echo ""
