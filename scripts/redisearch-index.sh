#!/bin/bash
# Build RediSearch index for sync-engine data

set -e

echo ""
echo "🔍 Building RediSearch index for sync-engine data..."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Check existing keys
# ─────────────────────────────────────────────────────────────────────────────
KEYS=$(redis-cli KEYS 'sync:*')
KEY_COUNT=$(echo "$KEYS" | grep -c . || echo "0")

echo "📦 Found $KEY_COUNT keys with 'sync:' prefix"

if [ "$KEY_COUNT" -eq 0 ]; then
    echo "   └─ ⚠️  No data found. Run 'cargo run --example basic_usage' first!"
    exit 0
fi

echo "$KEYS" | while read key; do
    [ -n "$key" ] && echo "   └─ $key"
done

# ─────────────────────────────────────────────────────────────────────────────
# Drop existing index (if any)
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🗑️  Dropping existing index (if any)..."

if redis-cli FT.DROPINDEX sync_idx 2>/dev/null; then
    echo "   └─ ✅ Dropped existing 'sync_idx'"
else
    echo "   └─ ℹ️  No existing index to drop"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Create RediSearch index
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "📝 Creating RediSearch index 'sync_idx'..."

redis-cli FT.CREATE sync_idx \
    ON JSON \
    PREFIX 1 sync: \
    SCHEMA \
    '$.version' AS version NUMERIC SORTABLE \
    '$.timestamp' AS timestamp NUMERIC SORTABLE \
    '$.payload_hash' AS payload_hash TAG \
    '$.payload.name' AS name TEXT SORTABLE \
    '$.payload.role' AS role TAG \
    '$.payload.theme' AS theme TAG \
    '$.payload.version' AS app_version TAG \
    '$.payload.requests' AS requests NUMERIC SORTABLE \
    '$.payload.latency_p99' AS latency_p99 NUMERIC SORTABLE

echo "   └─ ✅ Index created successfully!"

# ─────────────────────────────────────────────────────────────────────────────
# Show index info
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "📊 Index Info:"
NUM_DOCS=$(redis-cli FT.INFO sync_idx | grep -A1 num_docs | tail -1)
echo "   └─ num_docs: $NUM_DOCS"

# ─────────────────────────────────────────────────────────────────────────────
# Test the index
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "🧪 Testing index with sample queries..."

ALICE_COUNT=$(redis-cli FT.SEARCH sync_idx '@name:Alice' LIMIT 0 0 | head -1)
echo "   └─ @name:Alice → $ALICE_COUNT results"

USER_COUNT=$(redis-cli FT.SEARCH sync_idx '@role:{user}' LIMIT 0 0 | head -1)
echo "   └─ @role:{user} → $USER_COUNT results"

REQUESTS_COUNT=$(redis-cli FT.SEARCH sync_idx '@requests:[1000 +inf]' LIMIT 0 0 | head -1)
echo "   └─ @requests:[1000 +inf] → $REQUESTS_COUNT results"

echo ""
echo "✨ RediSearch index ready! Try these queries:"
echo "   └─ redis-cli FT.SEARCH sync_idx '@name:Alice'"
echo "   └─ redis-cli FT.SEARCH sync_idx '@role:{admin}'"
echo "   └─ redis-cli FT.SEARCH sync_idx '@requests:[40000 +inf]'"
echo "   └─ redis-cli FT.SEARCH sync_idx '*' SORTBY timestamp DESC"
echo "   └─ ./scripts/redisearch-query.sh 'Alice'"
echo ""
