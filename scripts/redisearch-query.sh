#!/bin/bash
# Query RediSearch index with pretty output
#
# Usage:
#   ./scripts/redisearch-query.sh                    # All documents
#   ./scripts/redisearch-query.sh "Alice"            # Full-text search
#   ./scripts/redisearch-query.sh "@name:Bob"        # Field search
#   ./scripts/redisearch-query.sh "@role:{admin}"    # Tag search
#   ./scripts/redisearch-query.sh "@requests:[40000 +inf]"  # Numeric range

QUERY="${1:-*}"

echo ""
echo "🔍 RediSearch Query: $QUERY"
echo ""

# Check if index exists
if ! redis-cli FT.INFO sync_idx > /dev/null 2>&1; then
    echo "❌ Index 'sync_idx' not found!"
    echo "   └─ Run: ./scripts/redisearch-index.sh"
    exit 1
fi

# Execute search and get results
RESULT=$(redis-cli FT.SEARCH sync_idx "$QUERY" LIMIT 0 100)
COUNT=$(echo "$RESULT" | head -1)

echo "📊 Found $COUNT result(s)"
echo ""

if [ "$COUNT" -eq 0 ]; then
    echo "   └─ No matches for query: $QUERY"
    echo ""
    echo "💡 Try these queries:"
    echo "   └─ \"*\"           - All documents"
    echo "   └─ \"@name:Alice\" - Search by name field"
    echo "   └─ \"@role:{user}\" - Search by role tag"
    exit 0
fi

# Get the keys from result and fetch full JSON for each
redis-cli FT.SEARCH sync_idx "$QUERY" LIMIT 0 100 RETURN 0 | tail -n +2 | while read key; do
    [ -z "$key" ] && continue
    
    echo "┌─ $key"
    redis-cli JSON.GET "$key" | python3 -m json.tool 2>/dev/null | while read line; do
        echo "│  $line"
    done
    echo "└─────────────────────────────────────────────────────────"
    echo ""
done

echo "💡 Query examples:"
echo "   └─ Full-text:  ./scripts/redisearch-query.sh \"Alice\""
echo "   └─ Field:      ./scripts/redisearch-query.sh \"@name:Bob\""
echo "   └─ Tag:        ./scripts/redisearch-query.sh \"@role:{admin}\""
echo "   └─ Numeric:    ./scripts/redisearch-query.sh \"@requests:[40000 +inf]\""
echo "   └─ Combined:   ./scripts/redisearch-query.sh \"@role:{user} @name:Bob\""
echo ""
