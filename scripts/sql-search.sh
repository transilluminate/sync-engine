#!/bin/bash
# Search MySQL sync_items table
#
# Usage:
#   ./scripts/sql-search.sh                          # List all records
#   ./scripts/sql-search.sh "$.name" "Alice"         # JSON path search
#   ./scripts/sql-search.sh --like "admin"           # LIKE search in payload
#   ./scripts/sql-search.sh --where "version > 0"    # Custom WHERE clause

MYSQL="docker exec mysql mysql -utest -ptest -t test"

echo ""
echo "🔍 MySQL Search (sync_items table)"
echo ""

# Build query based on arguments
if [ "$1" = "--where" ]; then
    shift
    WHERE_CLAUSE="$*"
    QUERY="SELECT id, version, timestamp, SUBSTRING(payload_hash, 1, 16) as hash, payload, audit FROM sync_items WHERE $WHERE_CLAUSE"
elif [ "$1" = "--like" ]; then
    SEARCH_TERM="$2"
    QUERY="SELECT id, version, timestamp, SUBSTRING(payload_hash, 1, 16) as hash, payload, audit FROM sync_items WHERE payload LIKE '%$SEARCH_TERM%'"
elif [ -n "$1" ] && [ -n "$2" ]; then
    JSON_PATH="$1"
    VALUE="$2"
    # Use JSON_UNQUOTE to compare unquoted values, or use ->> operator
    QUERY="SELECT id, version, timestamp, SUBSTRING(payload_hash, 1, 16) as hash, payload, audit FROM sync_items WHERE JSON_UNQUOTE(JSON_EXTRACT(payload, '$JSON_PATH')) = '$VALUE'"
else
    QUERY="SELECT id, version, timestamp, SUBSTRING(payload_hash, 1, 16) as hash, payload, audit FROM sync_items ORDER BY timestamp DESC"
fi

echo "📝 Query: $QUERY"
echo ""

# Execute query
$MYSQL -e "$QUERY" 2>/dev/null

if [ $? -ne 0 ]; then
    echo "❌ Query failed. Make sure docker is running and table exists."
    echo "   └─ Run: ./scripts/clear.sh to recreate table"
    exit 1
fi

echo ""
echo "💡 Search options:"
echo "   └─ All records:     ./scripts/sql-search.sh"
echo "   └─ JSON path:       ./scripts/sql-search.sh '\$.name' 'Alice'"
echo "   └─ LIKE search:     ./scripts/sql-search.sh --like 'admin'"
echo "   └─ Custom WHERE:    ./scripts/sql-search.sh --where 'version > 0'"
echo ""
