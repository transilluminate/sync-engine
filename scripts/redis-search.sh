#!/bin/bash
# Manual Redis search (without RediSearch)
#
# Usage:
#   ./scripts/redis-search.sh                 # List all sync: keys
#   ./scripts/redis-search.sh "user.*"        # Keys matching pattern
#   ./scripts/redis-search.sh --json "Alice"  # Search JSON content

echo ""
echo "🔍 Redis Manual Search"
echo ""

# Parse arguments
JSON_SEARCH=false
PATTERN="*"

if [ "$1" = "--json" ]; then
    JSON_SEARCH=true
    SEARCH_TERM="${2:-}"
elif [ -n "$1" ]; then
    PATTERN="$1"
fi

KEY_PATTERN="sync:$PATTERN"
KEYS=$(redis-cli KEYS "$KEY_PATTERN")
KEY_COUNT=$(echo "$KEYS" | grep -c . 2>/dev/null || echo "0")

echo "📦 Pattern: $KEY_PATTERN → $KEY_COUNT keys found"
echo ""

if [ "$KEY_COUNT" -eq 0 ] || [ -z "$KEYS" ]; then
    echo "   └─ No keys found matching pattern: $KEY_PATTERN"
    echo ""
    echo "💡 Run 'cargo run --example basic_usage' to create test data"
    exit 0
fi

# Fetch and display each key
MATCH_COUNT=0

echo "$KEYS" | while read key; do
    [ -z "$key" ] && continue
    
    # Get JSON document
    JSON=$(redis-cli JSON.GET "$key" 2>/dev/null)
    
    if [ -z "$JSON" ]; then
        # Try regular GET for non-JSON keys
        BYTES=$(redis-cli GET "$key" 2>/dev/null | wc -c)
        JSON="<binary: $BYTES bytes>"
    fi
    
    # If doing JSON search, filter by content
    if [ "$JSON_SEARCH" = true ]; then
        if ! echo "$JSON" | grep -qi "$SEARCH_TERM"; then
            continue
        fi
    fi
    
    echo "┌─ $key"
    echo "$JSON" | python3 -m json.tool 2>/dev/null | while read line; do
        echo "│  $line"
    done || echo "│  $JSON"
    echo "└─────────────────────────────────────────────────────────"
    echo ""
done

if [ "$JSON_SEARCH" = true ]; then
    echo "📊 JSON search for '$SEARCH_TERM' complete"
    echo ""
fi

echo "💡 Search options:"
echo "   └─ Pattern match:  ./scripts/redis-search.sh \"user.*\""
echo "   └─ JSON content:   ./scripts/redis-search.sh --json \"Alice\""
echo "   └─ All keys:       ./scripts/redis-search.sh"
echo ""
