#!/bin/bash
# Display complete records from both Redis and MySQL
#
# Usage:
#   ./scripts/show-records.sh                 # Show all records from both
#   ./scripts/show-records.sh "user.alice"    # Show specific record
#   ./scripts/show-records.sh --redis         # Redis only
#   ./scripts/show-records.sh --sql           # MySQL only

MYSQL="docker exec mysql mysql -utest -ptest test"

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║              sync-engine: Full Record Display                 ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Parse arguments
SHOW_REDIS=true
SHOW_SQL=true
SPECIFIC_KEY=""

while [ $# -gt 0 ]; do
    case "$1" in
        --redis)
            SHOW_SQL=false
            shift
            ;;
        --sql)
            SHOW_REDIS=false
            shift
            ;;
        *)
            SPECIFIC_KEY="$1"
            shift
            ;;
    esac
done

# ─────────────────────────────────────────────────────────────────────────────
# Redis Records
# ─────────────────────────────────────────────────────────────────────────────
if [ "$SHOW_REDIS" = true ]; then
    echo "📦 REDIS (L2 Cache)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    if [ -n "$SPECIFIC_KEY" ]; then
        KEYS="sync:$SPECIFIC_KEY"
    else
        KEYS=$(redis-cli KEYS 'sync:*' 2>/dev/null)
    fi
    
    KEY_COUNT=$(echo "$KEYS" | grep -c . 2>/dev/null || echo "0")
    
    if [ "$KEY_COUNT" -eq 0 ] || [ -z "$KEYS" ]; then
        echo "   (no records found)"
    else
        echo "$KEYS" | while read key; do
            [ -z "$key" ] && continue
            
            echo "┌──────────────────────────────────────────────────────────────"
            echo "│ KEY: $key"
            echo "├──────────────────────────────────────────────────────────────"
            
            # Get JSON and pretty print
            JSON=$(redis-cli JSON.GET "$key" 2>/dev/null)
            
            if [ -n "$JSON" ]; then
                echo "$JSON" | python3 -m json.tool 2>/dev/null | while read line; do
                    echo "│ $line"
                done
            else
                # Try regular GET
                VALUE=$(redis-cli GET "$key" 2>/dev/null)
                echo "│ (binary or raw value)"
                echo "│ Length: $(echo "$VALUE" | wc -c) bytes"
            fi
            
            echo "└──────────────────────────────────────────────────────────────"
            echo ""
        done
    fi
    
    echo ""
fi

# ─────────────────────────────────────────────────────────────────────────────
# MySQL Records
# ─────────────────────────────────────────────────────────────────────────────
if [ "$SHOW_SQL" = true ]; then
    echo "📦 MYSQL (L3 Archive)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    if [ -n "$SPECIFIC_KEY" ]; then
        WHERE="WHERE id = '$SPECIFIC_KEY'"
    else
        WHERE=""
    fi
    
    # Get records in a format we can parse
    RECORDS=$($MYSQL -N -e "SELECT id, version, timestamp, payload_hash, payload, audit, created_at, updated_at FROM sync_items $WHERE ORDER BY timestamp DESC" 2>/dev/null)
    
    if [ -z "$RECORDS" ]; then
        echo "   (no records found)"
    else
        echo "$RECORDS" | while IFS=$'\t' read -r id version timestamp payload_hash payload audit created_at updated_at; do
            echo "┌──────────────────────────────────────────────────────────────"
            echo "│ ID: $id"
            echo "├──────────────────────────────────────────────────────────────"
            echo "│ version:      $version"
            echo "│ timestamp:    $timestamp"
            echo "│ payload_hash: ${payload_hash:0:32}..."
            echo "│ created_at:   $created_at"
            echo "│ updated_at:   $updated_at"
            echo "│"
            echo "│ payload:"
            echo "$payload" | python3 -m json.tool 2>/dev/null | while read line; do
                echo "│   $line"
            done
            
            if [ -n "$audit" ] && [ "$audit" != "NULL" ]; then
                echo "│"
                echo "│ audit:"
                echo "$audit" | python3 -m json.tool 2>/dev/null | while read line; do
                    echo "│   $line"
                done
            fi
            
            echo "└──────────────────────────────────────────────────────────────"
            echo ""
        done
    fi
    
    echo ""
fi

echo "💡 Options:"
echo "   └─ All records:      ./scripts/show-records.sh"
echo "   └─ Specific key:     ./scripts/show-records.sh \"user.alice\""
echo "   └─ Redis only:       ./scripts/show-records.sh --redis"
echo "   └─ MySQL only:       ./scripts/show-records.sh --sql"
echo ""
