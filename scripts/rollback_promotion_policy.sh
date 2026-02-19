#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
POLICY_FILE="$ROOT_DIR/config/promotion_policy.csv"
BACKUP_DIR="$ROOT_DIR/config/backups"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

mkdir -p "$BACKUP_DIR"

if [ -f "$POLICY_FILE" ]; then
  cp "$POLICY_FILE" "$BACKUP_DIR/promotion_policy_${TIMESTAMP}.csv"
fi

cat > "$POLICY_FILE" <<'CSV'
key,value
auto_push_bands,high
manual_review_bands,medium
require_strict_evidence_for_auto_push,true
min_auto_push_evidence_quality,0.80
min_auto_push_relevance_score,0.65
CSV

echo "promotion_policy_rollback=ok policy_file=$POLICY_FILE backup_dir=$BACKUP_DIR"
