#!/usr/bin/env bash
# Task 9: run the role-category analysis on exactly one Parquet partition.
# Usage: bash pa3/scripts/run_partition.sh PARTITION_PATH
set -u
set -o pipefail

export LC_ALL=C

DUCKDB="${DUCKDB:-/mnt/scratch/CS131_jelenag/tools/duckdb-current/duckdb}"

if [ "$#" -ne 1 ]; then
  echo "[ERROR] Usage: bash pa3/scripts/run_partition.sh PARTITION_PATH" >&2
  exit 1
fi

PARTITION_PATH="$1"
if [ ! -r "$PARTITION_PATH" ] || [ ! -s "$PARTITION_PATH" ]; then
  echo "[ERROR] Partition '$PARTITION_PATH' is missing or empty." >&2
  exit 1
fi

PA3_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PARTITION_ID="$(echo "$PARTITION_PATH" | sed 's|.*/||; s|\.parquet$||')"
RESULT_TSV="$PA3_DIR/out/partitions/${PARTITION_ID}_role_summary.tsv"

mkdir -p "$PA3_DIR/out/partitions"

echo "[INFO] Processing partition $PARTITION_ID from $PARTITION_PATH"

# Fill in the placeholders in the SQL file, then send it to DuckDB.
sed -e "s|PARTITION_PATH|$PARTITION_PATH|" \
    -e "s|PARTITION_ID|$PARTITION_ID|" \
    -e "s|RESULT_PATH|$RESULT_TSV|" \
    "$PA3_DIR/sql/analysis_partition.sql" | "$DUCKDB"

if [ "$?" -ne 0 ]; then
  echo "[ERROR] DuckDB failed on partition $PARTITION_ID" >&2
  exit 1
fi

# Header plus at least one data row with 5 fields.
if ! awk -F '\t' 'NF != 5 { bad = 1 } END { exit (bad || NR < 2) }' "$RESULT_TSV"; then
  echo "[ERROR] Result $RESULT_TSV is empty or malformed." >&2
  exit 1
fi

echo "[OK] Wrote $RESULT_TSV"
exit 0
