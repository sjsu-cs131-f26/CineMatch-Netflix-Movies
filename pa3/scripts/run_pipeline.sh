#!/usr/bin/env bash
set -u
set -o pipefail

export LC_ALL=C

DUCKDB="/mnt/scratch/CS131_jelenag/tools/duckdb-current/duckdb"

if [ "$#" -lt 1 ]; then
  echo "[ERROR] Usage: bash pa3/scripts/run_pipeline.sh INPUT_FILE" >&2
  exit 1
fi

RAW_INPUT="$1"
if [ ! -r "$RAW_INPUT" ] || [ ! -s "$RAW_INPUT" ]; then
  echo "[ERROR] Input file '$RAW_INPUT' does not exist, is not readable, or is empty." >&2
  exit 1
fi

INPUT_ABS="$(cd "$(dirname "$RAW_INPUT")" && pwd)/$(basename "$RAW_INPUT")"
PA3_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PA3_DIR" || exit 1

WORK_DIR="work"
OUT_DIR="out"
LOG_DIR="logs"
ANALYSIS_READY_TSV="$WORK_DIR/analysis_ready.tsv"
ANALYSIS_READY_PARQUET="$WORK_DIR/analysis_ready.parquet"
DUCKDB_FILE="$WORK_DIR/project.duckdb"
SHELL_RESULT="$OUT_DIR/shell_result.tsv"
DUCKDB_TEXT_RESULT="$OUT_DIR/duckdb_text_result.tsv"
DUCKDB_PARQUET_RESULT="$OUT_DIR/duckdb_parquet_result.tsv"
FINAL_COMPARISON="$OUT_DIR/result_comparison.txt"
FINAL_ANALYSIS="$OUT_DIR/final_analysis.tsv"

create_directories() {
  mkdir -p "$WORK_DIR" "$OUT_DIR" "$LOG_DIR"
}

build_analysis_ready_stage() {
  echo "[STAGE 1] Creating $ANALYSIS_READY_TSV..."
  bash scripts/prepare_data.sh "$INPUT_ABS" "$ANALYSIS_READY_TSV" "$LOG_DIR/prepare_data.log"
}

run_shell_stage() {
  echo "[STAGE 2] Running shell analysis -> $SHELL_RESULT..."
  bash scripts/run_shell_analysis.sh "$ANALYSIS_READY_TSV" "$SHELL_RESULT"
}

run_duckdb_stages() {
  echo "[STAGE 3] Building Parquet and running DuckDB analyses..."
  rm -f "$DUCKDB_FILE" "${DUCKDB_FILE}.wal" "$ANALYSIS_READY_PARQUET"
  "$DUCKDB" "$DUCKDB_FILE" < sql/build_parquet.sql > "$LOG_DIR/build_parquet.log" 2>&1 || return 1
  "$DUCKDB" "$DUCKDB_FILE" < sql/analysis_text.sql > "$LOG_DIR/duckdb_text.log" 2>&1 || return 1
  "$DUCKDB" "$DUCKDB_FILE" < sql/analysis_parquet.sql > "$LOG_DIR/duckdb_parquet.log" 2>&1 || return 1
}

verify_equivalence() {
  echo "[STAGE 4] Verifying three-way result equivalence..."
  local status1=0
  local status2=0
  {
    echo "shell result versus DuckDB text result"
    diff -u "$SHELL_RESULT" "$DUCKDB_TEXT_RESULT" || status1=$?
    echo "DuckDB text result versus DuckDB Parquet result"
    diff -u "$DUCKDB_TEXT_RESULT" "$DUCKDB_PARQUET_RESULT" || status2=$?
  } > "$FINAL_COMPARISON"

  if [ "$status1" -ne 0 ] || [ "$status2" -ne 0 ]; then
    echo "[ERROR] Result equivalence check FAILED! See $FINAL_COMPARISON" >&2
    return 1
  fi
  echo "ALL THREE RESULTS AGREE: shell_result.tsv, duckdb_text_result.tsv, and duckdb_parquet_result.tsv are identical." >> "$FINAL_COMPARISON"
  echo "[OK] All three results agree."
}

build_final_analysis() {
  echo "[STAGE 5] Creating $FINAL_ANALYSIS (major categories with >= 10 credits)..."
  awk -F '\t' -v OFS='\t' 'NR == 1 || ($3 + 0) >= 10' "$DUCKDB_PARQUET_RESULT" > "$FINAL_ANALYSIS"
}

verify_outputs() {
  echo "[STAGE 6] Verifying all required output artifacts..."
  for f in "$ANALYSIS_READY_TSV" "$ANALYSIS_READY_PARQUET" "$SHELL_RESULT" "$DUCKDB_TEXT_RESULT" "$DUCKDB_PARQUET_RESULT" "$FINAL_COMPARISON" "$FINAL_ANALYSIS"; do
    if [ ! -s "$f" ]; then
      echo "[ERROR] Required artifact '$f' is missing or empty." >&2
      return 1
    fi
  done

  for tsv in "$SHELL_RESULT" "$DUCKDB_TEXT_RESULT" "$DUCKDB_PARQUET_RESULT" "$FINAL_ANALYSIS"; do
    awk -F '\t' '
      NR == 1 && (NF != 4 || $1 != "role_rank" || $2 != "role_category" || $3 != "credit_count" || $4 != "avg_billing_order") { exit 1 }
      NR > 1 && (NF != 4 || $1 !~ /^[0-9]+$/ || $3 !~ /^[0-9]+$/ || $4 !~ /^[0-9]+\.[0-9]{2}$/) { exit 1 }
      END { if (NR < 2) exit 1 }
    ' "$tsv" || {
      echo "[ERROR] Structural validation failed for '$tsv'." >&2
      return 1
    }
  done
  echo "[OK] Pipeline completed and all outputs verified."
}

create_directories || exit 1
build_analysis_ready_stage || exit 1
run_shell_stage || exit 1
run_duckdb_stages || exit 1
verify_equivalence || exit 1
build_final_analysis || exit 1
verify_outputs || exit 1
exit 0
