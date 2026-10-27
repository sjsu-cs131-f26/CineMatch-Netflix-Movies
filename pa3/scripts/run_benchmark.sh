#!/usr/bin/env bash
# Task 6: time the shell, DuckDB-text, and DuckDB-Parquet paths 3 times each.
# Usage: bash pa3/scripts/run_benchmark.sh [RAW_PRINCIPALS_FILE]
set -u
set -o pipefail

export LC_ALL=C

DUCKDB="/mnt/scratch/CS131_jelenag/tools/duckdb-current/duckdb"
RAW_INPUT="${1:-/mnt/scratch/CS131_jelenag/projects/team06_sec02_fall2026/imdb_data/title.principals.tsv}"

if [ ! -r "$RAW_INPUT" ] || [ ! -s "$RAW_INPUT" ]; then
  echo "[ERROR] Input file '$RAW_INPUT' is missing, unreadable, or empty." >&2
  exit 1
fi

PA3_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$PA3_DIR/logs"
BENCHMARK_TSV="$PA3_DIR/out/benchmark.tsv"

# The benchmark uses its own folder so the committed sample outputs in pa3/out
# are not overwritten. The SQL files use relative paths (work/..., out/...),
# so running DuckDB from inside BENCH_DIR reads and writes the files here.
BENCH_DIR="$PA3_DIR/work/bench"
mkdir -p "$BENCH_DIR/work" "$BENCH_DIR/out" "$LOG_DIR"
rm -f "$BENCH_DIR/work/project.duckdb" "$BENCH_DIR/work/analysis_ready.parquet"
rm -f "$LOG_DIR/shell_timing.txt" "$LOG_DIR/duckdb_text_timing.txt" "$LOG_DIR/duckdb_parquet_timing.txt"

# Step 1: header + first 1,000,000 records, cleaned by the same prepare_data.sh.
echo "[STEP 1] Building the 1,000,000-record benchmark input..."
head -n 1000001 "$RAW_INPUT" > "$BENCH_DIR/work/raw_benchmark.tsv"
bash "$PA3_DIR/scripts/prepare_data.sh" "$BENCH_DIR/work/raw_benchmark.tsv" \
  "$BENCH_DIR/work/analysis_ready.tsv" "$LOG_DIR/benchmark_prepare.log" || exit 1

cd "$BENCH_DIR" || exit 1

# Step 2: one-time Parquet conversion, timed on its own.
echo "[STEP 2] Converting the same records to Parquet..."
{ time "$DUCKDB" work/project.duckdb < "$PA3_DIR/sql/build_parquet.sql" > /dev/null; } \
  2> "$LOG_DIR/parquet_conversion_timing.txt" || exit 1

# Step 3: run each path once and check the results match before timing anything.
echo "[STEP 3] Checking that all three paths give the same result..."
bash "$PA3_DIR/scripts/run_shell_analysis.sh" work/analysis_ready.tsv out/shell_result.tsv || exit 1
"$DUCKDB" work/project.duckdb < "$PA3_DIR/sql/analysis_text.sql" > /dev/null || exit 1
"$DUCKDB" work/project.duckdb < "$PA3_DIR/sql/analysis_parquet.sql" > /dev/null || exit 1

if diff out/shell_result.tsv out/duckdb_text_result.tsv > /dev/null &&
   diff out/duckdb_text_result.tsv out/duckdb_parquet_result.tsv > /dev/null; then
  VERIFIED="yes"
  echo "[OK] All three benchmark results agree."
else
  echo "[ERROR] Benchmark results differ. Not timing a wrong answer." >&2
  exit 1
fi

ROWS="$(tail -n +2 work/analysis_ready.tsv | wc -l)"
TEXT_BYTES="$(wc -c < work/analysis_ready.tsv)"
PARQUET_BYTES="$(wc -c < work/analysis_ready.parquet)"

# Turns the last "real 0m1.234s" line of a timing log into seconds (1.234).
real_seconds() {
  grep '^real' "$1" | tail -n 1 | awk '{ split($2, t, "m"); sub("s", "", t[2]); printf "%.3f\n", t[1] * 60 + t[2] }'
}

# Step 4: nine timed runs.
echo "[STEP 4] Timing each path 3 times..."
printf "implementation\tstorage_format\trun\tinput_rows\tinput_bytes\treal_seconds\tresult_verified\n" > "$BENCHMARK_TSV"

for run in 1 2 3; do
  { time bash "$PA3_DIR/scripts/run_shell_analysis.sh" work/analysis_ready.tsv out/shell_result.tsv; } \
    2>> "$LOG_DIR/shell_timing.txt" || exit 1
  printf "shell\ttext\t%s\t%s\t%s\t%s\t%s\n" "$run" "$ROWS" "$TEXT_BYTES" \
    "$(real_seconds "$LOG_DIR/shell_timing.txt")" "$VERIFIED" >> "$BENCHMARK_TSV"
done

for run in 1 2 3; do
  { time "$DUCKDB" work/project.duckdb < "$PA3_DIR/sql/analysis_text.sql" > /dev/null; } \
    2>> "$LOG_DIR/duckdb_text_timing.txt" || exit 1
  printf "duckdb\ttext\t%s\t%s\t%s\t%s\t%s\n" "$run" "$ROWS" "$TEXT_BYTES" \
    "$(real_seconds "$LOG_DIR/duckdb_text_timing.txt")" "$VERIFIED" >> "$BENCHMARK_TSV"
done

for run in 1 2 3; do
  { time "$DUCKDB" work/project.duckdb < "$PA3_DIR/sql/analysis_parquet.sql" > /dev/null; } \
    2>> "$LOG_DIR/duckdb_parquet_timing.txt" || exit 1
  printf "duckdb\tparquet\t%s\t%s\t%s\t%s\t%s\n" "$run" "$ROWS" "$PARQUET_BYTES" \
    "$(real_seconds "$LOG_DIR/duckdb_parquet_timing.txt")" "$VERIFIED" >> "$BENCHMARK_TSV"
done

# File sizes and conversion time for the report.
{
  echo "benchmark source: first 1000000 records of $RAW_INPUT"
  echo "analysis-ready rows: $ROWS"
  echo "text bytes: $TEXT_BYTES"
  echo "parquet bytes: $PARQUET_BYTES"
  echo "parquet conversion real seconds: $(real_seconds "$LOG_DIR/parquet_conversion_timing.txt")"
} > "$LOG_DIR/benchmark_sizes.txt"

echo "[OK] Wrote $BENCHMARK_TSV"
cat "$BENCHMARK_TSV"
cat "$LOG_DIR/benchmark_sizes.txt"
exit 0
