#!/usr/bin/env bash
# Task 9: build the release-era Parquet partitions and state/partition_manifest.tsv.
# Run once, after the full-data run has saved work/full_analysis_ready.parquet.
# Usage: bash pa3/scripts/build_partitions.sh [TITLE_BASICS_FILE]
set -u
set -o pipefail

export LC_ALL=C

DUCKDB="/mnt/scratch/CS131_jelenag/tools/duckdb-current/duckdb"
BASICS="${1:-/mnt/scratch/CS131_jelenag/projects/team06_sec02_fall2026/imdb_data/title.basics.tsv}"

PA3_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PA3_DIR" || exit 1

if [ ! -s work/full_analysis_ready.parquet ]; then
  echo "[ERROR] work/full_analysis_ready.parquet is missing. Run the full-data pipeline first." >&2
  exit 1
fi
if [ ! -r "$BASICS" ] || [ ! -s "$BASICS" ]; then
  echo "[ERROR] title.basics file '$BASICS' is missing or empty." >&2
  exit 1
fi

mkdir -p work/partitions state logs

# Step 1: title_id -> release era, from title.basics field 6 (startYear).
# Titles whose startYear is \N are skipped.
echo "[STEP 1] Assigning a release era to each title with awk..."
awk -F '\t' -v OFS='\t' '
NR == 1 { print "title_id", "era"; next }
$6 ~ /^[0-9]+$/ {
  if ($6 < 1980) era = "before_1980"
  else if ($6 < 2000) era = "1980_1999"
  else if ($6 < 2010) era = "2000_2009"
  else if ($6 < 2020) era = "2010_2019"
  else era = "2020_later"
  print $1, era
}
' "$BASICS" > work/title_era.tsv || exit 1

# Step 2: join the credits to their era and write one Parquet file per era.
echo "[STEP 2] Writing one Parquet partition per era with DuckDB..."
"$DUCKDB" work/partitions.duckdb < sql/build_partitions.sql > logs/build_partitions.log 2>&1 || {
  echo "[ERROR] DuckDB failed. See logs/build_partitions.log" >&2
  exit 1
}

# Step 3: manifest with the partition ID and absolute path of every partition.
echo "[STEP 3] Writing state/partition_manifest.tsv..."
printf "partition_id\tpartition_path\n" > state/partition_manifest.tsv
for f in "$PA3_DIR"/work/partitions/era_*.parquet; do
  id="$(echo "$f" | sed 's|.*/||; s|\.parquet$||')"
  printf "%s\t%s\n" "$id" "$f" >> state/partition_manifest.tsv
done

cat state/partition_manifest.tsv
echo "[INFO] Rows per era:"
cat logs/build_partitions.log
exit 0
