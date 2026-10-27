#!/usr/bin/env bash
# Task 9: called by cron once a day. Runs the next incomplete partition, then exits.
set -u
set -o pipefail

export LC_ALL=C
export PATH="/usr/local/bin:/usr/bin:/bin"
export DUCKDB="/mnt/scratch/CS131_jelenag/tools/duckdb-current/duckdb"

PA3_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PA3_DIR" || exit 1

MANIFEST="state/partition_manifest.tsv"
COMPLETED_DIR="state/completed"
CRON_LOG="logs/cron.log"

mkdir -p "$COMPLETED_DIR" logs out/partitions

now() {
  date '+%Y-%m-%d %H:%M:%S'
}

if [ ! -s "$MANIFEST" ]; then
  echo "$(now) [ERROR] Manifest $MANIFEST is missing or empty." >> "$CRON_LOG"
  exit 1
fi

# Pick the first partition in the manifest that has no success marker.
NEXT_ID=""
NEXT_PATH=""
while IFS=$'\t' read -r partition_id partition_path; do
  [ "$partition_id" = "partition_id" ] && continue
  [ -z "$partition_id" ] && continue
  if [ ! -e "$COMPLETED_DIR/${partition_id}.done" ]; then
    NEXT_ID="$partition_id"
    NEXT_PATH="$partition_path"
    break
  fi
done < "$MANIFEST"

if [ -z "$NEXT_ID" ]; then
  echo "$(now) [INFO] No work: every partition in $MANIFEST is already complete." >> "$CRON_LOG"
  exit 0
fi

START_TIME="$(now)"
bash scripts/run_partition.sh "$NEXT_PATH" > "logs/partition_${NEXT_ID}.out" 2> "logs/partition_${NEXT_ID}.err"
STATUS=$?
END_TIME="$(now)"

echo "$(now) partition_id=$NEXT_ID path=$NEXT_PATH start=\"$START_TIME\" end=\"$END_TIME\" exit_status=$STATUS" >> "$CRON_LOG"

if [ "$STATUS" -eq 0 ]; then
  echo "$NEXT_ID finished $END_TIME exit_status=0" > "$COMPLETED_DIR/${NEXT_ID}.done"
fi

exit "$STATUS"
