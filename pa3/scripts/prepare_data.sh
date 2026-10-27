#!/usr/bin/env bash

set -u
set -o pipefail
export LC_ALL=C

if [ "$#" -lt 2 ]; then
  echo "Usage: bash scripts/prepare_data.sh INPUT_FILE OUTPUT_FILE [LOG_FILE]" >&2
  exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"
LOG_FILE="${3:-logs/prepare_data.log}"

mkdir -p "$(dirname "$OUTPUT_FILE")" "$(dirname "$LOG_FILE")"

if [ ! -r "$INPUT_FILE" ] || [ ! -s "$INPUT_FILE" ]; then
  echo "[ERROR] Input file '$INPUT_FILE' is missing, unreadable, or empty." | tee -a "$LOG_FILE" >&2
  exit 1
fi

echo "[INFO] Starting prepare_data.sh on '$INPUT_FILE'" > "$LOG_FILE"

# Stage 1: awk field selection & validation + sed header/label normalization
awk -F '\t' -v OFS='\t' '
NR == 1 {
  if (NF != 6 || $1 != "tconst" || $2 != "ordering" || $3 != "nconst" || $4 != "category") {
    print "[ERROR] Invalid header in input file. Expected 6 IMDb title.principals columns." > "/dev/stderr"
    exit 2
  }
  print $1, $2, $3, $4
  next
}
{
  total_rows++
  if (NF == 6 && $1 ~ /^tt[0-9]+$/ && $2 ~ /^[0-9]+$/ && ($2 + 0) >= 1 && $3 ~ /^nm[0-9]+$/ && $4 != "" && $4 != "\\N") {
    valid_rows++
    print $1, $2, $3, $4
  } else {
    invalid_rows++
  }
}
END {
  printf "[INFO] Raw data rows read: %d | Valid rows kept: %d | Invalid rows dropped: %d\n", total_rows, valid_rows, invalid_rows > "/dev/stderr"
  if (valid_rows == 0) {
    print "[ERROR] Zero valid data rows produced after awk validation." > "/dev/stderr"
    exit 3
  }
}
' "$INPUT_FILE" 2>> "$LOG_FILE" \
| sed -e 's/\r$//' \
      -e '1s/^tconst\tordering\tnconst\tcategory$/title_id\tbilling_order\tperson_id\trole_category/' \
      -e '2,$s/\tarchive_footage$/\tarchive-footage/' \
      -e '2,$s/\tarchive_sound$/\tarchive-sound/' \
      -e '2,$s/\tproduction_designer$/\tproduction-designer/' \
> "$OUTPUT_FILE"

if [ "$?" -ne 0 ]; then
  echo "[ERROR] Pipeline failed during awk/sed extraction stage." | tee -a "$LOG_FILE" >&2
  exit 1
fi

# Stage 2: Verify expected field count (4) and dataset-specific invariant on work/analysis_ready.tsv
awk -F '\t' '
NR == 1 {
  if (NF != 4 || $1 != "title_id" || $2 != "billing_order" || $3 != "person_id" || $4 != "role_category") {
    print "[ERROR] Verification failed: normalized header mismatch." > "/dev/stderr"
    exit 1
  }
  next
}
{
  if (NF != 4) {
    printf "[ERROR] Verification failed at line %d: expected 4 fields, found %d\n", NR, NF > "/dev/stderr"
    exit 1
  }
  if ($1 !~ /^tt[0-9]+$/ || $2 !~ /^[0-9]+$/ || ($2 + 0) < 1 || $3 !~ /^nm[0-9]+$/ || $4 == "" || $4 == "\\N") {
    printf "[ERROR] Verification failed at line %d: invariant violated.\n", NR > "/dev/stderr"
    exit 1
  }
  verified_rows++
}
END {
  if (verified_rows == 0) {
    print "[ERROR] Verification failed: output contains no data rows." > "/dev/stderr"
    exit 1
  }
  printf "[INFO] Verification succeeded: %d analysis-ready rows verified with 4 tab-separated fields.\n", verified_rows > "/dev/stderr"
}
' "$OUTPUT_FILE" 2>> "$LOG_FILE"

if [ "$?" -ne 0 ]; then
  echo "[ERROR] Output verification failed on '$OUTPUT_FILE'." | tee -a "$LOG_FILE" >&2
  exit 1
fi

chmod 644 "$OUTPUT_FILE"
exit 0
