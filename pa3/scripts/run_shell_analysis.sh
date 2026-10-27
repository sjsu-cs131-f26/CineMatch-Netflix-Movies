#!/usr/bin/env bash
set -u
set -o pipefail

export LC_ALL=C

INPUT_TSV="${1:-work/analysis_ready.tsv}"
OUTPUT_TSV="${2:-out/shell_result.tsv}"

if [ ! -r "$INPUT_TSV" ] || [ ! -s "$INPUT_TSV" ]; then
  echo "[ERROR] Input '$INPUT_TSV' is missing or empty." >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_TSV")"

{
  printf "role_rank\trole_category\tcredit_count\tavg_billing_order\n"
  awk -F '\t' -v OFS='\t' '
  NR > 1 && ($2 + 0) <= 10 {
    cnt[$4]++
    sum[$4] += ($2 + 0)
  }
  END {
    for (cat in cnt) {
      printf "%s\t%d\t%.2f\n", cat, cnt[cat], (sum[cat] / cnt[cat])
    }
  }
  ' "$INPUT_TSV" \
  | sort -t $'\t' -k2,2nr -k1,1 \
  | awk -F '\t' -v OFS='\t' '{ print NR, $1, $2, $3 }'
} > "$OUTPUT_TSV"
