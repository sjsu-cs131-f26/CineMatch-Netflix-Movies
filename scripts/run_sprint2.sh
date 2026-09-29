#!/bin/bash
# CineMatch Sprint 2: run from the repo root with: bash scripts/run_sprint2.sh
# Dataset: IMDb title.principals.tsv, tab-separated, 1 header row, missing values are \N
# Fields: 1 tconst, 2 ordering, 3 nconst, 4 category, 5 job, 6 characters
# Results go to out/*.txt, timing and errors go to out/time_*.txt

DATASET="/mnt/scratch/CS131_jelenag/projects/team06_sec02_fall2026/imdb_data/title.principals.tsv"
export LC_ALL=C

mkdir -p out data/samples

echo "Sample: header + first 1000 rows"
head -n 1001 "$DATASET" > data/samples/sample_1000.tsv

echo "Profile: size and row count"
echo 'Command: du -b $DATASET' > out/profile.txt
du -b "$DATASET" >> out/profile.txt
echo 'Command: wc -l $DATASET (includes 1 header line)' >> out/profile.txt
{ time wc -l "$DATASET" >> out/profile.txt ; } 2> out/time_rowcount.txt

echo "Frequency table: category (field 4)"
{ time tail -n +2 "$DATASET" | cut -f4 | sort | uniq -c | sort -nr > out/freq_category.txt ; } 2> out/time_freq_category.txt

echo "Frequency table: job (field 5)"
{ time tail -n +2 "$DATASET" | cut -f5 | grep -v -x '\\N' | sort | uniq -c | sort -nr > out/freq_job.txt ; } 2> out/time_freq_job.txt

echo "Top 10 people by credits (field 3)"
{ time tail -n +2 "$DATASET" | cut -f3 | sort | uniq -c | sort -nr | head -n 10 | tee out/top10_nconst.txt ; } 2> out/time_top10_nconst.txt

echo "Filter: credits with missing job (field 5 is \N)"
{ time tail -n +2 "$DATASET" | cut -f5 | grep -E -x '\\N' | wc -l | tee out/filter_missing_job.txt ; } 2> out/time_filter_missing_job.txt

echo "Skinny table: unique category and job pairs (fields 4,5)"
{ time tail -n +2 "$DATASET" | cut -f4,5 | sort -u > out/skinny_unique.tsv ; } 2> out/time_skinny_unique.txt

echo "Done"
