#!/bin/bash
# Dataset path and assumptions: 
# Using the IMDb title.principals.tsv dataset. 
DATASET="/mnt/scratch/CS131_jelenag/projects/team06_sec02_fall2026/imdb_data/title.principals.tsv"

echo "1. Generating two frequency tables (Categories and Jobs)..."
time tail -n +2 "$DATASET" | cut -f4 | sort | uniq -c | sort -nr > out/freq_category.txt
time tail -n +2 "$DATASET" | cut -f5 | sort | uniq -c | sort -nr > out/freq_job.txt

echo "2. Generating Top-10 entity list..."
time tail -n +2 "$DATASET" | cut -f3 | sort | uniq -c | sort -nr | head -n 10 | tee out/top_10_entities.txt

echo "3. Filtering dataset for 'stunt' and counting..."
time tail -n +2 "$DATASET" | grep -iE "stunt" | wc -l | tee out/filter_count.txt

echo "4. Generating deduplicated skinny table..."
time tail -n +2 "$DATASET" | cut -f1,3 | sort -u > out/skinny_table.txt

echo "5. Profiling dataset size and row count..."
ls -lh "$DATASET" | tee out/profile.txt
time wc -l "$DATASET" >> out/profile.txt


