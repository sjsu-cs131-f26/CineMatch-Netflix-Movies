#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="cs131-11-5-2026-hadikhan"
BUCKET="cinematch-team6-pa5-fa26"
REGION="us-central1"
SERVICE_ACCOUNT="976390621265-compute@developer.gserviceaccount.com"
SUBNET="default"
BATCH_ID="${1:-pa5-task-b-20261125-01}"

INPUT="gs://$BUCKET/pa5/prepared_credits/part-00000-82a720be-34e6-49c5-9869-a4705a80fa9d-c000.snappy.parquet"
OUTPUT="gs://$BUCKET/pa5/task_b/$BATCH_ID"

PROPERTIES="dataproc.tier=standard,spark.dynamicAllocation.enabled=false"
PROPERTIES="$PROPERTIES,spark.executor.instances=2,spark.executor.cores=4"
PROPERTIES="$PROPERTIES,spark.driver.cores=4"
PROPERTIES="$PROPERTIES,spark.driver.memory=12g,spark.driver.memoryOverhead=4g"
PROPERTIES="$PROPERTIES,spark.executor.memory=12g,spark.executor.memoryOverhead=4g"
PROPERTIES="$PROPERTIES,spark.dataproc.driver.disk.size=250g"
PROPERTIES="$PROPERTIES,spark.dataproc.executor.disk.size=250g"
PROPERTIES="$PROPERTIES,spark.sql.files.maxPartitionBytes=8m,spark.task.cpus=1"
PROPERTIES="$PROPERTIES,spark.sql.adaptive.coalescePartitions.enabled=false"
PROPERTIES="$PROPERTIES,spark.sql.shuffle.partitions=32"

time gcloud dataproc batches submit pyspark task_b_billing_cutoff.py \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --batch="$BATCH_ID" \
  --version=2.3 \
  --ttl=10m \
  --service-account="$SERVICE_ACCOUNT" \
  --subnet="$SUBNET" \
  --deps-bucket="gs://$BUCKET" \
  --staging-bucket="$BUCKET" \
  --properties="$PROPERTIES" \
  -- "$INPUT" "$OUTPUT" "$BATCH_ID" 102980307 79193269
