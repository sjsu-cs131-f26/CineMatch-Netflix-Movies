import sys
import time
from pyspark.sql import SparkSession, functions as F

if len(sys.argv) < 6:
    print(
        "Usage: task_b_billing_cutoff.py INPUT_PATH OUTPUT_ROOT "
        "BATCH_ID EXPECTED_TOTAL EXPECTED_TOP10"
    )
    sys.exit(1)

INPUT_PATH = sys.argv[1]
OUTPUT_ROOT = sys.argv[2].rstrip("/")
BATCH_ID = sys.argv[3]
EXPECTED_TOTAL = int(sys.argv[4])
EXPECTED_TOP10 = int(sys.argv[5])

spark = (
    SparkSession.builder
    .appName("CineMatch PA5 Task B Billing Cutoff")
    .getOrCreate()
)

# Read prepared Parquet and check schema before analysis
credits = spark.read.parquet(INPUT_PATH)
expected_cols = [
    "title_id",
    "billing_order",
    "person_id",
    "role_category",
    "is_top10",
]
assert credits.columns == expected_cols, (
    f"Unexpected columns: {credits.columns}"
)

# Timed analysis block: scan, group, sort, and collect small tables
analysis_start = time.time()

role_cutoff_df = (
    credits
    .groupBy("role_category")
    .agg(
        F.count("*").alias("total_credits"),
        F.sum(F.when(F.col("is_top10") == True, 1).otherwise(0)).alias(
            "top10_credits"
        ),
        F.sum(F.when(F.col("is_top10") == False, 1).otherwise(0)).alias(
            "beyond_top10_credits"
        ),
        F.sum("billing_order").alias("sum_billing_all"),
        F.sum(
            F.when(
                F.col("is_top10") == True, F.col("billing_order")
            ).otherwise(0)
        ).alias("sum_billing_top10"),
    )
    .orderBy(F.desc("beyond_top10_credits"), F.asc("role_category"))
)
role_cutoff_rows = role_cutoff_df.collect()

with_tier = (
    credits
    .withColumn(
        "billing_tier",
        F.when(F.col("billing_order") <= 3, "lead_1_to_3")
         .when(F.col("billing_order") <= 10, "supporting_4_to_10")
         .otherwise("beyond_top10_11_plus")
    )
    .withColumn(
        "tier_order",
        F.when(F.col("billing_order") <= 3, 1)
         .when(F.col("billing_order") <= 10, 2)
         .otherwise(3)
    )
)

role_tiers_df = (
    with_tier
    .groupBy("role_category", "tier_order", "billing_tier")
    .agg(
        F.count("*").alias("credit_count"),
        F.sum("billing_order").alias("sum_billing_order"),
    )
    .orderBy(F.asc("role_category"), F.asc("tier_order"))
)
role_tier_rows = role_tiers_df.collect()

analysis_seconds = round(time.time() - analysis_start, 3)

# Validate totals against direct counts and expected counts
total_rows = credits.count()
top10_rows = credits.filter(F.col("is_top10") == True).count()
beyond_top10_rows = credits.filter(F.col("is_top10") == False).count()

assert total_rows == top10_rows + beyond_top10_rows
if EXPECTED_TOTAL >= 0:
    assert total_rows == EXPECTED_TOTAL, (
        f"Expected {EXPECTED_TOTAL} total rows, got {total_rows}"
    )
if EXPECTED_TOP10 >= 0:
    assert top10_rows == EXPECTED_TOP10, (
        f"Expected {EXPECTED_TOP10} top-10 rows, got {top10_rows}"
    )

sum_role_total = 0
sum_role_top10 = 0
sum_role_beyond = 0
role_totals = {}
cutoff_out = []

for row in role_cutoff_rows:
    role = row["role_category"]
    tot = int(row["total_credits"])
    t10 = int(row["top10_credits"])
    bey = int(row["beyond_top10_credits"])
    s_all = int(row["sum_billing_all"])
    s_t10 = int(row["sum_billing_top10"])

    assert tot == t10 + bey, f"Row count mismatch for {role}"
    sum_role_total += tot
    sum_role_top10 += t10
    sum_role_beyond += bey
    role_totals[role] = tot

    top10_pct = f"{(100.0 * t10 / tot):.2f}" if tot > 0 else "0.00"
    excluded_pct = f"{(100.0 * bey / tot):.2f}" if tot > 0 else "0.00"
    share_excl_pct = (
        f"{(100.0 * bey / beyond_top10_rows):.2f}"
        if beyond_top10_rows > 0 else "0.00"
    )
    avg_all = f"{(s_all / tot):.2f}" if tot > 0 else "0.00"
    avg_t10 = f"{(s_t10 / t10):.2f}" if t10 > 0 else "0.00"

    cutoff_out.append((
        role,
        tot,
        t10,
        bey,
        top10_pct,
        excluded_pct,
        share_excl_pct,
        avg_all,
        avg_t10,
    ))

assert sum_role_total == total_rows
assert sum_role_top10 == top10_rows
assert sum_role_beyond == beyond_top10_rows

tier_sum_by_role = {}
sum_tier_total = 0
tier_out = []

for row in role_tier_rows:
    role = row["role_category"]
    t_ord = int(row["tier_order"])
    tier = row["billing_tier"]
    cnt = int(row["credit_count"])
    s_ord = int(row["sum_billing_order"])

    sum_tier_total += cnt
    tier_sum_by_role[role] = tier_sum_by_role.get(role, 0) + cnt
    pct_role = (
        f"{(100.0 * cnt / role_totals[role]):.2f}"
        if role_totals[role] > 0 else "0.00"
    )
    avg_ord = f"{(s_ord / cnt):.2f}" if cnt > 0 else "0.00"

    tier_out.append((role, t_ord, tier, cnt, pct_role, avg_ord))

assert sum_tier_total == total_rows
assert tier_sum_by_role == role_totals

overall_top10_pct = (
    f"{(100.0 * top10_rows / total_rows):.2f}" if total_rows > 0 else "0.00"
)
overall_excl_pct = (
    f"{(100.0 * beyond_top10_rows / total_rows):.2f}"
    if total_rows > 0 else "0.00"
)

checks_out = [
    ("batch_id", BATCH_ID, "Task B run identifier"),
    ("input_path", INPUT_PATH, "prepared Parquet input object"),
    ("total_rows", str(total_rows), "all prepared credit rows"),
    ("top10_rows", str(top10_rows), "rows with is_top10 == True"),
    ("beyond_top10_rows", str(beyond_top10_rows), "rows with is_top10 == False"),
    ("overall_top10_retention_pct", overall_top10_pct, "percent kept by top-10 filter"),
    ("overall_excluded_pct", overall_excl_pct, "percent dropped by top-10 filter"),
    ("distinct_roles", str(len(cutoff_out)), "distinct role_category labels"),
    ("role_cutoff_output_rows", str(len(cutoff_out)), "rows in role_cutoff_summary"),
    ("role_tier_output_rows", str(len(tier_out)), "rows in role_billing_tiers"),
    ("analysis_seconds", str(analysis_seconds), "scan + group + sort + collect seconds"),
    ("validation_passed", "True", "all count and partition assertions passed"),
]

# Save small results under a new Task B prefix using errorifexists mode
cutoff_df = spark.createDataFrame(
    cutoff_out,
    [
        "role_category",
        "total_credits",
        "top10_credits",
        "beyond_top10_credits",
        "top10_retention_pct",
        "excluded_by_top10_pct",
        "share_of_all_excluded_pct",
        "avg_billing_all",
        "avg_billing_top10",
    ],
)
tiers_df = spark.createDataFrame(
    tier_out,
    [
        "role_category",
        "tier_order",
        "billing_tier",
        "credit_count",
        "pct_of_role",
        "avg_billing_order",
    ],
)
checks_df = spark.createDataFrame(
    checks_out,
    ["check", "value", "note"],
)

(
    cutoff_df.coalesce(1)
    .write.mode("errorifexists")
    .option("header", True)
    .csv(f"{OUTPUT_ROOT}/role_cutoff_summary")
)
(
    tiers_df.coalesce(1)
    .write.mode("errorifexists")
    .option("header", True)
    .csv(f"{OUTPUT_ROOT}/role_billing_tiers")
)
(
    checks_df.coalesce(1)
    .write.mode("errorifexists")
    .option("header", True)
    .csv(f"{OUTPUT_ROOT}/task_b_checks")
)

print("=== TASK B VALIDATION PASSED ===")
for row in checks_out:
    print(row)
print("=== ROLE CUTOFF SUMMARY ===")
for row in cutoff_out:
    print(row)
print("=== ROLE BILLING TIERS ===")
for row in tier_out:
    print(row)

spark.stop()
