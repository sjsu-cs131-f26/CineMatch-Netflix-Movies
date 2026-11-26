import csv
import sys
from time import perf_counter

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


input_path, expected_csv, output_path, job_id = sys.argv[1:5]
expected_rows, expected_top10, input_bytes = [int(x) for x in sys.argv[5:8]]
spark = SparkSession.builder.appName("CineMatch Task A Benchmark").getOrCreate()
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

with open(expected_csv) as file:
    expected = list(csv.DictReader(file))

print("Spark version:", spark.version)
print("Application ID:", spark.sparkContext.applicationId)
print("Input:", input_path, "Bytes:", input_bytes)
print("Session run order: 200, 32, 32, 200, 200, 32; no explicit caching")

data = spark.read.parquet(input_path)
input_rows = data.count()
top10_rows = data.filter(F.col("is_top10") == True).count()
assert input_rows == expected_rows
assert top10_rows == expected_top10
assert data.dtypes == [
    ("title_id", "string"), ("billing_order", "int"),
    ("person_id", "string"), ("role_category", "string"), ("is_top10", "boolean")]
print("INPUT_CHECKS:", input_rows, top10_rows, data.dtypes)


def analysis_1():
    prepared = spark.read.parquet(input_path)
    return (prepared.filter(F.col("is_top10") == True)
        .groupBy("role_category")
        .agg(F.count("*").alias("credit_count"),
             F.format_string("%.2f", F.avg("billing_order")).alias("avg_billing_order"))
        .withColumn("role_rank", F.row_number().over(Window.orderBy(
            F.desc("credit_count"), F.asc("role_category"))))
        .select("role_rank", "role_category", "credit_count", "avg_billing_order")
        .orderBy("role_rank"))


benchmark = []
comparisons = []
saved_results = []
runs = {200: 0, 32: 0}
for partitions in [200, 32, 32, 200, 200, 32]:
    runs[partitions] = runs[partitions] + 1
    run = runs[partitions]
    approach = "shuffle_" + str(partitions)
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    spark.sparkContext.setJobGroup(approach + "_run_" + str(run), "Task A Analysis 1")

    start = perf_counter()
    result = analysis_1()
    rows = result.collect()
    elapsed = perf_counter() - start

    differences = abs(len(rows) - len(expected))
    for i in range(min(len(rows), len(expected))):
        actual, reference = rows[i], expected[i]
        if (actual["role_rank"] != int(reference["role_rank"])
            or actual["role_category"] != reference["role_category"]
            or actual["credit_count"] != int(reference["credit_count"])
            or abs(float(actual["avg_billing_order"])
                   - float(reference["avg_billing_order"])) > 0.000000001):
            differences = differences + 1
    verified = differences == 0
    benchmark.append([approach, run, input_rows, input_bytes, elapsed,
                      len(rows), verified, job_id])
    comparisons.append([approach, run, len(expected), len(rows), differences, verified])
    saved_results.append(rows)
    print("MEASUREMENT:", benchmark[-1], "differing_rows:", differences, flush=True)
    if run == 1:
        result.explain("formatted")

for approach in ["shuffle_200", "shuffle_32"]:
    times = sorted([row[4] for row in benchmark if row[0] == approach])
    print("TIMES:", approach, times, "median:", times[1], "range:", times[2] - times[0])

benchmark_header = ["approach", "run", "input_rows", "input_bytes", "elapsed_seconds",
                    "output_rows", "result_verified", "job_id"]
comparison_header = ["approach", "run", "expected_rows", "output_rows", "differing_rows",
                     "result_verified"]
for name, records, header in [
    ("benchmark_full", benchmark, benchmark_header),
    ("result_comparison", comparisons, comparison_header)
]:
    text_rows = [tuple(str(value) for value in row) for row in records]
    spark.createDataFrame(text_rows, header).coalesce(1).write.option("header", True).csv(
        output_path + "/" + name)

for i in range(len(saved_results)):
    spark.createDataFrame(saved_results[i]).coalesce(1).write.option("header", True).csv(
        output_path + "/result_" + str(i + 1))

assert all(row[6] for row in benchmark), "A timed result differed from the reference"
spark.stop()
