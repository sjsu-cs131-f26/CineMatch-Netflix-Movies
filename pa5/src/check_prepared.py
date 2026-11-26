import csv
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


input_path, expected_csv, output_csv = sys.argv[1:4]
expected_rows, expected_top10 = [int(x) for x in sys.argv[4:6]]
spark = SparkSession.builder.appName("CineMatch prepared-data check").getOrCreate()
prepared = spark.read.parquet(input_path)
rows = prepared.count()
top10 = prepared.filter(F.col("is_top10") == True)
top10_rows = top10.count()
schema_matches = prepared.dtypes == [
    ("title_id", "string"), ("billing_order", "int"),
    ("person_id", "string"), ("role_category", "string"), ("is_top10", "boolean")]
wrong_flags = prepared.filter(
    F.col("is_top10").isNull()
    | (F.col("is_top10") != (F.col("billing_order") <= 10))).count()

result = (top10.groupBy("role_category")
    .agg(F.count("*").alias("credit_count"),
         F.format_string("%.2f", F.avg("billing_order")).alias("avg_billing_order"))
    .withColumn("role_rank", F.row_number().over(Window.orderBy(
        F.desc("credit_count"), F.asc("role_category"))))
    .select("role_rank", "role_category", "credit_count", "avg_billing_order")
    .orderBy("role_rank").collect())
with open(expected_csv) as file:
    expected = list(csv.DictReader(file))
differences = abs(len(result) - len(expected))
for i in range(min(len(result), len(expected))):
    actual, reference = result[i], expected[i]
    if (actual[0] != int(reference["role_rank"])
        or actual[1] != reference["role_category"]
        or actual[2] != int(reference["credit_count"])
        or abs(float(actual[3]) - float(reference["avg_billing_order"])) > 0.000000001):
        differences = differences + 1

checks = [
    ["prepared rows", rows, expected_rows],
    ["top-10 rows", top10_rows, expected_top10],
    ["schema matches", schema_matches, True],
    ["incorrect top-10 flags", wrong_flags, 0],
    ["Analysis 1 output rows", len(result), len(expected)],
    ["Analysis 1 differing rows", differences, 0]]
with open(output_csv, "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["check", "actual", "expected"])
    writer.writerows(checks)
print("Spark version:", spark.version, "Master:", spark.sparkContext.master)
print("Input:", input_path)
for check in checks:
    print(check)
assert all(check[1] == check[2] for check in checks)
spark.stop()
