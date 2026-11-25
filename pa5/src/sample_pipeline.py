from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("CineMatch PA5 Full Preparation")
    .getOrCreate()
)

INPUT_PATH = "gs://cinematch-team6-pa5-fa26/pa5/sample/principals_sample.tsv"


LOOKUP_PATH = "gs://cinematch-team6-pa5-fa26/pa5/sample/title_basics_lookup.tsv"

PARQUET_PATH = (
    "gs://cinematch-team6-pa5-fa26/"
    "pa5/prepared_credits"
)

schema = (
    "tconst STRING, ordering INT, nconst STRING, "
    "category STRING, job STRING, characters STRING"
)

raw = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .schema(schema)
    .csv(INPUT_PATH)
)

rows_before_cleaning = raw.count()

credits = raw.select(
    F.col("tconst").alias("title_id"),
    F.col("ordering").alias("billing_order"),
    F.col("nconst").alias("person_id"),
    F.col("category").alias("role_category")
)

invalid = credits.filter(
    F.col("title_id").isNull()
    | F.col("person_id").isNull()
    | F.col("billing_order").isNull()
    | (F.col("billing_order") < 1)
    | F.col("role_category").isNull()
    | (F.col("role_category") == "\\N")
)

excluded_rows = invalid.count()

valid = credits.filter(
    F.col("title_id").isNotNull()
    & F.col("person_id").isNotNull()
    & F.col("billing_order").isNotNull()
    & (F.col("billing_order") >= 1)
    & F.col("role_category").isNotNull()
    & (F.col("role_category") != "\\N")
)

rows_after_cleaning = valid.count()

repeated_keys = (
    valid.groupBy("title_id", "billing_order")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

prepared = valid.withColumn(
    "is_top10",
    F.col("billing_order") <= 10
)

prepared.write.mode("overwrite").parquet(PARQUET_PATH)

restored = spark.read.parquet(PARQUET_PATH)

print("Rows before cleaning:", rows_before_cleaning)
print("Rows excluded:", excluded_rows)
print("Rows after cleaning:", rows_after_cleaning)
print("Repeated (title_id, billing_order) keys:", repeated_keys)
print("Prepared rows:", prepared.count())
print("Parquet read-back rows:", restored.count())
print("Parquet read-back top-10 rows:",
      restored.filter(F.col("is_top10")).count())

print("Prepared schema:")
prepared.printSchema()

print(
    "The excluded-row count is the union of all invalid conditions. "
    "Individual quality-condition counts may overlap and should not be added."
)

lookup_schema = (
    "tconst STRING, title_type STRING, primary_title STRING, "
    "original_title STRING, is_adult INT, start_year STRING, "
    "end_year STRING, runtime_minutes STRING, genres STRING"
)

lookup = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .schema(lookup_schema)
    .csv(LOOKUP_PATH)
)

top10_df = prepared.filter(F.col("is_top10") == True)

analysis1 = (
    top10_df
    .groupBy("role_category")
    .agg(
        F.count("*").alias("credit_count"),
        F.format_string(
            "%.2f", F.avg("billing_order")
        ).alias("avg_billing_order")
    )
    .withColumn(
        "role_rank",
        F.row_number().over(
            Window.orderBy(
                F.desc("credit_count"),
                F.asc("role_category")
            )
        )
    )
    .select(
        "role_rank",
        "role_category",
        "credit_count",
        "avg_billing_order"
    )
    .orderBy("role_rank")
)

top10_with_year = (
    top10_df
    .join(
        lookup,
        top10_df.title_id == lookup.tconst,
        "inner"
    )
    .select(
        top10_df["*"],
        lookup["start_year"].cast("int").alias("start_year")
    )
)

top10_with_era = (
    top10_with_year
    .withColumn(
        "release_era",
        F.when(F.col("start_year").isNull(), "unknown")
         .when(F.col("start_year") < 1980, "before_1980")
         .when(F.col("start_year") <= 1999, "1980_1999")
         .when(F.col("start_year") <= 2009, "2000_2009")
         .when(F.col("start_year") <= 2019, "2010_2019")
         .otherwise("2020_or_later")
    )
    .withColumn(
        "era_order",
        F.when(F.col("release_era") == "before_1980", 1)
         .when(F.col("release_era") == "1980_1999", 2)
         .when(F.col("release_era") == "2000_2009", 3)
         .when(F.col("release_era") == "2010_2019", 4)
         .when(F.col("release_era") == "2020_or_later", 5)
         .otherwise(6)
    )
)

analysis2 = (
    top10_with_era
    .groupBy("era_order", "release_era", "role_category")
    .agg(
        F.count("*").alias("credit_count"),
        F.format_string(
            "%.2f", F.avg("billing_order")
        ).alias("avg_billing_order")
    )
    .orderBy(
        F.asc("era_order"),
        F.desc("credit_count"),
        F.asc("role_category")
    )
    .select(
        "release_era",
        "role_category",
        "credit_count",
        "avg_billing_order"
    )
)

duplicate_lookup_keys = (
    lookup
    .groupBy("tconst")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

unmatched_top10 = (
    top10_df
    .join(
        lookup,
        top10_df.title_id == lookup.tconst,
        "left_anti"
    )
    .count()
)

top10_rows_before_join = top10_df.count()
rows_after_join = top10_with_era.count()

analysis1.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(
    "gs://cinematch-team6-pa5-fa26/pa5/sample/analysis_1"
)

analysis2.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(
    "gs://cinematch-team6-pa5-fa26/pa5/sample/analysis_2"
)

checks_rows = [
    ["rows before cleaning", rows_before_cleaning,
     "full title.principals input rows"],
    ["rows excluded", excluded_rows,
     "union of invalid required-value conditions"],
    ["rows after cleaning", rows_after_cleaning,
     "rows retained after the PA4 cleaning rules"],
    ["repeated (title_id, billing_order) keys", repeated_keys,
     "checked but not removed"],
    ["prepared rows", prepared.count(),
     "should equal rows after cleaning"],
    ["Parquet read-back rows", restored.count(),
     "should equal prepared rows"],
    ["Parquet read-back top-10 rows",
     restored.filter(F.col("is_top10")).count(),
     "derived-column check"],
    ["prepared schema", str(prepared.dtypes),
     "full prepared-data schema"]
]

checks_rows.extend([
    ["duplicate lookup keys", duplicate_lookup_keys,
     "lookup key is tconst; duplicate keys checked"],
    ["top-10 rows before join", top10_rows_before_join,
     "top-10 prepared credits"],
    ["unmatched top-10 rows", unmatched_top10,
     "top-10 credits without a matching basics record"],
    ["rows after join", rows_after_join,
     "rows retained by the inner join"],
    ["analysis 1 output rows", analysis1.count(),
     "complete Analysis 1 result"],
    ["analysis 2 output rows", analysis2.count(),
     "complete Analysis 2 result"]
])

checks_df = spark.createDataFrame(
    [(str(check), str(value), str(note))
     for check, value, note in checks_rows],
    ["check", "value", "note"]
)

checks_df.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(
    "gs://cinematch-team6-pa5-fa26/pa5/sample/quality_checks_full"
)

spark.stop()
