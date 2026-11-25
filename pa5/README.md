# PA5 Final Sprint

## Full Dataset Inventory

### Source

The data comes from the official IMDb Non-Commercial Datasets:
https://datasets.imdbws.com/

### Files used
| File | Description | Cloud Storage path |
|---|---|---|
| `title.basics.tsv.gz` | IMDb title information, including title type and release year | `gs://cinematch-team6-pa5-fa26/imdb/title.basics.tsv.gz` |
| `title.principals.tsv.gz` | IMDb cast/crew credits, role categories, and billing order | `gs://cinematch-team6-pa5-fa26/imdb/title.principals.tsv.gz` |

### Input sizes and row counts before cleaning

| File | Compressed bytes in Cloud Storage | Rows before cleaning |
|---|---:|---:|
| `title.basics.tsv.gz` | 230,969,188 | 12,999,649 |
| `title.principals.tsv.gz` | 792,191,292 | 102,980,307 |
| **Total** | **1,023,160,480** | **115,979,956** |

The files are compressed TSV files. Spark can read the `.tsv.gz` files directly from Cloud Storage without manually uncompressing them.

### Full-data cleaning and verification

The full `title.principals.tsv.gz` input contained 102,980,307 rows. The PA4 cleaning rules excluded 0 rows, leaving 102,980,307 prepared rows. No repeated `(title_id, billing_order)` keys were removed.

The prepared Parquet read-back also contained 102,980,307 rows, including 79,193,269 top-10 credits.

The title-basics lookup had 0 duplicate `tconst` keys. Of the 79,193,269 top-10 credits, 2,512 had no matching title-basics record. The inner join therefore retained 79,190,757 rows for the era analysis. These unmatched
lookup records are reported separately from cleaning exclusions.

## Dataproc Execution

Region: us-central1  
Batch ID: pa5-prep-full-04 
Pipeline file: `pa5/src/full_pipeline.py`  
Input storage: Google Cloud Storage  
Prepared output: `gs://cinematch-team6-pa5-fa26/pa5/prepared_credits`

The pipeline reads compressed TSV files from Cloud Storage, applies the existing PA4 cleaning rules, creates the `is_top10` column, joins credits to title basics using `title_id = tconst`, runs both analyses, and saves the prepared Parquet data and small analysis tables back to Cloud Storage.

## Pipeline Design Choices

The raw IMDb inputs are compressed TSV files because that is the format provided by IMDb. The cleaned credit table is saved as Parquet because Parquet is a columnar format that is more appropriate for repeated Spark analysis.

The credit-to-title join uses `title_id = tconst`. The title-basics lookup is used to add release-year information to each credit. The lookup key is checked for duplicates, and unmatched credit records are counted.

Shuffles can occur during the grouped aggregations by role and release era, during the join, and during the window operation used to order Analysis 1 results. `coalesce(1)` is used only when saving the small analysis and quality-check tables so they are easier to inspect; it is not used on the large prepared dataset.

### Cloud Credit Check

Before running the full benchmark, the project had a $50.00 Google Cloud credit balance with 100% remaining. The credit was listed as available under “CS131 - Processing Big Data: Tools and Techniques.”

## Full-Data Benchmark Plan

The benchmark compares two Spark implementations of the PA3/PA4 Analysis 1 question. Both approaches use the same full IMDb input files, cleaning rules, top-10 billing filter, grouping, ordering, and numeric precision.

Approach 1 uses the default Spark configuration. Approach 2 changes only the Spark shuffle-partition setting. The timer will include input reading through computation and collection of the complete small result, but will exclude setup, Spark startup, output-file writing, and charting.

Three runs will be recorded for each approach, for six total measurements.
