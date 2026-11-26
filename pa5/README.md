# PA5 - Task A: Full-data cloud execution and benchmark

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

The earlier $50.00 balance predates cloud runs and is not a current balance. On November 25, Aneesh confirmed that sufficient credits remain; no exact current balance was supplied or independently verified. The completed benchmark used the Class 24 fixed resources and a ten-minute TTL. These bound the resources and lifetime, but are not a dollar spending cap.

## Task A status (November 25, 2026)

The existing preparation batch `pa5-prep-full-04` succeeded. Its final configuration and usage are in `evidence/pa5-prep-full-04-final.yaml`; it ran from 04:01:03.385898Z to 04:49:53.777392Z (48 minutes 50.391 seconds, including provisioning). Its resolved runtime was 2.2.91. The earlier `*-config.yaml` is a RUNNING snapshot, not the final state. Existing Analysis 1, Analysis 2, and quality-check outputs have not been overwritten or rerun.

The recovered full object passed a new local Spark validation in Cloud Shell: 102,980,307 rows, 79,193,269 top-10 rows, matching schema, zero incorrect top-10 flags, and all 13 Analysis 1 rows matching the saved full reference. See `evidence/recovered_full_checks.csv` and `evidence/recovered-full-check.txt`. The downloaded file MD5 matched the live object metadata (`evidence/full-input-md5.txt`). This validation is not a timed benchmark.

The new benchmark script passed a small local Spark test in Cloud Shell: 1,000 prepared rows, 969 top-10 rows, and all 10 output roles matched `pa4/out/analysis_1.csv` on every run. See `evidence/sample_result_comparison.csv` and the original Spark log `evidence/sample-test.txt`. These are correctness checks on the sample, not full-data timings or distributed evidence.

The full benchmark **succeeded** as `pa5-task-a-20261125-06`, application `app-20261125203816957-0082`, on November 25. All six runs used the complete prepared input and returned 13 roles, with zero differing rows on every run. The downloaded CSVs were also checked independently against the reference; see `evidence/downloaded-results-check.txt`.

The required measurements are in `out/benchmark_full.csv` (exactly six records) and `out/result_comparison.csv`. The final batch description, complete driver output including both physical plans, CLI submission output, submitted command, six result tables, and actual task-overlap screenshot are in `evidence/`. Results, duration, cost estimate, and limitations are documented below. A teammate's fresh-environment review is still required before merging; the changes are prepared for a pull request; merge and Canvas submission remain pending.

Earlier attempts 01–05 failed before Spark started because the execution service account lacked seven Dataproc Worker permissions. Worker had initially been granted to user accounts. Hadi's November 25 update added the exact service account to `roles/dataproc.worker` on this project, after which attempt 06 ran successfully. Earlier batch descriptions and errors are retained as `evidence/benchmark-attempt-*.yaml` and `evidence/submission-attempt-*.txt`. The original bucket-metadata denial and staging flag correction are also preserved. No failed attempt supplies a benchmark timing, and no zero-charge claim is made for them. The subnet-description check remained denied `compute.subnetworks.get`, but did not prevent submission or execution.

Task B, new analyses, charts, presentation, and the complete final report remain outside this change.

## Parallel execution in the benchmark

The [benchmark Spark UI](https://console.cloud.google.com/dataproc/batches/us-central1/pa5-task-a-20261125-06/sparkApplications/applications/app-20261125203816957-0082/stages?project=cs131-11-5-2026-hadikhan) showed **stage 8, attempt 0, associated job 6**, from the first timed Analysis 1 query. It had 69 successful scan/partial-aggregation tasks. The actual screenshot `evidence/benchmark-stage-8-overlap.png` shows task 150 (index 8) on executor 1, host `10.128.0.22`, and task 166 (index 24) on executor 0, host `10.128.0.20`. Both launched at **20:38:40Z (12:38:40 PM Pacific)** and lasted 5.72 and 4.93 seconds. These intervals overlap even with second-rounded launch times, proving work ran concurrently on two executors. Five displayed task records were transcribed to `evidence/benchmark-stage-8-tasks.csv`; the screenshot is the original UI capture, sorted by duration.

The task table also showed many scan tasks with zero records and five tasks reading records. An 8 MiB split setting on this single Parquet file does not imply 69 equally useful pieces of work. This limits parallelism and helps explain why these results should not be generalized to a differently partitioned dataset. The global ranking still gathers the small grouped result into one partition. The saved final plans show `hashpartitioning(role_category, 200)` versus `hashpartitioning(role_category, 32)` and a single-partition exchange for ranking; adaptive coalescing did not erase the intended difference.

## Observed distributed execution from the successful preparation batch

Read-only inspection of the retained [Spark UI](https://console.cloud.google.com/dataproc/batches/us-central1/pa5-prep-full-04/sparkApplications/applications/app-20261125040213992-0816/stages?project=cs131-11-5-2026-hadikhan) found overlapping tasks in **stage 11, attempt 0, associated job 7**, with 20/20 successful tasks. The task table shows task 7 on executor 1 (10.128.0.9) and task 8 on executor 0 (10.128.0.8), both launched at November 24, 8:14:25 PM UTC-8 (November 25, 04:14:25Z). Their displayed durations are 6.60 and 6.24 seconds, so the intervals overlap even allowing for the table's rounded times. Eight tasks started in that displayed second across the two executors.

The actual stage/task screenshot is `evidence/pa5-prep-full-04-stage-11-overlap.png`; the 20 displayed task records were transcribed to `evidence/pa5-prep-full-04-stage-11-tasks.csv`. The screenshot shows the stage ID, executor IDs, launch times, and durations, rather than a graphical timeline. Reopen stage 11's View Details to inspect the source. Configuration and usage are in the existing final YAML. Reading the old staging bucket's driver log was denied with `storage.objects.get`; that limitation is saved in `evidence/preparation-log-read-error.txt`. Those historical driver logs were not recovered. The new benchmark has its own accessible complete driver log and concurrency screenshot, so Task A execution evidence does not depend on recovering the older logs. This older screenshot proves concurrent execution in the preparation job only. The original preparation batch had autoscaling **on**, as shown in its Console summary, unlike the new fixed-resource benchmark.

## Recovered prepared input

The sample pipeline previously overwrote the full prepared directory. The original full object was restored from soft deletion in the previous session. A fresh listing on November 25 found both it and the sample in the live directory. The sample was moved with `--no-clobber` to its own prefix; the full object was left intact. The sample pipeline now writes to that separate prefix.

```sh
gcloud storage mv --no-clobber \
  gs://cinematch-team6-pa5-fa26/pa5/prepared_credits/part-00000-9e10b347-facd-4811-bba4-329dcf3e8d74-c000.snappy.parquet \
  gs://cinematch-team6-pa5-fa26/pa5/sample/prepared_credits/part-00000-9e10b347-facd-4811-bba4-329dcf3e8d74-c000.snappy.parquet
```

That recovery command has already run; do not repeat it. The benchmark names the full object explicitly rather than reading a directory that another script could change.

| Input | Bytes | Live generation observed |
|---|---:|---|
| Full `part-00000-82a720be-34e6-49c5-9869-a4705a80fa9d-c000.snappy.parquet` | 571,124,374 | 1795597237992826 |
| Sample `part-00000-9e10b347-facd-4811-bba4-329dcf3e8d74-c000.snappy.parquet` | 9,033 | See live object metadata after move |

The full object's CRC32C is `02gjfw==` and MD5 is `CXIFmQe2vpJKIsqQItBPoQ==`. Its restored creation time is 2026-11-25T09:00:38Z. The earlier soft-deleted generation was 1795580361101317; restoration assigned a new live generation. The directory's zero-byte `_SUCCESS` marker came from the sample write and does not establish full-data coverage.

IMDb refreshes its [source files daily](https://data.imdb.com/non-commercial-datasets/). Our chosen version is the frozen pair already in the team bucket, not whatever the live IMDb download endpoint serves today. Bucket generations and checksums identify that snapshot; upload time alone does not establish the original IMDb release date. The live raw objects were checked against the recorded sizes: principals generation `1795566943219159`, CRC32C `kup8iQ==`; basics generation `1795566642369704`, CRC32C `WeDEOg==`. Their metadata is saved in `evidence/principals-object.yaml` and `evidence/basics-object.yaml`; the restored input is in `evidence/prepared-object.yaml`. `evidence/input-listing.txt` confirms the sample and full object were separate after the move. The raw counts in the inventory above are the teammate's recorded preparation counts, not a new raw-file recount. Retain object metadata before future runs:

```sh
gcloud storage objects describe gs://cinematch-team6-pa5-fa26/imdb/title.principals.tsv.gz
gcloud storage objects describe gs://cinematch-team6-pa5-fa26/imdb/title.basics.tsv.gz
gcloud storage objects describe gs://cinematch-team6-pa5-fa26/pa5/prepared_credits/part-00000-82a720be-34e6-49c5-9869-a4705a80fa9d-c000.snappy.parquet
```

`full_pipeline.py` prepares the credits **before** its title-basics join. It selects title ID, billing position, person ID, and the original role label; excludes NULL required fields, positions below 1, and role `\\N`; then derives `is_top10`. It checks repeated credit keys without removing them. The existing quality-check CSV records 0 exclusions and all 102,980,307 credits retained. Do not add cleaning filters or replace underscore labels in the benchmark.

To regenerate only if recovery proves unusable, use the existing `full_pipeline.py` with the fixed raw objects. It writes with overwrite mode to successful team output paths, so first make a copy of the script and change all output destinations to a new recovery prefix. Submit that copy using the class resource limits, then verify counts/schema and the complete small outputs. Regeneration is not needed merely to run Analysis 1 and has not been repeated here.

## Simple benchmark design and class references

Page numbers below refer to PDF pages, including title pages. The local decks and reference notebook were read before implementation. The [Canvas modules](https://sjsu.instructure.com/courses/1629284/modules), [Class 24 activity](https://sjsu.instructure.com/courses/1629284/assignments/7942943), and [PA5 Task A](https://sjsu.instructure.com/courses/1629284/assignments/7944294) were also inspected.

| Choice | Class/project example supporting it |
|---|---|
| Keep the existing filter, role groups, two-decimal average, rank, and tie rule | PA4 notebook Analysis 1 cell; `src/full_pipeline.py` Analysis 1; Class 19 pp. 26, 37, 51; Class 20 pp. 34-36 |
| Reuse typed Parquet without a title lookup join | Reference notebook sections 8 and 11; existing preparation saves before enrichment; Class 18 p. 15 |
| Collect every small result row | Class 19 p. 48; Class 24 pp. 24, 31; reference section 9 |
| Compare explicit 200 and 32 shuffle partitions with identical resources and scan settings | Class 24 pp. 19, 24-25; reference setup uses `spark.sql.shuffle.partitions`; Midterm 2 Prep p. 22. The value 32 is the class example; 200 is our explicit comparison value, not a claim about the managed runtime's default. |
| Disable adaptive partition coalescing in both approaches | Class 24 p. 25. This keeps Spark from merging the requested shuffle partitions away. Other adaptive behavior remains at the same runtime defaults for both approaches. |
| Request 8 MiB scan splits and one CPU per task in both approaches | Class 24 p. 25. Scan split size is a request, not a guarantee of task count or evenly sized Parquet row groups. |
| Three runs, rebuilding reads, no explicit caching | Parquet pp. 28-32; reference section 9; PA4 timing cells; PA5 Task A |
| Simple list loops, CSV output, numerical row comparison | PA4 benchmark and CSV-export cells. Tolerance is 0.000000001 on already formatted two-decimal averages; a one-cent difference fails. |
| Inspect physical plans after each approach's first timed run | Reference section 9; Classes 21-22 pp. 31, 35-37, 43 |
| Script arguments, one analysis function, named job groups, timing with collect, and writing collected rows | Class 24 activity attachment [`taxi_parallel_job.py`](https://sjsu.instructure.com/courses/1629284/files/89887202?wrap=1), pages 1–2: `sys.argv`, `def analyze`, `setJobGroup`, `perf_counter`, and `createDataFrame(rows)` |
| Fixed resources, timeout, commands, and task screenshot | Class 24 pp. 19, 21, 26-30; activity sections 1-5 |
| Count/schema/derived-column and complete-result validation | PA4 preparation checks; reference sections 3, 8-9; Class 24 p. 31 |

The comparison changes only `spark.sql.shuffle.partitions`. Both approaches rebuild the read and query using the entire exact Parquet object. `input_rows` is 102,980,307 and `input_bytes` is 571,124,374, before the top-10 filter. Raw gzip sizes are not the benchmark byte count. The benchmark uses the same labels and retained records as the preparation job.

No join is needed for Analysis 1. The historical era analysis uses an inner title lookup join; its unmatched-record count is separate from cleaning exclusions. For this benchmark the role aggregation redistributes partial counts/sums by role. The global ranking window gathers the small grouped table into one partition and sorts it; that final stage can be serial even when earlier scan/aggregation tasks overlap. `explain("formatted")` prints the actual exchanges. `coalesce(1)` is used only for writing already-collected small results, outside timing.

The timer starts immediately before `analysis_1()` calls `spark.read.parquet` and stops immediately after `collect()`. It includes read construction/metadata, filtering, grouping, formatting, ranking, sorting, and complete retrieval. This intentionally includes reading inside the timer as PA5 requires; the Class 24 demonstration put initial schema discovery outside its action timer. Spark startup, input validation, configuration changes, comparisons, plan printing, and output writing are excluded.

All six runs share one Spark session, in this order:

| Session order | Approach | Approach repetition |
|---:|---|---:|
| 1 | shuffle_200 | 1 |
| 2 | shuffle_32 | 1 |
| 3 | shuffle_32 | 2 |
| 4 | shuffle_200 | 2 |
| 5 | shuffle_200 | 3 |
| 6 | shuffle_32 | 3 |

Neither Spark inputs nor results are explicitly cached. Counts/schema checks happen before timing and may warm metadata and storage. Later runs can benefit from JVM/JIT initialization and system/storage caches. Rebuilding the DataFrame does not clear these caches. Plan printing after first runs is outside the timer but adds a small pause. The order distributes early and late positions across approaches; six observations in one session do not remove order effects or support broad speedup claims.

Each timed result is compared at every sorted row position with the complete saved full-data reference, including rank, role label, exact credit count, and the formatted average. Extra or missing rows contribute to `differing_rows`. Matching both approaches to the same reference verifies their agreement at the stated precision. Each run gets its own comparison row and flag. All six collected result tables are saved for inspection.

## Reproduce the small test (no Dataproc batch)

In a fresh Cloud Shell session, upload `src/benchmark_task_a.py` and `src/check_prepared.py` using the menu taught in Class 24. The supplied programs use the Spark session configured by `spark-submit`; they do not install dependencies or set a local master internally.

```sh
mkdir -p ~/cinematch-task-a
cd ~/cinematch-task-a
git clone https://github.com/sjsu-cs131-f26/CineMatch-Netflix-Movies.git reference-repo
cp ~/benchmark_task_a.py .
cp ~/check_prepared.py .
cp reference-repo/pa4/out/analysis_1.csv expected_sample.csv
cp reference-repo/pa5/out/analysis_1_full.csv expected_full.csv
python3 -m pip install --target /tmp/cinematch-spark pyspark==3.5.3
gcloud storage cp gs://cinematch-team6-pa5-fa26/pa5/sample/prepared_credits/part-00000-9e10b347-facd-4811-bba4-329dcf3e8d74-c000.snappy.parquet sample.parquet
PYTHONPATH=/tmp/cinematch-spark SPARK_LOCAL_IP=127.0.0.1 /tmp/cinematch-spark/bin/spark-submit --master 'local[2]' benchmark_task_a.py sample.parquet expected_sample.csv sample-test local 1000 969 9033 > sample-test.log 2>&1
cat sample-test/result_comparison/part-*.csv
```

Use a fresh `sample-test` output directory for each rerun: writes deliberately fail if the output already exists. Its `benchmark_full` subdirectory is a script-generated name inside **sample-test**; do not copy those sample timings to the repository's full benchmark CSV. Cloud Shell local Spark uses one machine, regardless of its two task threads.

The passing sample used Spark 3.5.3, Python 3.12.3, OpenJDK 21.0.12.1, and Google Cloud SDK 592.0.0 in Cloud Shell (see `evidence/cloud-shell-versions.txt`). The script's SHA-256 was `cfba5f886d37fe48bddde131fb1cb1ce1981b3301c0af5ff9853549751c355cf`. This records the local test environment. The completed managed batch used runtime 2.3.44, Spark 3.5.3, and a Java 17 installation, as documented below from its actual metadata/logs.

## Check the recovered full file without submitting a batch

These commands download the single object to Cloud Shell and run local validation. They do not regenerate the data, submit a managed batch, or produce benchmark measurements. The object download and local scans still use storage operations; they are not a promise of zero account usage.

```sh
gcloud storage cat gs://cinematch-team6-pa5-fa26/pa5/prepared_credits/part-00000-82a720be-34e6-49c5-9869-a4705a80fa9d-c000.snappy.parquet > /tmp/cinematch-full-stream.parquet
md5sum /tmp/cinematch-full-stream.parquet
PYTHONPATH=/tmp/cinematch-spark SPARK_LOCAL_IP=127.0.0.1 /tmp/cinematch-spark/bin/spark-submit --master 'local[2]' --conf spark.sql.shuffle.partitions=32 check_prepared.py /tmp/cinematch-full-stream.parquet expected_full.csv recovered_full_checks.csv 102980307 79193269 > recovered-full-check.log 2>&1
cat recovered_full_checks.csv
```

The expected MD5 is `0972059907b6be924a22ca9022d04fa1`, equivalent to the metadata's base64 MD5. Two sliced `gcloud storage cp` transfers stalled and were cancelled before validation; their incomplete temporary files were not read by Spark. The completed streaming copy matched that MD5. This is an operational download change, using the `gcloud storage cat` command shown in Class 24's activity.

The check compares current counts, types, the derived flag, and every Analysis 1 output row with the existing full reference. The full check completed on November 25 at about 09:47Z (01:47 Pacific) with all six checks passing. Its checker also passed independently on the preserved sample (`evidence/sample_prepared_checks.csv`). Combined with the recovered object's identity and the original full preparation checks, this checks reuse coverage. It is not a new raw-versus-prepared row-by-row audit; row counts and group totals alone cannot establish arbitrary record-level equality.

## Recorded full cloud run and reproduction

The successful command used the existing execution account after Hadi corrected its project-level Worker grant. The user confirmed sufficient credits before submission. No new service account or IAM change is performed by the scripts.

Upload `src/submit_task_a.sh` along with the benchmark script, then run from the working directory containing `benchmark_task_a.py` and `expected_full.csv`:

```sh
cp ~/submit_task_a.sh .
bash submit_task_a.sh > submission.log 2>&1
```

`src/submit_task_a.sh` is the complete copyable Class 24 command. It requests standard tier, runtime 2.3, two fixed executors, four cores per executor and driver, 12 GiB heap plus 4 GiB overhead per process, and 250 GiB disk per process in us-central1. Dynamic allocation is disabled. The TTL is 10 minutes. It used the existing execution service account and the new `pa5/task_a/pa5-task-a-20261125-06` output prefix. This batch ID and output prefix have now been used successfully. For any authorized future rerun, change the ID to an unused value (for example, `pa5-task-a-20261125-07`) before submitting; keep the successful results intact. The exact submitted shell script is also preserved as `evidence/benchmark-submitted-command.sh`.

The actual final description was saved with:

```sh
gcloud dataproc batches describe pa5-task-a-20261125-06 --project=cs131-11-5-2026-hadikhan --region=us-central1 > benchmark-batch.yaml
```

For a stuck or unintended running batch:

```sh
gcloud dataproc batches cancel pa5-task-a-20261125-06 --project=cs131-11-5-2026-hadikhan --region=us-central1
```

The observed terminal state was SUCCEEDED. Closing Cloud Shell does not stop a running batch. The small CSV parts were retrieved after successful completion with:

```sh
gcloud storage cp 'gs://cinematch-team6-pa5-fa26/pa5/task_a/pa5-task-a-20261125-06/benchmark_full/part-*.csv' benchmark_full.csv
gcloud storage cp 'gs://cinematch-team6-pa5-fa26/pa5/task_a/pa5-task-a-20261125-06/result_comparison/part-*.csv' result_comparison.csv
```

Each directory contained one CSV part. These were downloaded to `pa5/out/`; all six per-run result parts were downloaded to `pa5/evidence/result_1.csv` through `result_6.csv` in session order. Local verification confirmed the exact required measurement header, six records, three per approach, 13 output rows each, full prepared-input size/count, matching comparison entries, correct batch ID, and exact equality of every saved result row to the full reference.

On the batch details page, open **View Spark UI**, locate the named benchmark job groups, and select a scan or aggregation stage. Save its stage ID, task count, executor IDs, and a screenshot of overlapping task intervals on two distinct executors. Save the driver logs (the description's `runtimeInfo.outputUri` identifies them), final batch YAML, plans, and task evidence under `pa5/evidence/`. Configuring two executors or running `local[2]` is not concurrency proof. UI access may need `dataproc.batches.sparkApplicationRead`; see Google's [batch monitoring guide](https://docs.cloud.google.com/managed-spark/docs/guides/monitor-troubleshoot-batches).

## Full measurements and findings

All times below are seconds, rounded to six decimal places for display; `out/benchmark_full.csv` retains the original values. Every run returned 13 rows and passed its own comparison with zero differing rows.

| Session order | Approach | Run | Elapsed seconds |
|---:|---|---:|---:|
| 1 | shuffle_200 | 1 | 9.612194 |
| 2 | shuffle_32 | 1 | 7.060400 |
| 3 | shuffle_32 | 2 | 6.290880 |
| 4 | shuffle_200 | 2 | 6.052947 |
| 5 | shuffle_200 | 3 | 6.104315 |
| 6 | shuffle_32 | 3 | 5.448975 |

| Approach | Median | Minimum | Maximum | Range |
|---|---:|---:|---:|---:|
| shuffle_200 | 6.104315 | 6.052947 | 9.612194 | 3.559247 |
| shuffle_32 | 6.290880 | 5.448975 | 7.060400 | 1.611426 |

The 200-partition median was about 0.187 seconds (3.0%) lower than the 32-partition median. That difference is smaller than the variation within either approach. The first timed query was slowest, while the last was fastest; initialization and system/storage cache effects remain plausible even though no Spark caching was requested. Preflight counts had already warmed some input state. These three runs per approach in one shared session do **not** establish a reliable partition-count speedup. Both settings correctly answer the same Analysis 1 question on all retained full records; the defensible finding is correctness and demonstrated parallel execution with modest, variable timings under these specific conditions.

The final API description records runtime **2.3.44** and fixed resources matching the request: standard tier, driver 4 cores, two executors with 4 cores each, 12 GiB heap plus 4 GiB overhead per process, 250 GiB disk per process, dynamic allocation disabled, one CPU per task, 8 MiB scan splits, and adaptive shuffle coalescing disabled. Logs record Spark **3.5.3** and `JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-amd64`; an exact managed Java patch/Python version was not separately printed, so the managed runtime version identifies that environment. Cloud Shell's CLI version was 592.0.0. The managed batch is distinct from the local sample-test environment recorded above.

The batch was created at **20:36:53.441284Z**, entered RUNNING at **20:38:04.615781Z**, and succeeded at **20:40:18.848039Z** on November 25. Total creation-to-success duration was **205.406755 seconds** (3 minutes 25.407 seconds): 71.174497 pending plus 134.232258 running. The sum of the six query timers was **40.569711 seconds**. The CLI's `time` output was **219.222 seconds** (3 minutes 39.222 seconds), which also includes client submission/polling overhead. Startup, validation, plan printing, and output writes explain why the batch duration exceeds the sum of query times; they are excluded from those timers.

## Cost estimate

The [published us-central1 prices](https://cloud.google.com/products/managed-service-for-apache-spark/pricing) checked November 25 are $0.06 per standard DCU-hour and $0.000054795 per standard shuffle GiB-hour. The final batch description reports `milliDcuSeconds: 1701800` and `shuffleStorageGbSeconds: 100500`. Compute is `1701800 / 1000 / 3600 * 0.06 = $0.02836333`; shuffle storage is `100500 / 3600 * 0.000054795 = $0.00152969`. The successful benchmark's estimated total is **$0.02989303 (about three cents)**. This uses reported approximate usage, not just query durations or configured executor counts.

This estimate excludes storage operations, Cloud Shell activity, network, logging, taxes, earlier failed submissions, and other project activity. It is not an observed credit deduction or a verified remaining credit balance. Billing reports were inaccessible to Aneesh; an actual invoice/credit deduction needs billing evidence from someone with access.

For the **existing preparation batch**, the saved final YAML reports 33,997,650 milliDCU-seconds and 3,442,800 shuffle-storage GB-seconds. Applying the same rates gives approximately $0.56663 compute + $0.05240 shuffle = **$0.61903**. This is an estimate for `pa5-prep-full-04`, separate from the new benchmark. Its total batch duration was 2,930.391 seconds.

AI assistance was used to inspect materials, draft the scripts/documentation, and operate the validation workflow. The team must review and understand the changes; the PA5 instructions require a teammate who did not write the code to review and test the sample in a fresh environment before merging. The Task A changes are prepared for a pull request. Merge is pending the required teammate review and fresh-environment sample test; no Canvas submission was made.

## Task A audit before merge (November 25)

The [PA5 assignment](https://sjsu.instructure.com/courses/1629284/assignments/7944294) and Class 24 activity/script were reopened during this review. The local benchmark code is byte-for-byte identical to the sample-tested and successful full-run script (SHA-256 above); no paid rerun was needed for this documentation review.

| Requirement | Review result and evidence |
|---|---|
| Full input inventory, source/version, bytes, exclusions, and reuse coverage | Pass: raw object metadata, recovered object's identity/MD5, original full quality checks, and new recovered/full-batch row/schema/total checks. The original IMDb publication date is unknown; the chosen snapshot is identified by object generations/checksums. |
| Actual overlapping tasks on two executors, batch/config/logs | Pass: stage 8 screenshot and task records, `benchmark-batch.yaml`, `benchmark-driver.txt`. |
| Same Analysis 1 and unrelated settings fixed | Pass: exact PA4 grouping/filter/format/rank/tie rule, 200 vs 32 shuffle partitions, identical fixed resources, adaptive coalescing disabled; both physical plans retained. |
| Small-sample correctness before full timing | Pass: all six sample runs passed before successful full batch submission. |
| Credit check, bounded resources, timeout | Pass: user confirmed sufficient credits; fixed two executors and 600-second TTL appear in actual batch metadata. No exact remaining balance was supplied. |
| Three full runs per approach, rebuilt reads, no explicit cache | Pass: six ordered measurements and unchanged script; one shared session and preflight/cache effects disclosed. |
| Read-through-complete-collect timing | Pass: `perf_counter()` encloses rebuilt read/query plus `collect()`; setup, comparisons, and writes remain outside. |
| Every result row checked and six measurement records | Pass: all six tables exactly match the 13-row saved reference, including ordering and formatted averages. CSV fields, per-run comparison flags, and summed credit totals were independently rechecked after download. |
| Commands, versions, shuffle choices, timings, medians, variation, duration, cost, limitations | Pass: sections above; cost uses final approximate usage rather than a claimed billing deduction. |
| Simple class/project code only | Pass: two small standalone Spark scripts and one class-style shell command; no classes, framework, ML, added dependencies on workers, or generalized utilities. The one query function follows the class script. Basic CSV/list/loop comparison follows PA4. |
| Teammate PR review and sample in a fresh environment before merge | **Pending**: required explicitly under “Submit through the team repository.” AI review does not replace this step. |

For the teammate: review the three new scripts and the sample-output path fix, run “Reproduce the small test” in a fresh Cloud Shell environment, and confirm all six comparisons report zero differing rows. Record the result on the PR before merging. A full paid rerun is not required for peer sample review. The `sample-test` output directory must be unused (or choose a new sample-only output name) because output writes protect existing results.
