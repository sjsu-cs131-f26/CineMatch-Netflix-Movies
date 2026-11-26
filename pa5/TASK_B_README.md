# PA5 Task B: Full-Dataset Top-10 Billing Cutoff & Billing-Tier Audit

## 1. Research Question
Previous CineMatch analyses (Analysis 1, Analysis 2, and Task A) filter the prepared IMDb credits table to `is_top10 == True` (`79,193,269` of `102,980,307` rows), dropping **`23,787,038` credits (`23.10%`)**.

**Task B asks:** Across the entire `102,980,307`-row prepared dataset, which crew roles lose the largest share of their credits outside the top-10 billing cutoff, and how are each role's credits distributed across three billing tiers: **Lead (`1–3`)**, **Supporting Top-10 (`4–10`)**, and **Beyond Top-10 (`11+`)**?

## 2. How Task B Differs from Existing Work
- **Analysis 1 (PA4 / PA5):** Ranks roles and computes average billing order using **only** `is_top10 == True` (`79.19M` rows).
- **Analysis 2 (PA4 / PA5):** Compares `is_top10 == True` roles across release eras after joining with `title.basics.tsv.gz`.
- **Task A (PA5):** Benchmarks Analysis 1 at 200 vs. 32 shuffle partitions on the same `is_top10 == True` subset.
- **Task B (New):** Evaluates **all `102,980,307` prepared credit rows** (both `is_top10 == True` and `is_top10 == False`) to measure selection bias caused by the top-10 cutoff and break every role into Lead, Supporting Top-10, and Beyond Top-10 tiers.

## 3. Method & Class Material Alignment
`pa5/src/task_b_billing_cutoff.py` reads the recovered full prepared Parquet object directly (`gs://cinematch-team6-pa5-fa26/pa5/prepared_credits/part-00000-82a720be-34e6-49c5-9869-a4705a80fa9d-c000.snappy.parquet`, `571,124,374` bytes) and performs two basic Spark DataFrame aggregations without modifying retained records, role labels, or existing outputs:
1. **Role cutoff summary (`role_cutoff_summary`):** Groups by `role_category` and aggregates total, top-10, and beyond-top-10 credit counts plus billing-order sums, sorted deterministically by `beyond_top10_credits DESC, role_category ASC`.
2. **Billing-tier distribution (`role_billing_tiers`):** Derives `billing_tier` (`lead_1_to_3`, `supporting_4_to_10`, `beyond_top10_11_plus`) using `F.when(...).otherwise(...)` and aggregates credit counts and billing sums per `(role_category, tier_order, billing_tier)`.

**Supporting course references:**
- Fixed 2-executor Dataproc Serverless configuration (`2` executors × `4` cores, `12g` heap + `4g` overhead, `250g` disk, `runtime 2.3`, `--ttl=10m`) and session initialization without `.master(...)`: *Class 24 Slides 13, 20–21*.
- `spark.read.parquet(...)`, `filter(...)`, `groupBy(...).agg(F.count("*"), F.sum(...))`: *Class 24 Slides 16–17* and `pa5/src/full_pipeline.py`.
- `F.when(...).otherwise(...)` bucketing and `coalesce(1)` on small output tables: `pa5/src/full_pipeline.py`.
- `errorifexists` write protection under a fresh batch prefix, `assert` validation over complete collected tables, and timing boundary definitions: *Class 24 Slides 24–25, 27*.
- Colab `local[1]` pre-flight sample check: *Class 24 Slide 26*.

## 4. Cloud Execution, Validation & Evidence
- **GCP Project / Region:** `cs131-11-5-2026-hadikhan` / `us-central1`
- **Batch ID:** `pa5-task-b-20261125-01` (`uuid: 25cff6b2-98e4-4dce-a9b1-17f9d849503b`)
- **Runtime:** Dataproc Serverless `2.3.44` (Apache Spark `3.5.3`)
- **Output Prefix (separate from prepared data & Task A):** `gs://cinematch-team6-pa5-fa26/pa5/task_b/pa5-task-b-20261125-01`
- **Timing Boundaries:**
  - Timed Spark analysis (`scan + group + sort + collect`): **`28.79` seconds**
  - Batch state duration (`2026-11-26T02:24:28Z` `PENDING` → `02:25:39Z` `RUNNING` → `02:26:57Z` `SUCCEEDED`): **`149.4` seconds** (`77.8s` `RUNNING`)
  - CLI submission wall time (`time gcloud ...`): **`2m 42.200s`**
- **Cost & Credit Note:** Remaining education credits were confirmed by the project billing owner (Hadi Khan) before submission (exact dollar balance screenshot pending). Using the Class 24 Slide 14 illustrative `us-central1` compute/shuffle rate (~$0.06 for 5 minutes on this identical fixed 2-executor config), a ~2.5-minute batch is roughly ~$0.03 in compute/shuffle before GCS/network/logging.
- **Validation:**
  - Pre-flight sample test on the preserved 1,000-row sample Parquet passed all assertions in Colab `local[1]` (`969` top-10, `31` beyond-top-10; saved separately in `pa5/out/task_b_*_sample.csv`).
  - Full run passed all count, schema, and partition assertions (`validation_passed = True`), and every role's `top10_credits` and `avg_billing_top10` matches `pa5/out/analysis_1_full.csv` with **0 differing rows**.

## 5. Full-Dataset Findings & What They Mean for CineMatch
Source: `pa5/out/task_b_role_cutoff_full.csv` and `pa5/out/task_b_role_billing_tiers_full.csv` (`102,980,307` total rows).

| role_category | total_credits | top10_credits | beyond_top10_credits | top10_retention_pct | excluded_by_top10_pct | share_of_all_excluded_pct | avg_billing_all | avg_billing_top10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| writer | 12,225,836 | 6,335,888 | 5,889,948 | 51.82% | 48.18% | 24.76% | 9.51 | 5.88 |
| producer | 7,581,271 | 3,968,771 | 3,612,500 | 52.35% | 47.65% | 15.19% | 10.12 | 6.09 |
| editor | 5,577,137 | 2,021,420 | 3,555,717 | 36.24% | 63.76% | 14.95% | 12.97 | 6.60 |
| director | 8,690,342 | 5,705,245 | 2,985,097 | 65.65% | 34.35% | 12.55% | 7.20 | 4.81 |
| cinematographer | 4,135,823 | 1,676,986 | 2,458,837 | 40.55% | 59.45% | 10.34% | 12.08 | 6.66 |
| composer | 3,263,843 | 1,191,137 | 2,072,706 | 36.49% | 63.51% | 8.71% | 12.27 | 6.50 |
| casting_director | 1,177,224 | 134,099 | 1,043,125 | 11.39% | 88.61% | 4.39% | 17.50 | 6.77 |
| production_designer | 1,198,399 | 184,668 | 1,013,731 | 15.41% | 84.59% | 4.26% | 16.96 | 7.55 |
| actor | 24,355,032 | 23,722,026 | 633,006 | 97.40% | 2.60% | 2.66% | 4.85 | 4.61 |
| actress | 18,394,427 | 18,013,966 | 380,461 | 97.93% | 2.07% | 1.60% | 4.72 | 4.52 |
| self | 15,705,834 | 15,585,163 | 120,671 | 99.23% | 0.77% | 0.51% | 3.73 | 3.65 |
| archive_footage | 660,783 | 642,897 | 17,886 | 97.29% | 2.71% | 0.08% | 5.45 | 5.22 |
| archive_sound | 14,356 | 11,003 | 3,353 | 76.64% | 23.36% | 0.01% | 7.54 | 4.87 |

**What this means for CineMatch decision-makers:**
1. **A top-10 billing filter is almost lossless for on-screen cast, but severely censors behind-the-scenes craft roles.** Across all `23,787,038` excluded rows, **`22,631,661` (`95.14%`)** belong to the eight behind-the-scenes crew roles (`writer`, `producer`, `editor`, `director`, `cinematographer`, `composer`, `casting_director`, `production_designer`). By contrast, `actor` (`97.40%`), `actress` (`97.93%`), and `self` (`99.23%`) are almost completely retained, and over `41%`–`57%` of their credits sit in Lead (`1–3`) billing.
2. **CineMatch should not apply a uniform `billing_order <= 10` cutoff when building crew-based recommendations.** Cutting at position 10 discards **`88.61%`** of `casting_director` credits, **`84.59%`** of `production_designer` credits, **`63.76%`** of `editor` credits, **`63.51%`** of `composer` credits, **`59.45%`** of `cinematographer` credits, **`48.18%`** of `writer` credits, **`47.65%`** of `producer` credits, and even **`34.35%`** of `director` credits. For craft-driven movie matching (e.g., matching films by composer, cinematographer, writer, or director), CineMatch should retain all billing positions for crew roles and only cap cast depth if needed.
3. **Why full-data execution mattered:** In the 1,000-row early-cinema sample, only `3.10%` of credits fell outside the top 10 (because older/shorter titles had tiny crews). Only on the full `102.98M`-row dataset did the true `23.10%` exclusion rate and massive behind-the-scenes loss emerge.

## 6. Limitations
- **Pre-join title scope:** Because `prepared_credits` precedes the `title.basics.tsv.gz` join, counts include all IMDb title types (`movie`, `tvEpisode`, `short`, etc.), not feature films exclusively.
- **Credit-row granularity:** Counts reflect `(title_id, billing_order)` credit rows rather than deduplicated unique `person_id` counts.
- **IMDb billing-order convention:** IMDb orders principal cast ahead of key crew on most titles, so higher `billing_order` for crew roles reflects IMDb listing order rather than creative importance.
- **No external ratings/engagement outcomes:** This audit quantifies data coverage and selection bias; it does not directly measure downstream recommendation accuracy.

## 7. Reproduction & Peer Review Sample Test
To test the Task B script on the preserved sample Parquet in a fresh Colab environment (free, no cloud batch needed):
1. Download `gs://cinematch-team6-pa5-fa26/pa5/sample/prepared_credits/part-00000-9e10b347-facd-4811-bba4-329dcf3e8d74-c000.snappy.parquet` and upload it alongside `pa5/src/task_b_billing_cutoff.py`.
2. Run:
   ```bash
   spark-submit --master "local[1]" task_b_billing_cutoff.py \
     <PATH_TO_SAMPLE_PARQUET> /content/sample_task_b_out sample-peer-check 1000 969
bash pa5/src/submit_task_b.sh pa5-task-b-<YYYYMMDD>-<NN>


## 8. Verification summary

The Task B full-data run completed successfully with batch ID `pa5-task-b-20261125-01`.

The full prepared input contained 102,980,307 rows. The output tables contain 13 role-cutoff rows and 39 role-tier rows. All count, schema, partition, and cross-analysis checks passed.

The independent sample review was completed from a fresh repository clone using batch `pa5-peer-review-01`. The sample contained 1,000 rows, 0 exclusions, and 969 top-10 rows.
