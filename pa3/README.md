# PA3: Project Data Pipeline (Team 6, CineMatch)

This folder has our PA3 pipeline. It takes IMDb `title.principals.tsv`, cleans it with awk and sed,
runs the same analysis three ways (shell, DuckDB on text, DuckDB on Parquet), checks that all three
match, and writes `out/final_analysis.tsv`.

Question: which crew roles show up in the top 10 billing spots of IMDb titles, and how high are they
billed on average?

## Run the sample pipeline

From the repository root:

```bash
bash pa3/scripts/run_pipeline.sh pa3/data/sample/sample.tsv
```

It creates `pa3/work/analysis_ready.tsv` and `pa3/work/analysis_ready.parquet`, writes the three
results and `out/final_analysis.tsv`, and exits with a nonzero status if any stage or the three-way
`diff` check fails. DuckDB is run from
`/mnt/scratch/CS131_jelenag/tools/duckdb-current/duckdb` on the IBM server.

## Other scripts (run on the IBM server with the full data)

| Script | What it does |
|---|---|
| `scripts/prepare_data.sh` | awk/sed cleaning stage used by the pipeline |
| `scripts/run_shell_analysis.sh` | shell version of the analysis |
| `scripts/run_benchmark.sh` | first 1,000,000 records, 3 timed runs of each path, writes `out/benchmark.tsv` |
| `scripts/build_partitions.sh` | builds the release-era Parquet partitions and `state/partition_manifest.tsv` |
| `scripts/run_partition.sh PARTITION_PATH` | runs the analysis on one partition, writes `out/partitions/` |
| `scripts/run_scheduled.sh` | cron wrapper: runs the next incomplete partition and logs to `logs/cron.log` |

The cron entry is in `schedule/pa3.cron`. Evidence for the background run and scheduled runs is in
`out/background_run_proof.txt` and `out/scheduling_proof.txt`. The report is `docs/technical_report.pdf`.

## Not committed

The full IMDb data, `work/` (cleaned full data, Parquet files, partitions, DuckDB database files), and
`*.log` files stay on the IBM server and are listed in `.gitignore`.
