-- Task 9: split the full analysis-ready credits into release-era Parquet partitions.
-- Run from pa3/ by scripts/build_partitions.sh, which first creates work/title_era.tsv.

CREATE OR REPLACE TABLE title_era AS
SELECT *
FROM read_csv_auto('work/title_era.tsv', delim = '\t', header = true);

CREATE OR REPLACE TABLE credits_with_era AS
SELECT c.title_id, c.billing_order, c.person_id, c.role_category, t.era
FROM read_parquet('work/full_analysis_ready.parquet') AS c
JOIN title_era AS t ON c.title_id = t.title_id;

COPY (SELECT title_id, billing_order, person_id, role_category FROM credits_with_era WHERE era = 'before_1980')
TO 'work/partitions/era_before_1980.parquet' (FORMAT parquet);

COPY (SELECT title_id, billing_order, person_id, role_category FROM credits_with_era WHERE era = '1980_1999')
TO 'work/partitions/era_1980_1999.parquet' (FORMAT parquet);

COPY (SELECT title_id, billing_order, person_id, role_category FROM credits_with_era WHERE era = '2000_2009')
TO 'work/partitions/era_2000_2009.parquet' (FORMAT parquet);

COPY (SELECT title_id, billing_order, person_id, role_category FROM credits_with_era WHERE era = '2010_2019')
TO 'work/partitions/era_2010_2019.parquet' (FORMAT parquet);

COPY (SELECT title_id, billing_order, person_id, role_category FROM credits_with_era WHERE era = '2020_later')
TO 'work/partitions/era_2020_later.parquet' (FORMAT parquet);

-- Row counts per era, for the report.
SELECT era, COUNT(*) AS credit_rows
FROM credits_with_era
GROUP BY era
ORDER BY era;
