-- Task 9: same analysis as sql/analysis_parquet.sql, for one partition.
-- scripts/run_partition.sh uses sed to fill in PARTITION_PATH, PARTITION_ID, and RESULT_PATH.
COPY (
  WITH filtered_credits AS (
    SELECT role_category, billing_order
    FROM read_parquet('PARTITION_PATH')
    WHERE billing_order <= 10
  ),
  role_summary AS (
    SELECT
      role_category,
      COUNT(*) AS credit_count,
      printf('%.2f', AVG(CAST(billing_order AS DOUBLE))) AS avg_billing_order
    FROM filtered_credits
    GROUP BY role_category
  )
  SELECT
    'PARTITION_ID' AS partition_id,
    ROW_NUMBER() OVER (ORDER BY credit_count DESC, role_category ASC) AS role_rank,
    role_category,
    credit_count,
    avg_billing_order
  FROM role_summary
  ORDER BY credit_count DESC, role_category ASC
)
TO 'RESULT_PATH' (HEADER, DELIMITER '\t');
