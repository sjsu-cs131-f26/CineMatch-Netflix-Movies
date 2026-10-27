COPY (
  WITH filtered_credits AS (
    SELECT
      title_id,
      billing_order,
      person_id,
      role_category
    FROM read_parquet('work/analysis_ready.parquet')
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
    ROW_NUMBER() OVER (ORDER BY credit_count DESC, role_category ASC) AS role_rank,
    role_category,
    credit_count,
    avg_billing_order
  FROM role_summary
  ORDER BY credit_count DESC, role_category ASC
)
TO 'out/duckdb_parquet_result.tsv' (HEADER, DELIMITER '\t');
