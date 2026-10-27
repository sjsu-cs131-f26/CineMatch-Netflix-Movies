COPY (
  SELECT
    CAST(title_id AS VARCHAR) AS title_id,
    CAST(billing_order AS BIGINT) AS billing_order,
    CAST(person_id AS VARCHAR) AS person_id,
    CAST(role_category AS VARCHAR) AS role_category
  FROM read_csv(
    'work/analysis_ready.tsv',
    delim = '\t',
    header = true,
    columns = {
      'title_id': 'VARCHAR',
      'billing_order': 'BIGINT',
      'person_id': 'VARCHAR',
      'role_category': 'VARCHAR'
    }
  )
)
TO 'work/analysis_ready.parquet' (FORMAT parquet);
