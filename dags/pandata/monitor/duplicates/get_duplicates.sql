WITH duplicates AS (
  SELECT
    {id_fields},
    COUNT(*) > 1 AS has_duplicates
  FROM `{project}.{dataset}.{table}`
  {partition_filter}
  GROUP BY {id_fields}
  HAVING has_duplicates
)

SELECT
  COUNT(*) = 0 AS has_no_duplicates
FROM duplicates
