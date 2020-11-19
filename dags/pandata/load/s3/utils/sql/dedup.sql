WITH ranked_by_updated_at AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        rdbms_id,
        {id_col}
      ORDER BY dwh_last_modified DESC
    ) = 1 AS is_latest,
  FROM `{table_id}`
)

SELECT * EXCEPT (is_latest)
FROM ranked_by_updated_at
WHERE is_latest
