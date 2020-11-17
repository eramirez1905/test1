WITH rank_latest AS (
  SELECT
    *,
    ROW_NUMBER() OVER
      (
        PARTITION BY id, country_code, source
        ORDER BY created_at_utc DESC
      ) = 1
    AS is_latest
  FROM `{project_id}.pandata_intermediate.vci_categories_inc`
  WHERE id IS NOT NULL
)

SELECT
  id  || '_' || country_code || '_' || source AS uuid,
  id,
  country_code,
  source,
  name,
  description,
  created_at_utc,
  _ingested_at_utc
FROM rank_latest
WHERE is_latest
