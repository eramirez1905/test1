CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_vendors_is_active_history`
PARTITION BY created_date
CLUSTER BY entity_id, vendor_code AS
WITH df_vendors AS (
  SELECT v.global_entity_id AS entity_id
    , v.content.vendor_id AS vendor_code
    , CAST(MIN(v.timestamp) OVER vendor_window AS DATE) AS created_date
    , v.content.timezone
    , MIN(v.timestamp) OVER vendor_window AS created_at
    , CAST(v.timestamp AS DATE) AS updated_date
    , v.timestamp AS updated_at
    , v.content.active AS is_active
    , LAG(v.timestamp) OVER vendor_window AS prev_updated_at
    , LAG(v.content.active) OVER vendor_window AS prev_is_active
  FROM `{{ params.project_id }}.dl.data_fridge_vendor_stream` v
  WINDOW vendor_window AS (
    PARTITION BY v.global_entity_id, v.content.vendor_id
    ORDER BY v.timestamp
  )
), vendors_updates AS (
  SELECT *
  FROM df_vendors
  WHERE is_active != prev_is_active
    OR prev_is_active IS NULL
)
SELECT entity_id
  , vendor_code
  , created_date
  , timezone
  , is_active
  , created_at
  , updated_at
  , COALESCE(
      LEAD(updated_at) OVER (PARTITION BY entity_id, vendor_code ORDER BY updated_at),
      '{{ next_execution_date }}'
    ) AS next_updated_at
FROM vendors_updates
