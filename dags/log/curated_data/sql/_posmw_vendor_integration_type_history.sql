CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._posmw_vendor_integration_type_history`
PARTITION BY created_date
CLUSTER BY region, vendor_id AS
WITH posmw_vendor_integration_type_dataset AS (
  SELECT posmwint.created_date
    , posmwint.region
    , CAST(posmwint.vendor_code AS INT64) AS vendor_id
    , posmwint.integration_type
    , posmwint.created_at
    , posmwint.updated_at
    , COALESCE(
        LEAD(posmwint.updated_at) OVER (PARTITION BY posmwint.region, posmwint.vendor_code ORDER BY posmwint.updated_at),
        CAST('{{ next_ds }}' AS TIMESTAMP)
      ) AS next_updated_at
    , ROW_NUMBER() OVER (PARTITION BY posmwint.region, posmwint.vendor_code ORDER BY posmwint.updated_at) AS _row_number
  FROM `{{ params.project_id }}.hl.posmw_vendor_integration_type` posmwint
  WHERE SAFE_CAST(posmwint.vendor_code AS INT64) IS NOT NULL
)
SELECT created_date
  , region
  , vendor_id
  , integration_type
  , created_at
  , IF(_row_number = 1, created_at, updated_at) AS updated_at
  , next_updated_at
FROM posmw_vendor_integration_type_dataset
