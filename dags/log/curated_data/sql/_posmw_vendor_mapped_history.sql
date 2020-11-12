CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._posmw_vendor_mapped_history`
PARTITION BY created_date
CLUSTER BY region, vendor_id AS
WITH posmw_vendor_mapped_dataset AS (
  SELECT posmvm.created_date
    , posmvm.region
    , CAST(posmvm.vendor_code AS INT64) AS vendor_id
    , posmvm.chain_mapped_id
    , posmvm.created_at
    , posmvm.updated_at
    , COALESCE(
        LEAD(posmvm.updated_at) OVER (PARTITION BY posmvm.region, posmvm.vendor_code ORDER BY posmvm.updated_at),
        CAST('{{ next_ds }}' AS TIMESTAMP)
      ) AS next_updated_at
    , ROW_NUMBER() OVER (PARTITION BY posmvm.region, posmvm.vendor_code ORDER BY posmvm.updated_at) AS _row_number
  FROM `{{ params.project_id }}.hl.posmw_vendor_mapped` posmvm
  WHERE SAFE_CAST(posmvm.vendor_code AS INT64) IS NOT NULL
)
SELECT created_date
  , region
  , vendor_id
  , chain_mapped_id
  , created_at
  , IF(_row_number = 1, created_at, updated_at) AS updated_at
  , next_updated_at
FROM posmw_vendor_mapped_dataset
