CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._posmw_chain_mapped_history`
PARTITION BY created_date
CLUSTER BY region, chain_id AS
WITH posmw_chain_mapped_dataset AS (
SELECT posmvc.created_date
  , posmvc.region
  , posmvc.id AS chain_id
  , posmvc.name AS pos_chain_name
  , posmvc.created_at
  , posmvc.updated_at
  , COALESCE(
      LEAD(posmvc.updated_at) OVER (PARTITION BY posmvc.region, posmvc.id ORDER BY posmvc.updated_at),
      CAST('{{ next_ds }}' AS TIMESTAMP)
    ) AS next_updated_at
    , ROW_NUMBER() OVER (PARTITION BY posmvc.region, posmvc.id ORDER BY posmvc.updated_at) AS _row_number
FROM `{{ params.project_id }}.hl.posmw_chain_mapped` posmvc
)
SELECT created_date
  , region
  , chain_id
  , pos_chain_name
  , created_at
  , IF(_row_number = 1, created_at, updated_at) AS updated_at
  , next_updated_at
FROM posmw_chain_mapped_dataset
