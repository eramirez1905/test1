WITH vendors AS (
  SELECT vendor_code
    , entity_id
    , count(*) AS count
  FROM `{{ params.project_id }}.cl.vendors_v2`
  GROUP BY 1,2
)
SELECT COUNTIF(count != 1) = 0 AS duplicates_count
FROM vendors
