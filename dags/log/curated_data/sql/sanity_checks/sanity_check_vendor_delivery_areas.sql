WITH vendors_delivery_areas AS (
  SELECT d.id
    , d.platform
    , v.country_code
    , h.transaction_id
    , count(*) AS count
  FROM `{{ params.project_id }}.cl._vendors_delivery_areas` v
    , v.delivery_areas d
    , d.history h
  GROUP BY 1,2,3,4
  HAVING count > 1
)
SELECT COUNTIF(count != 1) = 0 AS duplicates_count
FROM vendors_delivery_areas
