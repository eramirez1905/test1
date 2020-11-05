WITH vendor_performance AS (
  SELECT created_hour_local
    , entity_id
    , vendor_code
    , COUNT(*) AS count
  FROM `{{ params.project_id }}.cl.vendor_kpi`
  GROUP BY 1,2,3
)
SELECT COUNTIF(count != 1) = 0 AS duplication_check
FROM vendor_performance
