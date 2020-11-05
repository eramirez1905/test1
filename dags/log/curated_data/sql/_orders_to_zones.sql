CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._orders_to_zones`
PARTITION BY created_date
CLUSTER BY country_code, order_id, zone_id AS
SELECT country_code
  , order_id
  , zone_id
  , created_date
FROM `{{ params.project_id }}.cl._orders_to_zones_ranked`
WHERE rank = 1
