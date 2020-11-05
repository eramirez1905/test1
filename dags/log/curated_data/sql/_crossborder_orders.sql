CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._crossborder_orders`
PARTITION BY created_date AS
WITH crossborder_dataset AS (
  SELECT o.country_code
     , o.order_id
     , ci.id AS city_id
     , o.created_date
     , MAX(o.is_dropoff_zone_equal_to_pickup_zone) AS is_dropoff_zone_equal_to_pickup_zone
  FROM `{{ params.project_id }}.cl._orders_to_zones_ranked` o
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON o.country_code = co.country_code
  LEFT JOIN UNNEST (co.cities) ci
  LEFT JOIN UNNEST (ci.zones) z ON o.zone_id = z.id
  GROUP BY 1,2,3,4
)
SELECT o.country_code
  , o.order_id
  , o.city_id
  , o.created_date
FROM crossborder_dataset o
WHERE NOT is_dropoff_zone_equal_to_pickup_zone
