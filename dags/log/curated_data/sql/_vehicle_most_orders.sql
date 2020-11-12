CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vehicle_most_orders`
PARTITION BY report_date_local AS
WITH vehicle_dataset AS (
  SELECT country_code
    , rider_id
    , id
    , DATE(rider_dropped_off_at, o.timezone) AS report_date_local
    , vehicle.vehicle_bag
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d
  WHERE d.delivery_status = 'completed'
), vehicle_most_orders_ordered AS (
  SELECT country_code
    , rider_id
    , report_date_local
    , vehicle_bag
    , ROW_NUMBER() OVER (PARTITION BY country_code, rider_id, report_date_local ORDER BY COUNT(DISTINCT id) DESC) AS row_number
    , COUNT(id) AS deliveries
  FROM vehicle_dataset
  GROUP BY 1,2,3,4
)
SELECT * EXCEPT (row_number, deliveries)
FROM vehicle_most_orders_ordered
WHERE row_number = 1
