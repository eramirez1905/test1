CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rider_timings_breakdown`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK) AS start_date
    , CAST('{{ next_ds }}' AS DATE) AS end_date
), orders_data AS(
  SELECT region
    , country_code
    , order_id
    , created_date
    , entity.display_name AS entity_display_name
    , deliveries.id AS delivery_id
    , deliveries.rider_id AS rider_id
    , original_scheduled_pickup_at
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) deliveries
  WHERE created_date >= (SELECT start_date FROM parameters)
    AND created_date <= (SELECT end_date FROM parameters)
    AND delivery_status = 'completed'
), timings_data AS(
  SELECT country_code
    , country_name
    , city_id
    , city_name
    , zone_id
    , zone_name
    , entity_id
    , vertical_type
    , vehicle_bag
    , is_preorder
    , created_date
    , DATE(rider_dropped_off_at, timezone) AS delivery_date
    , rider_id
    , DATE(rider_dropped_off_at, timezone) AS report_date
    , FORMAT_DATE('%G-%V', DATE(rider_dropped_off_at, timezone)) as report_week
    , timezone
    , order_id
    , delivery_id
    , reaction_time
    , to_vendor_time
    , at_vendor_time
    , to_customer_time
    , at_customer_time
    , idle_time
    , rider_effective_time                           
  FROM `{{ params.project_id }}.cl.utr_timings`
  WHERE created_date >= (SELECT start_date FROM parameters)
    AND created_date <= (SELECT end_date FROM parameters)
), merge_orders_timings_data AS(
  SELECT o.region
    , o.country_code
    , t.country_name
    , t.city_id
    , t.city_name
    , t.zone_id
    , t.zone_name
    , t.entity_id
    , o.entity_display_name
    , t.vertical_type
    , t.vehicle_bag
    , t.is_preorder
    , report_date
    , report_week
    , o.order_id AS o_order_id
    , t.order_id
    , o.delivery_id AS o_delivery_id
    , t.delivery_id
    , reaction_time
    , to_vendor_time
    , at_vendor_time
    , to_customer_time
    , at_customer_time
    , (reaction_time + to_vendor_time + at_vendor_time + to_customer_time + at_customer_time) AS rider_effective_time
    , idle_time
  FROM orders_data o
  LEFT JOIN timings_data t USING (created_date, country_code, delivery_id)
)
SELECT region
  , country_code
  , country_name
  , city_id
  , city_name
  , zone_id
  , zone_name
  , entity_id
  , entity_display_name
  , vertical_type
  , vehicle_bag
  , is_preorder
  , report_date
  , report_week
  , COUNT(order_id) AS completed_orders
  , COUNT(delivery_id) AS completed_deliveries
  , SUM(reaction_time) / 60  AS reaction_time_sum
  , COUNT(reaction_time) AS reaction_time_count
  , SUM(to_vendor_time) / 60 AS to_vendor_time_sum
  , COUNT(to_vendor_time) AS to_vendor_time_count
  , SUM(at_vendor_time) / 60 AS at_vendor_time_sum
  , COUNT(at_vendor_time) AS at_vendor_time_count
  , SUM(to_customer_time ) / 60 AS to_customer_time_sum
  , COUNT(to_customer_time) AS to_customer_time_count
  , SUM(at_customer_time) / 60 AS at_customer_time_sum
  , COUNT(at_customer_time) AS at_customer_time_count
  , SUM(rider_effective_time) / 60  AS rider_effective_time_sum
  , COUNT(rider_effective_time) AS rider_effective_time_count
  , SUM(idle_time) / 60 AS idle_time_sum
  , COUNT(idle_time) AS idle_time_count
FROM merge_orders_timings_data
WHERE report_date < '{{ next_ds }}'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
