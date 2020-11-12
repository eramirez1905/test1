CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rider_timings_breakdown_statistics`
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
    , reaction_time / 60 AS reaction_time
    , to_vendor_time / 60 AS to_vendor_time
    , at_vendor_time / 60 AS at_vendor_time
    , to_customer_time / 60 AS to_customer_time
    , at_customer_time / 60 AS at_customer_time
    , idle_time / 60  AS idle_time
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
    , t.idle_time
  FROM orders_data o
  LEFT JOIN timings_data t USING (created_date, country_code, delivery_id)
), percentile_dataset AS (
  SELECT country_code
    , entity_display_name
    , report_week
    , delivery_id
    -- Reaction Time
    , PERCENTILE_CONT(reaction_time, 0.5) OVER (PARTITION BY country_code, entity_display_name, report_week) AS reaction_time_median_country
    , PERCENTILE_CONT(reaction_time, 0.05) OVER (PARTITION BY country_code, entity_display_name, report_week) AS reaction_time_5_country
    , PERCENTILE_CONT(reaction_time, 0.25) OVER (PARTITION BY country_code, entity_display_name, report_week) AS reaction_time_25_country
    , PERCENTILE_CONT(reaction_time, 0.75) OVER (PARTITION BY country_code, entity_display_name, report_week) AS reaction_time_75_country
    , PERCENTILE_CONT(reaction_time, 0.95) OVER (PARTITION BY country_code, entity_display_name, report_week) AS reaction_time_95_country
    -- To Vendor Time
    , PERCENTILE_CONT(to_vendor_time, 0.5) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_vendor_time_median_country
    , PERCENTILE_CONT(to_vendor_time, 0.05) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_vendor_time_5_country
    , PERCENTILE_CONT(to_vendor_time, 0.25) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_vendor_time_25_country
    , PERCENTILE_CONT(to_vendor_time, 0.75) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_vendor_time_75_country
    , PERCENTILE_CONT(to_vendor_time, 0.95) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_vendor_time_95_country
    -- At Vendor Time
    , PERCENTILE_CONT(at_vendor_time, 0.5) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_vendor_time_median_country
    , PERCENTILE_CONT(at_vendor_time, 0.05) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_vendor_time_5_country
    , PERCENTILE_CONT(at_vendor_time, 0.25) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_vendor_time_25_country
    , PERCENTILE_CONT(at_vendor_time, 0.75) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_vendor_time_75_country
    , PERCENTILE_CONT(at_vendor_time, 0.95) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_vendor_time_95_country
    -- To Customer Time
    , PERCENTILE_CONT(to_customer_time, 0.5) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_customer_time_median_country
    , PERCENTILE_CONT(to_customer_time, 0.05) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_customer_time_5_country
    , PERCENTILE_CONT(to_customer_time, 0.25) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_customer_time_25_country
    , PERCENTILE_CONT(to_customer_time, 0.75) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_customer_time_75_country
    , PERCENTILE_CONT(to_customer_time, 0.95) OVER (PARTITION BY country_code, entity_display_name, report_week) AS to_customer_time_95_country
    -- At Customer Time
    , PERCENTILE_CONT(at_customer_time, 0.5) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_customer_time_median_country
    , PERCENTILE_CONT(at_customer_time, 0.05) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_customer_time_5_country
    , PERCENTILE_CONT(at_customer_time, 0.25) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_customer_time_25_country
    , PERCENTILE_CONT(at_customer_time, 0.75) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_customer_time_75_country
    , PERCENTILE_CONT(at_customer_time, 0.95) OVER (PARTITION BY country_code, entity_display_name, report_week) AS at_customer_time_95_country
    -- Idle Time
    , PERCENTILE_CONT(idle_time, 0.5) OVER (PARTITION BY country_code, entity_display_name, report_week) AS idle_time_median_country
    , PERCENTILE_CONT(idle_time, 0.05) OVER (PARTITION BY country_code, entity_display_name, report_week) AS idle_time_5_country
    , PERCENTILE_CONT(idle_time, 0.25) OVER (PARTITION BY country_code, entity_display_name, report_week) AS idle_time_25_country
    , PERCENTILE_CONT(idle_time, 0.75) OVER (PARTITION BY country_code, entity_display_name, report_week) AS idle_time_75_country
    , PERCENTILE_CONT(idle_time, 0.95) OVER (PARTITION BY country_code, entity_display_name, report_week) AS idle_time_95_country
    -- Rider Effective Time
    , PERCENTILE_CONT(rider_effective_time, 0.5) OVER (PARTITION BY country_code, entity_display_name, report_week) AS rider_effective_time_median_country
    , PERCENTILE_CONT(rider_effective_time, 0.05) OVER (PARTITION BY country_code, entity_display_name, report_week) AS rider_effective_time_5_country
    , PERCENTILE_CONT(rider_effective_time, 0.25) OVER (PARTITION BY country_code, entity_display_name, report_week) AS rider_effective_time_25_country
    , PERCENTILE_CONT(rider_effective_time, 0.75) OVER (PARTITION BY country_code, entity_display_name, report_week) AS rider_effective_time_75_country
    , PERCENTILE_CONT(rider_effective_time, 0.95) OVER (PARTITION BY country_code, entity_display_name, report_week) AS rider_effective_time_95_country
  FROM merge_orders_timings_data
)
SELECT o.region
  , o.country_code
  , o.country_name
  , o.city_id
  , o.city_name
  , o.zone_id
  , o.zone_name
  , o.entity_id
  , o.entity_display_name
  , o.vertical_type
  , o.vehicle_bag
  , o.is_preorder
  , o.report_date
  , o.report_week
  , o.order_id
  , o.delivery_id
  , o.reaction_time
  , o.to_vendor_time
  , o.at_vendor_time
  , o.to_customer_time
  , o.at_customer_time
  , o.rider_effective_time
  , o.idle_time
  , p.reaction_time_median_country
  , p.reaction_time_5_country
  , p.reaction_time_25_country
  , p.reaction_time_75_country
  , p.reaction_time_95_country
  , p.to_vendor_time_median_country
  , p.to_vendor_time_5_country
  , p.to_vendor_time_25_country
  , p.to_vendor_time_75_country
  , p.to_vendor_time_95_country
  , p.at_vendor_time_median_country
  , p.at_vendor_time_5_country
  , p.at_vendor_time_25_country
  , p.at_vendor_time_75_country
  , p.at_vendor_time_95_country
  , p.to_customer_time_median_country
  , p.to_customer_time_5_country
  , p.to_customer_time_25_country
  , p.to_customer_time_75_country
  , p.to_customer_time_95_country
  , p.idle_time_median_country
  , p.idle_time_5_country
  , p.idle_time_25_country
  , p.idle_time_75_country
  , p.idle_time_95_country
  , p.rider_effective_time_median_country
  , p.rider_effective_time_5_country
  , p.rider_effective_time_25_country
  , p.rider_effective_time_75_country
  , p.rider_effective_time_95_country
  , p.at_customer_time_median_country
  , p.at_customer_time_5_country
  , p.at_customer_time_25_country
  , p.at_customer_time_75_country
  , p.at_customer_time_95_country
FROM merge_orders_timings_data o
LEFT JOIN percentile_dataset p ON o.country_code = p.country_code
  AND o.delivery_id = p.delivery_id
WHERE report_date < '{{ next_ds }}'
