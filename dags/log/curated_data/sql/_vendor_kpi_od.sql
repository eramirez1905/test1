CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendor_kpi_od`
PARTITION BY created_date_local
CLUSTER BY entity_id, vendor_code AS
WITH orders AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.orders`
), report_timings AS (
  SELECT o.country_code
    , o.order_id
    , vendor.id AS vendor_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS created_date_local
    , DATETIME_TRUNC(COALESCE(DATETIME(d.rider_dropped_off_at, o.timezone),
        DATETIME(d.created_at, o.timezone)), HOUR) AS created_hour_local
  FROM orders o
  LEFT JOIN UNNEST (deliveries) d ON d.is_primary
), reliable_rate_dataset AS (
  SELECT o.country_code AS country_code
    , o.city_id AS city_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS created_date_local
    , TIME_TRUNC(CAST(COALESCE(DATETIME(d.rider_dropped_off_at, o.timezone),
        DATETIME(d.created_at, o.timezone)) AS TIME), HOUR) AS created_hour_local
    , vendor.id AS vendor_id
    , o.order_id
    , o.timings.vendor_late AS vendor_late
    , o.timings.rider_late AS rider_late
  FROM orders o
  LEFT JOIN UNNEST (deliveries) d ON d.is_primary
), reliable_rate AS (
  SELECT d.country_code
    , d.city_id
    , d.created_date_local
    , d.created_hour_local
    , d.vendor_id
    , d.order_id
    , d.vendor_late
    , d.rider_late
    , CASE
        WHEN vendor_late <= 0
          THEN COUNT(order_id)
        WHEN vendor_late IS NULL
          THEN NULL
        WHEN vendor_late > rider_late AND rider_late >= 0 AND (vendor_late - rider_late) < 300
          THEN COUNT(order_id)
        WHEN vendor_late < rider_late AND (vendor_late - rider_late) < 300
          THEN COUNT(order_id)
        ELSE 0
      END AS reliable_rate_n
  FROM reliable_rate_dataset d
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
), near_pickup_data AS (
  SELECT o.country_code AS country_code
    , d.id AS delivery_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS created_date_local
    , COUNTIF(d.auto_transition.pickup) AS near_pickup
  FROM orders o
  LEFT JOIN UNNEST (deliveries) d
  GROUP BY 1, 2, 3
), crossborder_orders AS (
  SELECT b.country_code
    , b.city_id
    , rt.created_date_local
    , rt.created_hour_local
    , rt.vendor_id
    , COUNT(b.order_id) AS crossborder_orders
  FROM `{{ params.project_id }}.cl._crossborder_orders` b
  INNER JOIN report_timings rt USING (country_code, order_id)
  GROUP BY 1, 2, 3, 4, 5
), cities AS (
  SELECT o.country_code AS country_code
    , vendor.id AS vendor_id
    , o.city_id AS city_id
    , COUNT(DISTINCT o.order_id) AS orders_l4w
  FROM orders o
  LEFT JOIN UNNEST (deliveries) d ON d.is_primary
  WHERE d.delivery_status = 'completed'
  GROUP BY 1, 2, 3
), market_share AS (
  SELECT country_code
    , vendor_id
    , city_id
    , orders_l4w
    , SUM(orders_l4w) OVER (PARTITION BY country_code, city_id ORDER BY orders_l4w DESC ROWS BETWEEN UNBOUNDED
        PRECEDING AND CURRENT ROW) AS running_total
    , SUM(orders_l4w) OVER (PARTITION BY country_code, city_id) AS city_total
    , orders_l4w / SUM(orders_l4w) OVER (PARTITION BY country_code, city_id) AS city_market_share
    , NTILE(10) OVER (PARTITION BY country_code, city_id ORDER BY orders_l4w DESC) AS city_decile
    , NTILE(100) OVER (PARTITION BY country_code ORDER BY orders_l4w DESC) AS country_centile
   FROM cities
), dataset AS (
  SELECT o.country_code
    , o.city_id
    , o.entity.id AS entity_id
    , o.vendor.id AS vendor_id
    , vendor.vendor_code
    , vendor.name AS vendor_name
    , vendor.vertical_type
    , IF(('corporate' IN UNNEST (o.tags)) IS TRUE, TRUE, FALSE) AS  is_corporate
    , entity.display_name AS entity_display_name
    , o.order_id
    , o.is_preorder
    , d.id AS delivery_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS created_date_local
    , DATETIME_TRUNC(COALESCE(DATETIME(d.rider_dropped_off_at, o.timezone), DATETIME(d.created_at, o.timezone)), HOUR) AS created_hour_local
    , IF(d.delivery_status = 'completed', o.order_id, NULL) AS successful_orders
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE d.delivery_status = 'completed') AS successful_deliveries
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries)) AS total_deliveries
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE d.delivery_status = 'completed' AND stacked_deliveries >= 1) AS stacked_deliveries
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE d.delivery_status = 'completed' AND stacked_deliveries >= 1
        AND is_stacked_intravendor IS TRUE) AS stacked_deliveries_intravendor
    , IF(o.order_status = 'cancelled', o.order_id, NULL) AS order_cancelled
    , IF(is_preorder IS FALSE, o.timings.promised_delivery_time, NULL) AS promised_delivery_time
    , IF(is_preorder IS FALSE AND d.delivery_status = 'completed', o.timings.actual_delivery_time, NULL) AS delivery_time
    , o.timings.order_delay AS delivery_delay
    , o.timings.rider_late AS rider_late
    , o.timings.vendor_late AS vendor_late
    , IF(o.timings.rider_late > 300 AND o.timings.vendor_late > 300, o.order_id, NULL) AS rider_vendor_late
    , IF(o.timings.rider_late <= 300 AND o.timings.vendor_late <= 300, o.order_id, NULL) AS rider_vendor_on_time
    , IF(o.timings.vendor_late > 0, d.id, NULL) AS net_vendor_late
    , IF(o.timings.rider_late > 0, d.id, NULL) AS net_rider_late
    , IF(is_preorder IS FALSE, o.timings.dispatching_time, NULL) AS dispatching_time
    , IF(o.timings.vendor_late <= 300, d.id, NULL) AS vendor_on_time
    , IF(d.delivery_status = 'completed', d.pickup_distance_manhattan, NULL) AS pickup_distance_manhattan
    , IF(d.delivery_status = 'completed', d.pickup_distance_google, NULL) AS pickup_distance_google
    , IF(d.delivery_status = 'completed', d.dropoff_distance_manhattan, NULL) AS dropoff_distance_manhattan
    , IF(d.delivery_status = 'completed', d.dropoff_distance_google, NULL) AS dropoff_distance_google
    , IF(d.delivery_status = 'completed', d.delivery_distance, NULL)  AS delivery_distance
    , IF(d.delivery_status = 'completed', d.timings.bag_time, NULL) AS bag_time
    , IF(o.timings.rider_accepting_time > 0, o.timings.rider_accepting_time, NULL) AS rider_accepting_time
    , IF(o.timings.to_customer_time > 0, o.timings.to_customer_time, NULL) AS to_customer_time
    , IF(o.timings.to_vendor_time > 0, o.timings.to_vendor_time, NULL) AS to_vendor_time
    , IF(o.timings.at_customer_time > 0, o.timings.at_customer_time, NULL) AS at_customer_time
    , IF(o.timings.at_vendor_time > 0, o.timings.at_vendor_time, NULL) AS at_vendor_time
    , IF(o.timings.at_vendor_time_cleaned > 0, o.timings.at_vendor_time_cleaned, NULL) AS at_vendor_time_cleaned
    , d.rider_near_restaurant_at AS rider_near_restaurant_at
    , r.reliable_rate_n AS reliable_rate_n
    , IF(r.reliable_rate_n IS NOT NULL, r.order_id, NULL) AS reliable_rate_d
    , o.estimated_prep_time AS estimated_prep_time
    , o.estimated_prep_buffer AS estimated_prep_buffer
    , np.near_pickup AS near_pickup
    , o.timings.assumed_actual_preparation_time AS assumed_actual_preparation_time
    , CASE
        WHEN CAST(DATETIME(o.created_at, o.timezone) AS TIME) BETWEEN TIME_ADD(TIME '11:00:00', INTERVAL 30 MINUTE) AND '15:00:00'
          THEN 'lunch'
        WHEN CAST(DATETIME(o.created_at, o.timezone) AS TIME) BETWEEN '18:00:00' AND '23:00:00'
          THEN 'dinner'
        ELSE 'offpeak'
      END AS hour_bucket
  FROM orders o
  LEFT JOIN UNNEST (deliveries) d ON d.is_primary
  LEFT JOIN reliable_rate r ON o.country_code = r.country_code
    AND o.vendor.id = r.vendor_id
    AND o.city_id = r.city_id
    AND o.order_id = r.order_id
  LEFT JOIN near_pickup_data np ON o.country_code = np.country_code
    AND d.id = np.delivery_id
), final_agg AS (
  SELECT d.country_code
    , d.city_id
    , d.entity_id
    , d.entity_display_name
    , d.vendor_id
    , d.vendor_code
    , d.vendor_name
    , d.vertical_type
    , d.is_preorder
    , d.is_corporate
    , d.created_date_local
    , d.created_hour_local
    , hour_bucket
    , COUNT(successful_orders) AS successful_orders
    , COUNT(order_cancelled) AS cancelled_orders
    , SUM(successful_deliveries) AS successful_deliveries
    , SUM(total_deliveries) AS total_deliveries
    , SUM(stacked_deliveries) AS stacked_deliveries
    , SUM(stacked_deliveries_intravendor) AS stacked_deliveries_intravendor
    , SUM(promised_delivery_time) AS promised_delivery_time_sum
    , COUNT(promised_delivery_time) AS promised_delivery_time_count
    , SUM(delivery_time) AS delivery_time_sum
    , COUNT(delivery_time) AS delivery_time_count
    , COUNTIF((delivery_time / 60) > 40) AS delivery_time_over_40_count
    , COUNTIF((delivery_time / 60) > 45) AS delivery_time_over_45_count
    , COUNTIF((delivery_time / 60) > 50) AS delivery_time_over_50_count
    , COUNTIF((delivery_time / 60) > 60) AS delivery_time_over_60_count
    , SUM(delivery_delay) AS delivery_delay_sum
    , COUNT(delivery_delay) AS delivery_delay_count
    , COUNTIF((delivery_delay / 60) > 5) AS delivery_delay_over_5_count
    , COUNTIF((delivery_delay / 60) > 10) AS delivery_delay_over_10_count
    , COUNTIF((delivery_delay / 60) > 15) AS delivery_delay_over_15_count
    , SUM(IF(rider_late IS NOT NULL, rider_late, NULL)) AS rider_late_sum
    , COUNTIF(rider_late IS NOT NULL) AS rider_late_count
    , COUNT(net_rider_late) AS net_rider_late
    , COUNTIF((rider_late / 60) > 5) AS rider_late_over_5_count
    , COUNTIF((rider_late / 60) > 10) AS rider_late_over_10_count
    , COUNTIF((rider_late / 60) > 15) AS rider_late_over_15_count
    , SUM(IF(vendor_late IS NOT NULL, vendor_late, NULL)) AS vendor_late_sum
    , COUNTIF(vendor_late IS NOT NULL) AS vendor_late_count
    , COUNT(net_vendor_late) AS net_vendor_late
    , SUM(IF(vendor_late IS NOT NULL AND stacked_deliveries = 0, vendor_late, NULL)) AS vendor_late_sum_wo_stacked_deliveries
    , COUNTIF(vendor_late IS NOT NULL AND stacked_deliveries = 0) AS vendor_late_count_wo_stacked_deliveries
    , COUNTIF(vendor_late > 0 AND stacked_deliveries = 0) AS net_vendor_late_wo_stacked_deliveries
    , COUNTIF((vendor_late / 60) > 5) AS vendor_late_over_5_count
    , COUNTIF((vendor_late / 60) > 10) AS vendor_late_over_10_count
    , COUNTIF((vendor_late / 60) > 15) AS vendor_late_over_15_count
    , COUNTIF((vendor_late / 60) > 5 AND (vendor_late / 60) <= 10) AS vendor_late_over_5_less_than_10
    , COUNTIF((vendor_late / 60) > 10 AND (vendor_late / 60) <= 15) AS vendor_late_over_10_less_than_15
    , COUNTIF((vendor_late / 60) <= 5) AS vendor_on_time_count
    , COUNTIF((vendor_late / 60) <= 5 AND stacked_deliveries = 0) AS vendor_on_time_count_wo_stacked_deliveries
    , COUNT(rider_vendor_late) AS rider_vendor_late_count
    , COUNT(rider_vendor_on_time) AS rider_vendor_on_time_count
    , COUNTIF(vendor_late IS NOT NULL AND rider_late IS NOT NULL) AS rider_vendor_count
    , SUM(dispatching_time) AS dispatching_time_sum
    , COUNT(dispatching_time) AS dispatching_time_count
    , SUM(pickup_distance_manhattan) AS pickup_distance_manhattan_sum
    , COUNT(pickup_distance_manhattan) AS pickup_distance_manhattan_count
    , SUM(pickup_distance_google) AS pickup_distance_google_sum
    , COUNT(pickup_distance_google) AS pickup_distance_google_count
    , SUM(dropoff_distance_manhattan) AS dropoff_distance_manhattan_sum
    , COUNT(dropoff_distance_manhattan) AS dropoff_distance_manhattan_count
    , SUM(dropoff_distance_google) AS dropoff_distance_google_sum
    , COUNT(dropoff_distance_google) AS dropoff_distance_google_count
    , SUM(delivery_distance) AS delivery_distance_sum
    , COUNT(delivery_distance) AS delivery_distance_count
    , SUM(bag_time) AS bag_time_sum
    , COUNT(bag_time) AS bag_time_count
    , SUM(rider_accepting_time) AS rider_accepting_time_sum
    , COUNT(rider_accepting_time) AS rider_accepting_time_count
    , SUM(to_customer_time) AS to_customer_time_sum
    , COUNT(to_customer_time) AS to_customer_time_count
    , SUM(to_vendor_time) AS to_vendor_time_sum
    , COUNT(to_vendor_time) AS to_vendor_time_count
    , SUM(at_customer_time) AS at_customer_time_sum
    , COUNT(at_customer_time) AS at_customer_time_count
    , SUM(at_vendor_time) AS at_vendor_time_sum
    , COUNT(at_vendor_time) AS at_vendor_time_count
    , SUM(at_vendor_time_cleaned) AS at_vendor_time_cleaned_sum
    , COUNT(at_vendor_time_cleaned) AS at_vendor_time_cleaned_count
    , COUNT(rider_near_restaurant_at) AS rider_near_restaurant_at_count
    , SUM(reliable_rate_n) AS reliable_rate_n
    , COUNT(IF(reliable_rate_n IS NOT NULL, order_id, NULL)) AS reliable_rate_d
    , SUM(estimated_prep_time) AS estimated_prep_time_sum
    , COUNT(estimated_prep_time) AS estimated_prep_time_count
    , SUM(estimated_prep_buffer) AS estimated_prep_buffer_sum
    , COUNT(estimated_prep_buffer) AS estimated_prep_buffer_count
    , SUM(assumed_actual_preparation_time) AS assumed_actual_preparation_time_sum
    , COUNT(assumed_actual_preparation_time) AS assumed_actual_preparation_time_count
    , SUM(near_pickup) AS near_pickup_sum
    , COALESCE(SUM(crossborder_orders), 0) AS cross_border_orders
  FROM dataset d
  LEFT JOIN crossborder_orders co ON d.country_code = co.country_code
    AND d.vendor_id = co.vendor_id
    AND d.created_date_local = co.created_date_local
    AND d.created_hour_local = co.created_hour_local
    AND d.city_id = co.city_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
)
SELECT fa.created_date_local AS created_date_local
  , fa.created_hour_local
  , fa.entity_id AS entity_id
  , vendor_code
  , ARRAY_AGG(
      STRUCT(
        fa.country_code AS country_code
        , fa.city_id
        , ci.name AS city_name
        , fa.hour_bucket
        , vendor_name
        , vertical_type
        , is_preorder
        , is_corporate
        , successful_orders
        , cancelled_orders
        , successful_deliveries
        , total_deliveries
        , stacked_deliveries
        , stacked_deliveries_intravendor
        , promised_delivery_time_sum
        , promised_delivery_time_count
        , delivery_time_sum
        , delivery_time_count
        , delivery_time_over_40_count
        , delivery_time_over_45_count
        , delivery_time_over_50_count
        , delivery_time_over_60_count
        , delivery_delay_sum
        , delivery_delay_count
        , delivery_delay_over_5_count
        , delivery_delay_over_10_count
        , delivery_delay_over_15_count
        , rider_late_sum
        , rider_late_count
        , net_rider_late
        , rider_late_over_5_count
        , rider_late_over_10_count
        , rider_late_over_15_count
        , vendor_late_sum
        , vendor_late_count
        , net_vendor_late
        , vendor_late_sum_wo_stacked_deliveries
        , vendor_late_count_wo_stacked_deliveries
        , net_vendor_late_wo_stacked_deliveries
        , vendor_late_over_5_count
        , vendor_late_over_10_count
        , vendor_late_over_15_count
        , vendor_late_over_5_less_than_10
        , vendor_late_over_10_less_than_15
        , vendor_on_time_count
        , vendor_on_time_count_wo_stacked_deliveries
        , rider_vendor_late_count
        , rider_vendor_on_time_count
        , rider_vendor_count
        , pickup_distance_manhattan_sum AS pickup_distance_sum
        , pickup_distance_manhattan_count AS pickup_distance_count
        , pickup_distance_google_sum
        , pickup_distance_google_count
        , dropoff_distance_manhattan_sum AS dropoff_distance_sum
        , dropoff_distance_manhattan_count AS dropoff_distance_count
        , dropoff_distance_google_sum
        , dropoff_distance_google_count
        , delivery_distance_sum
        , delivery_distance_count
        , bag_time_sum
        , bag_time_count
        , rider_accepting_time_sum AS accepting_time_sum
        , rider_accepting_time_count AS accepting_time_count
        , dispatching_time_sum
        , dispatching_time_count
        , estimated_prep_time_sum
        , estimated_prep_time_count
        , estimated_prep_buffer_sum
        , estimated_prep_buffer_count
        , assumed_actual_preparation_time_sum
        , assumed_actual_preparation_time_count
        , to_vendor_time_sum
        , to_vendor_time_count
        , at_vendor_time_sum
        , at_vendor_time_count
        , at_vendor_time_cleaned_sum
        , at_vendor_time_cleaned_count
        , to_customer_time_sum
        , to_customer_time_count
        , at_customer_time_sum
        , at_customer_time_count
        , near_pickup_sum AS near_pickup
        , cross_border_orders
        , reliable_rate_n
        , reliable_rate_d
        , city_market_share
        , running_total
        , city_total
        , city_decile
        , country_centile
      )
    ) AS own_deliveries
FROM final_agg fa
LEFT JOIN `{{ params.project_id }}.cl.countries` c ON fa.country_code = c.country_code
LEFT JOIN UNNEST (cities) ci ON fa.city_id = ci.id
LEFT JOIN market_share m ON fa.country_code = m.country_code
  AND fa.vendor_id = m.vendor_id
  AND fa.city_id = m.city_id
GROUP BY 1, 2, 3, 4
