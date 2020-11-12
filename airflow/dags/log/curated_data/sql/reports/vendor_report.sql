CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.vendor_report`
PARTITION BY report_date
CLUSTER BY country_code, vendor_id AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK) AS start_date
    , CAST('{{ next_ds }}' AS DATE) AS end_date
), orders AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.orders`
  WHERE created_date >=  DATE_SUB((SELECT start_date FROM parameters), INTERVAL 1 DAY)
), report_timings AS (
  SELECT o.country_code
    , o.order_id
    , vendor.id AS vendor_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
    , TIME_TRUNC(CAST(COALESCE(DATETIME(d.rider_dropped_off_at, o.timezone), DATETIME(d.created_at, o.timezone)) AS TIME), HOUR) AS report_time
  FROM orders o
  LEFT JOIN UNNEST(deliveries) d ON d.is_primary IS TRUE
  WHERE CAST(DATETIME(d.created_at, d.timezone) AS DATE) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
), reliable_rate_dataset AS (
  SELECT o.country_code AS country_code
    , o.city_id AS city_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
    , TIME_TRUNC(CAST(COALESCE(DATETIME(d.rider_dropped_off_at, o.timezone), DATETIME(d.created_at, o.timezone)) AS TIME), HOUR) AS report_time
    , vendor.id AS vendor_id
    , o.order_id
    , o.timings.vendor_late
    , o.timings.rider_late
  FROM orders o
  LEFT JOIN UNNEST(deliveries) d ON d.is_primary IS TRUE
), reliable_rate AS (
  SELECT d.country_code AS country_code
    , d.city_id AS city_id
    , d.report_date AS report_date
    , d.report_time AS report_time
    , d.vendor_id AS vendor_id
    , d.order_id AS order_id
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
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
    , COUNTIF(d.auto_transition.pickup IS TRUE) AS near_pickup
  FROM orders o
  LEFT JOIN UNNEST(deliveries) d
  WHERE CAST(DATETIME(d.created_at, d.timezone) AS DATE) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
  GROUP BY 1, 2, 3
), crossborder_orders AS (
  SELECT b.country_code
    , b.city_id
    , rt.report_date
    , rt.report_time
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
  LEFT JOIN UNNEST(deliveries) d ON d.is_primary IS TRUE
  WHERE d.delivery_status = 'completed'
    AND CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
  GROUP BY 1, 2, 3
), market_share AS (
  SELECT country_code
    , vendor_id
    , city_id
    , orders_l4w
    , SUM(orders_l4w) OVER (PARTITION BY country_code, city_id ORDER BY orders_l4w DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
    , SUM(orders_l4w) OVER (PARTITION BY country_code, city_id) AS city_total
    , orders_l4w / SUM(orders_l4w) OVER (PARTITION BY country_code, city_id) AS city_market_share
    , NTILE(10) OVER (PARTITION BY country_code, city_id ORDER BY orders_l4w DESC) AS city_decile
    , NTILE(100) OVER (PARTITION BY country_code ORDER BY orders_l4w DESC) AS country_centile
   FROM cities
), dataset AS (
  SELECT o.country_code AS country_code
    , o.city_id AS city_id
    , vendor.id AS vendor_id
    , vendor.vendor_code AS vendor_code
    , vendor.name AS vendor_name
    , vendor.vertical_type AS vertical_type
    , IF(('corporate' IN UNNEST(o.tags)) IS TRUE, 1, 0) AS  is_corporate
    , entity.display_name AS entity_display_name
    , o.order_id AS order_id
    , o.is_preorder
    , o.created_date AS created_date
    , o.created_at
    , d.id AS delivery_id
    , d.created_at AS delivery_created_at
    , DATE(d.rider_dropped_off_at) AS delivery_date
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
    , TIME_TRUNC(CAST(COALESCE(DATETIME(d.rider_dropped_off_at, o.timezone), DATETIME(d.created_at, o.timezone)) AS TIME), HOUR) AS report_time
    , FORMAT_DATE("%G-%V", (COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)))) AS delivery_week
    , CASE
        WHEN FORMAT_DATE("%G-%V", (COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)))) = FORMAT_DATE("%G-%V", '{{ next_ds }}')
          THEN 'current_week'
        WHEN FORMAT_DATE("%G-%V", (COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)))) = FORMAT_DATE("%G-%V", DATE_SUB('{{ next_ds }}', INTERVAL 1 WEEK))
          THEN '1_week_ago'
        WHEN FORMAT_DATE("%G-%V", (COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)))) = FORMAT_DATE("%G-%V", DATE_SUB('{{ next_ds }}', INTERVAL 2 WEEK))
          THEN '2_weeks_ago'
        ELSE FORMAT_DATE("%G-%V", (COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone))))
      END AS week_relative
    , FORMAT_DATE("%Y-%m", (COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)))) AS delivery_month
    , FORMAT_DATE("%A", (COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)))) AS delivery_weekday
    , IF(d.delivery_status = 'completed', o.order_id, NULL) AS successful_orders
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE d.delivery_status = 'completed') AS successful_deliveries
    , IF(stacked_deliveries >= 1, d.id, NULL) AS stacked_deliveries
    , IF(stacked_deliveries >= 1 AND is_stacked_intravendor IS TRUE, d.id, NULL) AS stacked_deliveries_intravendor
    , IF(o.order_status = 'cancelled', o.order_id, NULL) AS order_cancelled
    , IF(is_preorder IS FALSE, o.timings.promised_delivery_time / 60, NULL) AS promised_delivery_time_mins
    , IF(is_preorder IS FALSE AND d.delivery_status = 'completed', o.timings.actual_delivery_time / 60, NULL) AS delivery_time_mins
    , (o.timings.order_delay / 60) AS delivery_delay_mins
    , (o.timings.rider_late / 60) AS rider_late_mins
    , (o.timings.vendor_late / 60) AS vendor_late_mins
    , IF(o.timings.rider_late > 300 AND o.timings.vendor_late > 300, o.order_id, NULL) AS rider_vendor_late
    , IF(o.timings.vendor_late > 0, d.id, NULL) AS net_vendor_late
    , IF(o.timings.rider_late > 0, d.id, NULL) AS net_rider_late
    , IF(is_preorder IS FALSE, o.timings.dispatching_time / 60, NULL) AS dispatching_time
    , IF(o.timings.vendor_late < 5, d.id, NULL) AS vendor_on_time
    , IF(d.delivery_status = 'completed', pickup_distance_manhattan / 1000, NULL) AS pickup_distance_manhattan_kms
    , IF(d.delivery_status = 'completed', pickup_distance_google / 1000, NULL) AS pickup_distance_google_kms
    , IF(d.delivery_status = 'completed', dropoff_distance_manhattan / 1000, NULL) AS dropoff_distance_manhattan_kms
    , IF(d.delivery_status = 'completed', dropoff_distance_google / 1000, NULL) AS dropoff_distance_google_kms
    , IF(d.delivery_status = 'completed', d.delivery_distance / 1000, NULL)  AS delivery_distance_kms
    , IF(d.delivery_status ='completed', d.timings.bag_time / 60, NULL) AS bag_time_mins
    , IF(o.timings.rider_accepting_time > 0, o.timings.rider_accepting_time / 60, NULL) AS rider_accepting_time_mins
    , IF(o.timings.to_customer_time > 0, o.timings.to_customer_time / 60, NULL) AS to_customer_time_mins
    , IF(o.timings.to_vendor_time > 0, o.timings.to_vendor_time / 60, NULL) AS to_vendor_time_mins
    , IF(o.timings.at_customer_time > 0, o.timings.at_customer_time / 60, NULL) AS at_customer_time_mins
    , IF(o.timings.at_vendor_time > 0, o.timings.at_vendor_time / 60, NULL) AS at_vendor_time_mins
    , IF(o.timings.at_vendor_time_cleaned > 0, o.timings.at_vendor_time_cleaned / 60, NULL) AS at_vendor_time_cleaned_mins
    , d.rider_near_restaurant_at AS rider_near_restaurant_at
    , r.reliable_rate_n AS reliable_rate_n
    , IF(r.reliable_rate_n IS NOT NULL, r.order_id, NULL) AS reliable_rate_d
    , (o.estimated_prep_time / 60) AS estimated_prep_time_mins
    , (o.estimated_prep_buffer / 60) AS estimated_prep_buffer_mins
    , np.near_pickup AS near_pickup
    , (o.timings.assumed_actual_preparation_time / 60) AS assumed_actual_preparation_time_mins
    , CASE
        WHEN CAST(DATETIME(o.created_at, o.timezone) AS TIME) BETWEEN TIME_ADD(TIME '11:00:00', INTERVAL 30 MINUTE) AND '15:00:00'
          THEN 'lunch'
        WHEN CAST(DATETIME(o.created_at, o.timezone) AS TIME) BETWEEN '18:00:00' AND '23:00:00'
          THEN 'dinner'
        ELSE 'offpeak'
      END AS hour_bucket
  FROM orders o
  LEFT JOIN UNNEST(deliveries) d ON d.is_primary IS TRUE
  LEFT JOIN reliable_rate r ON o.country_code = r.country_code
    AND vendor.id = r.vendor_id
    AND report_date = r.report_date
    AND report_time = r.report_time
    AND o.city_id = r.city_id
    AND o.order_id = r.order_id
  LEFT JOIN near_pickup_data np ON o.country_code = np.country_code
    AND d.id = np.delivery_id
), final_agg AS (
  SELECT d.country_code AS country_code
    , d.city_id AS city_id
    , d.vendor_id AS vendor_id
    , d.vendor_code AS vendor_code
    , d.vendor_name AS vendor_name
    , d.vertical_type AS vertical_type
    , d.is_preorder
    , d.entity_display_name AS entity_display_name
    , is_corporate
    , d.report_date AS report_date
    , d.report_time AS report_time
    , delivery_date
    , delivery_weekday
    , delivery_week
    , week_relative
    , delivery_month
    , hour_bucket
    , COUNT(successful_orders) AS successful_orders
    , COUNT(order_cancelled) AS cancelled_orders
    , SUM(successful_deliveries) AS successful_deliveries
    , COUNT(stacked_deliveries) AS stacked_deliveries
    , COUNT(stacked_deliveries_intravendor) AS stacked_deliveries_intravendor
    , SUM(promised_delivery_time_mins) AS promised_delivery_time_sum
    , COUNT(promised_delivery_time_mins) AS promised_delivery_time_count
    , SUM(delivery_time_mins) AS delivery_time_sum
    , COUNT(delivery_time_mins) AS delivery_time_count
    , COUNT(IF(delivery_time_mins > 40, delivery_time_mins, NULL)) AS delivery_time_over_40_count
    , COUNT(IF(delivery_time_mins > 45, delivery_time_mins, NULL)) AS delivery_time_over_45_count
    , COUNT(IF(delivery_time_mins > 50, delivery_time_mins, NULL)) AS delivery_time_over_50_count
    , COUNT(IF(delivery_time_mins > 60, delivery_time_mins, NULL)) AS delivery_time_over_60_count
    , SUM(delivery_delay_mins) AS delivery_delay_sum
    , COUNT(delivery_delay_mins) AS delivery_delay_count
    , COUNT(IF(delivery_delay_mins > 5, delivery_delay_mins, NULL)) AS delivery_delay_over_5_count
    , COUNT(IF(delivery_delay_mins > 10, delivery_delay_mins, NULL)) AS delivery_delay_over_10_count
    , COUNT(IF(delivery_delay_mins > 15, delivery_delay_mins, NULL)) AS delivery_delay_over_15_count
    , SUM(IF(rider_late_mins IS NOT NULL, rider_late_mins, NULL)) AS rider_late_sum
    , COUNT(IF(rider_late_mins IS NOT NULL, rider_late_mins, NULL)) AS rider_late_count
    , COUNT(IF(rider_late_mins > 5, rider_late_mins, NULL)) AS rider_late_over_5_count
    , COUNT(IF(rider_late_mins > 10, rider_late_mins, NULL)) AS rider_late_over_10_count
    , COUNT(IF(rider_late_mins > 15, rider_late_mins, NULL)) AS rider_late_over_15_count
    , COUNT(IF(vendor_late_mins > 5, vendor_late_mins, NULL)) AS vendor_late_over_5_count
    , COUNT(IF(vendor_late_mins > 10, vendor_late_mins, NULL)) AS vendor_late_over_10_count
    , COUNT(IF(vendor_late_mins > 15, vendor_late_mins, NULL)) AS vendor_late_over_15_count
    , SUM(IF(vendor_late_mins IS NOT NULL, vendor_late_mins, NULL)) AS vendor_late_sum
    , COUNT(IF(vendor_late_mins IS NOT NULL, vendor_late_mins, NULL)) AS vendor_late_count
    , COUNT(rider_vendor_late) AS rider_vendor_late
    , COUNT(net_vendor_late) AS net_vendor_late
    , COUNT(net_rider_late) AS net_rider_late
    , COUNT(IF(vendor_late_mins < 5, vendor_late_mins, NULL)) AS vendor_on_time_count
    , COUNT(IF(vendor_late_mins > 5 AND vendor_late_mins < 10
        AND vendor_late_mins < 15, vendor_late_mins, NULL)
        ) AS vendor_late_over_5_less_than_10_or_15
    , COUNT(IF(vendor_late_mins > 10 AND vendor_late_mins < 15, vendor_late_mins, NULL)) AS vendor_late_over_10_less_than_15
    , SUM(dispatching_time) AS dispatching_time_sum
    , COUNT(dispatching_time) AS dispatching_time_count
    , SUM(pickup_distance_manhattan_kms) AS pickup_distance_manhattan_sum
    , COUNT(pickup_distance_manhattan_kms) AS pickup_distance_manhattan_count
    , SUM(pickup_distance_google_kms) AS pickup_distance_google_sum
    , COUNT(pickup_distance_google_kms) AS pickup_distance_google_count
    , SUM(dropoff_distance_manhattan_kms) AS dropoff_distance_manhattan_sum
    , COUNT(dropoff_distance_manhattan_kms) AS dropoff_distance_manhattan_count
    , SUM(dropoff_distance_google_kms) AS dropoff_distance_google_sum
    , COUNT(dropoff_distance_google_kms) AS dropoff_distance_google_count
    , SUM(delivery_distance_kms) AS delivery_distance_sum
    , COUNT(delivery_distance_kms) AS delivery_distance_count
    , SUM(bag_time_mins) AS bag_time_sum
    , COUNT(bag_time_mins) AS bag_time_count
    , SUM(rider_accepting_time_mins) AS rider_accepting_time_sum
    , COUNT(rider_accepting_time_mins) AS rider_accepting_time_count
    , SUM(to_customer_time_mins) AS to_customer_time_sum
    , COUNT(to_customer_time_mins) AS to_customer_time_count
    , SUM(to_vendor_time_mins) AS to_vendor_time_sum
    , COUNT(to_vendor_time_mins) AS to_vendor_time_count
    , SUM(at_customer_time_mins) AS at_customer_time_sum
    , COUNT(at_customer_time_mins) AS at_customer_time_count
    , SUM(at_vendor_time_mins) AS at_vendor_time_sum
    , COUNT(at_vendor_time_mins) AS at_vendor_time_count
    , SUM(at_vendor_time_cleaned_mins) AS at_vendor_time_cleaned_sum
    , COUNT(at_vendor_time_cleaned_mins) AS at_vendor_time_cleaned_count
    , COUNT(rider_near_restaurant_at) AS rider_near_restaurant_at_count
    , SUM(reliable_rate_n) AS reliable_rate_n
    , COUNT(IF(reliable_rate_n IS NOT NULL, order_id, NULL)) AS reliable_rate_d
    , SUM(estimated_prep_time_mins) AS estimated_prep_time_sum
    , COUNT(estimated_prep_time_mins) AS estimated_prep_time_count
    , SUM(estimated_prep_buffer_mins) AS estimated_prep_buffer_sum
    , COUNT(estimated_prep_buffer_mins) AS estimated_prep_buffer_count
    , SUM(assumed_actual_preparation_time_mins) AS assumed_actual_preparation_time_sum
    , COUNT(assumed_actual_preparation_time_mins) AS assumed_actual_preparation_time_count
    , SUM(near_pickup) AS near_pickup_sum
    , COALESCE(SUM(crossborder_orders), 0) AS cross_border_orders
  FROM dataset d
  LEFT JOIN crossborder_orders co ON d.country_code = co.country_code
    AND d.vendor_id = co.vendor_id
    AND d.report_date = co.report_date
    AND d.report_time = co.report_time
    AND d.city_id = co.city_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
)
SELECT c.country_name AS country_name
  , fa.country_code AS country_code
  , fa.city_id AS hu_city_id
  , ci.name AS city
  , fa.entity_display_name AS entity_display_name
  , fa.vendor_id AS vendor_id
  , fa.report_date AS report_date
  , CAST(fa.report_time AS STRING) AS report_time
  , fa.delivery_date AS delivery_date
  , fa.hour_bucket AS hour_bucket
  , delivery_week
  , week_relative
  , delivery_month
  , delivery_weekday
  , vendor_name
  , vendor_code
  , vertical_type
  , is_preorder
  , is_corporate
  , successful_orders
  , cancelled_orders
  , successful_deliveries
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
  , delivery_delay_over_5_count
  , delivery_delay_over_10_count
  , delivery_delay_over_15_count
  , delivery_delay_count
  , rider_late_over_5_count
  , rider_late_over_10_count
  , rider_late_over_15_count
  , rider_late_sum
  , rider_late_count
  , vendor_late_over_5_count
  , vendor_late_over_10_count
  , vendor_late_over_15_count
  , vendor_late_over_5_less_than_10_or_15
  , vendor_late_over_10_less_than_15
  , vendor_late_count
  , rider_vendor_late
  , net_vendor_late
  , net_rider_late
  , vendor_on_time_count AS on_time
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
  , to_customer_time_sum AS d_driving_to_customer_time_sum
  , to_customer_time_count AS d_driving_to_customer_time_count
  , to_vendor_time_sum AS d_driving_to_restaurant_time_sum
  , to_vendor_time_count AS d_driving_to_restaurant_time_count
  , at_customer_time_sum AS o_at_customer_time_sum
  , at_customer_time_count AS o_at_customer_time_count
  , at_vendor_time_sum AS o_at_restaurant_time_sum
  , at_vendor_time_count AS o_at_restaurant_time_count
  , at_vendor_time_cleaned_sum AS o_at_restaurant_time_cleaned_sum
  , at_vendor_time_cleaned_count AS o_at_restaurant_time_cleaned_count
  , estimated_prep_time_sum AS estimated_prep_duration_sum
  , estimated_prep_time_count
  , estimated_prep_buffer_sum
  , estimated_prep_buffer_count
  , assumed_actual_preparation_time_sum
  , assumed_actual_preparation_time_count
  , vendor_late_sum AS sum_vendor_late_mins_new
  , near_pickup_sum AS near_pickup
  , cross_border_orders
  , reliable_rate_n
  , reliable_rate_d
  , city_market_share
  , running_total/ city_total AS restaurant_running_percentage
  , city_decile
  , country_centile
FROM final_agg fa
LEFT JOIN `{{ params.project_id }}.cl.countries` c ON fa.country_code = c.country_code
LEFT JOIN UNNEST(cities) ci ON fa.city_id = ci.id
LEFT JOIN market_share m ON fa.country_code = m.country_code
  AND fa.vendor_id = m.vendor_id
  AND fa.city_id = m.city_id
WHERE fa.report_date > (SELECT start_date FROM parameters)
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  AND fa.country_code NOT LIKE '%dp%'
