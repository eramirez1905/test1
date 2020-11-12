CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.logistics_dashboard_v2`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 1 YEAR) AS start_date
), dataset AS (
  SELECT o.country_code
    , o.zone_id
    , o.order_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
    , FORMAT_DATE("%G-%V", COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone))) AS start_dateweek
    , FORMAT_DATE("%Y-%m", COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone))) AS start_date_month
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE d.delivery_status = 'completed') AS deliveries
    , (SELECT COUNTIF(t.state = 'courier_notified') FROM UNNEST (deliveries) d LEFT JOIN d.transitions t) AS rider_notified_count
    , (SELECT COUNTIF(t.state = 'accepted') FROM UNNEST (deliveries) d LEFT JOIN d.transitions t) AS rider_accepted_count
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) WHERE stacked_deliveries >= 1) AS stacked_deliveries_count
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) WHERE stacked_deliveries >= 1 AND is_stacked_intravendor IS TRUE) AS stacked_intravendor_count
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) WHERE stacked_deliveries = 1) AS single_stacked_count
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) WHERE stacked_deliveries = 2) AS double_stacked_count
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) WHERE stacked_deliveries = 3) AS triple_stacked_count
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) WHERE stacked_deliveries > 3) AS over_triple_stacked_count
    , d.city_id
    , d.delivery_status
    , d.vehicle.vehicle_bag
    , d.auto_transition.dropoff AS auto_transition_dropoff
    , o.order_status
    , o.timezone
    , o.platform
    , o.entity.display_name AS entity_display_name
    , o.entity.brand_id AS brand_id
    , o.is_preorder
    , o.vendor.vertical_type
    , o.platform_order_id
    , o.platform_order_code
    , d.id AS delivery_id
    , d.rider_near_restaurant_at
    , d.rider_picked_up_at
    , d.rider_id
    , d.rider_near_customer_at
    , o.original_scheduled_pickup_at
    , (o.estimated_prep_time / 60) AS estimated_prep_time_mins
    , (o.timings.estimated_driving_time / 60) AS estimated_driving_time_mins
    , (o.timings.zone_stats.mean_delay) AS mean_delay_mins
    , (o.timings.assumed_actual_preparation_time / 60) AS assumed_actual_preparation_time_mins
    , (o.estimated_prep_buffer / 60) AS estimated_prep_buffer_mins
    , (o.timings.rider_late / 60) AS rider_late_mins
    , (o.timings.vendor_late / 60) AS vendor_late_mins
    , (o.timings.to_vendor_time / 60) AS to_vendor_time_mins
    , (o.timings.at_customer_time / 60) AS at_customer_time_mins
    , (o.timings.to_customer_time / 60) AS to_customer_time_mins
    , (o.timings.promised_delivery_time / 60) AS promised_delivery_time_mins
    , (o.timings.actual_delivery_time / 60) AS actual_delivery_time_mins
    , (o.timings.rider_accepting_time / 60) AS rider_accepting_time_mins
    , (o.timings.rider_reaction_time / 60) AS rider_reaction_time_mins
    , (o.timings.at_vendor_time / 60) AS at_vendor_time_mins
    , (o.timings.at_vendor_time_cleaned /60) AS  at_vendor_time_cleaned_mins
    , (o.timings.order_delay / 60) AS order_delay_mins
    , (o.timings.estimated_courier_delay / 60) AS estimated_delay
    , (o.timings.estimated_walk_in_duration / 60) AS vendor_walk_in_time
    , (o.timings.customer_walk_in_time / 60) AS customer_walk_in_time
    , (o.timings.hold_back_time / 60) AS hold_back_time
    , IF(is_outlier_pickup_distance_manhattan IS FALSE, od.pickup_distance_manhattan, NULL) AS pickup_distance_manhattan_km
    , IF(is_outlier_pickup_distance_google IS FALSE, od.pickup_distance_google, NULL ) AS pickup_distance_google_km
    , IF(is_outlier_dropoff_distance_manhattan IS FALSE, od.dropoff_distance_manhattan, NULL) AS dropoff_distance_manhattan_km
    , IF(is_outlier_dropoff_distance_google IS FALSE, od.dropoff_distance_google, NULL) AS dropoff_distance_google_km
    , IF(is_outlier_delivery_distance IS FALSE, od.delivery_distance, NULL) AS delivery_distance
    , d.rider_accepted_at
    , o.sent_to_vendor_at
    , o.cancellation.reason AS cancellation_reason
    , o.cancellation.source AS cancellation_source
    , CONCAT(CAST('https://' AS STRING), CAST(o.country_code AS STRING), CAST('.usehurrier.com/dispatcher/order_details/' AS STRING), CAST(o.platform_order_code AS STRING)) AS order_url
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d ON d.is_primary
  LEFT JOIN `{{ params.project_id }}.cl._outlier_deliveries` od ON o.country_code = od.country_code
    AND d.id = od.delivery_id
  WHERE o.created_date >= (SELECT start_date FROM parameters)
), final AS (
  SELECT 'hurrier_orders' AS _source
    , co.region
    , co.country_name
    , ci.name AS city_name
    , zo.name AS zone_name
    , d.country_code
    , d.zone_id
    , d.order_id
    , d.report_date
    , d.start_dateweek
    , d.start_date_month
    , d.city_id
    , d.deliveries
    , d.rider_notified_count
    , d.rider_accepted_count
    , d.stacked_deliveries_count
    , d.stacked_intravendor_count
    , d.single_stacked_count
    , d.double_stacked_count
    , d.triple_stacked_count
    , d.over_triple_stacked_count
    , d.delivery_distance
    , d.delivery_status
    , d.vehicle_bag
    , d.auto_transition_dropoff
    , d.order_status
    , d.timezone
    , d.platform
    , d.platform_order_code
    , d.entity_display_name
    , d.brand_id
    , d.is_preorder
    , d.vertical_type
    , d.platform_order_id
    , d.delivery_id
    , d.rider_near_restaurant_at
    , d.rider_picked_up_at
    , d.rider_id
    , d.rider_near_customer_at
    , d.original_scheduled_pickup_at
    , d.estimated_prep_time_mins
    , d.estimated_driving_time_mins
    , d.mean_delay_mins
    , d.assumed_actual_preparation_time_mins
    , d.estimated_prep_buffer_mins
    , d.rider_late_mins
    , d.vendor_late_mins
    , d.to_vendor_time_mins
    , d.at_customer_time_mins
    , d.to_customer_time_mins
    , d.promised_delivery_time_mins
    , d.actual_delivery_time_mins
    , d.rider_accepting_time_mins
    , d.rider_reaction_time_mins
    , d.at_vendor_time_mins
    , d.at_vendor_time_cleaned_mins
    , d.order_delay_mins
    , d.estimated_delay
    , d.vendor_walk_in_time
    , d.customer_walk_in_time
    , IF(is_preorder, NULL, d.hold_back_time) AS hold_back_time
    , IF(d.delivery_status = 'completed', d.pickup_distance_manhattan_km, NULL) AS pickup_distance_manhattan_km
    , IF(d.delivery_status = 'completed', d.pickup_distance_google_km, NULL) AS pickup_distance_google_km
    , IF(d.delivery_status = 'completed', d.dropoff_distance_manhattan_km, NULL) AS dropoff_distance_manhattan_km
    , IF(d.delivery_status = 'completed', d.dropoff_distance_google_km, NULL) AS dropoff_distance_google_km
    , d.rider_accepted_at
    , d.sent_to_vendor_at
    , (SELECT name FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_name
    , (SELECT type FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_type
    , IF(d.delivery_status = 'completed', d.order_id, NULL) AS order_completed
    , IF(d.order_status = 'cancelled', d.order_id, NULL) AS order_cancelled
    , IF(is_preorder IS FALSE AND d.delivery_status = 'completed', d.actual_delivery_time_mins, NULL) AS actual_delivery_time
    , IF(is_preorder IS FALSE, d.promised_delivery_time_mins, NULL) AS promised_delivery_time
    , d.vendor_late_mins AS vendor_late
    , IF(vendor_late_mins > 10, d.order_id, NULL) AS vendor_late_n
    , IF(rider_late_mins > 10, d.order_id, NULL) AS rider_late_n
    , IF(order_delay_mins > 10, d.order_id, NULL) AS order_late_n
    , order_delay_mins AS order_late_d
    , rider_late_mins AS rider_late_d
    , NULL AS working_hours
    , NULL AS break_hours
    , NULL AS working_time
    , NULL AS busy_time
    , d.order_url
    , IF(cancellation_reason IN ('TRAFFIC_MANAGER_NO_RIDER', 'NO_COURIER') AND cancellation_source = 'issue_service' AND d.order_status = 'cancelled', d.order_id, NULL) AS order_failed_to_assign
  FROM dataset d
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON d.country_code = co.country_code
  LEFT JOIN UNNEST(co.cities) ci ON d.city_id = ci.id
  LEFT JOIN UNNEST(ci.zones) zo ON zo.id = d.zone_id
  LEFT JOIN `{{ params.project_id }}.cl.riders` r ON d.country_code = r.country_code
    AND d.rider_id = r.rider_id
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE d.country_code NOT LIKE '%dp%'

  UNION ALL

  SELECT 'rooster' AS _source
    , co.region
    , co.country_name
    , ci.name AS city_name
    , zo.name AS zone_name
    , s.country_code
    , s.zone_id
    , NULL AS order_id
    , e.day AS report_date
    , FORMAT_DATE("%G-%V", e.day) AS start_dateweek
    , FORMAT_DATE("%Y-%m", e.day) AS start_date_month
    , s.city_id
    , NULL AS deliveries
    , NULL AS rider_notified_count
    , NULL AS rider_accepted_count
    , NULL AS stacked_deliveries_count
    , NULL AS stacked_intravendor_count
    , NULL AS double_stacked_count
    , NULL AS triple_stacked_count
    , NULL AS over_triple_stacked_count
    , NULL AS single_stacked_count
    , NULL AS delivery_distance
    , NULL AS delivery_status
    , s.vehicle_bag
    , NULL AS auto_transition_dropoff
    , NULL AS order_status
    , s.timezone
    , NULL AS platform
    , NULL AS platform_order_code
    , NULL AS entity_display_name
    , NULL AS brand_id
    , CAST(NULL AS BOOL) AS is_preorder
    , CAST(NULL AS STRING) AS vertical_type
    , NULL AS platform_order_id
    , NULL AS delivery_id
    , NULL AS rider_near_restaurant_at
    , NULL AS rider_picked_up_at
    , s.rider_id
    , NULL AS rider_near_customer_at
    , NULL AS original_scheduled_pickup_at
    , NULL AS estimated_prep_time_mins
    , NULL AS estimated_driving_time_mins
    , NULL AS mean_delay_mins
    , NULL AS assumed_actual_preparation_time_mins
    , NULL AS estimated_prep_buffer_mins
    , NULL AS rider_late_mins
    , NULL AS vendor_late_mins
    , NULL AS to_vendor_time_mins
    , NULL AS at_customer_time_mins
    , NULL AS to_customer_time_mins
    , NULL AS promised_delivery_time_mins
    , NULL AS actual_delivery_time_mins
    , NULL AS rider_accepting_time_mins
    , NULL AS rider_reaction_time_mins
    , NULL AS at_vendor_time_mins
    , NULL AS at_vendor_time_cleaned_mins
    , NULL AS order_delay_mins
    , NULL AS estimated_delay
    , NULL AS vendor_walk_in_time
    , NULL AS customer_walk_in_time
    , NULL AS hold_back_time
    , NULL AS pickup_distance_manhattan_km
    , NULL AS pickup_distance_google_km
    , NULL AS dropoff_distance_manhattan_km
    , NULL AS dropoff_distance_google_km
    , NULL AS rider_accepted_at
    , NULL AS sent_to_vendor_at
    , (SELECT name FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_name
    , (SELECT type FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_type
    , NULL AS order_completed
    , NULL AS order_cancelled
    , NULL AS actual_delivery_time
    , NULL AS promised_delivery_time
    , NULL AS vendor_late_n
    , NULL AS vendor_late_mins
    , NULL AS rider_late_n
    , NULL AS order_late_n
    , NULL AS order_late_d
    , NULL AS rider_late_d
    , (e.duration / 60 / 60) AS working_hours
    , (b.duration / 60 / 60) AS break_hours
    , NULL AS working_time
    , NULL AS busy_time
    , NULL AS order_url
    , NULL AS order_failed_to_assign
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  LEFT JOIN UNNEST(s.actual_break_time_by_date) b ON e.day = b.day
  LEFT JOIN `{{ params.project_id }}.cl.riders` r ON s.country_code = r.country_code
    AND s.rider_id = r.rider_id
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON s.country_code = co.country_code
  LEFT JOIN UNNEST(co.cities) ci ON s.city_id = ci.id
  LEFT JOIN UNNEST(ci.zones) zo ON s.zone_id = zo.id
  WHERE s.shift_state = 'EVALUATED'
    AND e.status = 'ACCEPTED'
    AND DATE(s.actual_start_at, s.timezone) >= (SELECT start_date FROM parameters)
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND s.country_code NOT LIKE '%dp%'

  UNION ALL

  SELECT 'rider_hurrier_transitions' AS _source
    , b.region
    , b.country_name
    , b.city_name
    , b.zone_name
    , b.country_code
    , b.zone_id
    , NULL AS order_id
    , b.working_day AS report_date
    , FORMAT_DATE("%G-%V", b.working_day) AS start_dateweek
    , FORMAT_DATE("%Y-%m", b.working_day) AS start_date_month
    , b.city_id
    , NULL AS deliveries
    , NULL AS rider_notified_count
    , NULL AS rider_accepted_count
    , NULL AS stacked_deliveries_count
    , NULL AS stacked_intravendor_count
    , NULL AS single_stacked_count
    , NULL AS double_stacked_count
    , NULL AS triple_stacked_count
    , NULL AS over_triple_stacked_count
    , NULL AS delivery_distance
    , NULL AS delivery_status
    , b.vehicle_bag
    , NULL AS auto_transition_dropoff
    , NULL AS order_status
    , b.timezone
    , NULL AS platform
    , NULL AS platform_order_code
    , NULL AS entity_display_name
    , NULL AS brand_id
    , CAST(NULL AS BOOL) AS is_preorder
    , CAST(NULL AS STRING) AS vertical_type
    , NULL AS platform_order_id
    , NULL AS delivery_id
    , NULL AS rider_near_restaurant_at
    , NULL AS rider_picked_up_at
    , b.rider_id
    , NULL AS rider_near_customer_at
    , NULL AS original_scheduled_pickup_at
    , NULL AS estimated_prep_time_mins
    , NULL AS estimated_driving_time_mins
    , NULL AS mean_delay_mins
    , NULL AS assumed_actual_preparation_time_mins
    , NULL AS estimated_prep_buffer_mins
    , NULL AS rider_late_mins
    , NULL AS vendor_late_mins
    , NULL AS to_vendor_time_mins
    , NULL AS at_customer_time_mins
    , NULL AS to_customer_time_mins
    , NULL AS promised_delivery_time_mins
    , NULL AS actual_delivery_time_mins
    , NULL AS rider_accepting_time_mins
    , NULL AS rider_reaction_time_mins
    , NULL AS at_vendor_time_mins
    , NULL AS at_vendor_time_cleaned_mins
    , NULL AS order_delay_mins
    , NULL AS estimated_delay
    , NULL AS vendor_walk_in_time
    , NULL AS customer_walk_in_time
    , NULL AS hold_back_time
    , NULL AS pickup_distance_manhattan_km
    , NULL AS pickup_distance_google_km
    , NULL AS dropoff_distance_manhattan_km
    , NULL AS dropoff_distance_google_km
    , NULL AS rider_accepted_at
    , NULL AS sent_to_vendor_at
    , b.contract_name
    , b.latest_contract_type AS contract_type
    , NULL AS order_completed
    , NULL AS order_cancelled
    , NULL AS actual_delivery_time
    , NULL AS promised_delivery_time
    , NULL AS vendor_late_n
    , NULL AS vendor_late
    , NULL AS rider_late_n
    , NULL AS order_late_n
    , NULL AS order_late_d
    , NULL AS rider_late_d
    , NULL AS working_hours
    , NULL AS break_hours
    , (IF(b.state = 'working', duration, 0)) AS working_time
    , IF(b.state = 'busy', duration, 0) AS busy_time
    , NULL AS order_url
    , NULL AS order_failed_to_assign
  FROM `{{ params.project_id }}.cl._rider_working_time` b
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON b.country_code = co.country_code
  LEFT JOIN UNNEST(co.cities) ci ON b.city_id = ci.id
  LEFT JOIN UNNEST(ci.zones) zo ON b.zone_id = zo.id
  WHERE working_day >= (SELECT start_date FROM parameters)
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND b.country_code NOT LIKE '%dp%'
), data_aggregations AS (
  SELECT region
    , report_date
    , country_name
    , country_code
    , entity_display_name
    , brand_id
    , city_name
    , zone_name
    , start_dateweek
    , start_date_month
    , city_id
    , contract_name
    , contract_type
    , vehicle_bag
    , is_preorder
    , vertical_type
    , _source
    , COUNT(order_completed) AS orders_completed
    , COUNT(order_cancelled) AS orders_cancelled
    , COUNT(order_failed_to_assign) AS orders_failed_to_assign
    , SUM(deliveries) AS deliveries
    , SUM(rider_notified_count) AS rider_notified
    , SUM(rider_accepted_count) AS rider_accepted
    , SUM(stacked_deliveries_count) AS stacked_deliveries
    , SUM(stacked_intravendor_count) AS stacked_intravendor
    , SUM(single_stacked_count) AS single_stacked
    , SUM(double_stacked_count) AS double_stacked
    , SUM(triple_stacked_count) AS triple_stacked
    , SUM(over_triple_stacked_count) AS over_triple_stacked
    , COUNT(vendor_late_n) AS vendor_late_n
    , COUNT(vendor_late) AS vendor_late_d
    , COUNT(rider_late_n) AS rider_late_n
    , COUNT(rider_late_d) AS rider_late_d
    , COUNT(order_late_n) AS order_late_n
    , COUNT(order_late_d) AS order_late_d
    , SUM(estimated_delay) AS estimated_delay_n
    , COUNT(estimated_delay) AS estimated_delay_d
    , SUM(vendor_walk_in_time) AS vendor_walk_in_time_numerator
    , COUNT(IF(vendor_walk_in_time > 0, vendor_walk_in_time, NULL)) AS vendor_walk_in_time_denominator
    , SUM(customer_walk_in_time) AS customer_walk_in_time_numerator
    , COUNT(IF(customer_walk_in_time > 0, customer_walk_in_time, NULL)) AS customer_walk_in_time_denominator
    , COUNT(IF(promised_delivery_time > 0 AND is_preorder IS FALSE, promised_delivery_time, NULL)) AS promised_dt_denominator
    , SUM(IF(promised_delivery_time > 0 AND is_preorder IS FALSE, promised_delivery_time, NULL)) AS promised_dt_numerator
    , SUM(IF(is_preorder IS FALSE, actual_delivery_time, 0)) AS dt_numerator
    , COUNT(IF(actual_delivery_time > 60 AND is_preorder IS FALSE, actual_delivery_time, NULL)) AS dt_over_60_count
    , COUNT(IF(actual_delivery_time < 20 AND is_preorder IS FALSE, actual_delivery_time, NULL)) AS dt_below_20_count
    , COUNT(IF(actual_delivery_time > 45 AND is_preorder IS FALSE, actual_delivery_time, NULL)) AS dt_over_45_count
    , COUNT(IF(is_preorder IS FALSE, order_completed, NULL)) AS dt_denominator
    , (COUNT(order_completed) + COUNT(order_cancelled)) AS gross_orders
    , SUM(assumed_actual_preparation_time_mins) AS assumed_actual_preparation_time_numerator
    , COUNT(assumed_actual_preparation_time_mins) AS assumed_actual_preparation_time_denominator
    , SUM(estimated_prep_time_mins) AS estimated_prep_time_mins_n
    , COUNT(estimated_prep_time_mins) AS estimated_prep_time_mins_d
    , SUM(estimated_prep_buffer_mins) AS estimated_prep_buffer_mins_n
    , COUNT(estimated_prep_buffer_mins) AS estimated_prep_buffer_mins_d
    , SUM(estimated_driving_time_mins) AS estimated_driving_time_mins_n
    , COUNT(estimated_driving_time_mins) AS estimated_driving_time_mins_d
    , SUM(mean_delay_mins) AS mean_delay_mins_n
    , COUNT(mean_delay_mins) AS mean_delay_mins_d
    , SUM(IF(rider_near_restaurant_at IS NOT NULL, to_vendor_time_mins, NULL)) AS to_vendor_time_mins_n
    , COUNT(IF(rider_near_restaurant_at IS NOT NULL, to_vendor_time_mins, NULL)) AS to_vendor_time_mins_d
    , SUM(IF(at_customer_time_mins > 0 AND auto_transition_dropoff IS NULL, at_customer_time_mins, NULL)) AS at_customer_time_mins_n
    , COUNT(IF(at_customer_time_mins > 0 AND auto_transition_dropoff IS NULL, at_customer_time_mins, NULL)) AS at_customer_time_mins_d
    , SUM(IF(rider_near_customer_at IS NOT NULL, to_customer_time_mins, NULL)) AS to_customer_time_mins_n
    , COUNT(IF(rider_near_customer_at IS NOT NULL, to_customer_time_mins, NULL)) AS to_customer_time_mins_d
    , SUM(IF(rider_accepted_at IS NOT NULL, rider_accepting_time_mins, NULL)) AS rider_accepting_time_mins_n
    , COUNT(IF(rider_accepted_at IS NOT NULL, rider_accepting_time_mins, NULL)) AS rider_accepting_time_mins_d
    , SUM(rider_reaction_time_mins) AS rider_reaction_time_mins_n
    , COUNT(rider_reaction_time_mins) AS rider_reaction_time_mins_d
    , SUM(hold_back_time) AS hold_back_time_n
    , COUNT(hold_back_time) AS hold_back_time_d
    , SUM(IF(rider_near_restaurant_at IS NOT NULL, at_vendor_time_mins, NULL)) AS at_vendor_time_mins_n
    , COUNT(IF(rider_near_restaurant_at IS NOT NULL, at_vendor_time_mins, NULL)) AS at_vendor_time_mins_d
    , SUM(IF(rider_near_restaurant_at IS NOT NULL, at_vendor_time_cleaned_mins, NULL)) AS at_vendor_time_cleaned_mins_n
    , COUNT(IF(rider_near_restaurant_at IS NOT NULL, at_vendor_time_cleaned_mins, NULL)) AS at_vendor_time_cleaned_mins_d
    , SUM(delivery_distance) AS delivery_distance_sum
    , COUNT(delivery_distance) AS delivery_distance_count
    , SUM(pickup_distance_manhattan_km) AS pickup_distance_manhattan_km_sum
    , COUNT(pickup_distance_manhattan_km) AS pickup_distance_manhattan_km_count
    , SUM(pickup_distance_google_km) AS pickup_distance_google_km_sum
    , COUNT(pickup_distance_google_km) AS pickup_distance_google_km_count
    , SUM(dropoff_distance_manhattan_km) AS dropoff_distance_manhattan_km_sum
    , COUNT(dropoff_distance_manhattan_km) AS dropoff_distance_manhattan_km_count
    , SUM(dropoff_distance_google_km) AS dropoff_distance_google_km_sum
    , COUNT(dropoff_distance_google_km) AS dropoff_distance_google_km_count
    , SUM(working_hours) AS working_hours_rooster
    , SUM(COALESCE(break_hours,0)) AS break_hours_rooster
    , SUM(working_time) AS working_time_hurrier
    , SUM(busy_time) AS busy_time_hurrier
  FROM final
  WHERE report_date < '{{ next_ds }}'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
), utr_dataset AS (
  SELECT  f.region
    , p.display_name AS entity_display_name
    , p.brand_id
    , report_date
    , f.country_name
    , f.country_code
    , city_name
    , zone_name
    , start_dateweek
    , start_date_month
    , city_id
    , contract_name
    , contract_type
    , vehicle_bag
    , CAST(NULL AS BOOL) AS is_preorder
    , CAST(NULL AS STRING) AS vertical_type
    , 'utr' AS _source
    , SUM(orders_completed) AS orders_completed
    , SUM(working_hours_rooster) AS working_hours_rooster
    , SUM(break_hours_rooster) AS break_hours_rooster
    , SUM(working_time_hurrier) AS working_time_hurrier
    , SUM(busy_time_hurrier) AS busy_time_hurrier
  FROM data_aggregations f
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON f.country_code = co.country_code
  LEFT JOIN UNNEST (co.platforms) p
  WHERE report_date < '{{ next_ds }}'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
)
SELECT region
    , report_date
    , country_name
    , country_code
    , entity_display_name
    , brand_id
    , city_name
    , zone_name
    , start_dateweek
    , start_date_month
    , city_id
    , contract_name
    , contract_type
    , vehicle_bag
    , is_preorder
    , vertical_type
    , _source
    , orders_completed
    , orders_cancelled
    , orders_failed_to_assign
    , deliveries
    , rider_notified
    , rider_accepted
    , stacked_deliveries
    , stacked_intravendor
    , single_stacked
    , double_stacked
    , triple_stacked
    , over_triple_stacked
    , vendor_late_n
    , vendor_late_d
    , rider_late_n
    , rider_late_d
    , order_late_n
    , order_late_d
    , estimated_delay_n
    , estimated_delay_d
    , vendor_walk_in_time_numerator
    , vendor_walk_in_time_denominator
    , customer_walk_in_time_numerator
    , customer_walk_in_time_denominator
    , promised_dt_denominator
    , promised_dt_numerator
    , dt_numerator
    , dt_over_60_count
    , dt_below_20_count
    , dt_over_45_count
    , dt_denominator
    , gross_orders
    , assumed_actual_preparation_time_numerator
    , assumed_actual_preparation_time_denominator
    , estimated_prep_time_mins_n
    , estimated_prep_time_mins_d
    , estimated_driving_time_mins_n
    , estimated_driving_time_mins_d
    , mean_delay_mins_n
    , mean_delay_mins_d
    , estimated_prep_buffer_mins_n
    , estimated_prep_buffer_mins_d
    , to_vendor_time_mins_n
    , to_vendor_time_mins_d
    , at_customer_time_mins_n
    , at_customer_time_mins_d
    , to_customer_time_mins_n
    , to_customer_time_mins_d
    , rider_accepting_time_mins_n
    , rider_accepting_time_mins_d
    , rider_reaction_time_mins_n
    , rider_reaction_time_mins_d
    , hold_back_time_n
    , hold_back_time_d
    , at_vendor_time_mins_n
    , at_vendor_time_mins_d
    , at_vendor_time_cleaned_mins_n
    , at_vendor_time_cleaned_mins_d
    , delivery_distance_sum
    , delivery_distance_count
    , pickup_distance_manhattan_km_sum
    , pickup_distance_manhattan_km_count
    , pickup_distance_google_km_sum
    , pickup_distance_google_km_count
    , dropoff_distance_manhattan_km_sum
    , dropoff_distance_manhattan_km_count
    , dropoff_distance_google_km_sum
    , dropoff_distance_google_km_count
    , working_hours_rooster
    , break_hours_rooster
    , working_time_hurrier
    , busy_time_hurrier
FROM data_aggregations

UNION ALL

SELECT region
  , report_date
  , country_name
  , country_code
  , entity_display_name
  , brand_id
  , city_name
  , zone_name
  , start_dateweek
  , start_date_month
  , city_id
  , contract_name
  , contract_type
  , vehicle_bag
  , NULL AS is_preorder
  , NULL AS vertical_type
  , 'utr' AS _source
  , orders_completed
  , NULL AS orders_cancelled
  , NULL AS orders_failed_to_assign
  , NULL AS deliveries
  , NULL AS rider_notified
  , NULL AS rider_accepted
  , NULL AS stacked_deliveries
  , NULL AS stacked_intravendor
  , NULL AS single_stacked
  , NULL AS double_stacked
  , NULL AS triple_stacked
  , NULL AS over_triple_stacked
  , NULL AS vendor_late_n
  , NULL AS vendor_late_d
  , NULL AS rider_late_n
  , NULL AS rider_late_d
  , NULL AS order_late_n
  , NULL AS order_late_d
  , NULL AS estimated_delay_n
  , NULL AS estimated_delay_d
  , NULL AS vendor_walk_in_time_numerator
  , NULL AS vendor_walk_in_time_denominator
  , NULL AS customer_walk_in_time_numerator
  , NULL AS customer_walk_in_time_denominator
  , NULL AS promised_dt_denominator
  , NULL AS promised_dt_numerator
  , NULL AS dt_numerator
  , NULL AS dt_over_60_count
  , NULL AS dt_below_20_count
  , NULL AS dt_over_45_count
  , NULL AS dt_denominator
  , NULL AS gross_orders
  , NULL AS assumed_actual_preparation_time_numerator
  , NULL AS assumed_actual_preparation_time_denominator
  , NULL AS estimated_prep_time_mins_n
  , NULL AS estimated_prep_time_mins_d
  , NULL AS estimated_driving_time_mins_n
  , NULL AS estimated_driving_time_mins_d
  , NULL AS mean_delay_mins_n
  , NULL AS mean_delay_mins_d
  , NULL AS estimated_prep_buffer_mins_n
  , NULL AS estimated_prep_buffer_mins_d
  , NULL AS to_vendor_time_mins_n
  , NULL AS to_vendor_time_mins_d
  , NULL AS at_customer_time_mins_n
  , NULL AS at_customer_time_mins_d
  , NULL AS to_customer_time_mins_n
  , NULL AS to_customer_time_mins_d
  , NULL AS rider_accepting_time_mins_n
  , NULL AS rider_accepting_time_mins_d
  , NULL AS rider_reaction_time_mins_n
  , NULL AS rider_reaction_time_mins_d
  , NULL AS hold_back_time_n
  , NULL AS hold_back_time_d
  , NULL AS at_vendor_time_mins_n
  , NULL AS at_vendor_time_mins_d
  , NULL AS at_vendor_time_cleaned_mins_n
  , NULL AS at_vendor_time_cleaned_mins_d
  , NULL AS delivery_distance_sum
  , NULL AS delivery_distance_count
  , NULL AS pickup_distance_manhattan_km_sum
  , NULL AS pickup_distance_manhattan_km_count
  , NULL AS pickup_distance_google_km_sum
  , NULL AS pickup_distance_google_km_count
  , NULL AS dropoff_distance_manhattan_km_sum
  , NULL AS dropoff_distance_manhattan_km_count
  , NULL AS dropoff_distance_google_km_sum
  , NULL AS dropoff_distance_google_km_count
  , working_hours_rooster
  , break_hours_rooster
  , working_time_hurrier
  , busy_time_hurrier
FROM utr_dataset f
WHERE report_date < '{{ next_ds }}'

