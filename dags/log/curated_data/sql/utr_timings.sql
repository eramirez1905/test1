CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.utr_timings`
PARTITION BY created_date AS
WITH orders_data AS (
  SELECT o.region
    , o.country_code
    , c.country_name
    , o.city_id
    , ci.name AS city_name
    , o.zone_id
    , z.name AS zone_name
    , entity.id AS entity_id
    , entity.display_name AS entity_display_name
    , vendor.vertical_type AS vertical_type
    , d.vehicle.vehicle_bag AS vehicle_bag
    , created_date
    , DATE(d.rider_dropped_off_at) AS delivery_date
    , d.created_at
    , o.timezone
    , platform_order_code
    , o.order_id
    , d.id AS delivery_id
    , vendor.id AS vendor_id
    , vendor.vendor_code AS vendor_code
    , d.rider_id
    , d.delivery_status
    , d.rider_notified_at
    , d.rider_accepted_at
    , d.rider_near_restaurant_at
    , d.rider_picked_up_at
    , d.rider_near_customer_at
    , d.rider_dropped_off_at
    , is_preorder
    , d.transitions
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN `{{ params.project_id }}.cl.countries` c ON c.country_code = o.country_code
  LEFT JOIN UNNEST(cities) ci ON ci.id = o.city_id
  LEFT JOIN UNNEST(zones) z ON z.id = o.zone_id
  WHERE d.delivery_status = 'completed'
),  delivery_count_data AS (
  SELECT country_code
    , order_id
    , created_date
    , delivery_id
    , stacked_deliveries
    , MAX(delivery_count) AS delivery_count
    , intravendor_key
  FROM `{{ params.project_id }}.cl._utr_delivery_count`
  GROUP BY 1, 2, 3, 4, 5, 7
), orders_count_merge_data AS (
  -- Merge orders data with delivery count data.
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
    , o.created_date
    , o.delivery_date
    , o.created_at
    , o.timezone
    , o.order_id
    , o.delivery_id
    , o.vendor_id
    , o.vendor_code
    , o.rider_id
    , o.delivery_status
    , o.rider_notified_at
    , o.rider_accepted_at
    , o.rider_near_restaurant_at
    , o.rider_picked_up_at
    , o.rider_near_customer_at
    , o.rider_dropped_off_at
    , d.delivery_count
    , d.intravendor_key
    , o.is_preorder
  FROM orders_data o
  LEFT JOIN delivery_count_data d USING (created_date, country_code, delivery_id)
), calc_timings_data AS (
  SELECT country_code
    , country_name
    , city_id
    , city_name
    , zone_id
    , zone_name
    , entity_id
    , entity_display_name
    , vertical_type
    , vehicle_bag
    , created_date
    , delivery_date
    , created_at
    , timezone
    , order_id
    , delivery_id
    , vendor_id
    , vendor_code
    , rider_id
    , delivery_count
    , intravendor_key
    , is_preorder
    , rider_notified_at
    , rider_accepted_at
    , rider_near_restaurant_at
    , rider_picked_up_at
    , rider_near_customer_at
    , rider_dropped_off_at
    , TIMESTAMP_DIFF(rider_accepted_at, rider_notified_at, SECOND) AS reaction_time
    , TIMESTAMP_DIFF(rider_near_restaurant_at, rider_accepted_at, SECOND) / delivery_count AS to_vendor_time
    , TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND) / delivery_count AS at_vendor_time
    , TIMESTAMP_DIFF(rider_dropped_off_at, rider_near_customer_at, SECOND) AS at_customer_time
  FROM orders_count_merge_data
), timings_flagged_data AS (
  SELECT country_code
    , country_name
    , city_id
    , city_name
    , zone_id
    , zone_name
    , entity_id
    , entity_display_name
    , vertical_type
    , vehicle_bag
    , created_date
    , delivery_date
    , created_at
    , timezone
    , order_id
    , delivery_id
    , vendor_id
    , vendor_code
    , rider_id
    , delivery_count
    , intravendor_key
    , is_preorder
    , rider_notified_at
    , rider_accepted_at
    , rider_near_restaurant_at
    , rider_picked_up_at
    , rider_near_customer_at
    , rider_dropped_off_at
    , reaction_time
    -- Setting flag to consider the timings of the first accepted delivery in the stack.
    , MIN(rider_accepted_at) OVER (PARTITION BY country_code, delivery_date, intravendor_key) = rider_accepted_at
        AS _to_vendor_flag
    , to_vendor_time
    -- Setting flag to consider the timings of the first near restaurant at delivery in the stack.
    , MIN(rider_near_restaurant_at) OVER (PARTITION BY country_code, delivery_date, intravendor_key) =
        rider_near_restaurant_at AS _at_vendor_flag
    , at_vendor_time
    , at_customer_time
  FROM calc_timings_data
), flagged_timings_data AS (
  SELECT country_code
    , country_name
    , city_id
    , city_name
    , zone_id
    , zone_name
    , entity_id
    , entity_display_name
    , vertical_type
    , vehicle_bag
    , created_date
    , delivery_date
    , created_at
    , timezone
    , order_id
    , delivery_id
    , vendor_id
    , vendor_code
    , rider_id
    , delivery_count
    , intravendor_key
    , is_preorder
    , rider_notified_at
    , rider_accepted_at
    , rider_near_restaurant_at
    , rider_picked_up_at
    , rider_near_customer_at
    , rider_dropped_off_at
    , reaction_time
    , _to_vendor_flag
    , to_vendor_time
    , IF(_to_vendor_flag, to_vendor_time, 0) AS _to_vendor_time
    , _at_vendor_flag
    , at_vendor_time
    , IF(_at_vendor_flag, at_vendor_time, 0) AS _at_vendor_time
    , at_customer_time
  FROM timings_flagged_data
), updated_vendor_timings_data AS (
  SELECT country_code
    , country_name
    , city_id
    , city_name
    , zone_id
    , zone_name
    , entity_id
    , entity_display_name
    , vertical_type
    , vehicle_bag
    , created_date
    , delivery_date
    , created_at
    , timezone
    , order_id
    , delivery_id
    , vendor_id
    , vendor_code
    , rider_id
    , delivery_count
    , intravendor_key
    , is_preorder
    , rider_notified_at
    , rider_accepted_at
    , rider_near_restaurant_at
    , rider_picked_up_at
    , rider_near_customer_at
    , rider_dropped_off_at
    , reaction_time
    , _to_vendor_flag
    , to_vendor_time
    , _to_vendor_time
    , MAX(_to_vendor_time) OVER (PARTITION BY country_code, delivery_date, intravendor_key) AS to_vendor_time_new
    , _at_vendor_flag
    , at_vendor_time
    , MAX(_at_vendor_time) OVER (PARTITION BY country_code, delivery_date, intravendor_key) AS at_vendor_time_new
    , at_customer_time
  FROM flagged_timings_data
), to_cust_timing_base_data AS (
  SELECT country_code
    , created_date
    , delivery_id
    , platform_order_code
    , vendor_id
    , o.rider_id AS delivery_rider_id
    , t.rider_id
    , t.state
    , t.created_at
    , LEAD(created_date) OVER (PARTITION BY country_code, o.rider_id ORDER BY o.country_code, o.rider_id, t.created_at) AS next_created_date
    , LEAD(state) OVER (PARTITION BY country_code, o.rider_id ORDER BY o.country_code, o.rider_id, t.created_at) AS next_state
    , LEAD(t.created_at) OVER (PARTITION BY country_code, o.rider_id ORDER BY o.country_code, o.rider_id, t.created_at) AS next_timestamp
    , LEAD(delivery_id) OVER (PARTITION BY country_code, o.rider_id ORDER BY o.country_code, o.rider_id, t.created_at) AS next_delivery
  FROM orders_data o
  LEFT JOIN UNNEST (transitions) t ON o.rider_id = t.rider_id
  WHERE t.state NOT IN ('dispatched', 'queued', 'pending', 'scheduled', 'left_pickup')
    AND o.delivery_status = 'completed'
), calc_to_cust_timings_data AS (
  SELECT country_code
    , created_date
    , delivery_id
    , platform_order_code
    , vendor_id
    , delivery_rider_id
    , rider_id
    , state
    , created_at
    , next_created_date
    , next_state
    , next_timestamp
    , next_delivery
    , TIMESTAMP_DIFF(next_timestamp, created_at, SECOND) AS to_cust_time
  FROM to_cust_timing_base_data
  WHERE next_state = 'near_dropoff'
    AND state IN ('accepted', 'picked_up', 'completed', 'near_dropoff', 'near_pickup')
), merge_timings_data AS (
  SELECT vt.country_code
    , country_name
    , city_id
    , city_name
    , zone_id
    , zone_name
    , entity_id
    , entity_display_name
    , vertical_type
    , vehicle_bag
    , vt.created_date
    , delivery_date
    , vt.created_at
    , timezone
    , order_id
    , vt.delivery_id
    , ct.delivery_id AS cust_delivery_id
    , next_delivery
    , vt.vendor_id
    , vt.vendor_code
    , vt.rider_id
    , delivery_count
    , intravendor_key
    , is_preorder
    , rider_notified_at
    , rider_accepted_at
    , rider_near_restaurant_at
    , rider_picked_up_at
    , rider_near_customer_at
    , rider_dropped_off_at
    , reaction_time
    , to_vendor_time_new AS to_vendor_time
    , at_vendor_time_new AS at_vendor_time
    , to_cust_time AS to_customer_time
    , at_customer_time
  FROM updated_vendor_timings_data vt
  LEFT JOIN calc_to_cust_timings_data ct ON vt.created_date = ct.next_created_date
    AND vt.country_code = ct.country_code
    AND vt.delivery_id = ct.next_delivery
), rider_working_time AS (
  SELECT working_day
    , country_code
    , SUM(IF(state = 'working', (duration * 60), 0)) AS working_time
    , SUM(IF(state = 'busy', (duration * 60), 0)) AS busy_time
    , SUM(IF(state = 'break', (duration * 60), 0)) AS break_time
  FROM `{{ params.project_id }}.cl._rider_working_time`
  GROUP BY 1, 2
), merge_orders_working_time_data AS(
  SELECT o.country_code
    , DATE(o.rider_dropped_off_at, o.timezone) AS delivery_date
    , w.working_day
    , COUNT(o.delivery_id)
    , MAX(w.working_time) / COUNT(delivery_id) AS working_time
    , MAX(w.busy_time) / COUNT(delivery_id) AS busy_time
    , MAX(w.break_time) / COUNT(delivery_id) AS break_time
  FROM orders_data o
  LEFT JOIN rider_working_time w ON DATE(o.rider_dropped_off_at, o.timezone) = w.working_day
    AND o.country_code = w.country_code
  GROUP BY 1, 2, 3
), costs_rider_working_time AS (
  SELECT FORMAT_DATE("%G-%V", working_day) AS working_week
    , country_code
    , city_id
    , SUM(IF(state = 'working', (duration * 60), 0)) AS working_time
    , SUM(IF(state = 'busy', (duration * 60), 0)) AS busy_time
    , SUM(IF(state = 'break', (duration * 60), 0)) AS break_time
  FROM `{{ params.project_id }}.cl._rider_working_time`
  GROUP BY 1, 2, 3
), merge_costs_data AS(
  SELECT t.country_code
    , t.city_id
    , t.created_week AS payment_week
    , SUM(total_costs) AS total_weekly_costs
    , SUM(total_costs_eur) AS total_weekly_costs_eur
    , SUM(working_time) AS total_weekly_working_seconds
  FROM `{{ params.project_id }}.cl._rider_weekly_costs` t
  LEFT JOIN costs_rider_working_time cwt ON t.country_code = cwt.country_code
    AND t.created_week = cwt.working_week
    AND t.city_id = cwt.city_id
  GROUP BY 1, 2, 3
), utr_timings_dataset AS (
  SELECT mt.country_code
    , mt.country_name
    , mt.city_id
    , mt.city_name
    , mt.zone_id
    , mt.zone_name
    , mt.entity_id
    , mt.entity_display_name
    , mt.vertical_type
    , mt.vehicle_bag
    , mt.created_date
    , mt.delivery_date
    , mt.created_at
    , mt.timezone
    , mt.order_id
    , mt.delivery_id
    , mt.vendor_id
    , mt.vendor_code
    , mt.rider_id
    , mt.delivery_count
    , mt.intravendor_key
    , mt.is_preorder
    , mt.rider_notified_at
    , mt.rider_accepted_at
    , mt.rider_near_restaurant_at
    , mt.rider_picked_up_at
    , mt.rider_near_customer_at
    , mt.rider_dropped_off_at
    , ROUND(mt.reaction_time, 3) AS reaction_time
    , ROUND(mt.to_vendor_time, 3) AS to_vendor_time
    , ROUND(mt.at_vendor_time, 3) AS at_vendor_time
    , ROUND(mt.to_customer_time, 3) AS to_customer_time
    , ROUND(mt.at_customer_time, 3) AS at_customer_time
    , ROUND(mwt.working_time, 3) AS working_time
    , ROUND(mwt.busy_time, 3) AS busy_time
    , ROUND(mwt.break_time, 3) AS break_time
    , ROUND(mwt.working_time -  mwt.busy_time, 3) AS idle_time
    , ROUND((mt.reaction_time + mt.to_vendor_time + mt.at_vendor_time + mt.to_customer_time + mt.at_customer_time), 3) AS rider_effective_time
    -- for cost purposes we need the total time rider spends on a delivery including the assigned idle time becasue this column is compared
    -- with the total working time for delivery_costs and delivery_costs_eur columns.
    , ROUND(reaction_time + to_vendor_time + at_vendor_time + to_customer_time + at_customer_time
       + (mwt.working_time -  mwt.busy_time), 3) AS costs_rider_effective_time
    -- as we are replicating total_weekly_costs and total_weekly_working_seconds by delivery, using any_value to get only
    -- as single value per delivery
    , (SELECT AS STRUCT ANY_VALUE(CAST(total_weekly_costs AS NUMERIC)) AS total_weekly_costs
        , ANY_VALUE(CAST(total_weekly_costs_eur AS NUMERIC)) AS total_weekly_costs_eur
        , ANY_VALUE(CAST(total_weekly_working_seconds AS NUMERIC)) AS total_weekly_working_seconds
       FROM merge_costs_data m
       WHERE m.country_code = mt.country_code
         AND FORMAT_DATE("%G-%V", mt.delivery_date) = m.payment_week
         AND m.city_id = mt.city_id
       ) AS cpo_dataset
  FROM merge_timings_data mt
  LEFT JOIN merge_orders_working_time_data mwt USING (country_code, delivery_date)
)
SELECT country_code
  , country_name
  , city_id
  , city_name
  , zone_id
  , zone_name
  , entity_id
  , entity_display_name
  , vertical_type
  , vehicle_bag
  , created_date
  , delivery_date
  , created_at
  , timezone
  , order_id
  , delivery_id
  , vendor_id
  , vendor_code
  , rider_id
  , delivery_count
  , intravendor_key
  , is_preorder
  , rider_notified_at
  , rider_accepted_at
  , rider_near_restaurant_at
  , rider_picked_up_at
  , rider_near_customer_at
  , rider_dropped_off_at
  , reaction_time
  , to_vendor_time
  , at_vendor_time
  , to_customer_time
  , at_customer_time
  , working_time
  , busy_time
  , break_time
  , idle_time
  , rider_effective_time
  , ROUND(cpo_dataset.total_weekly_costs * SAFE_DIVIDE(costs_rider_effective_time, cpo_dataset.total_weekly_working_seconds), 3) AS delivery_costs
  , ROUND(cpo_dataset.total_weekly_costs_eur * SAFE_DIVIDE(costs_rider_effective_time, cpo_dataset.total_weekly_working_seconds), 3) AS delivery_costs_eur
FROM utr_timings_dataset
