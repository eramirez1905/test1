CREATE OR REPLACE TABLE il.rider_payroll_report_orders
PARTITION BY report_date AS
  SELECT ops_o.country_code
    , co.country_name AS country
    , c.city_name
    , zo.zone_name
    , FORMAT_TIME("%T", CAST(DATETIME(COALESCE(d.food_delivered, d.created_at), ops_o.timezone) AS TIME)) AS report_hour
    , FORMAT_DATE("%G-%V", CAST(DATETIME(COALESCE(ops_o.food_delivered, ops_o.created_at), ops_o.timezone) AS DATE)) AS report_week
    , FORMAT_DATE("%A", CAST(DATETIME(COALESCE(ops_o.food_delivered, ops_o.created_at), ops_o.timezone) AS DATE)) AS day_of_the_week
    , FORMAT_DATE("%Y-%m", CAST(DATETIME(COALESCE(ops_o.food_delivered, ops_o.created_at), ops_o.timezone) AS DATE)) AS report_month
    , CAST(DATETIME(COALESCE(ops_o.food_delivered, ops_o.created_at), ops_o.timezone) AS DATE) AS report_date
    , FORMAT_TIME("%T", CAST(DATETIME(COALESCE(ops_o.food_delivered, ops_o.created_at), ops_o.timezone) AS TIME)) AS report_time
    , COALESCE(CONCAT(ops_o.country_code, '-', CAST(r.rider_id AS STRING)), 'N/A') AS rider_uuid
    , COALESCE(CAST(r.rider_id AS STRING), 'N/A') AS rider_id
    , COALESCE(r.first_name, 'N/A') AS rider_name
    , COALESCE(r.email_address, 'N/A') AS email_address
    , CAST(s.schedule_start_time_local AS DATE) AS shift_start_date
    , ops_o.order_code
    , d.delivery_id
    , d.delivery_status
    , ops_o.rider_tip_local AS tips
    , d.delivery_distance AS delivery_distance
    , d.pickup_distance
    , CASE WHEN d.redelivery THEN 1 ELSE 0 END AS redelivery
    , CASE WHEN d.delivery_status = 'completed' THEN 1 ELSE 0 END AS completed_delivery_flag
    , CASE WHEN d.delivery_status = 'cancelled' AND d.food_picked_up IS NOT NULL THEN 1 ELSE 0 END AS cancelled_but_picked_up_flag
    , ops_o.rider_tip_local / NULLIF(COUNT(CASE WHEN d.delivery_status = 'completed' THEN d.delivery_id ELSE NULL END)
       OVER (PARTITION BY d.country_code, ops_o.order_code), 0) AS delivery_tip
    , ops_o.rider_tip_local AS order_tip
    , ops_o.order_amount / NULLIF(COUNT(CASE WHEN d.delivery_status = 'completed' THEN d.delivery_id ELSE NULL END)
       OVER (PARTITION BY d.country_code, ops_o.order_code), 0) AS delivery_value
    , (ops_o.cod_collect_at_dropoff / 100) AS cod_collect_at_dropoff
    , d.dropoff_distance_google
    , d.pickup_distance_google
  FROM il.deliveries d
  LEFT JOIN il.orders ops_o ON ops_o.order_id = d.order_id
    AND ops_o.country_code = d.country_code
  LEFT JOIN il.schedules s ON d.rider_id = s.rider_id
    AND d.country_code = s.country_code
    AND d.food_delivered BETWEEN s.schedule_start_time AND s.schedule_end_time
  LEFT JOIN il.riders r ON ops_o.country_code = r.country_code
    AND r.rider_id = d.rider_id
  LEFT JOIN il.cities c ON d.city_id = c.city_id
    AND d.country_code = c.country_code
  LEFT JOIN il.countries co ON co.country_code = ops_o.country_code
  LEFT JOIN il.zones zo ON d.country_code = zo.country_code
    AND (SELECT DISTINCT dropoff_zone_id FROM UNNEST(d.transitions) WHERE to_state = 'completed') = zo.zone_id
  WHERE d.delivery_status = 'completed'
    OR (d.delivery_status = 'cancelled' AND d.food_picked_up IS NOT NULL)
;
