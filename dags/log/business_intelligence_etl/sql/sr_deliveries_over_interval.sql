CREATE OR REPLACE TABLE il.sr_deliveries_over_interval
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK) AS start_date
    , DATE_ADD('{{ next_ds }}', INTERVAL 2 WEEK) AS end_date
), dates AS (
  SELECT date_time
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_TRUNC(TIMESTAMP_SUB((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), INTERVAL 50 DAY), DAY),
    TIMESTAMP_TRUNC((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), HOUR), INTERVAL 15 MINUTE)
  ) AS date_time
), report_dates AS (
  SELECT CAST(date_time AS DATE) AS report_date
    , date_time AS start_datetime
    , TIMESTAMP_ADD(date_time, INTERVAL 15 MINUTE) AS end_datetime
  FROM dates
), countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.timezone AS timezone
    , zo.id AS zone_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
), temp_deliveries AS (
  SELECT DISTINCT d.country_code
    , d.order_id
    , d.delivery_id
    , d.food_delivered
    , ops.food_delivered AS order_food_delivered
    , d.delivery_delay
    , d.delivery_time
    , d.stacked_flag
    , ops.preorder
    , d.city_id
    , d.rider_id
    , CAST(d.created_at AS DATE) AS order_date
    , d.rider_accepted AS rider_accepted
    , z.zone_id
    , d.timezone
  FROM il.deliveries d
  LEFT JOIN il.orders ops ON ops.country_code = d.country_code
    AND ops.order_id = d.order_id
  LEFT JOIN cl._orders_to_zones z ON ops.country_code = z.country_code
    AND ops.order_id = z.order_id
  LEFT JOIN countries zz ON zz.country_code = z.country_code
    AND zz.zone_id = z.zone_id
  WHERE d.delivery_status = 'completed'
    -- the below first where condition uses ops.created_date in UTC to optimize the cost, the 5 days intervals is to take into account all possible preorders including the timezone adjustment.
    AND d.report_date >= (SELECT DATE_SUB(start_date, INTERVAL 5 DAY) FROM parameters)
    AND CAST(DATETIME(d.food_delivered, d.timezone) AS DATE) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
), deliveries_cutting AS (
  SELECT r.country_code
    , r.order_id
    , r.delivery_id
    , r.food_delivered
    , r.order_food_delivered
    , r.delivery_delay
    , r.delivery_time
    , r.stacked_flag
    , r.preorder
    , r.city_id
    , r.rider_id
    , r.order_date
    , r.rider_accepted
    , r.zone_id
    , r.timezone
    , MAX(lro_order.order_id) AS is_contained_in_order
    , COALESCE(MIN(ro_order.rider_accepted), r.food_delivered) AS food_delivered_cut
    , ROW_NUMBER() OVER (PARTITION BY r.country_code, r.order_id, r.delivery_id) AS rn_delivery
    , ROW_NUMBER() OVER (PARTITION BY r.country_code, r.order_id) AS rn_order
  FROM temp_deliveries r
  LEFT JOIN temp_deliveries ro_order ON r.order_date = ro_order.order_date
    AND r.rider_id = ro_order.rider_id
    AND r.country_code = ro_order.country_code
    AND r.city_id = ro_order.city_id
    AND ro_order.rider_accepted < r.food_delivered
    AND r.food_delivered < ro_order.food_delivered
    AND r.rider_accepted < ro_order.rider_accepted
  LEFT JOIN temp_deliveries lro_order ON r.order_date = lro_order.order_date
    AND r.rider_id = lro_order.rider_id
    AND r.country_code = lro_order.country_code
    AND r.city_id = lro_order.city_id
    AND r.order_id <> lro_order.order_id
    AND r.rider_accepted >= lro_order.rider_accepted AND r.food_delivered <= lro_order.food_delivered
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 ,13, 14, 15
)
SELECT dc.country_code AS country_code
  , CAST(DATETIME(dates.start_datetime, dc.timezone) AS DATE) AS report_date_local
  , DATETIME(dates.start_datetime, dc.timezone) AS start_datetime_local
  , DATETIME(dates.end_datetime, dc.timezone) AS end_datetime_local
  , dc.city_id
  , dc.zone_id
  , dc.timezone
  , COUNT(IF(dc.food_delivered >= dates.start_datetime AND dc.food_delivered < dates.end_datetime AND rn_delivery = 1,
      delivery_id,
      NULL)
    ) AS deliveries
  , COUNT(IF(dc.order_food_delivered >= dates.start_datetime AND dc.order_food_delivered < dates.end_datetime AND rn_order = 1,
      order_id,
      NULL)
    ) AS orders_delivered
FROM deliveries_cutting dc
INNER JOIN report_dates dates ON dates.report_date = CAST(dc.food_delivered AS DATE)
GROUP BY 1, 2, 3, 4, 5, 6, 7
;
