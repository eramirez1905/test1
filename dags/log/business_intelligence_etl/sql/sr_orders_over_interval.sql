CREATE OR REPLACE TABLE il.sr_orders_over_interval
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
), temp_delays AS (
  SELECT f.country_code
    , geo_zone_id as zone_id
    , z.city_id
    , z.timezone
    , CAST(TIMESTAMP_SECONDS(60 * DIV(UNIX_SECONDS(f.created_at) + 30, 60)) AS TIME) AS time
    , CAST(f.created_at AS DATE) AS report_date
    , AVG(f.delay) AS delay
    , AVG(utr) AS utr
  FROM `{{ params.project_id }}.ml.hurrier_zone_stats` f
  LEFT JOIN countries z ON z.country_code = f.country_code
    AND z.zone_id = f.geo_zone_id
  WHERE f.created_date BETWEEN (SELECT DATE_SUB(start_date, INTERVAL 5 DAY) FROM parameters) AND (SELECT end_date FROM parameters)
  GROUP BY 1, 2, 3, 4, 5, 6
), temp_orders AS (
  SELECT od.country_code
    , od.order_id
    , od.food_delivered
    , od.order_date
    , od.city_id
    , od.zone_id
    , od.staffing_needed_timestamp
    , od.dispatcher_expected_delivery_time
    , od.created_at
    , od.preorder
    , od.delivery_time
    , delay AS delay_in_mins
    , utr
    , od.delivery_delay
    , od.timezone
  FROM (
    SELECT DISTINCT ops.country_code
      , ops.order_id
      , ops.food_delivered
      , ops.delivery_time_eff AS delivery_time
      , ops.delivery_delay_eff AS delivery_delay
      , ops.preorder
      , ops.city_id
      , z.zone_id
      , ops.timezone
      , CAST(ops.created_at AS DATE) AS order_date
      , ops.created_at AS created_at
      , CASE
          WHEN ops.preorder
            THEN TIMESTAMP_SUB(ops.dispatcher_expected_delivery_time, INTERVAL 30 MINUTE)
          ELSE TIMESTAMP_ADD(ops.created_at, INTERVAL 10 MINUTE)
        END AS staffing_needed_timestamp
      , ops.dispatcher_expected_delivery_time
      , DATETIME(ops.dispatcher_expected_delivery_time, ops.timezone) AS dispatcher_expected_delivery_time_local
    FROM il.orders ops
    LEFT JOIN il.deliveries s ON s.country_code = ops.country_code
      AND s.order_id = ops.order_id
    LEFT JOIN cl._orders_to_zones z ON ops.country_code = z.country_code
      AND ops.order_id = z.order_id
    -- the below first where condition uses ops.created_date in UTC to optimize the cost, the 5 days intervals is to take into account all possible preorders including the timezone adjustment.
    WHERE ops.created_date BETWEEN (SELECT DATE_SUB(start_date, INTERVAL 5 DAY) FROM parameters) AND (SELECT end_date FROM parameters)
      AND s.delivery_status = 'completed'
  ) od
  LEFT JOIN temp_delays d ON od.country_code = d.country_code
    AND od.city_id = d.city_id
    AND od.zone_id = d.zone_id
    AND CAST(od.staffing_needed_timestamp AS DATE) = d.report_date
    AND CAST(TIMESTAMP_SECONDS(60 * DIV(UNIX_SECONDS(od.staffing_needed_timestamp) + 30, 60)) AS TIME) = d.time
)
 SELECT DISTINCT bl.country_code
    , CAST(DATETIME(dates.start_datetime, bl.timezone) AS DATE) AS report_date_local
    , city_id
    , bl.zone_id
    , DATETIME(dates.start_datetime, bl.timezone) AS start_datetime_local
    , DATETIME(dates.end_datetime, bl.timezone) AS end_datetime_local
    , COUNT(IF(bl.food_delivered >= dates.start_datetime AND bl.food_delivered < dates.end_datetime,
        bl.order_id,
        NULL)
      ) AS orders_delivered
    , COUNT(IF(bl.created_at >= dates.start_datetime AND bl.created_at < dates.end_datetime,
        bl.order_id,
        NULL)
      ) AS orders_created
    , COUNT(IF(bl.staffing_needed_timestamp >= dates.start_datetime AND bl.staffing_needed_timestamp < dates.end_datetime,
        bl.order_id,
        NULL)
      ) AS orders_actuals
    , SUM(IF(bl.staffing_needed_timestamp >= dates.start_datetime AND bl.staffing_needed_timestamp < dates.end_datetime AND bl.preorder IS FALSE,
        TIMESTAMP_DIFF(bl.dispatcher_expected_delivery_time, bl.created_at, MINUTE),
        NULL)
      ) AS promised_dt_n
    , COUNT(IF(bl.staffing_needed_timestamp >= dates.start_datetime AND bl.staffing_needed_timestamp < dates.end_datetime AND preorder IS FALSE,
        bl.order_id,
        NULL)
      ) AS promised_dt_d
    , SUM(IF(bl.staffing_needed_timestamp >= dates.start_datetime AND bl.staffing_needed_timestamp < dates.end_datetime AND preorder IS FALSE,
        bl.delivery_time,
        NULL)
      ) AS delivery_time_n
    , COUNT(IF(bl.staffing_needed_timestamp >= dates.start_datetime AND bl.staffing_needed_timestamp < dates.end_datetime AND preorder IS FALSE AND delivery_time IS NOT NULL,
        bl.order_id,
        NULL)
      ) AS delivery_time_d
    , SUM(IF(bl.food_delivered >= dates.start_datetime AND bl.food_delivered < dates.end_datetime, bl.delay_in_mins, NULL)) AS delay_in_mins
    , COUNT(IF(bl.food_delivered >= dates.start_datetime AND bl.food_delivered < dates.end_datetime AND delivery_delay >= 10,
        bl.order_id,
        NULL)
      ) AS delivery_delay_10_count
    , COUNT(IF(bl.food_delivered >= dates.start_datetime AND bl.food_delivered < dates.end_datetime AND delivery_delay IS NOT NULL,
        bl.order_id,
        NULL)
      ) AS delivery_delay_count
  FROM temp_orders bl
  INNER JOIN report_dates dates ON dates.report_date = CAST(bl.staffing_needed_timestamp AS DATE)
  GROUP BY 1, 2, 3, 4, 5, 6
;
