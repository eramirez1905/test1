CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._sr_latest_forecast_dataset`
PARTITION BY report_date_local
CLUSTER BY country_code, city_id, zone_id AS
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
  SELECT c.country_code
    , ci.id AS city_id
    , zo.id AS zone_id
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) zo
), latest_demand AS (
  SELECT o.country_code
    , c.city_id
    , o.zone_id
    , DATE(demand_for_local) AS report_date_local
    , d.demand_for_local AS start_datetime_local
    , d.riders_needed
    , d.no_shows_expected
    , 1 = ROW_NUMBER() OVER (PARTITION BY o.country_code, d.starting_point_id, demand_for_local ORDER BY f.created_at DESC) AS is_demand_most_recent
  FROM `{{ params.project_id }}.cl.order_forecasts` o
  LEFT JOIN UNNEST(forecasts) f
  LEFT JOIN UNNEST(f.riders_demand) d
  LEFT JOIN countries c ON o.country_code = c.country_code
    AND o.zone_id = c.zone_id
  WHERE o.created_date >= (SELECT DATE_SUB(start_date, INTERVAL 1 DAY) FROM parameters)
    AND DATE(demand_for_local) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
    AND ARRAY_LENGTH(riders_demand) > 0
), latest_forecast_dataset AS (
  SELECT o.country_code
    , c.city_id
    , o.zone_id
    , DATE(d.start_datetime, o.timezone) AS report_date_local
    , DATETIME(d.start_datetime, o.timezone) AS start_datetime_local
    , IF(model_name = 'rooster', orders, NULL) AS original_orders
    , IF(model_name = 'rooster_adjusted', orders, NULL) AS exp_orders_latest
    , NULL AS exp_riders_latest
    , NULL AS exp_no_shows_latest
  FROM `{{ params.project_id }}.cl.order_forecasts` o
  LEFT JOIN UNNEST(forecasts) f
  INNER JOIN report_dates d ON f.forecast_for BETWEEN start_datetime AND end_datetime
  LEFT JOIN countries c ON o.country_code = c.country_code
    AND o.zone_id = c.zone_id
  WHERE is_most_recent
    AND o.created_date >= (SELECT DATE_SUB(start_date, INTERVAL 1 DAY) FROM parameters)
    AND DATE(forecast_for_local) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
    AND model_name IN ('rooster', 'rooster_adjusted')

  UNION ALL
   SELECT d.country_code
     , d.city_id
     , d.zone_id
     , d.report_date_local
     , d.start_datetime_local
     , NULL AS original_orders
     , NULL AS exp_orders_latest
     , d.riders_needed AS exp_riders_latest
     , d.no_shows_expected AS exp_no_shows_latest
   FROM latest_demand d
   LEFT JOIN countries c ON d.country_code = c.country_code
     AND d.zone_id = c.zone_id
   WHERE is_demand_most_recent
)
SELECT country_code
  , city_id
  , zone_id
  , report_date_local
  , start_datetime_local
  , SUM(original_orders) AS original_orders
  , SUM(exp_orders_latest) AS exp_orders_latest
  , SUM(exp_riders_latest) AS exp_riders_latest
  , SUM(exp_no_shows_latest) AS exp_no_shows_latest
FROM latest_forecast_dataset
GROUP BY 1, 2, 3, 4, 5
