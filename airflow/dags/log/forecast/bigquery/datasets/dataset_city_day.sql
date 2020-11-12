CREATE TEMPORARY FUNCTION get_created_date(X ANY TYPE) AS
(
  DATE_TRUNC(CAST(X AS DATE), ISOWEEK)
); 

CREATE OR REPLACE TABLE forecasting.dataset_city_day
PARTITION BY created_date
CLUSTER BY country_code AS
WITH timezones AS
(
  SELECT
    country_code,
    id AS city_id,
    timezone
  FROM `fulfillment-dwh-production.dl.hurrier_cities`
  WHERE
    active
),
orders_hurrier AS
(
  SELECT
    t.country_code,
    t.city_id,
    DATE(t.datetime_local) AS date,
    SUM(o.orders) AS orders,
    SUM(o.orders_all) AS orders_all,
    SUM(o.orders_completed) AS orders_completed,
    SUM(o.orders_cancelled) AS orders_cancelled
  FROM `forecasting.timeseries` t
  LEFT JOIN `forecasting.orders_timeseries` o
    ON t.country_code = o.country_code
    AND t.zone_id = o.zone_id
    AND t.datetime = o.datetime
  GROUP BY 1,2,3
  ORDER BY 1,2,3
),
orders_lost AS
(
  SELECT
    t.country_code,
    t.city_id,
    DATE(t.datetime_local) AS date,
    SUM(o.orders_lost_net) AS orders_lost_net,
    SUM(IF(o.event_type IN ('close', 'outage'), o.orders_lost_net, 0)) AS orders_close_lost_net,
    SUM(IF(o.event_type IN ('shrink', 'shrink_legacy'), o.orders_lost_net, 0)) AS orders_shrink_lost_net
  FROM `forecasting.timeseries` t
  LEFT JOIN `forecasting.orders_lost_timeseries` o 
    ON t.country_code = o.country_code
    AND t.zone_id = o.zone_id
    AND t.datetime = o.datetime
  GROUP BY 1,2,3
  ORDER BY 1,2,3
),
orders_platform AS
(
  SELECT
    country_code,
    city_id,
    date,
    -- from platform orders consider completed to ignore vendor cancellations
    orders_completed AS orders,
    orders_completed + orders_cancelled AS orders_all,
    orders_completed,
    orders_cancelled
  FROM `forecasting.orders_pandora_legacy`
),
orders_hurrier_and_platform AS
(
  SELECT
    country_code,
    city_id,
    date,
    COALESCE(op.orders, 0) AS orders_platform,
    COALESCE(oh.orders, 0) AS orders_hurrier,
    COALESCE(op.orders_all, 0) AS orders_all_platform,
    COALESCE(oh.orders_all, 0) AS orders_all_hurrier,
    COALESCE(op.orders_completed, 0) AS orders_completed_platform,
    COALESCE(oh.orders_completed, 0) AS orders_completed_hurrier,
    COALESCE(op.orders_cancelled, 0) AS orders_cancelled_platform,
    COALESCE(oh.orders_cancelled, 0) AS orders_cancelled_hurrier,
    COALESCE(ol.orders_lost_net, 0) AS orders_lost_net,
    COALESCE(ol.orders_close_lost_net, 0) AS orders_close_lost_net,
    COALESCE(ol.orders_shrink_lost_net, 0) AS orders_shrink_lost_net
  FROM orders_platform op
  FULL JOIN orders_hurrier oh USING (country_code, city_id, date)
  FULL JOIN orders_lost ol USING (country_code, city_id, date)
),
start_dates AS
(
  SELECT
    country_code,
    city_id,
    MIN(date) AS start_date,
    -- migration_date is when hurrier had 90% of completed platform orders for the first time
    MIN(CASE WHEN orders_completed_hurrier >= 0.9 * orders_completed_platform THEN date ELSE NULL END) AS migration_date
  FROM orders_hurrier_and_platform
  WHERE 
    date IS NOT NULL
    AND (orders_platform IS NOT NULL OR orders_hurrier IS NOT NULL)
  GROUP BY 1,2
),
timeseries AS
(
  SELECT
    country_code,
    city_id,
    date,
    migration_date
  FROM start_dates,
  UNNEST(GENERATE_DATE_ARRAY(start_date, DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 DAY)) AS date
),
-- add orders to grid
orders AS
(
  SELECT
    t.country_code,
    t.city_id,
    t.date,
    t.migration_date,
    COALESCE(o.orders_platform, 0) AS orders_platform,
    COALESCE(o.orders_hurrier, 0) AS orders_hurrier,
    COALESCE(o.orders_all_platform, 0) AS orders_all_platform,
    COALESCE(o.orders_all_hurrier, 0) AS orders_all_hurrier,
    COALESCE(o.orders_lost_net, 0) AS orders_lost_net,
    COALESCE(o.orders_close_lost_net, 0) AS orders_close_lost_net,
    COALESCE(o.orders_shrink_lost_net, 0) AS orders_shrink_lost_net,
    COALESCE(o.orders_completed_platform, 0) AS orders_completed_platform,
    COALESCE(o.orders_completed_hurrier, 0) AS orders_completed_hurrier,
    COALESCE(o.orders_cancelled_platform, 0) AS orders_cancelled_platform,
    COALESCE(o.orders_cancelled_hurrier, 0) AS orders_cancelled_hurrier
  FROM timeseries t
  LEFT JOIN orders_hurrier_and_platform o USING (country_code, city_id, date)
)
SELECT
  country_code,
  city_id,
  date,
  date >= migration_date AS is_hurrier,
  CASE WHEN date >= migration_date THEN orders_hurrier ELSE orders_platform END AS orders,
  CASE WHEN date >= migration_date THEN orders_all_hurrier ELSE orders_all_platform END AS orders_all,
  CASE WHEN date >= migration_date THEN orders_completed_hurrier ELSE orders_completed_platform END AS orders_completed,
  CASE WHEN date >= migration_date THEN orders_cancelled_hurrier ELSE orders_cancelled_platform END AS orders_cancelled,
  orders_lost_net,
  orders_close_lost_net,
  orders_shrink_lost_net,
  DATE_TRUNC(date, ISOWEEK) AS created_date
FROM orders o
LEFT JOIN timezones tz USING (country_code, city_id)
WHERE 
  -- do not fetch partial local day
  o.date < DATE(DATETIME('{{next_execution_date}}', tz.timezone))
