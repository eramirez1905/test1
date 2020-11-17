CREATE OR REPLACE TABLE forecasting.orders_lost_timeseries
PARTITION BY created_date
CLUSTER BY country_code AS
SELECT
  country_code,
  zone_id,
  datetime,
  event_type,
  SUM(orders_lost_net) AS orders_lost_net,
  DATE(datetime) AS created_date
FROM lost_orders.orders_lost_extrapolated
GROUP BY 1,2,3,4,6
