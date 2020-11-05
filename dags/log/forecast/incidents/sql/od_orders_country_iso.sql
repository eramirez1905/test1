CREATE OR REPLACE TABLE incident_forecast.od_orders_country_iso AS
WITH order_hist AS (
    SELECT
      country_iso,
      DATETIME(datetime, timezone) AS datetime_local,
      SUM(orders_all) AS orders_od
    -- we always want to use the prod order forecast
    FROM `fulfillment-dwh-production.forecasting.dataset_zone_hour`
    LEFT JOIN `fulfillment-dwh-production.cl.countries`
     USING (country_code)
    GROUP BY 1, 2
), zone_forecasts AS (
    SELECT
      c.country_iso,
      o.country_code,
      o.zone_id,
      f.forecast_for_local AS datetime_local,
      f.orders AS orders,
      row_number() OVER (PARTITION BY o.country_code, o.zone_id, f.forecast_for_local) AS row_number
    FROM `fulfillment-dwh-production.cl.order_forecasts` o,
    UNNEST(forecasts) f
    LEFT JOIN `fulfillment-dwh-production.cl.countries` c USING (country_code)
    WHERE
     o.model_name = 'rooster' AND
     f.is_most_recent  IS TRUE
), country_iso_forecasts AS (
    SELECT
      country_iso,
      datetime_local,
      SUM(orders) AS order_forecast
    FROM zone_forecasts
    WHERE
     row_number = 1
    GROUP BY 1, 2
)
SELECT 
  country_iso,
  datetime_local AS datetime_local,
  COALESCE(orders_od, order_forecast) AS orders_od
FROM order_hist 
FULL OUTER JOIN country_iso_forecasts
 USING (country_iso, datetime_local)
