CREATE OR REPLACE TABLE incident_forecast.training_data_country_iso AS
WITH cc_incidents AS (
    SELECT
      country_iso,
      brand,
      contact_center,
      dispatch_center,
      timezone_local,
      timezone_cs_ps,
      timezone_dp,
      datetime_local,
      weekday,
      hour,
      incident_type,
      -- sum across all languages within the country/brand/incident_type
      SUM(incidents) AS incidents
    FROM incident_forecast.filtered_cc_incidents
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
), daily_peaks AS (
    SELECT
      country_iso,
      incident_type,
      DATE(datetime_local) AS date,
      MAX(incidents) AS daily_peak
    FROM cc_incidents
    WHERE incidents > 0
    GROUP BY 1, 2, 3
), weekday_importance AS (
    SELECT
      country_iso,
      incident_type,
      EXTRACT(DAYOFWEEK FROM date) AS weekday,
      AVG(daily_peak) AS weekday_avg_peak
    FROM daily_peaks
    GROUP BY 1, 2, 3
), orders AS (
    SELECT
      country_iso,
      datetime_local,
      orders_od,
      COALESCE(LAG (orders_od, 1) OVER (PARTITION BY country_iso ORDER BY datetime_local), orders_od) AS orders_od_l1,
      COALESCE(LAG (orders_od, 2) OVER (PARTITION BY country_iso ORDER BY datetime_local), orders_od) AS orders_od_l2,
      COALESCE(LAG (orders_od, 3) OVER (PARTITION BY country_iso ORDER BY datetime_local), orders_od) AS orders_od_l3
    FROM incident_forecast.od_orders_country_iso
)
SELECT
  i.country_iso,
  i.brand,
  i.contact_center,
  i.dispatch_center,
  i.timezone_local,
  i.timezone_cs_ps,
  i.timezone_dp,
  i.datetime_local,
  i.hour,
  i.incident_type,
  i.incidents,
  COALESCE(o.orders_od, 0) AS orders_od,
  COALESCE(o.orders_od_l1, 0) AS orders_od_l1,
  COALESCE(o.orders_od_l2, 0) AS orders_od_l2,
  COALESCE(o.orders_od_l3, 0) AS orders_od_l3,
  w.weekday_avg_peak,
FROM cc_incidents i
LEFT JOIN orders o
 USING (country_iso, datetime_local)
LEFT JOIN weekday_importance w
 USING(country_iso, incident_type, weekday)
