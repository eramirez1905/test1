CREATE OR REPLACE TABLE incident_forecast.training_data_country_iso_{{ params.country_iso_low }} AS
SELECT
  country_iso,
  brand,
  contact_center,
  dispatch_center,
  timezone_local,
  timezone_cs_ps,
  timezone_dp,
  datetime_local,
  hour,
  incident_type,
  incidents,
  orders_od,
  orders_od_l1,
  orders_od_l2,
  orders_od_l3,
  weekday_avg_peak
FROM incident_forecast.training_data_country_iso
WHERE
 country_iso = '{{ params.country_iso }}'
ORDER BY brand, incident_type, datetime_local
