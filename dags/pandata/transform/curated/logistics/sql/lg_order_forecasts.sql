SELECT
  order_forecasts.uuid,
  order_forecasts.lg_zone_id,
  order_forecasts.lg_zone_uuid,
  countries.rdbms_id,
  order_forecasts.country_code,
  order_forecasts.model_name,
  order_forecasts.orders_expected,
  order_forecasts.is_valid,
  order_forecasts.is_most_recent,
  order_forecasts.timezone,
  order_forecasts.forecast_for_utc,
  order_forecasts.forecast_for_local,
  order_forecasts.created_at_utc,
  order_forecasts.created_date_utc,
  order_forecasts.starting_points,
FROM `{project_id}.pandata_intermediate.lg_order_forecasts` AS order_forecasts
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON order_forecasts.country_code = countries.lg_country_code
