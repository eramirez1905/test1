 CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.order_forecasts`
 PARTITION BY created_date
 CLUSTER BY country_code, zone_id, model_name AS
 WITH cities_zones AS (
   SELECT co.country_code
     , ci.timezone AS timezone
     , zo.id AS zone_id
     , zo.zone_shape_updated_at AS polygon_updated_at
   FROM `{{ params.project_id }}.cl.countries` co
   LEFT JOIN UNNEST(co.cities) ci
   LEFT JOIN UNNEST(ci.zones) zo
 ), forecasts_ds_raw AS (
   SELECT f.country_code
     , DATE(f.created_at) AS created_date
     , f.zone_id
     , f.datetime AS forecast_for
     , f.timezone
     , f.model_name
     , f.created_at
     , f.orders
     -- rider demand is not on rooster for these models, creating then NULL array of struct for UNION purposes
    , [STRUCT(CAST(NULL AS INT64) AS starting_point_id
         , CAST(NULL AS DATETIME) AS demand_for_local
         , CAST(NULL AS NUMERIC) AS riders_needed
         , CAST(NULL AS NUMERIC) AS no_shows_expected
      )] AS riders_demand
     , z.polygon_updated_at
   FROM `fulfillment-dwh-production.ds_model_outputs.order_forecasts` f
   LEFT JOIN cities_zones z ON z.country_code = f.country_code
     AND z.zone_id = f.zone_id
   WHERE z.zone_id IS NOT NULL
 ), forecasts_rooster_raw AS (
   SELECT f.country_code
     , DATE(f.created_at) AS created_date
     , f.zone_id
     , f.forecast_for
     , z.timezone
     , 'rooster' AS model_name
     , f.created_at
     , f.orders
     , rd.riders_demand
     , z.polygon_updated_at
   FROM `{{ params.project_id }}.dl.forecast_orders_forecasts` f
   LEFT JOIN cities_zones z ON z.country_code = f.country_code
     AND z.zone_id = f.zone_id
   LEFT JOIN `{{ params.project_id }}.cl._forecasts_rider_demand` rd ON f.country_code = rd.country_code
     AND f.created_at = rd.forecasted_at
     AND f.forecast_for = rd.forecast_for
     AND f.zone_id = rd.zone_id
   WHERE z.zone_id IS NOT NULL
 ), forecasts_rooster_adjusted_raw AS (
   SELECT f.country_code
     , DATE(f.created_at) AS created_date
     , f.zone_id
     , f.forecast_for
     , z.timezone
     , 'rooster_adjusted' AS model_name
     , f.created_at
     , f.adjusted_orders AS orders
     , rd.riders_demand
     , z.polygon_updated_at
   FROM `fulfillment-dwh-production.dl.forecast_forecasts` f
   LEFT JOIN cities_zones z ON z.country_code = f.country_code
     AND z.zone_id = f.zone_id
   LEFT JOIN `{{ params.project_id }}.cl._forecasts_rider_demand` rd ON f.country_code = rd.country_code
     AND f.created_at = rd.forecasted_at
     AND f.forecast_for = rd.forecast_for
     AND f.zone_id = rd.zone_id
   WHERE z.zone_id IS NOT NULL
 ), forecasts_all_raw AS (
   SELECT * FROM forecasts_ds_raw
   UNION ALL
   SELECT * FROM forecasts_rooster_raw
   UNION ALL
   SELECT * FROM forecasts_rooster_adjusted_raw
 ), forecasts AS (
   SELECT f.country_code
     , f.created_date
     , f.zone_id
     , f.timezone
     , f.model_name
     , f.forecast_for
     , f.orders
     , f.riders_demand
     , f.created_at
     , TIMESTAMP_DIFF(f.created_at, f.polygon_updated_at, DAY) >= 1 AS is_valid
     , 1 = ROW_NUMBER() OVER (PARTITION BY f.country_code, f.model_name, f.zone_id, f.forecast_for ORDER BY f.created_at DESC) is_most_recent
   FROM forecasts_all_raw f
 )
 SELECT f.country_code
   , f.created_date
   , f.zone_id
   , f.model_name
   , f.timezone
   , ARRAY_AGG(
       STRUCT(f.created_at
         , f.forecast_for
         , DATETIME(f.forecast_for, f.timezone) AS forecast_for_local
         , f.orders
         , f.is_valid
         , f.is_most_recent
         , f.riders_demand
       ) ORDER BY created_at DESC, f.is_most_recent, f.forecast_for
     ) AS forecasts
 FROM forecasts f
 GROUP BY 1, 2, 3, 4, 5
