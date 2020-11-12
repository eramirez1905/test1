WITH distinct_forecasts AS ( -- can remove this once the duplicates are resolved upstream
  SELECT * EXCEPT (is_latest)
  FROM (
  SELECT
    order_forecasts.country_code,
    order_forecasts.created_date,
    order_forecasts.zone_id,
    order_forecasts.model_name,
    order_forecasts.timezone,
    forecasts.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        order_forecasts.country_code,
        order_forecasts.zone_id,
        order_forecasts.model_name,
        forecasts.created_at,
        forecasts.forecast_for
    ) = 1 AS is_latest -- order is irrelevant since they are exact duplicates
    FROM `fulfillment-dwh-production.curated_data_shared.order_forecasts` AS order_forecasts
    CROSS JOIN UNNEST (order_forecasts.forecasts) AS forecasts
    WHERE order_forecasts.created_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
  )
  WHERE is_latest
),

riders_demand_agg_forecasts AS (
  SELECT
    distinct_forecasts.country_code,
    distinct_forecasts.zone_id,
    distinct_forecasts.model_name,
    distinct_forecasts.created_at,
    distinct_forecasts.forecast_for,
    ARRAY_AGG(
      STRUCT(
        riders_demand.starting_point_id AS id,
        `{project_id}`.pandata_intermediate.LG_UUID(riders_demand.starting_point_id, distinct_forecasts.country_code) AS uuid,
        riders_demand.riders_needed,
        riders_demand.no_shows_expected,
        TIMESTAMP(riders_demand.demand_for_local) AS demand_for_local,
        TIMESTAMP(riders_demand.demand_for_local, distinct_forecasts.timezone) AS demand_for_utc
      )
    ) AS starting_points
  FROM distinct_forecasts
  CROSS JOIN UNNEST (distinct_forecasts.riders_demand) AS riders_demand
  GROUP BY
    distinct_forecasts.country_code,
    distinct_forecasts.zone_id,
    distinct_forecasts.model_name,
    distinct_forecasts.created_at,
    distinct_forecasts.forecast_for
)

SELECT
  TO_BASE64(SHA256(
    distinct_forecasts.country_code || '_' ||
    CAST(distinct_forecasts.zone_id AS STRING) || '_' ||
    distinct_forecasts.model_name || '_' ||
    STRING(distinct_forecasts.created_at) || '_' ||
    STRING(distinct_forecasts.forecast_for)
  )) AS uuid,
  distinct_forecasts.zone_id AS lg_zone_id,
  `{project_id}`.pandata_intermediate.LG_UUID(distinct_forecasts.zone_id, distinct_forecasts.country_code) AS lg_zone_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(distinct_forecasts.country_code) AS country_code,

  distinct_forecasts.model_name,
  distinct_forecasts.orders AS orders_expected,

  distinct_forecasts.is_valid,
  distinct_forecasts.is_most_recent,

  distinct_forecasts.timezone,
  distinct_forecasts.forecast_for AS forecast_for_utc,
  TIMESTAMP(distinct_forecasts.forecast_for_local) AS forecast_for_local,
  distinct_forecasts.created_at AS created_at_utc,
  distinct_forecasts.created_date AS created_date_utc,
  riders_demand_agg_forecasts.starting_points,
FROM distinct_forecasts
LEFT JOIN riders_demand_agg_forecasts
       ON distinct_forecasts.country_code = riders_demand_agg_forecasts.country_code
      AND distinct_forecasts.zone_id = riders_demand_agg_forecasts.zone_id
      AND distinct_forecasts.model_name = riders_demand_agg_forecasts.model_name
      AND distinct_forecasts.created_at = riders_demand_agg_forecasts.created_at
      AND distinct_forecasts.forecast_for = riders_demand_agg_forecasts.forecast_for
