CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._forecasts_rider_demand`
PARTITION BY created_date AS
WITH countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.timezone AS timezone
    , zo.id AS zone_id
    , zo.geo_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
)
SELECT c.timezone
  , forecasted_at
  , d.country_code
  , d.zone_external_id AS zone_id
  , d.forecast_for
  , d.created_date
  -- orders_expected are forecasted on 30 minutes level while table is on 15 minutes level because riders are forecasted in 15 minutes interval.
  , MIN(d.orders_expected) AS orders_expected
  , ARRAY_AGG(
      STRUCT(starting_point_external_id AS starting_point_id
        , DATETIME(demand_for, timezone) AS demand_for_local
        , SAFE_CAST(d.riders_needed_distributed AS NUMERIC) AS riders_needed
        , SAFE_CAST(d.no_shows_expected_distributed AS NUMERIC) AS no_shows_expected
    )) AS riders_demand
FROM `{{ params.project_id }}.dl.forecast_demands` d
LEFT JOIN countries c ON d.country_code = c.country_code
  AND d.zone_external_id = c.zone_id
GROUP BY 1, 2, 3, 4, 5, 6

