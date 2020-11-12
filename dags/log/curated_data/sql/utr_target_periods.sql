CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.utr_target_periods` AS
WITH countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.timezone AS timezone
    , zo.id AS zone_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
), utr_dataset AS (
  SELECT t.country_code
    , t.created_at
    , co.city_id
    , co.timezone
    , t.zone_external_id AS zone_id
    , t.name AS period_name
    , t.weekday
    , t.`from` AS start_time
    , t.`to` AS end_time
    , t.updated_at
    , t.suggested_utr
    , t.utr
    , ROW_NUMBER() OVER(PARTITION BY t.country_code, t.created_at, t.zone_external_id, t.name, t.weekday, t.`from`, t.`to` ORDER BY updated_at DESC) AS _row_number
  FROM `{{ params.project_id }}.hl.forecast_utr_target_periods` t
  LEFT JOIN countries co ON t.country_code = co.country_code
    AND t.zone_external_id = co.zone_id
)
SELECT country_code
  , created_at
  , city_id
  , timezone
  , zone_id
  , period_name
  , weekday
  , start_time
  , end_time
  , ARRAY_AGG(
      STRUCT(updated_at
        , suggested_utr AS suggested
        , utr
        , 1 = _row_number AS is_latest
    ) ORDER BY updated_at DESC) AS utr
FROM utr_dataset
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
