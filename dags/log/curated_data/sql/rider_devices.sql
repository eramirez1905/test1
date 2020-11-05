CREATE TEMP FUNCTION extract_properties(_key STRING, _properties ANY TYPE) AS (
  (SELECT value.string_value FROM UNNEST(_properties) WHERE key = _key)
)
;
WITH countries AS (
  SELECT c.country_code
    , c.country_name
    -- taking one timezone per country to use in the final table as city is not usable in dl.road_runner_events
    , (SELECT timezone FROM UNNEST (cities) ci LIMIT 1) AS timezone
  FROM `{{ params.project_id }}.cl.countries` c
), devices_dataset AS (
  SELECT TIMESTAMP_MICROS(event_timestamp) AS received_at
    , SAFE_CAST(extract_properties("employee_id", user_properties) AS INT64) AS rider_id
    , CAST(extract_properties("country", user_properties) AS STRING) AS country
    , created_date
    , STRUCT(
        device.category AS category
        , device.mobile_brand_name AS mobile_brand_name
        , device.mobile_model_name AS mobile_model_name
        , device.mobile_marketing_name AS mobile_marketing_name
        , device.operating_system AS operating_system
        , device.advertising_id AS advertising_id
        , device.language AS language
      ) AS device
  FROM `{{ params.project_id }}.dl.road_runner_events`
  {%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
    AND event_name = 'screen_opened'
  {%- endif %}
)
SELECT LOWER(e.country) AS country_code
  , e.created_date
  , c.timezone
  , e.rider_id
  , e.received_at
  , e.device
FROM devices_dataset e
LEFT JOIN countries c ON LOWER(e.country) = c.country_code
