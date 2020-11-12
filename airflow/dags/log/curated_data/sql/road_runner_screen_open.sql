CREATE TEMP FUNCTION extract_event_params(_key STRING, _properties ANY TYPE) AS (
  (SELECT value.string_value FROM UNNEST(_properties) WHERE key = _key)
);

SELECT
  PARSE_DATE('%Y%m%d', event_date) AS created_date
  , user_pseudo_id
  , event_timestamp
  , platform
  , app_info.version
  , extract_event_params('employeeType', event_params) AS employeeType
  , extract_event_params('userId', event_params) AS rider_id
  , extract_event_params('locationCountry', event_params) AS country
  , extract_event_params('locationCity', event_params) AS city
  , extract_event_params('screenName', event_params) AS screen_name
  , extract_event_params('shiftId', event_params) AS shift_id
  , extract_event_params('currentWorkingStatus', event_params) AS working_status
FROM `{{ params.project_id }}.dl.road_runner_events` AS t
WHERE event_name = 'screen_opened' and app_info.id ="com.foodora.courier"
{%- if not params.backfill %}
  AND created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
{%- endif %}
