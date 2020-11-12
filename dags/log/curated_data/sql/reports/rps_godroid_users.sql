CREATE TEMP FUNCTION extract_godroid_params(_key STRING, _properties ANY TYPE) AS (
  (SELECT value.string_value FROM UNNEST(_properties) WHERE key = _key)
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_godroid_users`
PARTITION BY created_date AS
WITH godroid_data AS (
  SELECT created_date
    , created_at AS event_id
    , client.version
    , vendor_id
    , vendor_code
    , CASE
        WHEN region = 'eu'
          THEN 'Europe'
        WHEN region IN ('ap', 'kr')
          THEN 'Asia'
        WHEN region = 'mena'
          THEN 'MENA'
        WHEN region = 'us'
          THEN 'America'
        ELSE region
      END AS region
    , country_code
    , UPPER(device.id) AS device_id
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
  WHERE client.name = 'GODROID'
    AND created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 30 DAY)
)
SELECT created_date
  , version
  , region
  , country_code
  , device_id
  , vendor_id
  , vendor_code AS platform_vendor_code
  , COUNT(DISTINCT event_id) AS event_ct
FROM godroid_data
GROUP BY 1,2,3,4,5,6,7
