CREATE TEMP FUNCTION parse_tags(json STRING)
RETURNS STRUCT<
  delivery_provider ARRAY<STRING>,
  delivery_types ARRAY<STRING>,
  cuisine ARRAY<STRING>,
  halal BOOL,
  chains ARRAY<STRING>,
  tag ARRAY<STRING>,
  vertical_type STRING,
  customer_types ARRAY<STRING>,
  characteristic ARRAY<STRING>
>
LANGUAGE js AS """
  return JSON.parse(json);
""";

CREATE TEMP FUNCTION parse_message(json STRING)
RETURNS ARRAY<STRUCT<id STRING,
    value STRING
    >>
LANGUAGE js AS """
  const messages = [];
  const raw = JSON.parse(json) || [];
  Object.keys(raw)
        .forEach((id) => {
         let value = raw[id];
         messages.push({id, value})
        });
  return messages;
""";

CREATE TEMPORARY FUNCTION standardise_delivery_provider(delivery_provider ARRAY<STRING>, delivery_types ARRAY<STRING>)
RETURNS ARRAY<STRING> AS(
  (ARRAY(
    SELECT DISTINCT
      CASE
        WHEN delivery_types IN ('Hurrier', 'platform_delivery')
          THEN 'OWN_DELIVERY'
        WHEN delivery_types IN ('vendor_delivery', 'partner_delivery', 'Restaurant')
          THEN 'VENDOR_DELIVERY'
        WHEN delivery_types = 'pickup'
          THEN 'PICKUP'
        ELSE
          NULL
      END
    FROM UNNEST(ARRAY_CONCAT(delivery_provider, delivery_types)) delivery_types)
  )
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.delivery_areas_events`
PARTITION BY created_date
CLUSTER BY country_code, event_id AS
WITH countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.name
    , ci.timezone AS timezone
    , zo.id AS zone_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
), countries_timezone AS (
  SELECT c.country_code
    -- taking one timezone per country to use in the final table
    , (SELECT timezone FROM UNNEST (cities) ci LIMIT 1) AS timezone
  FROM `{{ params.project_id }}.cl.countries` c
), porygon_transaction_start AS (
  SELECT country_code
    , id
    , issued_at
  FROM `{{ params.project_id }}.dl.porygon_transaction`
), porygon_transaction_end AS (
  SELECT country_code
    , id
    , issued_at
  FROM `{{ params.project_id }}.dl.porygon_transaction`
), poygon_event_settings AS (
  SELECT country_code
    , id
    , transaction_id
    , platform
    , parse_tags(tags) AS tags
    , parse_message(message) AS message
  FROM `{{ params.project_id }}.dl.porygon_events_versions`
), poygon_events_details AS (
  SELECT v.country_code
    , v.id AS event_id
    , v.created_at AS active_from
    , IF(end_transaction_id IS NULL AND operation_type != 2, NULL, v.updated_at) AS active_to
    , CASE
        WHEN operation_type = 0
          THEN 'created'
        WHEN operation_type = 1
          THEN 'updated'
        WHEN operation_type = 2
          THEN 'deleted'
        ELSE NULL
      END AS operation_type
    , STRUCT(co.city_id
        , co.name
      ) AS city
    , COALESCE(co.timezone, ct.timezone) AS timezone
    , v.zone_id
    , v.transaction_id
    , v.end_transaction_id
    , ts.issued_at AS start_at
    , te.issued_at AS end_at
    , TIMESTAMP_DIFF(te.issued_at, ts.issued_at, SECOND) AS duration
    , v.is_active
    , v.action
    , v.value
    , v.activation_threshold
    , v.deactivation_threshold
    , v.title
    , v.shape_sync AS is_shape_in_sync
    , s.message
    , SAFE.ST_GEOGFROMTEXT(v.shape_wkt) AS shape
    , STRUCT(standardise_delivery_provider(COALESCE(s.tags.delivery_provider, []), COALESCE(s.tags.delivery_types, [])) AS delivery_provider
        , s.tags.cuisine AS cuisines
        , COALESCE(s.tags.halal, FALSE) AS is_halal
        , s.tags.tag AS tag
        , s.tags.chains AS chains
        , s.tags.vertical_type AS vertical_type
        , s.tags.customer_types AS customer_types
        , s.tags.characteristic AS characteristics
      ) AS tags
    , v.created_date
  FROM `{{ params.project_id }}.dl.porygon_events_versions` v
  LEFT JOIN poygon_event_settings s ON v.country_code = s.country_code
    AND v.id = s.id
    AND v.platform = s.platform
    AND v.transaction_id = s.transaction_id
  LEFT JOIN countries co ON v.country_code = co.country_code
    AND v.zone_id = co.zone_id
  LEFT JOIN countries_timezone ct ON v.country_code = ct.country_code
  LEFT JOIN porygon_transaction_start ts ON v.country_code = ts.country_code
    AND v.transaction_id = ts.id
  LEFT JOIN porygon_transaction_end te ON v.country_code = te.country_code
    AND v.end_transaction_id = te.id
)
SELECT *
FROM poygon_events_details
