CREATE TEMP FUNCTION parse_customer(request_json STRING, response_json STRING, version STRING)
RETURNS STRUCT<
    id STRING,
    user_id STRING,
    session STRUCT<
      id STRING,
      `timestamp` TIMESTAMP
    >,
    latitude FLOAT64,
    longitude FLOAT64,
    apply_abtest BOOL,
    variant STRING,
    tag STRING
>
LANGUAGE js AS
"""
    {% macro javascript_functions() -%}{% include "dynamic_pricing_user_sessions.js" -%}{% endmacro -%}
    {{ javascript_functions()|indent }}
  return parse_dynamic_pricing_sessions(request_json, response_json, version);
""";

CREATE TEMP FUNCTION parse_vendors(json STRING)
RETURNS ARRAY<
  STRUCT<
    id string,
    delivery_fee STRUCT<
      total NUMERIC,
      travel_time NUMERIC,
      fleet_utilisation NUMERIC
    >,
    minimum_order_value STRUCT<
      total NUMERIC
    >,
    tags ARRAY<STRING>
  >
>
LANGUAGE js AS
"""
  const payload = JSON.parse(json);
  let vendors = payload.vendors || [];
  if (payload.vendor)
    vendors = [payload.vendor];
  return vendors;
""";

SELECT region
  , country_code
  , global_entity_id AS entity_id
  , created_at
  , endpoint_name AS endpoint
  , version
  , (SELECT AS STRUCT c.* EXCEPT (longitude, latitude)
      , SAFE.ST_GEOGPOINT(c.longitude, c.latitude) AS location
     FROM (SELECT parse_customer(JSON_EXTRACT(request, "$.customer"), JSON_EXTRACT(response, "$.customer"), version) AS c)
    ) AS customer
  , parse_vendors(JSON_EXTRACT(response, "$")) AS vendors
  , SAFE_CAST(JSON_EXTRACT_SCALAR(request, "$.promised_delivery_time") AS TIMESTAMP) AS promised_delivery_time
  , created_date
FROM `{{ params.project_id }}.dl.dynamic_pricing_request_response_logs`
{%- if params.backfill %}
WHERE created_date >= '0001-01-01'
{%- else %}
WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
{%- endif %}
