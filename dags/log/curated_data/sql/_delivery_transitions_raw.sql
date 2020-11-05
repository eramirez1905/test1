WITH riders AS (
  SELECT r.country_code
    , r.rider_id
    , courier_id
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(hurrier_courier_ids) courier_id
), delivery_transitions AS (
  SELECT country_code
    , id
    , delivery_id
    , to_state
    , created_at
    , CAST(created_at AS DATE) AS created_date
    , sort_key
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.auto_transition') AS BOOL) AS auto_transition
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.courier_id') AS INT64) AS courier_id
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.courier_long') AS FLOAT64) AS courier_longitude
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.courier_lat') AS FLOAT64) AS courier_latitude
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.courier_zone_id') AS INT64) AS rider_starting_point_id
    , STRUCT(
        CAST(JSON_EXTRACT_SCALAR(metadata, '$.user_id') AS INT64) AS user_id
        , JSON_EXTRACT_SCALAR(metadata, '$.performed_by') AS performed_by
        , JSON_EXTRACT_SCALAR(metadata, '$.reason') AS reason
        , JSON_EXTRACT_SCALAR(metadata, '$.issue_type') AS issue_type
        , JSON_EXTRACT_SCALAR(metadata, '$.comment') AS comment
      ) AS actions
    , JSON_EXTRACT_SCALAR(metadata, '$.dispatch_type') AS dispatch_type
    , JSON_EXTRACT_SCALAR(metadata, '$.undispatch_type') AS undispatch_type
    , JSON_EXTRACT_SCALAR(metadata, '$.event_type') AS event_type
    , JSON_EXTRACT_SCALAR(metadata, '$.update_reason') AS update_reason
    , SAFE_CAST(JSON_EXTRACT_SCALAR(metadata, '$.estimated_pickup_arrival_at') AS TIMESTAMP) AS estimated_pickup_arrival_at
    , SAFE_CAST(JSON_EXTRACT_SCALAR(metadata, '$.estimated_dropoff_arrival_at') AS TIMESTAMP) AS estimated_dropoff_arrival_at
  FROM `{{ params.project_id }}.dl.hurrier_delivery_transitions` t
  {%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
  {%- endif %}
)
SELECT t.*
  , r.rider_id
FROM delivery_transitions t
LEFT JOIN riders r ON r.country_code = t.country_code
  AND r.courier_id = t.courier_id
