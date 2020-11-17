CREATE TEMPORARY FUNCTION parse_new_deliveries(json STRING)
RETURNS ARRAY<
  STRUCT<
    delivery STRUCT<
      picked_up_at STRING,
      capacity INT64,
      assignment STRUCT<
        forbidden_couriers_ids INT64,
        required_couriers_ids INT64,
        vehicle_couriers_ids INT64,
        courier_id INT64
      >,
      skills STRING,
      tags STRING,
      dropoff STRUCT<
        zone_ids ARRAY<INT64>,
        arriving_time STRING,
        leaving_time STRING,
        expected_at STRING,
        location STRUCT<
          longitude FLOAT64,
          latitude FLOAT64
        >
      >,
      dispatched_at STRING,
      stacking_group STRING,
      code STRING,
      status_updated_at STRING,
      estimated_preparation STRUCT<
        buffer INT64,
        duration INT64
      >,
      status STRING,
      id INT64,
      pickup STRUCT<
        leaving_time STRING,
        expected_at STRING,
        duration INT64,
        location STRUCT<
          longitude FLOAT64,
          latitude FLOAT64
        >
      >
    >
  >
>
LANGUAGE js AS """
return JSON.parse(json);
""";

CREATE TEMPORARY FUNCTION parse_couriers(json STRING)
RETURNS ARRAY<
  STRUCT<
    schedule STRUCT<
      updated_at STRING,
      ends_at STRING,
      starts_at STRING,
      zone_id INT64
    >,
    location STRUCT<
      updated_at STRING,
      longitude FLOAT64,
      latitude FLOAT64
    >,
    vehicle STRUCT<
      capacity INT64,
      updated_at STRING,
      profile STRING,
      id INT64,
      speed_in_km_h FLOAT64
    >,
    courier STRUCT<
      ghost BOOL,
      skills STRING,
      status_updated_at STRING,
      status STRING,
      id INT64
    >
  >
>
LANGUAGE js AS """
return JSON.parse(json);
""";

CREATE TEMPORARY FUNCTION parse_deliveries_in_progress(json STRING)
RETURNS ARRAY<
  STRUCT<
    delivery STRUCT<
      picked_up_at STRING,
      capacity INT64,
      assignment STRUCT<
        forbidden_couriers_ids INT64,
        required_couriers_ids INT64,
        vehicle_couriers_ids INT64,
        courier_id INT64
      >,
      skills STRING,
      tags STRING,
      dropoff STRUCT<
        zone_ids ARRAY<INT64>,
        location STRUCT<
          longitude FLOAT64,
          latitude FLOAT64
        >
      >,
      dispatched_at STRING,
      updated_at STRING,
      stacking_group STRING,
      code STRING,
      status_updated_at STRING,
      estimated_preparation STRUCT<
        buffer INT64,
        duration INT64
      >,
      status STRING,
      id INT64,
      pickup STRUCT<
        leaving_time STRING,
        duration INT64,
        location STRUCT<
          longitude FLOAT64,
          latitude FLOAT64
        >
      >
    >
  >
>
LANGUAGE js AS """
return JSON.parse(json);
""";

CREATE TEMPORARY FUNCTION parse_routes(json STRING)
RETURNS ARRAY<
  STRUCT<
    actions ARRAY<
      STRUCT<
        arrival_at STRING,
        delivery_id INT64,
        end_at STRING,
        type STRING
      >
    >,
    id INT64
  >
>
LANGUAGE js AS """
return JSON.parse(json);
""";

CREATE TEMPORARY FUNCTION parse_algorithm_config(json STRING)
RETURNS STRUCT<
  city_base_angle INT64,
  distance_caps STRUCT<
    enabled BOOL,
    to_dropoff STRUCT<
      bike INT64,
      car INT64,
      walker INT64
    >,
    to_pickup STRUCT<
      bike INT64,
      car INT64,
      walker INT64
    >
  >,
  enabled_skills ARRAY<STRING>,
  forbidden_constraints ARRAY<STRING>,
  forbidden_penalties ARRAY<STRING>,
  iteration_strategy STRUCT<
    acceptor STRUCT<
      memorised_solutions INT64,
      type STRING
    >,
    initial_insertion_strategy STRUCT<
      parallelism INT64,
      type STRING,
      vehicle_switch BOOL
    >,
    search_strategies ARRAY<
      STRUCT<
        id STRING,
        recreate STRUCT<
          parallelism INT64,
          type STRING,
          vehicle_switch BOOL
        >,
        ruin STRUCT<
          percent_to_be_removed FLOAT64,
          type STRING
        >,
        weight FLOAT64
      >
    >
  >,
  max_iterations INT64,
  max_route_size INT64,
  parallelism_enabled BOOL,
  penalties ARRAY<
    STRUCT<
      coefficient INT64,
      exponent INT64,
      threshold INT64,
      type STRING,
      formula STRING
    >
  >,
  tags STRUCT<y STRING>,
  variation_coefficient STRUCT<
    no_iterations INT64,
    threshold  INT64
  >,
  stacking_constraints STRUCT<
    enabled BOOL,
    max_pickup_waiting_time  STRING
  >
>
LANGUAGE js AS """
return JSON.parse(json);
""";

CREATE TEMPORARY FUNCTION parse_costs(json STRING)
RETURNS STRUCT<
  TotalDeliveryTime INT64,
  LateDeliveryPenalty FLOAT64,
  DistancePenalty FLOAT64,
  FoodInBagPenalty FLOAT64,
  LatePickupPenalty FLOAT64,
  WaitingAtPickupPenalty FLOAT64
  >
LANGUAGE js AS """
return JSON.parse(json);
""";

SELECT id
  , country_code
  , created_date
  , fleet_id
  , started_at
  , parse_new_deliveries(JSON_EXTRACT(data, '$.new_deliveries')) as new_deliveries
  , parse_couriers(JSON_EXTRACT(data, '$.couriers')) as couriers
  , parse_new_deliveries(JSON_EXTRACT(data, '$.deliveries_in_progress')) as deliveries_in_progress
  , parse_routes(JSON_EXTRACT(data, '$.routes')) as routes
  , parse_routes(JSON_EXTRACT(data, '$.result_routes')) as result_routes
  , parse_algorithm_config(JSON_EXTRACT(data, '$.algorithm_config')) as algorithm_config
  , SAFE_CAST(JSON_EXTRACT(data, '$.cost') AS FLOAT64 ) as cost
  , JSON_EXTRACT(data, '$.processing_time') as processing_time
  , JSON_EXTRACT(data,'$.iterations') as iterations
  , parse_costs(JSON_EXTRACT(data,'$.costs')) as costs
FROM `{{ params.project_id }}.dl.dispatch_service_dispatch_cycles`
{%- if not params.backfill %}
WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
{%- else %}
WHERE created_date >= '0001-01-01'
{%- endif %}
