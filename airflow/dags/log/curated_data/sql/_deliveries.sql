CREATE TEMP FUNCTION parse_dte(json STRING)
RETURNS
  ARRAY<
    STRUCT<
      vehicle_type STRING,
      time FLOAT64,
      distance FLOAT64
    >
  >
LANGUAGE js AS """
  const filtersRaw = JSON.parse(json) || [];
  let result = [];
  for(key in filtersRaw) {
    result.push({'vehicle_type': key, 'time': filtersRaw[key].time, 'distance': filtersRaw[key].distance})
  };
  return result;
  """;

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._deliveries`
PARTITION BY created_date
CLUSTER BY country_code, order_id AS
WITH order_cancelled_transition AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._order_transitions`
  WHERE most_recent IS TRUE
    AND to_state = 'cancelled'
), hurrier_platforms AS (
  SELECT country_code
     , entity_id
     , display_name AS entity_display_name
     , hurrier_platforms
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(c.platforms) platform
  LEFT JOIN UNNEST(platform.hurrier_platforms) hurrier_platforms
), deliveries AS (
  SELECT d.* EXCEPT (id)
   , d.id AS delivery_id
   , STRUCT(
       ve.id
       , ve.name
       , ve.profile
       , CONCAT(ve.name, ' - ', b.name) AS vehicle_bag
       , b.name AS bag_name
     ) AS vehicle
  FROM `{{ params.project_id }}.dl.hurrier_deliveries` d
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_vehicle_types` ve ON ve.country_code = d.country_code
    AND ve.id = d.vehicle_type_id
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_vehicles` v ON ve.country_code = v.country_code
    AND v.vehicle_type_id = ve.id
    AND v.id = d.vehicle_id
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_vehicle_bag_types` b ON v.country_code = b.country_code
    AND v.vehicle_bag_type_id = b.id
    AND v.vehicle_type_id = d.vehicle_type_id
), primary_deliveries AS (
  SELECT d.country_code
    , d.id
    -- we want to take the first completed delivery by created_at or the first cancelled if there are no completed deliveries.
    , d.id = FIRST_VALUE(d.id) OVER (PARTITION BY d.country_code, d.order_id ORDER BY d.status DESC, d.created_at ASC) AS is_primary
  FROM `{{ params.project_id }}.dl.hurrier_deliveries` d
), riders AS (
  SELECT r.country_code
    , r.rider_id
    , courier_id
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(hurrier_courier_ids) courier_id
), transitions AS (
    SELECT *
      , ROW_NUMBER() OVER (PARTITION BY country_code, delivery_id, to_state ORDER BY created_at DESC, sort_key DESC) AS _row_number_desc
      , ROW_NUMBER() OVER (PARTITION BY country_code, delivery_id, to_state ORDER BY created_at ASC, sort_key DESC) AS _row_number_asc
    FROM `{{ params.project_id }}.cl._delivery_transitions_raw`
), delivery_transitions AS (
  SELECT t.country_code
    , t.delivery_id
    , ARRAY_AGG(STRUCT(
        t.to_state AS state
      , t.rider_id
      , t.auto_transition
      , t.created_at
      , SAFE.ST_GEOGPOINT(t.courier_longitude, t.courier_latitude) AS geo_point
      , (_row_number_desc = 1) AS latest
      , (_row_number_asc = 1) AS first
      , t.rider_starting_point_id
      , t.actions
      , t.dispatch_type
      , t.undispatch_type
      , t.event_type
      , t.update_reason
      , t.estimated_pickup_arrival_at
      , t.estimated_dropoff_arrival_at
    )) AS transitions
  FROM transitions t
  GROUP BY 1,2
)
SELECT d.country_code
  , d.order_id
  , CAST(d.created_at AS DATE) AS created_date
  , ARRAY_AGG(STRUCT(d.country_code
    , d.delivery_id AS id
    , d.created_at
    , p.is_primary
    , d.redelivery AS is_redelivery
    , r.rider_id
    , da.city_id
    , da.timezone
    , d.status AS delivery_status
    , d.reason AS delivery_reason
    , d.delivery_distance
    , (SELECT created_at FROM UNNEST(dt.transitions) WHERE state = 'courier_notified' AND latest) AS rider_notified_at
    , d.courier_accepted_at AS rider_accepted_at
    , (SELECT created_at FROM UNNEST(dt.transitions) WHERE state = 'near_pickup' AND latest) AS rider_near_restaurant_at
    , d.courier_picked_up_at AS rider_picked_up_at
    , (SELECT created_at FROM UNNEST(dt.transitions) WHERE state = 'near_dropoff' AND latest) AS rider_near_customer_at
    , d.courier_dropped_off_at AS rider_dropped_off_at
    , STRUCT(
        (SELECT auto_transition FROM UNNEST(dt.transitions) WHERE state = 'near_pickup' AND latest) AS pickup
        , (SELECT auto_transition FROM UNNEST(dt.transitions) WHERE state = 'near_dropoff' AND latest) AS dropoff
      ) AS auto_transition
    , (SELECT geo_point FROM UNNEST(dt.transitions) WHERE state = 'picked_up' AND latest) AS pickup
    , (SELECT geo_point FROM UNNEST(dt.transitions) WHERE state = 'completed' AND latest) AS dropoff
    , (SELECT geo_point FROM UNNEST(dt.transitions) WHERE state = 'accepted' AND latest) AS accepted
    , (SELECT rider_starting_point_id FROM UNNEST(dt.transitions) WHERE state = 'completed' AND latest) AS rider_starting_point_id
    , (SELECT created_at FROM UNNEST(dt.transitions) WHERE state = 'dispatched' AND first) AS first_rider_dispatched_at
    , d.distance_travelled_pickup_manhattan AS pickup_distance_manhattan
    , d.distance_travelled_dropoff_manhattan AS dropoff_distance_manhattan
    , d.distance_travelled_pickup_google AS pickup_distance_google
    , d.distance_travelled_dropoff_google AS dropoff_distance_google
    , d.distance_travelled_pickup_dte AS pickup_distance_dte
    , d.distance_travelled_dropoff_dte AS dropoff_distance_dte
    , parse_dte(d.distance_and_time_estimated_dte) AS estimated_distance_and_time_dte
    , COALESCE(d.dropoff_duration, 0) AS dropoff_duration
    , d.courier_dispatched_at AS rider_dispatched_at
    , (SELECT created_at FROM UNNEST(dt.transitions) WHERE state = 'left_pickup' and latest) AS rider_left_vendor_at
    , ARRAY(SELECT AS STRUCT * FROM dt.transitions ORDER BY created_at) AS transitions
    , d.vehicle
    , (s.deliveries_in_bag_past - 1) + (s.deliveries_in_bag_future - 1) AS stacked_deliveries
    , (s.is_in_bag_intravendor_past OR s.is_in_bag_intravendor_future) AS is_stacked_intravendor
    , ARRAY(
       (SELECT AS STRUCT sp.delivery_id FROM UNNEST(_deliveries_in_bag_past) sp WHERE sp.delivery_id <> d.delivery_id)
      UNION ALL
       (SELECT AS STRUCT sf.delivery_id FROM UNNEST(_deliveries_in_bag_future) sf WHERE sf.delivery_id <> d.delivery_id)
      ) AS stacked_deliveries_details
  )) AS deliveries
FROM deliveries d
LEFT JOIN delivery_transitions dt ON dt.country_code = d.country_code
  AND dt.delivery_id = d.delivery_id
LEFT JOIN riders r ON r.country_code = d.country_code
  AND r.courier_id = d.courier_id
LEFT JOIN `{{ params.project_id }}.cl._addresses` da ON da.country_code = d.country_code
  AND da.id = d.dropoff_address_id
LEFT JOIN primary_deliveries p ON d.country_code = p.country_code
  AND d.delivery_id = p.id
LEFT JOIN `{{ params.project_id }}.cl.stacked_deliveries` s ON d.country_code = s.country_code
  AND d.delivery_id = s.delivery_id
GROUP BY 1,2,3
