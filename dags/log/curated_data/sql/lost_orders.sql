CREATE TEMPORARY FUNCTION parse_event_id_transaction_id(event STRING)
RETURNS ARRAY<
  STRUCT<
    id INT64,
    transaction_id INT64
  >
>
LANGUAGE js AS """
const event_id_arr = event.split("_");
const result = event_id_arr.reduce(function(result, value, index, array) {
  if (index % 2 === 0){
    const sub_arr = array.slice(index, index + 2);
    const obj = {
      'id': parseInt(sub_arr[0]) || null,
      'transaction_id': parseInt(sub_arr[1]) || null
    }
    result.push(obj);
  }
  return result;
}, []);
return result;
""";

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.lost_orders`
PARTITION BY created_date AS
WITH countries AS (
  SELECT co.country_code
     , ci.id AS city_id
     , ci.timezone
     , zo.id AS zone_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) zo
), lost_orders AS (
  SELECT lo.country_code
    , DATE(lo.datetime) AS created_date
    , co.timezone
    , co.city_id
    , lo.event_type
    , lo.zone_id
    , lo.event_id AS forecasting_event_id
    , parse_event_id_transaction_id(event_id) AS events
    , ARRAY_AGG(
        STRUCT(datetime AS interval_start
          , starts_at
          , ends_at
          , CAST(ROUND(orders_extrapolated) AS INT64) AS estimate
          , CAST(ROUND(orders_lost_net) AS INT64) AS net
      )) AS lost_orders
  FROM `{{ params.project_id }}.lost_orders.orders_lost_extrapolated` lo
  LEFT JOIN countries co ON lo.country_code = co.country_code
    AND lo.zone_id = co.zone_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), porygon_events AS (
  SELECT e.country_code
    , e.event_id
    , e.created_date
    , e.created_at
    , e.timezone
    , t.transaction_id
    , t.end_transaction_id
    , t.start_at
    , t.end_at
    , t.zone_id
  FROM `{{ params.project_id }}.cl.porygon_events` e
  LEFT JOIN UNNEST(e.transactions) t
), lost_orders_dataset AS (
  SELECT lo.country_code
    , lo.created_date
    , lo.event_type
    , lo.city_id
    , lo.zone_id
    , lo.timezone
    , lo.forecasting_event_id
    , ARRAY_AGG(
        STRUCT(pe.event_id AS id
          , pe.transaction_id
          , pe.end_transaction_id
          , pe.start_at AS starts_at
          , pe.end_at AS ends_at
        ) ORDER BY start_at, end_at
      ) AS events
  FROM lost_orders lo
  LEFT JOIN UNNEST(lo.events) ue
  LEFT JOIN porygon_events pe ON pe.country_code = lo.country_code
   AND pe.transaction_id = ue.transaction_id
   AND pe.zone_id = pe.zone_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT f.country_code
  , f.created_date
  , f.event_type
  , f.zone_id
  , f.city_id
  , f.timezone
  , l.lost_orders
  , (SELECT AS STRUCT MIN(f.starts_at) AS starts_at, MAX(f.ends_at) AS ends_at FROM UNNEST(f.events) f) AS timings
  , f.events
FROM lost_orders_dataset f
LEFT JOIN lost_orders l USING (country_code, created_date, event_type, zone_id, forecasting_event_id)
