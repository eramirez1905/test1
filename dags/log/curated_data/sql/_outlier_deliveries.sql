CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._outlier_deliveries`
PARTITION BY created_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 1 YEAR) AS start_date
), orders AS (
  SELECT o.country_code
    , o.created_date
    , o.entity.id AS entity_id
    , d.id AS delivery_id
    , d.delivery_distance / 1000 AS delivery_distance
    , d.dropoff_distance_manhattan / 1000 AS dropoff_distance_manhattan
    , d.dropoff_distance_google / 1000 AS dropoff_distance_google
    , d.pickup_distance_manhattan / 1000 AS pickup_distance_manhattan
    , d.pickup_distance_google / 1000 AS pickup_distance_google
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  WHERE created_date >= (SELECT start_date FROM parameters)
   AND d.delivery_status = 'completed'
), interquartile_range AS (
  SELECT country_code
    , created_date
    , entity_id
    , delivery_id
    , PERCENTILE_CONT(dropoff_distance_manhattan, 0.98) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_98_dropoff_distance_manhattan
    , PERCENTILE_CONT(dropoff_distance_manhattan, 0.02) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_2_dropoff_distance_manhattan
    , PERCENTILE_CONT(dropoff_distance_google, 0.98) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_98_dropoff_distance_google
    , PERCENTILE_CONT(dropoff_distance_google, 0.02) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_2_dropoff_distance_google
    , PERCENTILE_CONT(pickup_distance_manhattan, 0.98) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_98_pickup_distance_manhattan
    , PERCENTILE_CONT(pickup_distance_manhattan, 0.02) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_2_pickup_distance_manhattan
    , PERCENTILE_CONT(pickup_distance_google, 0.98) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_98_pickup_distance_google
    , PERCENTILE_CONT(pickup_distance_google, 0.02) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_2_pickup_distance_google
    , PERCENTILE_CONT(delivery_distance, 0.98) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_98_delivery_distance
    , PERCENTILE_CONT(delivery_distance, 0.02) OVER (PARTITION BY country_code, entity_id, created_date) AS perc_2_delivery_distance
 FROM orders
 ), limits AS (
  SELECT DISTINCT country_code
    , created_date
    , entity_id
    , (perc_98_delivery_distance * 2) AS upper_limit_delivery_distance
    , SAFE_DIVIDE(perc_2_delivery_distance, 2) AS lower_limit_delivery_distance
    , (perc_98_dropoff_distance_manhattan * 2)  AS upper_limit_dropoff_distance_manhattan
    , SAFE_DIVIDE(perc_98_dropoff_distance_manhattan, 2) AS lower_limit_dropoff_distance_manhattan
    , (perc_98_dropoff_distance_google * 2) AS upper_limit_dropoff_distance_google
    , SAFE_DIVIDE(perc_2_dropoff_distance_google, 2) AS lower_limit_dropoff_distance_google
    , (perc_98_pickup_distance_manhattan * 2) AS upper_limit_pickup_distance_manhattan
    , SAFE_DIVIDE(perc_2_pickup_distance_manhattan, 2) AS lower_limit_pickup_distance_manhattan
    , (perc_98_pickup_distance_google * 2) AS upper_limit_pickup_distance_google
    , SAFE_DIVIDE(perc_98_pickup_distance_google, 2) AS lower_limit_pickup_distance_google
  FROM interquartile_range l

), dataset_agg AS (
  SELECT l.country_code
    , l.created_date
    , l.upper_limit_delivery_distance
    , l.lower_limit_delivery_distance
    , l.upper_limit_dropoff_distance_manhattan
    , l.lower_limit_dropoff_distance_manhattan
    , l.upper_limit_dropoff_distance_google
    , l.lower_limit_dropoff_distance_google
    , l.upper_limit_pickup_distance_manhattan
    , l.lower_limit_pickup_distance_manhattan
    , l.upper_limit_pickup_distance_google
    , l.lower_limit_pickup_distance_google
    , ARRAY_AGG(STRUCT(o.delivery_id
        , o.delivery_distance
        , o.dropoff_distance_manhattan
        , o.dropoff_distance_google
        , o.pickup_distance_manhattan
        , o.pickup_distance_google
    )) AS deliveries
  FROM limits l
  LEFT JOIN orders o USING (country_code, created_date, entity_id)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)
SELECT l.country_code
  , l.created_date
  , d.delivery_id
  , d.delivery_distance >= upper_limit_delivery_distance AS is_outlier_delivery_distance
  , d.dropoff_distance_manhattan >= upper_limit_dropoff_distance_manhattan AS is_outlier_dropoff_distance_manhattan
  , d.pickup_distance_manhattan >= upper_limit_pickup_distance_manhattan AS is_outlier_pickup_distance_manhattan
  , d.pickup_distance_google >= upper_limit_pickup_distance_google AS is_outlier_pickup_distance_google
  , d.dropoff_distance_google >= upper_limit_dropoff_distance_google AS is_outlier_dropoff_distance_google
  , d.delivery_distance
  , d.dropoff_distance_manhattan
  , d.dropoff_distance_google
  , d.pickup_distance_manhattan
  , d.pickup_distance_google
FROM dataset_agg l
LEFT JOIN UNNEST(deliveries) d
