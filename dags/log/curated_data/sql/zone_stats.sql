CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.zone_stats`
PARTITION BY created_date
CLUSTER BY country_code, zone_id, created_at_bucket AS
WITH zone_stats AS (
 SELECT geo_zone_id AS zone_id
    , delay_constants
    , mean_delay
    , delay
    , created_at
    , LEAD(created_at) OVER (PARTITION BY country_code, geo_zone_id ORDER BY country_code, geo_zone_id, created_at) AS next_created_at
    , country_code
  FROM `{{ params.project_id }}.dl.hurrier_zone_stats`
), zone_stats_agg AS (
  SELECT country_code
    , zone_id
    , created_at_bucket
    , DATE(created_at_bucket) AS created_date
    , ARRAY_AGG(
        STRUCT(created_at
          , mean_delay
          , delay AS estimated_courier_delay
        )
      ) AS stats
  FROM zone_stats
  LEFT JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_TRUNC(created_at, MINUTE), TIMESTAMP_TRUNC(next_created_at, MINUTE), INTERVAL 1 MINUTE)) created_at_bucket
  GROUP BY 1,2,3,4
)
SELECT *
FROM zone_stats_agg
