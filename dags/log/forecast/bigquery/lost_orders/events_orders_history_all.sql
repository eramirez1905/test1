CREATE OR REPLACE TABLE `lost_orders.events_orders_history_all` AS
WITH selected_orders AS (
  SELECT
    o.country_code,
    o.order_placed_at,
    o.zone_id,
    CAST(o.order_placed_at AS DATE) AS order_placed_at_date
  FROM `fulfillment-dwh-production.cl.orders` AS o
  WHERE 
    'preorder' NOT IN UNNEST(o.tags)
), 
selected_events AS (
  SELECT
    country_code,
    zone_id,
    event_type,
    event_id_merged as event_id,
    datetime_starts_at,
    starts_at,
    ends_at
  FROM `lost_orders.events_merged_aligned`
  WHERE starts_at < ends_at
), 
events_time_unnested AS (
  SELECT
    e.country_code,
    e.event_type,
    e.event_id,
    e.zone_id,
    e.datetime_starts_at,
    e.starts_at,
    e.ends_at,
    TIMESTAMP_SUB(bucket_ends_at, INTERVAL -TIMESTAMP_DIFF(starts_at, ends_at, SECOND) SECOND) AS bucket_starts_at,
    bucket_ends_at,
    CAST(TIMESTAMP_SUB(bucket_ends_at, INTERVAL -TIMESTAMP_DIFF(starts_at, ends_at, SECOND) SECOND) AS DATE) AS bucket_starts_at_date,
    CAST(bucket_ends_at AS DATE) AS bucket_ends_at_date
  FROM selected_events AS e
  CROSS JOIN
    UNNEST(GENERATE_TIMESTAMP_ARRAY(e.ends_at,
        TIMESTAMP_SUB(e.ends_at, INTERVAL 7*7 DAY),
        INTERVAL -7 DAY)
    ) AS bucket_ends_at
), 
dataset AS (
  SELECT
    e.country_code,
    e.event_type,
    e.event_id,
    e.zone_id,
    e.datetime_starts_at,
    e.starts_at,
    e.ends_at,
    e.bucket_starts_at,
    e.bucket_ends_at,
    ARRAY_AGG(o) AS orders
  FROM events_time_unnested e
  LEFT JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(e.bucket_starts_at_date AS DATE), CAST(e.bucket_ends_at AS DATE))) bucket_day
  LEFT JOIN selected_orders o 
    ON o.order_placed_at_date = bucket_day
    AND (o.order_placed_at BETWEEN e.bucket_starts_at AND e.bucket_ends_at)
    AND o.country_code = e.country_code
    AND o.zone_id = e.zone_id
  GROUP BY 1,2,3,4,5,6,7,8,9
), 
final AS (
  SELECT 
    country_code,
    event_type,
    event_id,
    zone_id,
    datetime_starts_at,
    starts_at,
    ends_at,
    bucket_starts_at,
    bucket_ends_at,
    (SELECT COUNT(*) FROM UNNEST(orders)) AS num_orders,
    orders
  FROM dataset
)
SELECT 
  country_code,
  event_type,
  event_id,
  zone_id,
  datetime_starts_at,
  starts_at,
  ends_at,
  bucket_starts_at,
  bucket_ends_at,
  SUM(num_orders) AS num_orders
FROM final
GROUP BY 1,2,3,4,5,6,7,8,9
