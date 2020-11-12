CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_continuous_product_unavailability`
PARTITION BY created_date
CLUSTER BY entity_id, delivery_platform, country_name AS
WITH entities AS (
  SELECT en.region_short_name AS region
    , en.country_iso
    , LOWER(en.country_iso) AS country_code
    , en.country_name
    , p.entity_id
    , p.brand_name AS delivery_platform
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
), rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
), rps_vendor_attributes_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.rl.rps_vendor_attributes`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
), rps_orderproduct_availability As (
  SELECT region
    , orders.id AS order_id
    , product.id AS product_id
  FROM rps_vendor_client_events_dataset
  WHERE event_name = 'order_product_set_availability'
), rps_orderproducts AS (
  SELECT DATE(o.created_at, o.timezone) AS created_date
    , o.region
    , o.vendor.id AS vendor_id
    , o.entity.id AS entity_id
    , o.vendor.code AS vendor_code
    , p.id AS product_id
    , o.country_code
    , o.client.name AS client_name
    , o.client.version AS client_version
    , o.client.wrapper_type
    , o.client.wrapper_version
    , o.is_preorder
    , o.order_id
    , o.order_code
    , (o.order_status = 'cancelled') AS is_order_cancelled
    , (o.order_status = 'cancelled' AND o.cancellation.reason = 'ITEM_UNAVAILABLE' ) AS is_order_cancelled_item_unavailable
    , (o.order_status = 'cancelled' AND o.cancellation.reason = 'ITEM_UNAVAILABLE' AND o.cancellation.source = 'VENDOR_DEVICE' AND ev.order_id IS NOT NULL ) AS is_order_cancelled_item_unavailable_by_vendor
    , CAST(DATETIME(o.created_at, o.timezone) AS TIMESTAMP) AS created_at
  FROM rps_orders_dataset o
  CROSS JOIN UNNEST(ARRAY(SELECT AS STRUCT id FROM o.products GROUP BY 1)) p
  LEFT JOIN rps_orderproduct_availability ev ON o.order_id = ev.order_id
    AND p.id = ev.product_id
), rps_orderproducts_for_agg AS (
  SELECT *
    , TIMESTAMP_DIFF(next_order_cancelled_item_unavailable_by_vendor_at, created_at, MINUTE) AS next_order_time_interval_minutes
    , (next_order_id IS NOT NULL AND created_date = DATE_SUB(next_created_date, INTERVAL 0 DAY)) AS is_having_orders_same_day
    , (next_order_id IS NOT NULL AND created_date = DATE_SUB(next_created_date, INTERVAL 0 DAY) AND NOT next_is_order_cancelled) AS is_having_completed_orders_same_day
    , (next_order_id IS NOT NULL AND created_date = DATE_SUB(next_created_date, INTERVAL 0 DAY) AND next_is_order_cancelled) AS is_having_fail_orders_same_day
    , (next_order_id IS NULL
        OR (next_order_id IS NOT NULL AND created_date <> DATE_SUB(next_created_date, INTERVAL 0 DAY))
      ) AS is_having_no_orders_same_day
    , (next_order_id IS NOT NULL AND created_date = DATE_SUB(next_created_date, INTERVAL 1 DAY)) AS is_having_orders_next_day
    , (next_order_id IS NOT NULL AND created_date = DATE_SUB(next_created_date, INTERVAL 1 DAY) AND NOT next_is_order_cancelled) AS is_having_completed_orders_next_day
    , (next_order_id IS NOT NULL AND created_date = DATE_SUB(next_created_date, INTERVAL 1 DAY) AND next_is_order_cancelled) AS is_having_fail_orders_next_day
    , (next_order_id IS NULL
        OR (next_order_id IS NOT NULL AND created_date <> DATE_SUB(next_created_date, INTERVAL 1 DAY))
      ) AS is_having_no_orders_next_day
    , (next_order_id IS NOT NULL AND DATE_DIFF(next_created_date, created_date, DAY) BETWEEN 1 AND 7) AS is_having_orders_afterward
    , (next_order_id IS NOT NULL AND DATE_DIFF(next_created_date, created_date, DAY) BETWEEN 1 AND 7 AND NOT next_is_order_cancelled) AS is_having_completed_orders_afterward
    , (next_order_id IS NOT NULL AND DATE_DIFF(next_created_date, created_date, DAY) BETWEEN 1 AND 7 AND next_is_order_cancelled) AS is_having_fail_orders_afterward
    , (next_order_id IS NULL
        OR (next_order_id IS NOT NULL AND DATE_DIFF(next_created_date, created_date, DAY) BETWEEN 1 AND 7)
      ) AS is_having_no_orders_afterward
  FROM (
    SELECT op.created_date
      , EXTRACT(DAYOFWEEK FROM op.created_date) AS created_dow
      , op.region
      , en.delivery_platform
      , en.country_name
      , op.client_name
      , op.client_version
      , op.wrapper_type
      , op.wrapper_version
      , op.vendor_id
      , op.entity_id
      , op.vendor_code
      , op.product_id
      , op.is_preorder
      , op.order_id
      , op.order_code
      , op.is_order_cancelled
      , op.is_order_cancelled_item_unavailable
      , op.is_order_cancelled_item_unavailable_by_vendor
      , op.created_at
      , LEAD(IF(op.is_order_cancelled_item_unavailable_by_vendor, op.created_at, NULL)) OVER (vendor_product_window) AS next_order_cancelled_item_unavailable_by_vendor_at
      , LEAD(IF(op.is_order_cancelled_item_unavailable_by_vendor, op.created_date, NULL)) OVER (vendor_product_window) AS next_order_cancelled_item_unavailable_by_vendor_date
      , LEAD(IF(op.is_order_cancelled_item_unavailable_by_vendor, op.order_id, NULL)) OVER (vendor_product_window) AS next_order_id_cancelled_item_unavailable_by_vendor
      , LEAD(IF(op.is_order_cancelled_item_unavailable_by_vendor, op.is_order_cancelled_item_unavailable_by_vendor, NULL)) OVER (vendor_product_window) AS next_is_order_cancelled_item_unavailable_by_vendor
      , LEAD(op.order_id) OVER (vendor_product_window) AS next_order_id
      , LEAD(op.created_date) OVER (vendor_product_window) AS next_created_date
      , LEAD(op.is_order_cancelled) OVER (vendor_product_window) AS next_is_order_cancelled
    FROM rps_orderproducts op
    INNER JOIN entities en ON op.entity_id = en.entity_id
    WINDOW vendor_product_window AS (
      PARTITION BY op.entity_id, op.vendor_code, op.product_id
      ORDER BY op.created_at
    )
  )
)
SELECT op.created_date
  , CASE
      WHEN created_dow = 1
        THEN 'Sunday'
      WHEN created_dow = 2
        THEN 'Monday'
      WHEN created_dow = 3
        THEN 'Tuesday'
      WHEN created_dow = 4
        THEN 'Wednesday'
      WHEN created_dow = 5
        THEN 'Thursday'
      WHEN created_dow = 6
        THEN 'Friday'
      WHEN created_dow = 7
        THEN 'Saturday'
    END AS created_dow
  , EXTRACT(YEAR FROM op.created_date) AS created_year
  , FORMAT_DATE('%Y-%m', op.created_date) AS created_month
  , FORMAT_DATE('%Y-%V', op.created_date) AS created_week
  , EXTRACT(HOUR FROM op.created_at) AS created_hour
  , op.region
  , op.delivery_platform
  , op.country_name
  , COALESCE(op.client_name, '(UNKNOWN)') AS client_name
  , COALESCE(op.client_version, '(UNKNOWN)') AS client_version
  , op.entity_id
  , COALESCE(va.attributes.fixed_vendor_grade, '(UNKNOWN)') AS fixed_vendor_grade
  , COALESCE(va.attributes.fixed_is_new_vendor, FALSE) AS fixed_is_new_vendor
  , COALESCE(va.attributes.fixed_is_dormant_vendor, FALSE) AS fixed_is_dormant_vendor
  , COALESCE(va.attributes.fixed_is_unengaged_vendor, FALSE) AS fixed_is_unengaged_vendor
  , COALESCE(va.attributes.fixed_is_inoperative_vendor, FALSE) AS fixed_is_inoperative_vendor
  , op.is_preorder
  , COUNT(DISTINCT op.order_id) AS orders_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled, op.order_id, NULL)) AS fail_orders_total_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable, op.order_id, NULL)) AS fail_orders_item_unavailable_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor, op.order_id, NULL)) AS fail_orders_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor, op.next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor AND op.next_order_time_interval_minutes BETWEEN 0 AND 1, next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_in_1_min_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor AND op.next_order_time_interval_minutes BETWEEN 1 AND 14, next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_in_15_min_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor AND op.next_order_time_interval_minutes BETWEEN 15 AND 29, next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_in_30_min_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor AND op.next_order_time_interval_minutes BETWEEN 30 AND 59, next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_in_60_min_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor AND op.next_order_time_interval_minutes >= 60 AND op.created_date = next_order_cancelled_item_unavailable_by_vendor_date, next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_over_60_min_same_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor AND op.created_date = next_order_cancelled_item_unavailable_by_vendor_date, next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_in_same_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor AND op.created_date = DATE_SUB(next_order_cancelled_item_unavailable_by_vendor_date, INTERVAL 1 DAY), next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_in_next_1_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.next_is_order_cancelled_item_unavailable_by_vendor AND op.created_date > DATE_SUB(next_order_cancelled_item_unavailable_by_vendor_date, INTERVAL 1 DAY), next_order_id_cancelled_item_unavailable_by_vendor, NULL)) AS fail_next_order_after_x_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_completed_orders_next_day, op.order_id, NULL)) AS orders_having_completed_order_in_next_1_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_fail_orders_next_day AND op.next_is_order_cancelled_item_unavailable_by_vendor, op.order_id, NULL)) AS orders_having_fail_order_item_unavailable_by_vendor_in_next_1_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_fail_orders_next_day AND NOT COALESCE(op.next_is_order_cancelled_item_unavailable_by_vendor, FALSE), op.order_id, NULL)) AS orders_having_fail_order_not_item_unavailable_by_vendor_in_next_1_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND NOT op.is_having_orders_next_day, op.order_id, NULL)) AS orders_having_no_order_in_next_1_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_completed_orders_same_day, op.order_id, NULL)) AS orders_having_completed_order_in_same_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_fail_orders_same_day AND op.next_is_order_cancelled_item_unavailable_by_vendor, op.order_id, NULL)) AS orders_having_fail_order_item_unavailable_by_vendor_in_same_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_fail_orders_same_day AND NOT COALESCE(op.next_is_order_cancelled_item_unavailable_by_vendor, FALSE), op.order_id, NULL)) AS orders_having_fail_order_not_item_unavailable_by_vendor_in_same_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND NOT op.is_having_orders_same_day, op.order_id, NULL)) AS orders_having_no_order_in_same_day_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_completed_orders_afterward, op.order_id, NULL)) AS orders_having_completed_order_afterward_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_fail_orders_afterward AND op.next_is_order_cancelled_item_unavailable_by_vendor, op.order_id, NULL)) AS orders_having_fail_order_item_unavailable_by_vendor_afterward_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND op.is_having_fail_orders_afterward AND NOT COALESCE(op.next_is_order_cancelled_item_unavailable_by_vendor, FALSE), op.order_id, NULL)) AS orders_having_fail_order_not_item_unavailable_by_vendor_afterward_ct
  , COUNT(DISTINCT IF(op.is_order_cancelled_item_unavailable_by_vendor AND NOT op.is_having_orders_afterward, op.order_id, NULL)) AS orders_having_no_order_afterward_ct
FROM rps_orderproducts_for_agg op
LEFT JOIN rps_vendor_attributes_dataset va ON op.vendor_code = va.vendor_code
  AND op.entity_id = va.entity_id
  AND op.created_date = va.created_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
