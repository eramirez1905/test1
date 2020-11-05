CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_vendor_attributes`
PARTITION BY created_date AS
WITH date_series AS (
  SELECT *
  FROM UNNEST(GENERATE_DATE_ARRAY('{{ next_ds }}', DATE_SUB('{{ next_ds }}', INTERVAL 12 MONTH), INTERVAL -1 DAY)) AS created_date
), vendors AS (
  SELECT DISTINCT v.entity_id
    , v.vendor_code
    , rps.timezone
    , v.vertical_type
    , rps.client.name AS client_name
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  CROSS JOIN UNNEST(rps) rps
), dates_with_vendors AS (
  SELECT ds.created_date
    , CONCAT(
        EXTRACT(YEAR FROM ds.created_date)
        , '-'
        , EXTRACT(WEEK(MONDAY) FROM ds.created_date)
      ) AS created_week
    , CONCAT(
        EXTRACT(YEAR FROM DATE_SUB(ds.created_date, INTERVAL 7 DAY))
        , '-'
        , EXTRACT(WEEK(MONDAY) FROM DATE_SUB(ds.created_date, INTERVAL 7 DAY))
      ) AS previous_week
    , v.vertical_type
    , v.entity_id
    , v.vendor_code
    , v.timezone
    , v.client_name
  FROM date_series ds
  CROSS JOIN vendors v
), rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE entity.id IS NOT NULL
), rps_orders AS (
  SELECT DISTINCT DATE(created_at, timezone) AS created_date
    , DATETIME(created_at, timezone) AS created_at
    , entity.id AS entity_id
    , vendor.code AS vendor_code
    , order_id
    , order_status
    , cancellation.owner AS cancellation_owner
  FROM rps_orders_dataset
  WHERE created_date >= DATE_ADD(DATE_SUB('{{ next_ds }}', INTERVAL 12 MONTH), INTERVAL 28 DAY)
), rps_first_orders AS (
  SELECT entity.id AS entity_id
    , vendor.code AS vendor_code
    , MIN(CAST(DATETIME(created_at, timezone) AS DATE)) AS first_order_created_date
  FROM rps_orders_dataset
  GROUP BY 1,2
), daily_vendors_orders AS (
  SELECT dwv.created_date
    , dwv.created_week
    , dwv.previous_week
    , dwv.entity_id
    , dwv.vendor_code
    , dwv.timezone
    , dwv.vertical_type
    , fo.first_order_created_date
    , o.created_at
    , o.order_id
    , o.order_status
    , o.cancellation_owner
    , dwv.client_name
  FROM dates_with_vendors dwv
  LEFT JOIN rps_first_orders fo ON dwv.entity_id = fo.entity_id
    AND dwv.vendor_code = fo.vendor_code
  LEFT JOIN rps_orders o ON dwv.created_date = o.created_date
    AND dwv.entity_id = o.entity_id
    AND dwv.vendor_code = o.vendor_code
), weekly_vendors_orders as (
  SELECT created_week
    , entity_id
    , vendor_code
    , COUNT(order_id) OVER (PARTITION BY created_week, entity_id) AS entity_week_orders
    , COUNT(order_id) OVER (PARTITION BY created_week, entity_id, vendor_code) AS vendor_week_orders
  FROM daily_vendors_orders
), weekly_vendors_orders_share AS (
  SELECT entity_id
    , vendor_code
    , created_week
    , AVG(entity_week_orders) AS entity_week_orders
    , AVG(vendor_week_orders) AS vendor_week_orders
    , IF(AVG(entity_week_orders) > 0
         , AVG(vendor_week_orders) / AVG(entity_week_orders)
         , 0) AS vendors_week_orders_share
  FROM weekly_vendors_orders
  GROUP BY 1, 2, 3
), weekly_vendors_orders_cum AS (
  SELECT *
    , SUM(vendors_week_orders_share) OVER (
        PARTITION BY created_week, entity_id ORDER BY vendors_week_orders_share
      ) AS cumulative_vendors_week_orders_share
  FROM weekly_vendors_orders_share
), orders_att AS (
  SELECT created_date
    , created_week
    , previous_week
    , entity_id
    , vendor_code
    , timezone
    , vertical_type
    , first_order_created_date
    , created_at
    , order_id
    , order_status
    , COUNT(order_id) OVER (PARTITION BY created_date, entity_id, vendor_code) AS vendor_daily_orders
    , IF(order_status = 'completed'
        , COUNT(order_id) OVER (PARTITION BY created_date, entity_id, vendor_code, order_status)
        , NULL
      ) AS vendor_daily_successful_orders
    , client_name
  FROM daily_vendors_orders
), orders_base AS (
  SELECT o.*
    , wo.entity_week_orders AS entity_previous_week_orders
    , wo.vendor_week_orders AS vendor_previous_week_orders
    , wo.vendors_week_orders_share AS vendors_previous_week_orders_share
    , wo.cumulative_vendors_week_orders_share AS cumulative_vendors_previous_week_orders_share
  FROM orders_att o
  LEFT JOIN weekly_vendors_orders_cum wo ON o.entity_id = wo.entity_id
    AND o.vendor_code = wo.vendor_code
    AND o.previous_week = wo.created_week
), orders_base_att AS (
  SELECT entity_id
    , vendor_code
    , timezone
    , first_order_created_date
    , created_week
    , created_date
    , vertical_type
    , client_name
    , DATE_SUB(created_date, INTERVAL 7 DAY) AS fixed_previous_7_days_date
    , DATE_SUB(DATE(CURRENT_TIMESTAMP(), timezone), INTERVAL 7 DAY) AS previous_7_days_date
    , COALESCE(AVG(vendor_daily_orders), 0) AS vendor_daily_orders
    , COALESCE(AVG(vendor_daily_successful_orders), 0) AS vendor_daily_successful_orders
    , COALESCE(AVG(cumulative_vendors_previous_week_orders_share), 0) AS cumulative_vendors_previous_week_orders_share
  FROM orders_base
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), daily_orders AS (
  SELECT created_date
    , entity_id
    , vendor_code
    , COUNT(order_id) AS orders
    , COUNT(IF(order_status = 'completed', order_id, NULL)) AS successful_orders
    , COUNT(IF(cancellation_owner = 'VENDOR', order_id, NULL)) AS vendor_failed_orders
  FROM rps_orders
  GROUP BY 1, 2, 3
), previous_days_orders AS (
  SELECT entity_id
    , vendor_code
    , COUNT(order_id) AS orders
    , COUNT(IF(order_status = 'completed', order_id, NULL)) AS successful_orders
    , COUNT(IF(cancellation_owner = 'VENDOR', order_id, NULL)) AS vendor_failed_orders
  FROM daily_vendors_orders
  WHERE created_date >= DATE_SUB(DATE(CURRENT_TIMESTAMP(), timezone), INTERVAL 7 DAY)
  GROUP BY 1, 2
), daily_orders_base AS (
  SELECT ob.created_date
    , ob.created_week
    , ob.entity_id
    , ob.vendor_code
    , ob.timezone
    , ob.vertical_type
    , ob.client_name
    , COALESCE(ob.first_order_created_date, DATE("1970-01-01")) AS first_order_created_date
    , ob.vendor_daily_orders
    , ob.vendor_daily_successful_orders
    , ob.cumulative_vendors_previous_week_orders_share
    , COALESCE(SUM(do_fix.orders), 0) AS fixed_previous_7_days_orders
    , COALESCE(SUM(do_fix.successful_orders), 0) AS fixed_previous_7_days_successful_orders
    , COALESCE(SUM(do_fix.vendor_failed_orders), 0) AS fixed_previous_7_days_vendor_failed_orders
    , COALESCE(AVG(do_prev.orders), 0) AS previous_7_days_orders
    , COALESCE(AVG(do_prev.successful_orders), 0) AS previous_7_days_successful_orders
    , COALESCE(AVG(do_prev.vendor_failed_orders), 0) AS previous_7_days_vendor_failed_orders
    , ROW_NUMBER() OVER (PARTITION BY ob.entity_id, ob.vendor_code ORDER BY ob.created_date DESC) AS _row_number
  FROM orders_base_att ob
  LEFT JOIN daily_orders do_fix ON ob.entity_id = do_fix.entity_id
   AND ob.vendor_code = do_fix.vendor_code
   AND ob.fixed_previous_7_days_date <= do_fix.created_date
   AND ob.created_date > do_fix.created_date
  LEFT JOIN previous_days_orders do_prev ON ob.entity_id = do_prev.entity_id
   AND ob.vendor_code = do_prev.vendor_code
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
), main_calculations AS(
SELECT created_date
  , o.entity_id
  , o.vendor_code
  , STRUCT(
      CASE
        WHEN (s.entity_id IS NOT NULL) THEN s.vendor_grade
        WHEN (o.entity_id = 'TB_KW')
              AND (o.client_name IS NULL OR o.client_name != 'POS')
              OR
             (COALESCE(o.cumulative_vendors_previous_week_orders_share, 0) = 0
              AND o.entity_id NOT IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA'))
              OR
             (COALESCE(o.cumulative_vendors_previous_week_orders_share, 0) = 0
              AND o.entity_id IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA')
              AND (o.client_name IS NULL OR o.client_name != 'POS'))
          THEN 'NA'
        WHEN (o.cumulative_vendors_previous_week_orders_share <= 0.1
              AND o.entity_id NOT IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA'))
              OR
              (o.cumulative_vendors_previous_week_orders_share <= 0.1
              AND o.entity_id IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA')
              AND (o.client_name IS NULL OR o.client_name != 'POS'))
          THEN 'D'
        WHEN (o.cumulative_vendors_previous_week_orders_share <= 0.3
              AND o.entity_id NOT IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA'))
              OR
              (o.cumulative_vendors_previous_week_orders_share <= 0.3
              AND o.entity_id IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA')
              AND (o.client_name IS NULL OR o.client_name != 'POS'))
          THEN 'C'
        WHEN (o.cumulative_vendors_previous_week_orders_share <= 0.5
              AND o.entity_id NOT IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA'))
              OR
             (o.cumulative_vendors_previous_week_orders_share > 0.3
              AND o.entity_id IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA')
              AND (o.client_name IS NULL OR o.client_name != 'POS'))
          THEN 'B'
        WHEN (o.cumulative_vendors_previous_week_orders_share > 0.5
              AND o.entity_id NOT IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA'))
              OR
              (o.entity_id IN ('HF_EG', 'TB_KW', 'TB_AE', 'TB_QA', 'TB_BH', 'TB_JO', 'TB_OM', 'TB_SA')
              AND o.client_name = 'POS')
          THEN 'A'
        ELSE NULL
      END AS fixed_vendor_grade
    , (o.created_date BETWEEN o.first_order_created_date AND DATE_ADD(o.first_order_created_date, INTERVAL 28 DAY)) AS fixed_is_new_vendor
    , (o.fixed_previous_7_days_orders = 0) AS fixed_is_dormant_vendor
    , (o.fixed_previous_7_days_orders >= 1) AND (o.fixed_previous_7_days_successful_orders = 0) AS fixed_is_inoperative_vendor
    , (o.fixed_previous_7_days_vendor_failed_orders = 0) AND (o.fixed_previous_7_days_successful_orders > 0) AND (o.fixed_previous_7_days_successful_orders <= 7) AS fixed_is_unengaged_vendor
    ,  ARRAY_TO_STRING(
        ARRAY[IF((o.fixed_previous_7_days_vendor_failed_orders = 0) AND (o.fixed_previous_7_days_successful_orders > 0) AND (o.fixed_previous_7_days_successful_orders <= 7),'is_unengaged_vendor',NULL),
              IF(t.is_test,'is_ab_vendor',NULL),
              IF(o.fixed_previous_7_days_orders = 0,'is_dormant_vendor',NULL)],', '
      ) AS fixed_is_vm_uo_offlining_disabled_reason
    , IF(((o.fixed_previous_7_days_orders = 0) OR ((o.fixed_previous_7_days_vendor_failed_orders = 0) AND (o.fixed_previous_7_days_successful_orders > 0) AND (o.fixed_previous_7_days_successful_orders <= 7)) OR t.is_test), FALSE, TRUE) AS fixed_is_vm_uo_offlining
    , (DATE(CURRENT_TIMESTAMP(), timezone) BETWEEN o.first_order_created_date AND DATE_ADD(o.first_order_created_date, INTERVAL 28 DAY)) AS is_new_vendor
    , (o.previous_7_days_orders = 0) AS is_dormant_vendor
    , (o.previous_7_days_vendor_failed_orders = 0) AND (o.previous_7_days_successful_orders <= 7) AND (o.previous_7_days_successful_orders > 0) AS is_inoperative_vendor
    , (o.previous_7_days_vendor_failed_orders = 0) AND (o.previous_7_days_successful_orders <= 7) AND (o.previous_7_days_successful_orders > 0) AS is_unengaged_vendor
    , o.vendor_daily_orders AS total_orders
    , o.vendor_daily_successful_orders AS successful_orders
    , (_row_number = 1) AS is_latest_record
    , FALSE AS is_no_autocall_vendor
    , IF(t.is_test, TRUE ,FALSE) AS is_ab_vendor
    , vertical_type
    , client_name
  ) AS attributes
FROM daily_orders_base o
LEFT JOIN fulfillment-dwh-production.vendor_monitor.vendors_ab_test t on t.date=o.created_Date and o.entity_id=t.entity_id and o.vendor_code=t.vendor_code
LEFT JOIN `fulfillment-dwh-staging.kshitij.vendor_specific_attributes` s ON o.entity_id = s.entity_id AND o.vendor_code = CAST(s.Restaurant_ID AS STRING)
)
SELECT  m.*
    , IF((attributes.is_dormant_vendor OR attributes.is_unengaged_vendor OR attributes.is_ab_vendor), FALSE, TRUE) AS is_vm_uo_offlining
    , ARRAY_TO_STRING(
        ARRAY[IF(attributes.is_unengaged_vendor,'is_unengaged_vendor',NULL),
              IF(attributes.is_ab_vendor,'is_ab_vendor',NULL),
              IF(attributes.is_dormant_vendor,'is_dormant_vendor',NULL)],', '
      ) AS is_vm_uo_offlining_disabled_reason
    , IF((attributes.is_no_autocall_vendor OR attributes.is_ab_vendor), FALSE ,TRUE) AS is_vm_uo_autocall
    , ARRAY_TO_STRING(ARRAY[IF(attributes.is_no_autocall_vendor,'is_no_autocall_vendor', NULL), IF(attributes.is_ab_vendor,'is_ab_vendor', NULL)],', ') AS is_vm_uo_autocall_disabled_reason
FROM main_calculations m
