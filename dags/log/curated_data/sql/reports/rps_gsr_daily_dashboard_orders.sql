CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_daily_dashboard_orders`
PARTITION BY order_date
CLUSTER BY entity_id, region, brand_name , country_name AS
WITH countries AS (
  SELECT DISTINCT country_iso
    , country_name
  FROM `{{ params.project_id }}.cl.countries`
), entities AS (
  SELECT p.entity_id
    , p.brand_id
    , p.brand_name
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), vendors AS (
  SELECT v.entity_id
    , v.vendor_code
    , v.vertical_type
    , rps.country_iso
    , rps.timezone 
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  LEFT JOIN UNNEST(rps) rps
), rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders` o
  WHERE o.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 18 MONTH)
), rps_orders AS (
  SELECT CAST(DATETIME(o.created_at, o.timezone) AS DATE) AS created_date
    , o.region
    , o.country_iso
    , o.entity.id AS entity_id
    , o.device.manufacturer
    , o.device.mdm_service
    , o.service
    , o.transmission_flow
    , o.client.name AS application_client_name
    , o.client.pos_integration_flow AS application_integration_flow
    , o.client.pos_integration_name
    , o.client.version AS application_client_version
    , COALESCE(o.client.wrapper_type, '(UNKNOWN)') AS application_client_wrapper_type
    , o.vendor.code AS vendor_code
    , o.delivery_type
    , IF(o.order_status = 'cancelled' AND gfr.reason IS NULL, 'PLATFORM', o.cancellation.owner) AS cancellation_owner
    , IF(o.order_status = 'cancelled' AND gfr.reason IS NULL, 'GFR_INCORRECT', o.cancellation.reason) AS cancellation_reason
    , o.cancellation.stage AS cancellation_stage
    , o.cancellation.source AS cancellation_source
    , COUNT(DISTINCT IF(NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders
    , COUNT(DISTINCT IF(NOT o.order_status = 'completed' AND o.cancellation.owner = 'VENDOR', o.order_id, NULL)) AS no_fail_orders_vendor
    , COUNT(DISTINCT IF(NOT o.order_status = 'completed' AND o.cancellation.owner = 'TRANSPORT', o.order_id, NULL)) AS no_fail_orders_transport
    , COUNT(DISTINCT IF(NOT o.order_status = 'completed' AND o.cancellation.owner = 'PLATFORM', o.order_id, NULL)) AS no_fail_orders_platform
    , COUNT(DISTINCT IF(NOT o.order_status = 'completed' AND o.cancellation.owner NOT IN ('VENDOR','TRANSPORT','CUSTOMER','PLATFORM'), o.order_id, NULL)) AS no_fail_orders_unknown
    , COUNT(DISTINCT IF(o.delivery_type = 'OWN_DELIVERY' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders_od
    , COUNT(DISTINCT IF(o.delivery_type = 'PICKUP' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders_pu
    , COUNT(DISTINCT IF(o.delivery_type = 'VENDOR_DELIVERY' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders_rd
    , COUNT(DISTINCT IF(o.client.name = 'GODROID' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders_godroid
    , COUNT(DISTINCT IF(o.client.name = 'GOWIN' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders_gowin
    , COUNT(DISTINCT IF((o.client.pos_integration_flow IS NOT NULL OR o.client.name = 'POS') AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders_pos
    , COUNT(DISTINCT IF(o.client.pos_integration_flow = 'INDIRECT' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders_pos_indirect
    , COUNT(DISTINCT IF(o.client.pos_integration_flow = 'DIRECT' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_fail_orders_pos_direct
    , COUNT(DISTINCT IF(o.delivery_type = 'OWN_DELIVERY', o.order_id, NULL)) AS no_orders_od
    , COUNT(DISTINCT IF(o.delivery_type = 'PICKUP', o.order_id, NULL)) AS no_orders_pu
    , COUNT(DISTINCT IF(o.delivery_type = 'VENDOR_DELIVERY', o.order_id, NULL)) AS no_orders_rd
    , COUNT(DISTINCT IF(o.client.name = 'GODROID', o.order_id, NULL)) AS no_orders_godroid
    , COUNT(DISTINCT IF(o.client.name = 'GOWIN', o.order_id, NULL)) AS no_orders_gowin
    , COUNT(DISTINCT IF(o.client.pos_integration_flow IS NOT NULL OR o.client.name = 'POS', o.order_id, NULL)) AS no_orders_pos
    , COUNT(DISTINCT IF(o.client.pos_integration_flow = 'INDIRECT', o.order_id, NULL)) AS no_orders_pos_indirect
    , COUNT(DISTINCT IF(o.client.pos_integration_flow = 'DIRECT', o.order_id, NULL)) AS no_orders_pos_direct
    , COUNT(DISTINCT IF(o.service = 'OMA', o.order_id, NULL)) AS no_oma_orders
    , COUNT(DISTINCT IF(o.service = 'OMA' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_oma_failed_orders
    , COUNT(DISTINCT IF(o.transmission_flow = 'Grocery', o.order_id, NULL)) AS no_grocery_orders
    , COUNT(DISTINCT IF(o.transmission_flow = 'Grocery' AND NOT o.order_status = 'completed', o.order_id, NULL)) AS no_oma_grocery_failed_orders
    , COUNT(DISTINCT o.order_id) AS no_orders
    , COUNT(DISTINCT IF(o.device.is_ngt_device, o.order_id, NULL)) AS no_ngt_orders
  FROM rps_orders_dataset o
  LEFT JOIN `{{ params.project_id }}.cl._rps_gfr_cancellations` gfr ON o.cancellation.reason = gfr.reason
    AND o.cancellation.owner = gfr.owner
    AND o.delivery_type = gfr.delivery_type
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
), rps_orders_agg AS (
  SELECT o.region
    , o.created_date AS order_date
    , EXTRACT(YEAR FROM o.created_date) AS order_year
    , FORMAT_DATE('%Y-%m', o.created_date) AS order_month
    , FORMAT_DATE('%Y-%V', o.created_date) AS order_week
    , o.country_iso
    , o.entity_id
    , o.vendor_code
    , o.cancellation_owner
    , o.cancellation_reason
    , o.cancellation_source
    , o.cancellation_stage
    , o.delivery_type
    , o.service
    , o.application_client_name
    , o.application_integration_flow
    , o.pos_integration_name
    , o.application_client_wrapper_type
    , o.mdm_service
    , o.manufacturer
    , CONCAT(o.entity_id, '-', o.vendor_code) AS unique_vendor_code
    , o.no_fail_orders
    , o.no_fail_orders_vendor
    , o.no_fail_orders_transport
    , o.no_fail_orders_platform
    , o.no_fail_orders_unknown
    , o.no_fail_orders_od
    , o.no_fail_orders_pu
    , o.no_fail_orders_rd
    , o.no_fail_orders_godroid
    , o.no_fail_orders_gowin
    , o.no_fail_orders_pos
    , o.no_fail_orders_pos_indirect
    , o.no_fail_orders_pos_direct
    , o.no_orders_od
    , o.no_orders_pu
    , o.no_orders_rd
    , o.no_orders_godroid
    , o.no_orders_gowin
    , o.no_orders_pos
    , o.no_orders_pos_indirect
    , o.no_orders_pos_direct
    , o.no_oma_orders
    , o.no_oma_failed_orders
    , o.no_grocery_orders
    , o.no_oma_grocery_failed_orders
    , o.no_orders
    , o.no_ngt_orders
  FROM rps_orders o
)
SELECT o.*
  , en.brand_id
  , en.brand_name
  , c.country_name
  , v.vertical_type
FROM rps_orders_agg o
LEFT JOIN vendors v ON o.vendor_code = v.vendor_code
  AND o.entity_id = v.entity_id
INNER JOIN entities en ON o.entity_id = en.entity_id
INNER JOIN countries c ON o.country_iso = c.country_iso
