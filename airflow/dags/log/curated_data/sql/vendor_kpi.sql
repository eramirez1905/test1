CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.vendor_kpi`
PARTITION BY created_date_local
CLUSTER BY entity_id, vendor_code AS
WITH entities AS (
  SELECT p.entity_id
    , p.display_name
    , p.brand_id
    , p.timezone
    , en.country_iso
    , en.country_name
    , en.region_short_name AS region
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), rps_metrics_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendor_kpi_rps`
), od_metrics_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendor_kpi_od`
), rps_metrics_date_base AS (
  SELECT created_hour_local
    , entity_id
    , vendor_code
  FROM rps_metrics_dataset
  UNION DISTINCT
  SELECT created_hour_local
    , entity_id
    , vendor_code
  FROM od_metrics_dataset
)
SELECT CAST(b.created_hour_local AS DATE) AS created_date_local
  , b.created_hour_local
  , en.entity_id
  , en.display_name
  , en.brand_id
  , en.region
  , en.country_iso
  , en.country_name
  , en.timezone
  , b.vendor_code
  , STRUCT(
      rps.orders_ct
      , rps.success_orders_ct
      , rps.fail_orders_ct
      , rps.fail_orders_vendor_ct
      , rps.fail_orders_transport_ct
      , rps.fail_orders_platform_ct
      , rps.fail_orders_customer_ct
      , rps.fail_orders_fulfillment_ct
      , rps.fail_orders_unreachable_ct
      , rps.fail_orders_item_unavailable_ct
      , rps.fail_orders_no_response_ct
      , rps.fail_orders_closed_ct
      , rps.ngt_orders_ct
      , rps.godroid_orders_ct
      , rps.gowin_orders_ct
      , rps.pos_orders_ct
      , rps.pos_direct_orders_ct
      , rps.pos_indirect_orders_ct
      , rps.platform_client_orders_ct
      , rps.od_orders_ct
      , rps.vd_orders_ct
      , rps.pu_orders_ct
      , rps.preorders_ct
      , rps.asap_orders_ct
    ) AS orders
  , STRUCT(
      rps.total_transmission_time
      , rps.total_response_time
    ) AS order_timings
  , STRUCT(
      rps.fail_orders_customer_before_sent_to_vendor_ct
      , rps.fail_orders_platform_before_sent_to_vendor_ct
      , rps.orders_sent_to_vendor_ct
      , rps.fail_orders_customer_before_received_by_vendor_ct
      , rps.fail_orders_platform_before_received_by_vendor_ct
      , rps.orders_received_by_vendor_ct
      , rps.fail_orders_customer_before_vendor_response_ct
      , rps.fail_orders_platform_before_vendor_response_ct
      , rps.orders_accepted_by_vendor_ct
      , rps.orders_accepted_by_vendor_within_1_min_ct
      , rps.orders_accepted_by_vendor_over_1_min_ct
      , rps.orders_rejected_by_vendor_ct
      , rps.orders_modified_by_vendor_ct
      , rps.od_orders_estimated_time_within_20_mins_ct
      , rps.od_orders_estimated_time_20_to_35_mins_ct
      , rps.od_orders_estimated_time_over_35_mins_ct
      , rps.od_orders_preparation_time_within_8_mins_ct
      , rps.od_orders_preparation_time_8_to_20_mins_ct
      , rps.od_orders_preparation_time_over_20_mins_ct
      , rps.orders_picked_up_late_2min_ct
      , rps.orders_implicitly_accepted_by_vendor_ct
      , rps.orders_confirmed_by_vendor_above_30_sec_ct
      , rps.orders_sent_to_vendor_over_3_sec_ct
      , rps.orders_completed_late_ct
    ) AS defective_orders
  , STRUCT(
      rps.total_promised_delivery_time_client
      , rps.login_started_ct
      , rps.login_succeeded_ct
      , rps.login_failed_ct
    ) AS client
  , STRUCT(
      rps.slots_ct
      , rps.unreachables_ct
      , rps.unreachables_0_sec_ct
      , rps.unreachables_60_sec_ct
      , rps.unreachables_120_sec_ct
      , rps.unreachables_180_sec_ct
      , rps.unreachables_over_180_sec_ct
      , rps.unreachables_100_percent_ct
      , rps.total_unreachable_duration
      , rps.total_unreachable_duration_60_sec
      , rps.total_unreachable_duration_120_sec
      , rps.total_unreachable_duration_180_sec
      , rps.total_unreachable_duration_over_180_sec
      , rps.offlines_ct
      , rps.offline_1800_sec_ct
      , rps.offline_over_1800_sec_ct
    ) AS connectivity
  , lr.own_deliveries
FROM rps_metrics_date_base b
LEFT JOIN rps_metrics_dataset rps ON b.vendor_code = rps.vendor_code
  AND b.entity_id = rps.entity_id
  AND b.created_hour_local = rps.created_hour_local
LEFT JOIN od_metrics_dataset lr ON b.vendor_code = lr.vendor_code
  AND b.entity_id = lr.entity_id
  AND b.created_hour_local = lr.created_hour_local
INNER JOIN entities en ON b.entity_id = en.entity_id
