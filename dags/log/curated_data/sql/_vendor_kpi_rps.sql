CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendor_kpi_rps`
PARTITION BY created_date_local
CLUSTER BY entity_id, vendor_code AS
WITH rps_metrics_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendor_kpis_rps_orders`
), rps_metrics_client_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendor_kpis_rps_client`
), rps_metrics_connectivity_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendor_kpis_rps_connectivity`
), rps_metrics_date_base AS (
  SELECT created_hour_local
    , entity_id
    , vendor_code
  FROM rps_metrics_orders_dataset
  UNION DISTINCT
  SELECT created_hour_local
    , entity_id
    , vendor_code
  FROM rps_metrics_client_dataset
  UNION DISTINCT
  SELECT created_hour_local
    , entity_id
    , vendor_code
  FROM rps_metrics_connectivity_dataset
)
SELECT CAST(b.created_hour_local AS DATE) AS created_date_local
  , b.created_hour_local
  , b.entity_id
  , b.vendor_code
  , COALESCE(o.orders_ct, 0) AS orders_ct
  , COALESCE(o.success_orders_ct, 0) AS success_orders_ct
  , COALESCE(o.fail_orders_ct, 0) AS fail_orders_ct
  , COALESCE(o.fail_orders_vendor_ct, 0) AS fail_orders_vendor_ct
  , COALESCE(o.fail_orders_transport_ct, 0) AS fail_orders_transport_ct
  , COALESCE(o.fail_orders_platform_ct, 0) AS fail_orders_platform_ct
  , COALESCE(o.fail_orders_customer_ct, 0) AS fail_orders_customer_ct
  , COALESCE(o.fail_orders_fulfillment_ct, 0) AS fail_orders_fulfillment_ct
  , COALESCE(o.fail_orders_unreachable_ct, 0) AS fail_orders_unreachable_ct
  , COALESCE(o.fail_orders_item_unavailable_ct, 0) AS fail_orders_item_unavailable_ct
  , COALESCE(o.fail_orders_no_response_ct, 0) AS fail_orders_no_response_ct
  , COALESCE(o.fail_orders_closed_ct, 0) AS fail_orders_closed_ct
  , COALESCE(o.ngt_orders_ct, 0) AS ngt_orders_ct
  , COALESCE(o.godroid_orders_ct, 0) AS  godroid_orders_ct
  , COALESCE(o.gowin_orders_ct, 0) AS gowin_orders_ct
  , COALESCE(o.pos_orders_ct, 0) AS pos_orders_ct
  , COALESCE(o.pos_direct_orders_ct, 0) AS pos_direct_orders_ct
  , COALESCE(o.pos_indirect_orders_ct, 0) AS pos_indirect_orders_ct
  , COALESCE(o.platform_client_orders_ct, 0) AS platform_client_orders_ct
  , COALESCE(o.od_orders_ct, 0) AS od_orders_ct
  , COALESCE(o.vd_orders_ct, 0) AS vd_orders_ct
  , COALESCE(o.pu_orders_ct, 0) AS pu_orders_ct
  , COALESCE(o.preorders_ct, 0) AS preorders_ct
  , COALESCE(o.asap_orders_ct, 0) AS asap_orders_ct
  , COALESCE(o.total_transmission_time, 0) AS total_transmission_time
  , COALESCE(o.total_response_time, 0) AS total_response_time
  , COALESCE(o.fail_orders_customer_before_sent_to_vendor_ct, 0) AS fail_orders_customer_before_sent_to_vendor_ct
  , COALESCE(o.fail_orders_platform_before_sent_to_vendor_ct, 0) AS fail_orders_platform_before_sent_to_vendor_ct
  , COALESCE(o.orders_sent_to_vendor_ct, 0) AS orders_sent_to_vendor_ct
  , COALESCE(o.fail_orders_customer_before_received_by_vendor_ct, 0) AS fail_orders_customer_before_received_by_vendor_ct
  , COALESCE(o.fail_orders_platform_before_received_by_vendor_ct, 0) AS fail_orders_platform_before_received_by_vendor_ct
  , COALESCE(o.orders_received_by_vendor_ct, 0) AS orders_received_by_vendor_ct
  , COALESCE(o.fail_orders_customer_before_vendor_response_ct, 0) AS fail_orders_customer_before_vendor_response_ct
  , COALESCE(o.fail_orders_platform_before_vendor_response_ct, 0) AS fail_orders_platform_before_vendor_response_ct
  , COALESCE(o.orders_accepted_by_vendor_ct, 0) AS orders_accepted_by_vendor_ct
  , COALESCE(o.orders_accepted_by_vendor_within_1_min_ct, 0) AS orders_accepted_by_vendor_within_1_min_ct
  , COALESCE(o.orders_accepted_by_vendor_over_1_min_ct, 0) AS orders_accepted_by_vendor_over_1_min_ct
  , COALESCE(o.orders_rejected_by_vendor_ct, 0) AS orders_rejected_by_vendor_ct
  , COALESCE(o.orders_modified_by_vendor_ct, 0) AS orders_modified_by_vendor_ct
  , COALESCE(o.od_orders_estimated_time_within_20_mins_ct, 0) AS od_orders_estimated_time_within_20_mins_ct
  , COALESCE(o.od_orders_estimated_time_20_to_35_mins_ct, 0) AS od_orders_estimated_time_20_to_35_mins_ct
  , COALESCE(o.od_orders_estimated_time_over_35_mins_ct, 0) AS od_orders_estimated_time_over_35_mins_ct
  , COALESCE(o.od_orders_preparation_time_within_8_mins_ct, 0) AS od_orders_preparation_time_within_8_mins_ct
  , COALESCE(o.od_orders_preparation_time_8_to_20_mins_ct, 0) AS od_orders_preparation_time_8_to_20_mins_ct
  , COALESCE(o.od_orders_preparation_time_over_20_mins_ct, 0) AS od_orders_preparation_time_over_20_mins_ct
  , COALESCE(o.orders_picked_up_late_2min_ct, 0) AS orders_picked_up_late_2min_ct
  , COALESCE(o.orders_implicitly_accepted_by_vendor_ct, 0) AS orders_implicitly_accepted_by_vendor_ct
  , COALESCE(o.orders_confirmed_by_vendor_above_30_sec_ct, 0) AS orders_confirmed_by_vendor_above_30_sec_ct
  , COALESCE(o.orders_sent_to_vendor_over_3_sec_ct, 0) AS orders_sent_to_vendor_over_3_sec_ct
  , COALESCE(o.orders_completed_late_ct, 0) AS orders_completed_late_ct
  , COALESCE(ev.total_promised_delivery_time_client, 0) AS total_promised_delivery_time_client
  , COALESCE(ev.login_started_ct, 0) AS login_started_ct
  , COALESCE(ev.login_succeeded_ct, 0) AS login_succeeded_ct
  , COALESCE(ev.login_failed_ct, 0) AS login_failed_ct
  , COALESCE(con.slots_ct, 0) AS slots_ct
  , COALESCE(con.unreachables_ct, 0) AS unreachables_ct
  , COALESCE(con.unreachables_0_sec_ct, 0) AS unreachables_0_sec_ct
  , COALESCE(con.unreachables_60_sec_ct, 0) AS unreachables_60_sec_ct
  , COALESCE(con.unreachables_120_sec_ct, 0) AS unreachables_120_sec_ct
  , COALESCE(con.unreachables_180_sec_ct, 0) AS unreachables_180_sec_ct
  , COALESCE(con.unreachables_over_180_sec_ct, 0) AS unreachables_over_180_sec_ct
  , COALESCE(con.unreachables_100_percent_ct, 0) AS unreachables_100_percent_ct
  , COALESCE(con.total_unreachable_duration, 0) AS total_unreachable_duration
  , COALESCE(con.total_unreachable_duration_60_sec, 0) AS total_unreachable_duration_60_sec
  , COALESCE(con.total_unreachable_duration_120_sec, 0) AS total_unreachable_duration_120_sec
  , COALESCE(con.total_unreachable_duration_180_sec, 0) AS total_unreachable_duration_180_sec
  , COALESCE(con.total_unreachable_duration_over_180_sec, 0) AS total_unreachable_duration_over_180_sec
  , COALESCE(con.offlines_ct, 0) AS offlines_ct
  , COALESCE(con.offline_1800_sec_ct, 0) AS offline_1800_sec_ct
  , COALESCE(con.offline_over_1800_sec_ct, 0) AS offline_over_1800_sec_ct
FROM rps_metrics_date_base b
LEFT JOIN rps_metrics_orders_dataset o ON b.vendor_code = o.vendor_code
  AND b.entity_id = o.entity_id
  AND b.created_hour_local = o.created_hour_local
LEFT JOIN rps_metrics_client_dataset ev ON b.vendor_code = ev.vendor_code
  AND b.entity_id = ev.entity_id
  AND b.created_hour_local = ev.created_hour_local
LEFT JOIN rps_metrics_connectivity_dataset con ON b.vendor_code = con.vendor_code
  AND b.entity_id = con.entity_id
  AND b.created_hour_local = con.created_hour_local
