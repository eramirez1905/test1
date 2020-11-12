CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.weekly_flash_summary_vendors` AS
WITH vendors_deliveryarea_evaluation AS (
  SELECT v.entity_id
    , v.vendor_code
    , v.name AS vendor_name
    , pory.country_code
    , delivery_provider
    , his.drive_time
    , da.id AS delivery_area_id
    , his.operation_type
    , his.active_from AS his_active_from
    , his.active_to AS his_active_to
    , his.settings.delivery_fee.amount AS delivery_fee
    , DENSE_RANK() OVER (PARTITION BY v.entity_id, v.vendor_code, operation_type ORDER BY entity_id, vendor_code,
        active_from DESC) AS _rank
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  LEFT JOIN UNNEST(delivery_provider) delivery_provider ON delivery_provider IN ('OWN_DELIVERY')
  LEFT JOIN UNNEST(porygon) AS pory
  LEFT JOIN UNNEST(hurrier) AS hurrier
  LEFT JOIN UNNEST(delivery_areas) da ON da.is_deleted IS FALSE
  LEFT JOIN UNNEST(da.history) his ON his.operation_type IN ('updated', 'created')
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE pory.country_code NOT LIKE '%dp%'
), vendors_da_active_to_adj  AS (
  SELECT entity_id
    , vendor_code
    , vendor_name
    , country_code
    , delivery_provider
    , drive_time
    , delivery_area_id
    , operation_type
    , his_active_from
    , his_active_to
    , delivery_fee
    , _rank
    , CASE
        WHEN his_active_to IS NULL AND _rank <> 1
           THEN LAG(his_active_from) OVER (PARTITION BY entity_id, vendor_code ORDER BY his_active_from DESC)
        WHEN his_active_to IS NULL AND _rank = 1 THEN his_active_to
        WHEN his_active_to IS NOT NULL THEN his_active_to
      END AS his_active_to_new
  FROM vendors_deliveryarea_evaluation
  -- By filter on operation_type, we eliminate the vendors without delivery areas in Porygon
  WHERE delivery_provider IS NOT NULL
   AND delivery_area_id IS NOT NULL
), vendors_active_deliveryarea AS (
  SELECT entity_id
    , vendor_code
    , vendor_name
    , country_code
    , delivery_provider
    , drive_time
    , delivery_area_id
    , operation_type
    , his_active_from
    , his_active_to
    , delivery_fee
    , _rank
    , MAX(his_active_to_new) OVER (PARTITION BY entity_id, vendor_code, operation_type, _rank ORDER BY
      his_active_from DESC, _rank) AS his_active_to_new
  FROM vendors_da_active_to_adj
  WHERE his_active_to_new IS NULL
), vendors_distance_based_count AS (
  SELECT entity_id
    , vendor_code
    , COUNT(DISTINCT delivery_area_id) AS count_deliveryareas
    , COUNT(DISTINCT delivery_fee) AS count_distinct_delivery_fee
  FROM vendors_active_deliveryarea
  GROUP BY 1, 2
), vendors_if_dbdf_flat AS (
  SELECT entity_id
    , vendor_code
    , IF(count_distinct_delivery_fee = 1, TRUE, FALSE) AS is_dbdf_flat
  FROM vendors_distance_based_count
), vendors_dynamic_pricing_count AS (
  SELECT dpc.entity_id
    , dpc.vendor_code
    , COUNT(DISTINCT dpc.travel_time_fee_config.fee) AS travel_time_fee
    , COUNT(DISTINCT mov_fee_config.minimum_order_value) AS mov_fee
    , COUNT(DISTINCT delay_fee_config.fee) AS delay_fee
  FROM `{{ params.project_id }}.cl.dynamic_pricing_config` dpc
  WHERE dpc.is_active
  GROUP BY 1, 2
), vendors_if_dps_fee_flat AS (
  SELECT entity_id
    , vendor_code
    , IF(travel_time_fee = 1 AND mov_fee = 1 AND delay_fee = 1, TRUE, FALSE) AS is_dps_fee_flat
  FROM vendors_dynamic_pricing_count
), vendors_flat_df AS (
  SELECT v.entity_id
    , v.country_code
    , v.vendor_code
    , v.vendor_name
    , vidf.is_dbdf_flat
    , vidpsf.is_dps_fee_flat
  FROM (
    SELECT DISTINCT country_code
      , entity_id
      , vendor_code
      , vendor_name
    FROM vendors_active_deliveryarea) v
  LEFT JOIN vendors_if_dbdf_flat vidf ON v.entity_id = vidf.entity_id
    AND v.vendor_code = vidf.vendor_code
  LEFT JOIN vendors_if_dps_fee_flat vidpsf ON v.entity_id = vidpsf.entity_id
    AND v.vendor_code = vidpsf.vendor_code
), is_vendors_flat_df AS (
SELECT country_code
  , entity_id
  , vendor_code
  , vendor_name
  , is_dbdf_flat
  , is_dps_fee_flat
  , CASE
      WHEN (is_dbdf_flat IS FALSE AND is_dps_fee_flat IS TRUE) THEN TRUE
      WHEN (is_dbdf_flat IS TRUE AND is_dps_fee_flat IS NOT FALSE) THEN TRUE
      ELSE FALSE
    END AS is_flat
FROM vendors_flat_df
)
SELECT country_code
  , entity_id
  , COUNT(IF(is_flat, vendor_code, NULL)) AS flat_vendors
  , COUNT(vendor_code) AS total_vendors
FROM is_vendors_flat_df
GROUP BY 1, 2
