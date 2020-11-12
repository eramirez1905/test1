CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.dps_variable_df_report`
CLUSTER BY entity_id AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 30 DAY) AS start_date
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_date
), orders_data AS (
  -- Consider vendors that have had orders in the last 30 days only.
  SELECT created_date
    , entity.id AS entity_id
    , vendor.vendor_code AS vendor_code
    , COUNT(order_id) AS total_orders
  FROM `{{ params.project_id }}.cl.orders`
  WHERE created_date >= (SELECT start_date FROM parameters)
    AND created_date <= (SELECT end_date FROM parameters)
GROUP BY 1, 2, 3
), classify_date_filters AS (
  -- Classify date filters to enable users of the report to select vendors that have had orders in with 'x' date range.
  SELECT created_date
    , entity_id
    , vendor_code
    , total_orders
    , CASE
        WHEN total_orders > 0 AND created_date BETWEEN DATE_SUB((SELECT end_date FROM parameters), INTERVAL 6 DAY)
          AND DATE_SUB((SELECT end_date FROM parameters), INTERVAL 1 DAY) THEN 'Last 7 Days'
        WHEN total_orders > 0 AND created_date BETWEEN DATE_SUB((SELECT end_date FROM parameters), INTERVAL 13 DAY)
          AND DATE_SUB((SELECT end_date FROM parameters), INTERVAL 1 DAY) THEN 'Last 14 Days'
        WHEN total_orders > 0 AND created_date BETWEEN (SELECT start_date FROM parameters)
          AND (SELECT end_date FROM parameters) THEN 'Last 30 Days'
        ELSE 'Exclude'
      END AS date_filter
  FROM orders_data
), set_date_filters AS (
  -- Grouping by the values to only select distinct values from the above CTE.
  SELECT entity_id
    , vendor_code
    , date_filter
  FROM classify_date_filters
  GROUP BY 1, 2, 3
), vendor_data AS (
  SELECT entity_id
    , vendor_code
    , IF(dps.scheme_id IS NULL, FALSE, TRUE) AS is_dps_active
    , scheme_id
    , dps.is_scheme_fallback
    , dps.variant
    -- Setting NULL values to 0 to enable count of values for the below CTE. Values are NULL for upper threshold limits.
    , IF(pc.travel_time_config.fee IS NULL, 0, pc.travel_time_config.fee) AS travel_time_fee
    , IF(pc.delay_config.fee IS NULL, 0, pc.delay_config.fee) AS delay_fee
    , IF(pc.mov_config.minimum_order_value IS NULL, 0, pc.mov_config.minimum_order_value) AS minimum_order_value
    , (IF(pc.travel_time_config.fee IS NULL, 0, pc.travel_time_config.fee)
        + IF(pc.delay_config.fee IS NULL, 0, pc.delay_config.fee)
        + IF(pc.mov_config.minimum_order_value IS NULL, 0, pc.mov_config.minimum_order_value)) AS total_df
  FROM `{{ params.project_id }}.cl.vendors_v2`
  LEFT JOIN UNNEST (dps) dps
  LEFT JOIN UNNEST (vendor_config) vc
  LEFT JOIN UNNEST (pricing_config) pc
  WHERE 'VENDOR_DELIVERY' NOT IN UNNEST(delivery_provider)
), delivery_fee_data AS (
  SELECT v.entity_id
    , v.vendor_code
    , sd.date_filter
    , IF(COUNT(DISTINCT scheme_id) = 0, 1, 0)  AS count_no_price_scheme
    , IF(COUNT(DISTINCT scheme_id) = 1, 1, 0)  AS count_single_price_scheme
    , IF(COUNT(DISTINCT scheme_id) = 2 AND COUNT(DISTINCT variant) = 1, 1, 0)  AS count_double_price_scheme
    , IF(COUNT(DISTINCT scheme_id) = 3 AND COUNT(DISTINCT variant) = 1, 1, 0)  AS count_triple_price_scheme
    , IF(COUNT(DISTINCT scheme_id) > 3 AND COUNT(DISTINCT variant) = 1, 1, 0)  AS count_multiple_price_scheme
    , IF(COUNT(DISTINCT scheme_id) > 1 AND COUNT(DISTINCT variant) > 1, 1, 0) AS count_single_vendor_multiple_variant_scheme
    , IF(COUNT(DISTINCT total_df) > 1, 'Variable DF', 'Flat DF') AS category
    , IF(COUNT(DISTINCT travel_time_fee) > 1, 1, 0) AS variable_travel_time_fee
    , IF(COUNT(DISTINCT delay_fee) > 1, 1, 0) AS variable_delay_fee
    , IF(COUNT(DISTINCT minimum_order_value) > 1, 1, 0) AS variable_mov
  FROM vendor_data v
  LEFT JOIN set_date_filters sd USING (entity_id, vendor_code)
  GROUP BY 1, 2, 3
), type_of_delivery_fee AS (
  SELECT entity_id
    , date_filter
    , COUNTIF(category = 'Variable DF') AS count_variable_fee_vendors
    , COUNTIF(category = 'Flat DF') AS count_flat_fee_vendors
    , SUM(variable_travel_time_fee) AS count_variable_travel_time_fee
    , SUM(variable_delay_fee) AS count_variable_delay_fee
    , SUM(variable_mov) AS count_variable_mov
    , SUM(count_no_price_scheme) AS count_no_price_scheme
    , SUM(count_single_price_scheme) AS count_single_price_scheme
    , SUM(count_double_price_scheme) AS count_double_price_scheme
    , SUM(count_triple_price_scheme) AS count_triple_price_scheme
    , SUM(count_multiple_price_scheme) AS count_multiple_price_scheme
    , SUM(count_single_vendor_multiple_variant_scheme) AS count_single_vendor_multiple_variant_scheme
  FROM delivery_fee_data
  GROUP BY 1, 2
), total_vendor_count AS (
  SELECT v.entity_id
    , sd.date_filter
    , COUNT(DISTINCT v.vendor_code) AS total_vendors
    , COUNTIF(is_dps_active IS TRUE) AS is_dps_active_count
  FROM vendor_data v
  LEFT JOIN set_date_filters sd USING (entity_id, vendor_code)
  GROUP BY 1, 2
), countries_data AS (
  SELECT DISTINCT region
    , country_name
    , platforms.entity_id
    , platforms.display_name
    , platforms.brand_name
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(platforms) platforms
), aggregate_date AS (
  SELECT region
    , entity_id
    , display_name AS entity_display_name
    , country_name
    , brand_name
    , df.date_filter
    , count_variable_fee_vendors
    , count_flat_fee_vendors
    , IF(is_dps_active_count > SAFE_MULTIPLY(0.50, total_vendors), TRUE, FALSE) AS is_dps_active
    , count_variable_travel_time_fee
    , count_variable_delay_fee
    , count_variable_mov
    , count_no_price_scheme
    , count_single_price_scheme
    , count_double_price_scheme
    , count_triple_price_scheme
    , count_multiple_price_scheme
    , count_single_vendor_multiple_variant_scheme
    , total_vendors
    , is_dps_active_count
  FROM type_of_delivery_fee df
  LEFT JOIN total_vendor_count yv USING (entity_id, date_filter)
  LEFT JOIN countries_data USING (entity_id)
)
SELECT region
  , entity_id
  , entity_display_name
  , country_name
  , brand_name
  , date_filter
  , count_variable_fee_vendors
  , count_flat_fee_vendors
  , is_dps_active
  , count_variable_travel_time_fee
  , count_variable_delay_fee
  , count_variable_mov
  , count_no_price_scheme
  , count_single_price_scheme
  , count_double_price_scheme
  , count_triple_price_scheme
  , count_multiple_price_scheme
  , count_single_vendor_multiple_variant_scheme
  , total_vendors
  , is_dps_active_count
FROM aggregate_date
WHERE is_dps_active
  AND date_filter IS NOT NULL
