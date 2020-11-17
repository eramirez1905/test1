SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(vendor_id, rdbms_id) AS vendor_uuid,
  vendor_id,
  -- condition_object_id is a polymorphic field to be used with condition_type
  `{project_id}`.pandata_intermediate.PD_UUID(condition_object_id, rdbms_id) AS condition_object_uuid,
  condition_object_id,

  title,
  description,
  discount_text,
  banner_title,
  file_path,
  platforms,
  daily_limit,
  promotional_limit AS total_limit,
  ratio_foodpanda AS foodpanda_ratio,

  -- types
  bogo_discount_unit AS bogo_discount_unit_type,
  expedition_type,
  discount_type,
  condition_type,
  discount_mgmt_created_by AS discount_mgmt_created_by_type,
  discount_mgmt_updated_by AS discount_mgmt_updated_by_type,

  -- boolean
  CAST(active AS BOOLEAN) AS is_active,
  CAST(deleted AS BOOLEAN) AS is_deleted,
  IFNULL(is_pro_discount = 1, FALSE) AS is_pro_discount,
  IFNULL(new_customer_only = 1, FALSE) AS is_new_customer_only,
  condition_type = 'multiple_vendors' AS is_condition_multiple_vendors,
  condition_type = 'vendor' AS is_condition_vendor,
  condition_type = 'menucategory' AS is_condition_menu_category,
  condition_type = 'product' AS is_condition_product,
  condition_type = 'productvariation' AS is_condition_product_variation,
  condition_type = 'multiple_menu_categories' AS is_condition_multiple_menu_categories,
  condition_type = 'menu' AS is_condition_menu,
  condition_type = 'chain' AS is_condition_chain,
  condition_type = 'none' AS is_condition_none,

  -- currency
  maximum_discount_amount as maximum_discount_amount_local,
  minimum_order_value AS minimum_order_value_local,
  IF(
    discount_type = 'percentage',
    ROUND(SAFE_DIVIDE(CAST(discount_amount AS NUMERIC), 100), 2),
    CAST(discount_amount AS NUMERIC)
  ) AS amount_local,

  -- time
  PARSE_TIME("%T", IF(start_hour = '24:00:00', '00:00:00', start_hour)) AS start_time_local,
  PARSE_TIME("%T", IF(end_hour = '24:00:00', '23:59:00', end_hour)) AS end_time_local,

  -- timestamps
  DATE(start_date) AS start_date_local,
  DATE(end_date) AS end_date_local,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.discounts`
