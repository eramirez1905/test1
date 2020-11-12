SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(customer_id, rdbms_id) AS customer_uuid,
  customer_id,
  `{project_id}`.pandata_intermediate.PD_UUID(order_id, rdbms_id) AS order_uuid,
  order_id,
  `{project_id}`.pandata_intermediate.PD_UUID(productvariation_id, rdbms_id) AS product_variation_uuid,
  productvariation_id AS product_variation_id,

  title,
  description,
  variation_title,
  quantity,
  sold_out_option,
  special_instructions,
  IFNULL(type = 'half', FALSE) AS is_half,
  vat_rate / 100 AS vat_rate,

  container_price AS container_price_local,
  discount AS discount_local,
  price AS price_local,
  sponsorship AS sponsorship_local,
  toppings_half_price AS toppings_half_price_local,
  toppings_price AS toppings_price_local,
  total_price AS total_price_local,
  total_vat_amount AS total_vat_amount_local,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.orderproducts`
