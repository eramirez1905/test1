SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(orderproduct_id, rdbms_id) AS order_product_uuid,
  orderproduct_id AS order_product_id,
  `{project_id}`.pandata_intermediate.PD_UUID(toppingtemplateproduct_id, rdbms_id) AS topping_template_product_uuid,
  toppingtemplateproduct_id AS topping_template_product_id,

  title,
  half_position,
  price AS price_local,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.ordertoppings`
