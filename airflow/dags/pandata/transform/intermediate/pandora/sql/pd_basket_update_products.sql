SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(basket_update_id, rdbms_id) AS basket_update_uuid,
  basket_update_id,
  `{project_id}`.pandata_intermediate.PD_UUID(product_id, rdbms_id) AS product_uuid,
  product_id,
  `{project_id}`.pandata_intermediate.PD_UUID(productvariation_id, rdbms_id) AS product_variation_uuid,
  productvariation_id AS product_variation_id,

  action,
  original_quantity,
  reason,
  title,
  variation_title,

  packaging_fee_gross AS packaging_fee_gross_local,
  subtotal_gross AS subtotal_gross_local,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc
FROM `{project_id}.pandata_raw_ml_backend_latest.basket_update_product`
