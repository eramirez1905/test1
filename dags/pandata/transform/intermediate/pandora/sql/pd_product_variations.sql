SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(product_id, rdbms_id) AS product_uuid,
  product_id,

  code,
  title,
  price AS price_local,
  sponsorship AS sponsorship_price_local,
  container_price AS container_price_local,
  deleted = 1 AS is_deleted,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modifed_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.productvariations`
