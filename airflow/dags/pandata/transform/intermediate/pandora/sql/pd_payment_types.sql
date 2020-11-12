SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,

  title,
  description,
  position,
  checkout_text,
  brand_code AS brand_code_type,
  code AS code_type,
  subtype_code AS code_sub_type,
  payment_method AS payment_method_type,
  CAST(active AS BOOLEAN) AS is_active,
  CAST(is_delivery_enabled AS BOOLEAN) AS is_delivery_enabled,
  CAST(is_hosted AS BOOLEAN) AS is_hosted,
  CAST(is_pickup_enabled AS BOOLEAN) AS is_pickup_enabled,
  CAST(is_tokenisation_checked AS BOOLEAN) AS is_tokenisation_checked,
  CAST(is_tokenisation_enabled AS BOOLEAN) AS is_tokenisation_enabled,
  IFNULL(need_for_change = 1, FALSE) AS has_need_for_change,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc
FROM `{project_id}.pandata_raw_ml_backend_latest.paymenttypes`
