SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(master_category_id, rdbms_id) AS master_category_uuid,
  master_category_id,
  `{project_id}`.pandata_intermediate.PD_UUID(option_value_service_tax_id, rdbms_id) AS option_value_service_tax_uuid,
  option_value_service_tax_id,
  `{project_id}`.pandata_intermediate.PD_UUID(option_value_vat_id, rdbms_id) AS option_value_vat_uuid,
  option_value_vat_id,
  `{project_id}`.pandata_intermediate.PD_UUID(vendor_id, rdbms_id) AS vendor_uuid,
  vendor_id,

  title,
  description,
  position,
  CAST(deleted AS BOOLEAN) AS is_deleted,
  CAST(show_even_has_no_products AS BOOLEAN) AS is_shown_without_products,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc
FROM `{project_id}.pandata_raw_ml_backend_latest.menucategories`
