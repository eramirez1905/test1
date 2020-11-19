SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(main_vendor_id, rdbms_id) AS main_vendor_uuid,
  main_vendor_id,

  title,
  code,

  CAST(active AS BOOLEAN) AS is_active,
  CAST(deleted AS BOOLEAN) AS is_deleted,
  CAST(is_accepting_global_vouchers AS BOOLEAN) AS is_accepting_global_vouchers,
  CAST(is_always_grouping_by_chain_on_listing_page AS BOOLEAN) AS is_always_grouping_by_chain_on_listing_page,
  CAST(require_chain_stacking AS BOOLEAN) AS is_chain_stacking_required,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc
FROM `{project_id}.pandata_raw_ml_backend_latest.vendorschains`
