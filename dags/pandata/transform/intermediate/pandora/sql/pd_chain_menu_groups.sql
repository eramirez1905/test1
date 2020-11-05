SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(chain_id, rdbms_id) AS chain_uuid,
  chain_id,
  `{project_id}`.pandata_intermediate.PD_UUID(main_vendor_id, rdbms_id) AS main_vendor_uuid,
  main_vendor_id,

  title,
  description,

  CAST(deleted AS BOOLEAN) AS is_deleted,

  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.chain_menu_group`
