SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(vendor_id, rdbms_id) AS vendor_uuid,
  vendor_id,
  salesforce_id,

  CAST(best_in_city AS BOOLEAN) AS is_best_in_city,

  preorder_offset_time AS preorder_offset_time_in_minutes,
  rider_pickup_instructions,

  delivery_box,
  imprint,
  legal_name,

  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.vendors_additional_info`
