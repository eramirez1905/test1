SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(language_id, rdbms_id) AS language_uuid,
  language_id,

  title,
  description,
  email,
  fax,
  mobile,
  phone,
  prefered_contact AS preferred_contact,
  third_party,

  CAST(active AS BOOLEAN) AS is_active,
  type = 'own_delivery_foodpanda' AS is_type_own_delivery_foodpanda,
  type = 'own_delivery_third_party' AS is_type_own_delivery_third_party,
  type = 'vendor_delivery_third_party' AS is_type_vendor_delivery_third_party,
  CAST(express_delivery AS BOOLEAN) AS has_express_delivery,

  type,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc
FROM `{project_id}.pandata_raw_ml_backend_latest.deliveryprovider`
