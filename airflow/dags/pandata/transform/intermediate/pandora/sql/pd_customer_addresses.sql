SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(area_id, rdbms_id) AS area_uuid,
  area_id,
  `{project_id}`.pandata_intermediate.PD_UUID(city_id, rdbms_id) AS city_uuid,
  city_id,
  `{project_id}`.pandata_intermediate.PD_UUID(corporate_reference_id, rdbms_id) AS corporate_reference_uuid,
  corporate_reference_id,
  `{project_id}`.pandata_intermediate.PD_UUID(customer_id, rdbms_id) AS customer_uuid,
  customer_id,
  `{project_id}`.pandata_intermediate.PD_UUID(created_by, rdbms_id) AS created_by_user_uuid,
  created_by AS created_by_user_id,
  `{project_id}`.pandata_intermediate.PD_UUID(updated_by, rdbms_id) AS updated_by_user_uuid,
  updated_by AS updated_by_user_id,

  CAST(deleted AS BOOLEAN) AS is_deleted,
  CAST(is_default AS BOOLEAN) AS is_default,
  CAST(is_vendor_custom_location AS BOOLEAN) AS is_vendor_custom_location,
  IFNULL(is_verified = 1, FALSE) AS is_verified,

  title,
  type,
  CASE type
    WHEN 1 THEN 'home'
    WHEN 2 THEN 'work'
    ELSE 'other'
  END AS address_type,
  (
    IFNULL(address_line1, '') ||
    IFNULL(address_line2, '') ||
    IFNULL(address_line3, '') ||
    IFNULL(address_line4, '') ||
    IFNULL(address_line5, '')
  ) AS address,
  address_other,
  building,
  campus,
  city,
  district,
  company,
  customer_code,
  delivery_instructions,
  entrance,
  flat_number,
  floor,
  formatted_address,
  intercom,
  postcode,
  room,
  structure,
  position,

  NULLIF(latitude, 0.0) AS latitude,
  NULLIF(longitude, 0.0) AS longitude,

  last_usage AS last_usage_at_local,
  created_at AS created_at_local,
  updated_at AS updated_at_local,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.customeraddress`
