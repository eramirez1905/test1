SELECT
  uuid,
  id,
  rdbms_id,
  phone_number,
  dwh_last_modified_at_utc,
FROM `{project_id}.pandata_intermediate.pd_blacklisted_phone_numbers`
