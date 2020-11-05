SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(commission_id, rdbms_id) AS commission_uuid,
  commission_id,

  lower_bound,
  IF(standard_commission_value > 1, standard_commission_value / 100, standard_commission_value) AS standard_commission_value_percentage,
  standard_commission_value_fixed_amount AS standard_commission_value_fixed_amount_local,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_billing_latest.commission_tier`
