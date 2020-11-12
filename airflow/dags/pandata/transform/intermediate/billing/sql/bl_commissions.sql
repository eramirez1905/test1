SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(client_id, rdbms_id) AS client_uuid,
  client_id,

  commission_base AS commission_base_type,
  restaurant_revenue AS restaurant_revenue_type,
  tier_base AS tier_base_type,
  tier_calculation AS tier_calculation_type,

  invoicing_period,
  payment_charge_amount AS payment_charge_amount_local,
  payment_charge_percentage,

  start_date AS start_date_utc,
  start_date_utc AS start_at_utc,
  end_date AS end_date_utc,
  end_date_utc AS end_at_utc,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_billing_latest.commission`
