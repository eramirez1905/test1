SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(payment_id, country_code) AS uuid,
  payment_id AS id,
  `{project_id}`.pandata_intermediate.LG_UUID(rider_id, country_code) AS lg_rider_uuid,
  rider_id AS lg_rider_id,
  `{project_id}`.pandata_intermediate.LG_UUID(payment_cycle_id, country_code) AS lg_payment_cycle_uuid,
  payment_cycle_id AS lg_payment_cycle_id,
  `{project_id}`.pandata_intermediate.LG_UUID(payment_rule_id, country_code) AS lg_payments_deposit_rule_uuid,
  payment_rule_id AS lg_payments_deposit_rule_id,

  status,
  paid_amount AS paid_amount_local,
  remaining_amount AS remaining_amount_local,
  rider_name,
  region,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(country_code) AS country_code,

  timezone,
  created_date AS created_date_utc,
  payment_cycle_start_date AS payment_cycle_start_at_utc,
  payment_cycle_end_date AS payment_cycle_end_at_utc,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
FROM `fulfillment-dwh-production.curated_data_shared.rider_deposit_payments`
