SELECT
  rider_deposit_payments.uuid,
  rider_deposit_payments.id,
  countries.rdbms_id,
  rider_deposit_payments.country_code,
  rider_deposit_payments.lg_rider_uuid,
  rider_deposit_payments.lg_rider_id,
  rider_deposit_payments.lg_payment_cycle_uuid,
  rider_deposit_payments.lg_payment_cycle_id,
  rider_deposit_payments.lg_payments_deposit_rule_uuid,
  rider_deposit_payments.lg_payments_deposit_rule_id,
  rider_deposit_payments.status,
  rider_deposit_payments.paid_amount_local,
  rider_deposit_payments.remaining_amount_local,
  rider_deposit_payments.rider_name,
  rider_deposit_payments.region,
  rider_deposit_payments.timezone,
  rider_deposit_payments.created_date_utc,
  rider_deposit_payments.payment_cycle_start_at_utc,
  rider_deposit_payments.payment_cycle_end_at_utc,
  rider_deposit_payments.created_at_utc,
  rider_deposit_payments.updated_at_utc,
FROM `{project_id}.pandata_intermediate.lg_rider_deposit_payments` AS rider_deposit_payments
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON rider_deposit_payments.country_code = countries.lg_country_code