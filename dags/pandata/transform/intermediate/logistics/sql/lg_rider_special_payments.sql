SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(details.payment_id, rider_special_payments.country_code) AS uuid,
  details.payment_id AS id,
  `{project_id}`.pandata_intermediate.LG_UUID(rider_special_payments.rider_id, rider_special_payments.country_code) AS lg_rider_uuid,
  rider_special_payments.rider_id AS lg_rider_id,
  `{project_id}`.pandata_intermediate.LG_UUID(details.payment_cycle_id, rider_special_payments.country_code) AS lg_payment_cycle_uuid,
  details.payment_cycle_id AS lg_payment_cycle_id,

  rider_special_payments.region,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(rider_special_payments.country_code) AS country_code,
  rider_special_payments.rider_name,
  details.created_by AS created_by_email,
  details.status,
  details.reason,
  details.note,
  details.total AS paid_local,
  details.adjustment_type,
  rider_special_payments.timezone,
  details.payment_cycle_start_date AS payment_cycle_start_at_utc,
  details.payment_cycle_end_date AS payment_cycle_end_at_utc,
  details.created_at AS created_at_utc,
  rider_special_payments.created_date AS created_date_utc,
FROM `fulfillment-dwh-production.curated_data_shared.rider_special_payments` AS rider_special_payments
CROSS JOIN UNNEST(rider_special_payments.payment_details) AS details
