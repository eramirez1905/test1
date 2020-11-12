SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(id, country_code) AS uuid,
  id,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(country_code) AS country_code,

  threshold,
  status,
  applies_to AS applies_to_type,
  unit_type AS payment_unit_type,
  amount AS payment_unit_amount,
  duration AS duration_iso8601,
  created_by AS created_by_email,

  active AS is_active,
  signing_bonus AS has_signing_bonus,
  status = 'APPROVED' AS is_status_approved,
  status = 'REJECTED' AS is_status_rejected,
  status = 'PENDING' AS is_status_pending,

  signing_bonus_amount AS signing_bonus_local,

  start_date AS start_at_utc,
  end_date AS end_at_utc,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  created_date AS created_date_utc,
FROM `fulfillment-dwh-production.curated_data_shared.payments_referral_rules`
