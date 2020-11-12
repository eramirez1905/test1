SELECT
  payments_referral_rules.uuid,
  payments_referral_rules.id,
  countries.rdbms_id,
  payments_referral_rules.country_code,
  payments_referral_rules.threshold,
  payments_referral_rules.status,
  payments_referral_rules.applies_to_type,
  payments_referral_rules.payment_unit_type,
  payments_referral_rules.payment_unit_amount,
  payments_referral_rules.duration_iso8601,
  payments_referral_rules.created_by_email,
  payments_referral_rules.is_active,
  payments_referral_rules.has_signing_bonus,
  payments_referral_rules.is_status_approved,
  payments_referral_rules.is_status_rejected,
  payments_referral_rules.is_status_pending,
  payments_referral_rules.signing_bonus_local,
  payments_referral_rules.start_at_utc,
  payments_referral_rules.end_at_utc,
  payments_referral_rules.created_at_utc,
  payments_referral_rules.updated_at_utc,
  payments_referral_rules.created_date_utc,
FROM `{project_id}.pandata_intermediate.lg_payments_referral_rules` AS payments_referral_rules
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON payments_referral_rules.country_code = countries.lg_country_code
