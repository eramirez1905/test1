SELECT
  payments_basic_rules.uuid,
  payments_basic_rules.id,
  countries.rdbms_id,
  payments_basic_rules.country_code,
  payments_basic_rules.type,
  payments_basic_rules.sub_type,
  payments_basic_rules.name,
  payments_basic_rules.amount_local,
  payments_basic_rules.min_threshold_in_km,
  payments_basic_rules.max_threshold_in_km,
  payments_basic_rules.created_by_email,
  payments_basic_rules.is_approved,
  payments_basic_rules.status,
  payments_basic_rules.is_active,
  payments_basic_rules.is_pay_below_threshold,
  payments_basic_rules.acceptance_rate,
  payments_basic_rules.start_time,
  payments_basic_rules.end_time,
  payments_basic_rules.start_date_utc,
  payments_basic_rules.end_date_utc,
  payments_basic_rules.created_at_utc,
  payments_basic_rules.updated_at_utc,
  payments_basic_rules.applies_to,
FROM `{project_id}.pandata_intermediate.lg_payments_basic_rules` AS payments_basic_rules
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON payments_basic_rules.country_code = countries.lg_country_code