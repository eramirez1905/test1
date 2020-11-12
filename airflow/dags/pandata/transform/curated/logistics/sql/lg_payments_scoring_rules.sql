SELECT
  payments_scoring_rules.uuid,
  payments_scoring_rules.id,
  countries.rdbms_id,
  payments_scoring_rules.lg_city_uuid,
  payments_scoring_rules.lg_city_id,
  payments_scoring_rules.country_code,
  payments_scoring_rules.created_by_email,
  payments_scoring_rules.status,
  payments_scoring_rules.type,
  payments_scoring_rules.name,
  payments_scoring_rules.payment_unit,
  payments_scoring_rules.payment_unit_type,
  payments_scoring_rules.is_active,
  payments_scoring_rules.start_at_utc,
  payments_scoring_rules.end_at_utc,
  payments_scoring_rules.created_at_utc,
  payments_scoring_rules.updated_at_utc,
  payments_scoring_rules.applies_to,
  payments_scoring_rules.cost_factors,
FROM `{project_id}.pandata_intermediate.lg_payments_scoring_rules` AS payments_scoring_rules
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON payments_scoring_rules.country_code = countries.lg_country_code
