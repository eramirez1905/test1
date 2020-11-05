SELECT
  payments_deposit_rules.uuid,
  payments_deposit_rules.id,
  countries.rdbms_id,
  payments_deposit_rules.country_code,
  payments_deposit_rules.region,
  payments_deposit_rules.name,
  payments_deposit_rules.description,
  payments_deposit_rules.cycle_number,
  payments_deposit_rules.gross_earning_percentage,
  payments_deposit_rules.status,
  payments_deposit_rules.amount_local,
  payments_deposit_rules.created_by_email,
  payments_deposit_rules.approved_by_email,
  payments_deposit_rules.is_active,
  payments_deposit_rules.created_at_utc,
  payments_deposit_rules.updated_at_utc,
  payments_deposit_rules.approved_at_utc,
  payments_deposit_rules.created_date_utc,
  payments_deposit_rules.applies_to,
FROM `{project_id}.pandata_intermediate.lg_payments_deposit_rules` AS payments_deposit_rules
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON payments_deposit_rules.country_code = countries.lg_country_code
