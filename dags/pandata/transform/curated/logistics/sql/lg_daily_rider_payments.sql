SELECT
  rider_payments.uuid,
  countries.rdbms_id,
  rider_payments.lg_rider_uuid,
  rider_payments.lg_rider_id,

  rider_payments.rider_name,
  rider_payments.country_code,
  rider_payments.timezone,
  rider_payments.exchange_rate,
  rider_payments.total_payment_eur,
  rider_payments.total_payment_local,
  rider_payments.created_date_utc,
  rider_payments.details,
FROM `{project_id}.pandata_intermediate.lg_rider_payments` AS rider_payments
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON rider_payments.country_code = countries.lg_country_code
