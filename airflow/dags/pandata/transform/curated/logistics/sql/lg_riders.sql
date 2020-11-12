SELECT
  riders.id,
  riders.uuid,
  countries.rdbms_id,
  riders.reporting_to_lg_rider_id,
  riders.reporting_to_lg_rider_uuid,
  riders.country_code,
  riders.name,
  riders.email,
  riders.phone_number,
  riders.batch_number,
  riders.birth_date,
  riders.created_at_utc,
  riders.updated_at_utc,
  riders.contracts,
  riders.batches,
  riders.custom_fields,
  riders.absences_history,
  riders.vehicles,
  riders.starting_points,
  riders.feedbacks,
FROM `{project_id}.pandata_intermediate.lg_riders` AS riders
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON riders.country_code = countries.lg_country_code
