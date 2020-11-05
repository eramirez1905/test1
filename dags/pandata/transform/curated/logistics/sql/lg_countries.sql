SELECT
  pd_countries.rdbms_id,
  lg_countries.region,
  lg_countries.region_short_name,
  lg_countries.code,
  lg_countries.iso,
  lg_countries.name,
  lg_countries.currency_code,
  lg_countries.platforms,
  lg_countries.cities,
FROM `{project_id}.pandata_intermediate.lg_countries` AS lg_countries
INNER JOIN `{project_id}.pandata_intermediate.pd_countries` AS pd_countries
        ON lg_countries.iso = pd_countries.iso
