CREATE OR REPLACE FUNCTION `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(country_code STRING, city_id INT64)
OPTIONS (
  description="Replace city id of 1 with 0 in 't2' so that the city in 't2 is considered part of 'th'"
) AS (
  IF(country_code = 't2' AND city_id = 1, 0, city_id)
)
