CREATE OR REPLACE FUNCTION `{project_id}`.pandata_intermediate.EXCLUDE_T2(country_code STRING)
OPTIONS (
  description="Exclude 't2' and replace with 'th' so each country only has one country code and Thailand wouldn't have two."
) AS (
  IF(country_code = "t2", "th", country_code)
)
