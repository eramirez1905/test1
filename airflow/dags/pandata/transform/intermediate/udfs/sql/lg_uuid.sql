CREATE OR REPLACE FUNCTION `{project_id}`.pandata_intermediate.LG_UUID(id INT64, country_code STRING)
OPTIONS (
  description="Concatenates an id with country_code to create a unique id across sharded databases"
) AS (
  id || '_' || country_code
)
