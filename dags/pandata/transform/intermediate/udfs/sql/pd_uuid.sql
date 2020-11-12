CREATE OR REPLACE FUNCTION `{project_id}`.pandata_intermediate.PD_UUID(id INT64, rdbms_id INT64)
OPTIONS (
  description="Concatenates an id with rdbms_id to create a unique id across sharded databases"
) AS (
  id || '_' || rdbms_id
)
