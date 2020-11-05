CREATE OR REPLACE FUNCTION `{project_id}`.pandata_intermediate.ENTITY_UUID(id INT64, global_entity_id STRING)
OPTIONS (
  description="Concatenates an id with global_entity_id to create a unique id across sharded databases"
) AS (
  id || '_' || global_entity_id
)
