-- users
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.users`
CLUSTER BY rdbms_id, user_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_users`
