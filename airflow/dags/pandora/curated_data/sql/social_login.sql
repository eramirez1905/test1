-- social_login
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.social_login`
CLUSTER BY rdbms_id, social_login_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_social_login`
