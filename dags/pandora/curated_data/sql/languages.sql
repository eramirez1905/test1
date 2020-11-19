-- languages
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.languages`
CLUSTER BY rdbms_id, language_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_languages`
