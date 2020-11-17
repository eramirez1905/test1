-- option_value
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.option_value`
CLUSTER BY rdbms_id, option_id, vat_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_option_value`
