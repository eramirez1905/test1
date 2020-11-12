-- gcc_self_reported_data
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.gcc_self_reported_data`
CLUSTER BY entity_id, department, date AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_gcc_self_reported_data`
