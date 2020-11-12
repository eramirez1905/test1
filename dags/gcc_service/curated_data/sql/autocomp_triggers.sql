-- autocomp_triggers
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.autocomp_triggers`
CLUSTER BY event_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_autocomp_triggers`
