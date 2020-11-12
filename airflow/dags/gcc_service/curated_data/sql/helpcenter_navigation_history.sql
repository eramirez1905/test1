-- helpcenter_navigation_history
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.helpcenter_navigation_history`
CLUSTER BY event_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_helpcenter_navigation_history`
