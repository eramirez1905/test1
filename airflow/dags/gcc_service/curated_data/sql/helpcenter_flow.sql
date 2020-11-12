-- helpcenter_flow
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.helpcenter_flow`
CLUSTER BY flow_session_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_helpcenter_flow`
