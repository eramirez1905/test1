-- rooster_forecasted_incidents
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.rooster_forecasted_incidents`
CLUSTER BY entity_id, created_datetime AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_rooster_forecasted_incidents`
