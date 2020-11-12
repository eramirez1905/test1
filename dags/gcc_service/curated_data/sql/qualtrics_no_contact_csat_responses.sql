-- qualtrics_no_contact_csat_responses
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.qualtrics_no_contact_csat_responses`
CLUSTER BY response_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_qualtrics_no_contact_csat_responses`
