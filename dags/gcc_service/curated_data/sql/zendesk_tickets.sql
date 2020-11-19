-- zendesk_tickets
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.zendesk_tickets`
CLUSTER BY entity_id, ticket_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_zendesk_tickets`
