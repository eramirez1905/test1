-- salesforce_work_items
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.salesforce_work_items`
CLUSTER BY work_item_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_salesforce_work_items`
