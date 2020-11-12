-- first_order_uuid
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.first_order_uuid`
CLUSTER BY rdbms_id, new_customer_uuid AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_first_order_uuid`
