-- order_toppings
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.order_toppings`
CLUSTER BY rdbms_id, ordertopping_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_order_toppings`
