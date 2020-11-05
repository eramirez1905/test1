-- order_payments
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.order_payments`
CLUSTER BY rdbms_id, order_payment_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_order_payments`
