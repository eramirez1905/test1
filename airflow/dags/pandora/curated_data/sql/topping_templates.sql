-- topping_templates
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.topping_templates`
CLUSTER BY rdbms_id, product_variation_id, productvariationtoppingtemplate_id, toppingtemplate_id, topping_template_product_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_topping_templates`
