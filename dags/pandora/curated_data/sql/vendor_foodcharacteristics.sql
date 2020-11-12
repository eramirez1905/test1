-- vendor_foodcharacteristics
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.vendor_foodcharacteristics`
CLUSTER BY rdbms_id, vendor_id, foodcaracteristics_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_vendor_foodcharacteristics`
