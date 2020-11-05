-- vendors
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.vendors`
CLUSTER BY rdbms_id, vendor_id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_vendors`
