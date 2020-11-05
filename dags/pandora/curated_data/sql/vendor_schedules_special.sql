-- vendor_schedules_special
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_pandora.vendor_schedules_special`
CLUSTER BY rdbms_id, id AS
SELECT global_entity_id AS entity_id
  , * EXCEPT(global_entity_id)
FROM `{{ params.project_id }}.dl_pandora.il_vendor_schedules_special`
