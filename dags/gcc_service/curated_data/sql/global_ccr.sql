-- global_ccr
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.global_ccr`
CLUSTER BY global_ccr_code AS
SELECT *
FROM `{{ params.project_id }}.dl_gcc_service.gcc_global_ccr`
