-- playvox_calibrations
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.playvox_calibrations`
CLUSTER BY calibration_id AS
SELECT *
FROM `{{ params.project_id }}.dl_gcc_service.gcc_playvox_calibrations`
