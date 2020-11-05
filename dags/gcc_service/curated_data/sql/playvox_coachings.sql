-- playvox_coachings
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.playvox_coachings`
CLUSTER BY coaching_id AS
SELECT *
FROM `{{ params.project_id }}.dl_gcc_service.gcc_playvox_coachings`
