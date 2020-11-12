-- rooster_shifts
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.rooster_shifts`
CLUSTER BY rdbms_id, shift_id AS
SELECT *
FROM `{{ params.project_id }}.dl_gcc_service.gcc_rooster_shifts`
