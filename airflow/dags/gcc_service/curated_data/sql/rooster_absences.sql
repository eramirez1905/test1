-- rooster_absences
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.rooster_absences`
CLUSTER BY rdbms_id, absence_id, date_of_absence AS
SELECT *
FROM `{{ params.project_id }}.dl_gcc_service.gcc_rooster_absences`
