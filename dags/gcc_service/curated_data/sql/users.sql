-- users
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.users`
CLUSTER BY email AS
SELECT *
FROM `{{ params.project_id }}.dl_gcc_service.gcc_users`
