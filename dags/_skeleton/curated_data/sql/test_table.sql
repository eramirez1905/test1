-- This query is just an example
CREATE OR REPLACE TABLE `{{ params.project_id }}`.cl_BUSINESS_UNIT.test_table
PARTITION BY created_date AS
CLUSTER BY rdbms_id, shift_id, employee_id
SELECT *
FROM `{{ params.project_id }}`.dl_BUSINESS_UNIT.app_name_test_table
