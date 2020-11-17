-- playvox_evaluations_custom_questions
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.playvox_evaluations_custom_questions`
CLUSTER BY evaluation_id, customquestion_id AS
SELECT *
FROM `{{ params.project_id }}.dl_gcc_service.gcc_playvox_evaluations_custom_questions`
