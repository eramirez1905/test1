CREATE OR REPLACE VIEW `{{ params.project_id }}.cl_pandata.{{ params.table_name }}`
AS
SELECT *
FROM `{{ params.project_id }}.pandata_curated.{{ params.table_name }}`
