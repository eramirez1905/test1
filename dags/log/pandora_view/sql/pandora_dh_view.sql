CREATE OR REPLACE VIEW `{{ params.project_id }}.dh.{{ params.table_name }}` AS
SELECT *
FROM `{{ params.project_id }}.rl.{{ params.table_name }}`
