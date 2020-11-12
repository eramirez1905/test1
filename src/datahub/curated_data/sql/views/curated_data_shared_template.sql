CREATE OR REPLACE VIEW `{{ params.project_id }}.{{ params.curated_data_shared }}.{{ params.table.name }}` AS
SELECT *
FROM `{{ params.project_id }}.{{ params.curated_data_filtered }}.{{ params.table.name }}`
