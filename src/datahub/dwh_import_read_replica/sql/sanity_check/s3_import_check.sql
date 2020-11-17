-- Sanity check to check if the data is imported in the last 24 hours from s3
SELECT TIMESTAMP_DIFF('{{ next_ds }}', MAX(_ingested_at), HOUR) <= 24 as s3_file_imported_in_last_24_hours
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
