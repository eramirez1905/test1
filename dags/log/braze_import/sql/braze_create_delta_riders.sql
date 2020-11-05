SELECT *
FROM {{ params.staging_dataset }}.{{ params.staging_table }}_{{ ts_nodash }}
EXCEPT DISTINCT
SELECT *
FROM `{{ params.braze_dataset }}.{{ params.last_pushed_riders }}`
