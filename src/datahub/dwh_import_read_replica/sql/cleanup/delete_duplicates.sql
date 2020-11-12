DELETE
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
WHERE STRUCT(_ingested_at, {{ (params.pk_columns + params.extra_partition_columns) | join(', ') }}) IN (
    SELECT AS STRUCT * EXCEPT(_row_number) FROM (
      SELECT _ingested_at, {{ (params.pk_columns + params.extra_partition_columns) | join(', ') }}
        , ROW_NUMBER() OVER (PARTITION BY {{ (params.pk_columns + params.extra_partition_columns) | join(', ') }} ORDER BY _ingested_at DESC) AS _row_number
      FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
    )
    WHERE _row_number > 1
  )
