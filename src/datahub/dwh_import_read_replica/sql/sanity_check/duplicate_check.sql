WITH {{ params.table_name }} AS (
  SELECT {{ params.pk_columns | join (', ') }}
    , count(*) AS count
  FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
  {%- if params.require_partition_filter %}
  WHERE {{ params.time_partitioning }} > '0001-01-01'
  {%- endif %}
  GROUP BY {{ params.pk_columns | join (', ') }}
)
SELECT COUNTIF(count != 1) = 0 AS duplicates_count
FROM {{ params.table_name }}
