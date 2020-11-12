CREATE OR REPLACE VIEW `{{ params.project_id }}.{{ params.curated_data_filtered }}.{{ params.table.name }}` AS
{%- for column_name in params.table.columns %}
{%- if loop.first %}
SELECT {{ params.table.name }}.{{ column_name }}
{%- else %}
  , {{ params.table.name }}.{{ column_name }}
{%- endif %}
{%- endfor %}
FROM `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.table.name }}` AS {{ params.table.name }}
{%- if params.table.filter_by_entity or params.table.filter_by_country %}
INNER JOIN `{{ params.project_id }}.{{ params.acl_dataset }}.access_control` acl
{%- if params.table.filter_by_entity and params.table.filter_by_country %}
  ON {{ params.table.name }}.country_code IN UNNEST(acl.countries) AND {{ params.table.name }}.{{ params.table.entity_id_column }} IN UNNEST(acl.entities)
{%- elif params.table.filter_by_country %}
  ON {{ params.table.name }}.country_code IN UNNEST(acl.countries)
{%- elif params.table.filter_by_entity %}
  ON {{ params.table.name }}.{{ params.table.entity_id_column }} IN UNNEST(acl.entities)
{%- endif %}
{%- endif %}
{%- if params.table.require_partition_filter %}
 WHERE {{ params.table.time_partitioning.field }} > '0001-01-01'
{%- endif %}
