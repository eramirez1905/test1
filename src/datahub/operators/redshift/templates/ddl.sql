CREATE TABLE IF NOT EXISTS {{ ddl_operator.schema }}.{{ ddl_operator.table_name }}
(
    {%- for column in ddl_operator.columns %}
    {% if not loop.first %}, {% endif %}{{ column.name }} {{ column.data_type }}
    {%- if column.column_attributes %} {{ column.column_attributes }}{% endif %}
    {%- if column.column_constraints %} {{ column.column_constraints }}{% endif %}
    {%- endfor %}
)
BACKUP {{ ddl_operator.backup }}
{% if ddl_operator.get("diststyle") -%}
DISTSTYLE {{ ddl_operator.diststyle }}
{%- endif -%}
{%- if ddl_operator.get("distkey") %}
DISTKEY ({{ ddl_operator.distkey }})
{%- endif -%}
{%- if ddl_operator.get("sortkey") %}
{% if ddl_operator.sortkey.get("style") %}{{ ddl_operator.sortkey.style }} {% endif %}SORTKEY ({{ ddl_operator.sortkey.columns }})
{%- endif %}
;
{%- if ddl_operator.get("entity_to_privilege_grant") %}
{%- for entity, privilege in ddl_operator.entity_to_privilege_grant.items() %}
GRANT {{ privilege }} ON {{ ddl_operator.schema }}.{{ ddl_operator.table_name }} TO {{ entity }};
{%- endfor %}
{%- endif -%}
