# Curated Data

At root level, `curated_data` contains the configurations for the DAG that will create curated layer (cl) operators.

## Vertical (Level 1)

This entry is the name of the vertical that contains several operators in the DAG. As examples, we have `rps` and `logistics`.

## Vertical configuration (Level 2)

The configuration of the vertical. The required values are:

| FIELD | TYPE | DESCRIPTION |
| :--- | :--- | :--- |
| ui_color | `STRING` | A vertical's operators will have the same color, defined by this value. This field overrides the default color. |
| [reports](#Reports) | `STRING` | List of reports. Same configuration of [tables](#Tables). `filter_by_entity`, `columns`, `create_shared_view`, `is_stream` are not relevant and set to default values. |
| [tables](#Tables) | `ARRAY<STRING>` | The configurations for the operators of the vertical. See below for more information. |

## Tables

This level contains the configuration for the operators. The possible fields are shown in the table below:

| FIELD | TYPE | REQUIRED | DESCRIPTION |
| :--- | :--- | :--- | :--- |
| [name](#name) | `STRING` | MANDATORY | A unique name for the operator. |
| filter_by_entity | `BOOLEAN` | OPTIONAL | Only applies if `create_shared_view` is `true`. See `Shared Tables` for more information. Default value is `false`. |
| entity_id_column | `STRING` | OPTIONAL | Only applies if `create_shared_view` is `true` and `filter_by_entity` is `true`. See `Shared Tables` for more information. Default value is `entity.id`. |
| filter_by_country | `BOOLEAN` | OPTIONAL | Only applies if `create_shared_view` is `true`. See `Shared Tables` for more information. Default value is `true`. |
| columns | `ARRAY<STRING>` | OPTIONAL | Only applies if `create_shared_view` is `true`. It specifies which columns of the table will be shared with other entities. If create_shared_view is `false`, then the value should be an empty array: `[]`, Default value is `[]`. |
| dependencies | `ARRAY<STRING>` | OPTIONAL | Specifies on which tables it depends (it's upstream in Airflow). In other words, which tables have to be executed before these tables are created/updated. Default value is `[]` |
| [create_shared_view](#Create_Shared_View) | `BOOLEAN` | OPTIONAL | If the table is shared to other entities. Default value is `false`. |
| [is_stream](#Is_Stream) | `BOOLEAN` | OPTIONAL | See `Stream Tables` for more information. Default value is `false` |
| ui_color | `STRING` | OPTIONAL | It overrides the color of the operator with the one set. Default value is the color of the type of the operator. |
| [type](#type) | `STRING` | MANDATORY | See `Type` for more information. |
| time_partitioning | `STRING` | OPTIONAL | The column of the table to partition by, which should be a date. Default value is `null`. |
| cluster_fields | `ARRAY<STRING>` | OPTIONAL | The columns on which the table will be clustered by. The maximum can be four values (Big Query limit). Read more at [BigQueryLimitations](https://cloud.google.com/bigquery/docs/creating-clustered-tables#limitations). Default value is `null`. | 
| execution_timeout_minutes | `INTEGER` | OPTIONAL | Defines the execution timeout of the Airflow task. Default value is `20`. | 
| pool_name | `STRING` | OPTIONAL | Defines the pool_name of the task. | 
| bigquery_conn_id | `INTEGER` | OPTIONAL | Defines the `connection_id` used by the task. | 
| [sanity_checks](#sanity-checks) | `ARRAY<RECORD>` | OPTIONAL | List of sanity checks to run after the execution of the query defined in `name`. | 
| [policy_tags](#policy-tags) | `ARRAY<RECORD>` | OPTIONAL | Policy tags | 
| sql_template | `STRING` | OPTIONAL | Option to use specific sql template instead of one named `{name}.sql`. |
| template_params | `RECORD` | OPTIONAL | Supplies additional parameters for sql template, available under `params` key. |

### Name

For rps operators, always add the prefix `rps_`

### Type

The type of the operator. Possible values are `data` (default), `reports`, `sanity_check`. Read below for further information.

### Sanity Checks
 
It Performs checks against BigQuery.
It creates a `BigQueryCheckOperator`, it expects a sql query that will return a single row. Each value on that
first row is evaluated using python `bool` casting. If any of the values return `false` the check is failed and errors out.
    
Click on [BigQueryCheckOperator](https://incubator-airflow.readthedocs.io/en/airflow-1075/integration.html#bigquerycheckoperator) for more information.

| FIELD | TYPE | REQUIRED | DESCRIPTION |
| :--- | :--- | :--- | :--- |
| name | `STRING` | MANDATORY | The name of the sanity check. It matches the name of the file (without the extension `.sql`) in `curated_data/sql/sanity_checks` folder |
| is_blocking | `STRING` | OPTIONAL | Define if the sanity check should mark the downstream tasks failed. |


### Policy tags

| FIELD | TYPE | REQUIRED | DESCRIPTION |
| :--- | :--- | :--- | :--- |
| column | `STRING` | MANDATORY | The name of column to apply the tag. |
| tag | `STRING` | MANDATORY | The attribute `display_name` defined in the list of policy tags. |

#### Data (default)

This type creates a CuratedDataBigQueryOperator (see `CuratedDataBigQueryOperator.py` for more information).
This operator will create a table in Big Query. For this to happen, a SQL file with the exact name to the operator must exist in `curated_data/sql`, unless `sql_template` option is used.
In this SQL file, the name of the created table must also match the operator name.  

#### Reports

This type is specific to reports. It's the same of `Data` type except that the SQL file must be in `curated_data/sql/reports`.

### Create_Shared_View

If the table will be shared with other entities.

- Set the option `create_shared_view` to `true` and define `columns`.

- Filter by entity. If this field is set to `true`, then an entity is only able to query its own data. (also dependent on Filter by Country), name of this column is set in `entity_id_column` option.
E.G In saudi arabia we have two entities, so if this field is set to `true`, then Talabat would only be able to query its own data. 

- Filter by Country. If this field is set to `true`, then an entity is only able to query its own data in a country. (also dependent on Filter by entity)
E.G For Talabat we have many entities (TB_AE, TB_BH, and TB_JO and so on), so if this field is set to `true`, then Talabat local team would only be able to query data for the countries they are allowed to.
### Is_Stream

In case the source tables are streams (append-only data):

- Set the option `is_stream` to `true`, define `time_partitioning` and `cluster_fields`.

    Example:
    ```yaml
      - name: rps_connectivity
        is_stream: true
        time_partitioning: created_date
        cluster_fields: [entity_id, platform_restaurant_id]
    ```

- The query that generates the CL tables must not contain the statement `CREATE TABLE`
- Filter the source data using `next_ds` macro, wrap the where condition in a `if`:

    Example
    ```jinja2
    {%- if not params.backfill %}
        WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
    {%- endif %}
    ```
In case we add new columns to the destination table, it's required a backfill of the table.
To do so there is a specific DAG `curated-data-backfill`, trigger a manual run to backfill the table.


# Creating new tables

The following steps are a guide to creating new tables:

- Create a new operator entry in the `config.yaml`. In the `curated_data` section, in the corresponding vertical, under tables add an entry.

    Example for rps: 
    ```yaml
      - name: rps_new_table
        filter_by_entity: false
        dependencies:
          - rps_order_status
        dependencies_dl:
          - raw_table_on_which_rps_new_table_depends_on
    ```

- Create the SQL with name `rps_new_table.sql` in `curated_data/sql` and add the proper content.

- The array of `dependencies_dl` lists all of the `dl` tables on which the query of `rps_new_table.sql` depends on.

- Update the documentation of the vertical. E.G. In the folder `docs/rps/curated_data` create a new entry `rps_new_table.md`.

- Update the summary of the vertical. E.G In the file `docs/rps/SUMMARY.md`, add a reference to `rps_new_table.md`.

- Update the change log of the vertical. E.G. `docs/rps/CHANGELOG.md`, add an entry `- XXXX-XX-XX Add rps_new_table table.`
