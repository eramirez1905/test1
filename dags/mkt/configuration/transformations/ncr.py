from collections import OrderedDict

from airflow import AirflowException


IMPORT_DATABASE_NAME = "dwh_ncr"
IMPORT_TABLE_PREFIX = "braze_campaigns_"


def transform_ncr_config_to_dwh_merge_layer_databases(
    source: str, config: OrderedDict
) -> OrderedDict:
    """
    Transform the NCR configuration into a format to be consumed by Datahub import framework.

    Create one `DatabaseSettings` for all tables. `TableSettings` per brand.

    Intended to be used in a
    ```python
    common_config.add_transformation(
        ConfigTransformation(
            'ncr',
            'dwh_merge_layer_databases',
            transform_ncr_config_to_dwh_merge_layer_databases
        )
    )
    ```
    configuration call.
    """
    ncr_config = config[source]
    result = OrderedDict()

    database_redshift_def = OrderedDict(
        {
            "app_version:": "1",
            "project_name": "ncr",
            "is_regional": False,
            "source": "Redshift",
            "source_format": "parquet",
            "start_date": "2020-09-15",
            "tables": [],
        }
    )

    try:
        for brand_code, brand_config in ncr_config["ncr_brands"].items():
            table_ncr_brand_def = OrderedDict(
                {
                    "name": IMPORT_TABLE_PREFIX + brand_code,
                    "pk_columns": ["braze_external_id", "midas_booking_id"],
                    "filter_import_by_date": False,
                    "keep_last_import_only": True,
                    "keep_last_import_only_partition_by_pk": True,
                    "sanity_checks": {"created_date_null": False},
                    "raw_layer": {"created_at_column": None, "updated_at_column": None},
                    "redshift": {
                        "redshift_conn_id": ncr_config["airflow_redshift_connection"],
                        "source_format": "parquet",
                        "schema": ncr_config["ncr_schema"],
                        "table_name": "{{ brand_code }}_braze_campaigns",
                        "columns": ["*"],
                        "unload_options": [],
                        "execution_timeout": 20,
                    },
                    "s3": {
                        "aws_conn": ncr_config["ncr_s3_conn_id"],
                        "bucket": ncr_config["ncr_s3_bucket"],
                        "prefix": "ncr_braze_campaigns/{{ brand_code }}",
                    },
                }
            )

            database_redshift_def["tables"].append(table_ncr_brand_def)

    except KeyError as e:
        raise AirflowException(
            f"Key config {e} missing globally or for brand: '{brand_code}'"
        )

    result[IMPORT_DATABASE_NAME] = database_redshift_def

    return result
