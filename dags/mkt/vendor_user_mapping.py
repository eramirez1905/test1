"""#### Schedule the Marketing Tech vendor-user mapping project"""

from datetime import timedelta
from pathlib import Path

import mkt_import
import vendor_user_mapping
import pendulum
from airflow import DAG, configuration
from airflow.operators.postgres_operator import PostgresOperator

from configuration import config
from datahub.common import alerts
from datahub.curated_data.metadata_execution_table import MetadataExecutionTable
from datahub.operators.bigquery.bigquery_update_metadata_execution_operator import (
    BigQueryUpdateMetadataExecutionOperator,
)
from datahub.operators.redshift.redshift_truncate_operator import (
    RedshiftTruncateOperator,
)
from datahub.operators.redshift.redshift_create_table_operator import (
    RedshiftCreateTableOperator,
)


VENDOR_USER_MAPPING_CONFIG = config.get("vendor_user_mapping")

# Redshift
REDSHIFT_CONNECTION = VENDOR_USER_MAPPING_CONFIG.get("airflow_redshift_connection")
VENDOR_USER_MAPPING_SCHEMA = VENDOR_USER_MAPPING_CONFIG.get(
    "vendor_user_mapping_schema"
)

dags_folder = configuration.get("core", "dags_folder")
environment = configuration.get("datahub", "environment")
is_prod = True if environment == "production" else False


def create_mkt_vendor_user_mapping_dag_for_entity(dag_id, brand_config, brand_code):
    customers_orders_coordinates_table_name = (
        f"{brand_code}_customers_orders_coordinates"
    )
    restaurants_polygons_table_name = f"{brand_code}_restaurants_polygons"
    polygon_point_map_table_name = f"{brand_code}_polygon_point_map"
    vendor_user_geo_mapping_table_name = VENDOR_USER_MAPPING_CONFIG[
        "vendor_user_mapping_output_table"
    ].format(brand_code=brand_code)

    dag = DAG(
        dag_id=dag_id,
        description="Compute vendor user geo mapping",
        start_date=pendulum.parse(brand_config.get("dag_start_date")),
        schedule_interval=brand_config.get("schedule_interval"),
        catchup=False,
        user_defined_macros={
            **brand_config,
            "brand_code": brand_code,
            "vendor_user_mapping_schema": VENDOR_USER_MAPPING_SCHEMA,
            "environment": environment,
            "customers_orders_coordinates_table_name": customers_orders_coordinates_table_name,
            "restaurants_polygons_table_name": restaurants_polygons_table_name,
            "polygon_point_map_table_name": polygon_point_map_table_name,
            "vendor_user_geo_mapping_table_name": vendor_user_geo_mapping_table_name,
        },
        tags=[
            mkt_import.DEFAULT_ARGS["owner"],
            "vendor-user-mapping",
            brand_config.get("brand_common_name"),
        ],
        template_searchpath=vendor_user_mapping.__path__,
        default_args={
            **mkt_import.DEFAULT_ARGS,
            **{"on_failure_callback": alerts.setup_callback(), "retries": 0},
        },
    )

    with dag:
        dag.doc_md = __doc__

        # DDL tasks
        ddl_customers_orders_coordinates = RedshiftCreateTableOperator(
            task_id="ddl_customers_orders_coordinates",
            schema=VENDOR_USER_MAPPING_SCHEMA,
            table_name=customers_orders_coordinates_table_name,
            columns=[
                ("source_id", "SMALLINT"),
                ("customer_id", "VARCHAR(40)"),
                ("last_order_date", "TIMESTAMP"),
                ("coordinate", "VARCHAR(45)"),
                ("cluster_id", "SMALLINT"),
                ("valid_at", "TIMESTAMP", "DEFAULT GETDATE()"),
                ("dwh_row_hash", "VARCHAR(40)"),
            ],
            backup=is_prod,
            diststyle="EVEN",
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )
        ddl_restaurants_polygons = RedshiftCreateTableOperator(
            task_id="ddl_restaurants_polygons",
            schema=VENDOR_USER_MAPPING_SCHEMA,
            table_name=restaurants_polygons_table_name,
            columns=[
                ("source_id", "SMALLINT"),
                ("restaurant_id", "VARCHAR(36)"),
                ("cluster_id", "SMALLINT"),
                ("polygon", "VARCHAR(34180)"),
                ("valid_at", "TIMESTAMP", "DEFAULT GETDATE()"),
            ],
            backup=is_prod,
            diststyle="EVEN",
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )
        ddl_polygon_point_map = RedshiftCreateTableOperator(
            task_id="ddl_polygon_point_map",
            schema=VENDOR_USER_MAPPING_SCHEMA,
            table_name=polygon_point_map_table_name,
            columns=[
                ("source_id", "SMALLINT", "ENCODE AZ64"),
                ("cluster_id", "SMALLINT", "ENCODE AZ64"),
                ("polygon", "VARCHAR(32)", "ENCODE ZSTD"),
                ("coordinate", "VARCHAR(32)", "ENCODE ZSTD"),
            ],
            backup=is_prod,
            diststyle="ALL",
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )
        ddl_vendor_user_map = RedshiftCreateTableOperator(
            task_id="ddl_vendor_user_map",
            schema=VENDOR_USER_MAPPING_SCHEMA,
            table_name=vendor_user_geo_mapping_table_name,
            columns=[
                ("source_id", "SMALLINT", "ENCODE AZ64"),
                ("restaurant_id", "VARCHAR(36)", "ENCODE ZSTD"),
                ("customer_id", "VARCHAR(40)", "ENCODE ZSTD"),
            ],
            backup=is_prod,
            diststyle="KEY",
            distkey="customer_id",
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        # Truncate tasks
        truncate_customers_orders_coordinates = RedshiftTruncateOperator(
            task_id="truncate_customers_orders_coordinates",
            schema=VENDOR_USER_MAPPING_SCHEMA,
            table=customers_orders_coordinates_table_name,
            redshift_conn_id=REDSHIFT_CONNECTION,
        )
        truncate_restaurants_polygons = RedshiftTruncateOperator(
            task_id="truncate_restaurants_polygons",
            schema=VENDOR_USER_MAPPING_SCHEMA,
            table=restaurants_polygons_table_name,
            redshift_conn_id=REDSHIFT_CONNECTION,
        )
        truncate_polygon_point_map = RedshiftTruncateOperator(
            task_id="truncate_polygon_point_map",
            schema=VENDOR_USER_MAPPING_SCHEMA,
            table=polygon_point_map_table_name,
            redshift_conn_id=REDSHIFT_CONNECTION,
        )
        truncate_vendor_user_map = RedshiftTruncateOperator(
            task_id="truncate_vendor_user_map",
            schema=VENDOR_USER_MAPPING_SCHEMA,
            table=vendor_user_geo_mapping_table_name,
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        # Computation tasks
        compute_customers_orders_coordinates = PostgresOperator(
            task_id="compute_customers_orders_coordinates",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("customers_orders_coordinates.sql")),
        )
        compute_restaurants_polygons = PostgresOperator(
            task_id="compute_restaurants_polygons",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("restaurants_polygons.sql")),
        )
        compute_polygon_point_map = PostgresOperator(
            task_id="compute_polygon_point_map",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("polygon_point_map.sql")),
        )
        compute_vendor_user_map = PostgresOperator(
            task_id="compute_vendor_user_map",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("vendor_user_geo_mapping.sql")),
        )

        # Metadata Operator used to comunicate between DAGs that are expecting this data.
        update_metadata_execution_vendor_user_map = (
            BigQueryUpdateMetadataExecutionOperator(
                task_id=f"metadata-{vendor_user_geo_mapping_table_name}",
                bigquery_conn_id="bigquery_default",
                project_id=config.get("bigquery").get("project_id"),
                dataset_id=f"redshift_{VENDOR_USER_MAPPING_SCHEMA}",
                table_name=vendor_user_geo_mapping_table_name,
                metadata_execution_table=MetadataExecutionTable(),
                is_shared=False,
                execution_timeout=timedelta(minutes=15),
            )
        )

        (
            ddl_customers_orders_coordinates
            >> truncate_customers_orders_coordinates
            >> compute_customers_orders_coordinates
        )
        (
            ddl_restaurants_polygons
            >> truncate_restaurants_polygons
            >> compute_restaurants_polygons
        )
        (
            ddl_polygon_point_map
            >> truncate_polygon_point_map
            >> compute_polygon_point_map
        )
        (ddl_vendor_user_map >> truncate_vendor_user_map >> compute_vendor_user_map)

        compute_customers_orders_coordinates >> compute_polygon_point_map
        compute_restaurants_polygons >> compute_polygon_point_map
        compute_polygon_point_map >> compute_vendor_user_map
        compute_vendor_user_map >> update_metadata_execution_vendor_user_map

    return dag


for brand_code, brand_config in VENDOR_USER_MAPPING_CONFIG.get("brands").items():
    brand_common_name = brand_config.get("brand_common_name")
    version = brand_config.get("version")

    dag_id = f"mkt-vendor-user-mapping-{brand_common_name}-v{version}"

    globals()[dag_id] = create_mkt_vendor_user_mapping_dag_for_entity(
        dag_id=dag_id, brand_config=brand_config, brand_code=brand_code
    )
