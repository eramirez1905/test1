"""#### Schedule the Marketing Tech NCR project"""

from pathlib import Path, PosixPath
from datetime import timedelta

import mkt_import
import ncr
import pendulum
from airflow import DAG, configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from ncr.transform_crm_vendor_feed import transform_crm_vendor_feed
from operators.custom_http_sensor import CustomHttpSensor
from operators.s3_check_last_object_update_sensor import S3CheckLastObjectUpdateSensor
from utils.check_dwh_jenkins import check_dwh_jenkins_success
from utils.group_airflow_tasks import group_airflow_tasks
from utils.get_all_airflow_dependants import get_all_airflow_dependants
from utils.uploader.braze_push import push_jsonl_to_braze

from configuration import config
from datahub.common import alerts
from datahub.curated_data.metadata_execution_table import MetadataExecutionTable
from datahub.dwh_import_read_replica import DwhImportReadReplica
from datahub.operators.bigquery.bigquery_check_metadata_execution_operator import (
    BigQueryCheckMetadataExecutionOperator,
)
from datahub.operators.postgres_to_s3_operator import PostgresToS3Operator
from datahub.operators.redshift.redshift_truncate_operator import (
    RedshiftTruncateOperator,
)
from datahub.operators.redshift.redshift_to_s3_iam_operator import (
    RedshiftToS3IamOperator,
)
from datahub.operators.redshift.s3_to_redshift_iam_operator import (
    S3ToRedshiftIamOperator,
)
from datahub.operators.redshift.redshift_create_table_operator import (
    RedshiftCreateTableOperator,
)


NCR_CONFIG = config.get("ncr")

# Redshift
REDSHIFT_CONNECTION = NCR_CONFIG.get("airflow_redshift_connection")
NCR_SCHEMA = NCR_CONFIG.get("ncr_schema")

# S3
NCR_S3_BUCKET = NCR_CONFIG.get("ncr_s3_bucket")
NCR_S3_CONN_ID = NCR_CONFIG.get("ncr_s3_conn_id")

# CRM Vendor Feed
CRM_VENDOR_FEED_BUCKET = NCR_CONFIG.get("crm_vendor_feed_bucket")

# MIDAS NCR database
MIDAS_NCR_CONN_ID = NCR_CONFIG.get("ncr_midas_conn_id")

# Vendor User Mapping
VENDOR_USER_MAPPING_CONFIG = config.get("vendor_user_mapping")
VENDOR_USER_MAPPING_SCHEMA = VENDOR_USER_MAPPING_CONFIG["vendor_user_mapping_schema"]

USER_DEFINED_FILTERS = {
    "sql_list_filter": lambda input_list: "(" + str(list(input_list))[1:-1] + ")"
}

IMPORT_TABLE_PREFIX = "braze_campaigns_"

dags_folder = configuration.get("core", "dags_folder")
environment = configuration.get("datahub", "environment")

dwh_import_read_replicas = DwhImportReadReplica(config)


def create_mkt_ncr_dag_for_entity(dag_id, brand_config, brand_code):
    s3_brand_prefix = PosixPath("{task_id}", "{{{{ brand_code }}}}", "{{{{ ds }}}}")

    dag = DAG(
        dag_id=dag_id,
        description=f'Does all the NCR magic for {brand_config.get("brand_common_name").capitalize()}.',
        start_date=pendulum.parse(brand_config.get("dag_start_date")),
        schedule_interval=brand_config.get("schedule_interval"),
        catchup=False,
        user_defined_macros={
            **brand_config,
            "brand_code": brand_code,
            "ncr_schema": NCR_SCHEMA,
            "vendor_user_mapping_schema": VENDOR_USER_MAPPING_SCHEMA,
            "environment": environment,
        },
        user_defined_filters=USER_DEFINED_FILTERS,
        template_searchpath=ncr.__path__,
        orientation="TB",
        default_args={
            **mkt_import.DEFAULT_ARGS,
            **{"on_failure_callback": alerts.setup_callback(), "retries": 0},
        },
    )

    with dag:
        dag.doc_md = __doc__

        crm_vendor_feed_ddl = PostgresOperator(
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("ddl") / "crm_vendor_feed.sql"),
            task_id="ddl_crm_vendor_feed",
        )

        ddl_ncr_midas_campaign_bookings = PostgresOperator(
            task_id="ddl_ncr_midas_campaign_bookings",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("ddl") / Path("ncr_midas_campaign_bookings.sql")),
        )

        ddl_st_ncr_midas_campaign_bookings = PostgresOperator(
            task_id="ddl_st_ncr_midas_campaign_bookings",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("ddl") / Path("ncr_st_midas_campaign_bookings.sql")),
        )

        ddl_ncr_braze_customer = RedshiftCreateTableOperator(
            table_name="{{ brand_code }}_ncr_braze_customer",
            task_id="ddl_ncr_braze_customer",
            schema=NCR_SCHEMA,
            columns=[
                ("source_id", "SMALLINT", "NOT NULL"),
                ("customer_id", "VARCHAR(256)", "NOT NULL"),
                ("braze_external_id", "VARCHAR(256)", "NOT NULL"),
            ],
            diststyle="KEY",
            distkey="braze_external_id",
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        ddl_ncr_vendor_user_campaign_eligibility = PostgresOperator(
            task_id="ddl_ncr_vendor_user_campaign_eligibility",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("ddl") / Path("ncr_vendor_user_campaign_eligibility.sql")),
        )

        ddl_ncr_decision_engine_result = PostgresOperator(
            task_id="ddl_ncr_decision_engine_result",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("ddl") / Path("ncr_decision_engine_result.sql")),
        )

        ddl_ncr_decision_engine_result_history = PostgresOperator(
            task_id="ddl_ncr_decision_engine_result_history",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("ddl") / Path("ncr_decision_engine_result_history.sql")),
        )

        braze_table_columns = [
            ("braze_external_id", "VARCHAR(256)", "NOT NULL"),
            ("midas_booking_id", "INTEGER", "NOT NULL"),
            ("source_id", "SMALLINT", "NOT NULL"),
            ("source_code", "VARCHAR(25)", "NOT NULL"),
            ("customer_id", "VARCHAR(64)", "NOT NULL"),
            ("ncr_package_type", "VARCHAR(50)", "NOT NULL"),
            ("ncr_vendor_id", "VARCHAR(20)", "NOT NULL"),
            ("ncr_publish_date", "DATE", "NOT NULL"),
        ]

        ddl_braze_main = RedshiftCreateTableOperator(
            task_id="ddl_braze_main",
            table_name="{{ brand_code }}_braze_main",
            schema=NCR_SCHEMA,
            columns=braze_table_columns,
            diststyle="KEY",
            distkey="braze_external_id",
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        ddl_braze_push = RedshiftCreateTableOperator(
            task_id="ddl_braze_push",
            table_name="{{ brand_code }}_braze_push",
            schema=NCR_SCHEMA,
            columns=[
                ("attributes_jsonl", "VARCHAR(512)", "NOT NULL"),
            ],
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        ddl_braze_history = RedshiftCreateTableOperator(
            task_id="ddl_braze_history",
            table_name="{{ brand_code }}_braze_history",
            schema=NCR_SCHEMA,
            columns=[
                *braze_table_columns,
                ("valid_at", "TIMESTAMP", "DEFAULT SYSDATE"),
            ],
            diststyle="KEY",
            distkey="braze_external_id",
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        ddl_braze_campaign_id_mapping = RedshiftCreateTableOperator(
            task_id="ddl_braze_campaign_id_mapping",
            table_name="{{ brand_code }}_braze_campaign_id_mapping",
            schema=NCR_SCHEMA,
            columns=[
                ("brand_code", "CHARACTER(32)", "NOT NULL"),
                ("campaign_type", "CHARACTER(36)", "NOT NULL"),
                ("braze_campaign_id", "CHARACTER VARYING(256)", "NOT NULL"),
            ],
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        ddl_braze_campaigns = RedshiftCreateTableOperator(
            task_id="ddl_braze_campaigns",
            table_name="{{ brand_code }}_braze_campaigns",
            schema=NCR_SCHEMA,
            columns=[
                ("braze_external_id", "CHARACTER VARYING(256)"),
                ("midas_booking_id", "INTEGER"),
                ("source_id", "SMALLINT"),
                ("entity_id", "CHARACTER VARYING(25)"),
                ("customer_id", "CHARACTER VARYING(64)"),
                ("ncr_package_type", "CHARACTER VARYING(50)"),
                ("ncr_vendor_id", "CHARACTER VARYING(20)"),
                ("ncr_publish_date", "DATE"),
                ("campaign_name", "CHARACTER VARYING(1024)"),
                ("last_received", "TIMESTAMP WITHOUT TIME ZONE"),
                ("in_control", "BOOLEAN"),
                ("opened_notification", "BOOLEAN"),
            ],
            entity_to_privilege_grant={'GROUP "mkt_prd"': "ALL", "qa": "SELECT"},
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        (start_init, finish_init) = group_airflow_tasks(
            [
                crm_vendor_feed_ddl,
                ddl_ncr_braze_customer,
                ddl_ncr_midas_campaign_bookings,
                ddl_st_ncr_midas_campaign_bookings,
                ddl_ncr_vendor_user_campaign_eligibility,
                ddl_ncr_decision_engine_result,
                ddl_ncr_decision_engine_result_history,
                ddl_braze_main,
                ddl_braze_push,
                ddl_braze_history,
                ddl_braze_campaign_id_mapping,
                ddl_braze_campaigns,
            ],
            group_name="init",
        )

        # Import CRM vendor feed - productsup
        transform_crm_vendor_feed_task_id = "transform_crm_vendor_feed_task"
        final_crm_vendor_feed_key = str(s3_brand_prefix / "crm_vendor_feed.csv").format(
            task_id=transform_crm_vendor_feed_task_id
        )
        crm_vendor_feed_table = "{{ brand_code }}_crm_vendor_feed"

        check_last_crm_vendor_feed_update = S3CheckLastObjectUpdateSensor(
            s3_key="{{ crm_vendor_feed_key }}",
            s3_bucket=CRM_VENDOR_FEED_BUCKET,
            check_datetime="{{ ts }}",
            aws_conn_id=NCR_S3_CONN_ID,
            mode="reschedule",
            poke_interval=60 * 30,  # 30 mins
            timeout=60 * 60 * 2,  # 2 hrs
            task_id="check_last_crm_vendor_feed_update",
        )

        transform_crm_vendor_feed_task = PythonOperator(
            python_callable=transform_crm_vendor_feed,
            op_kwargs={
                "s3_key_src": "{{ crm_vendor_feed_key }}",
                "s3_bucket_src": CRM_VENDOR_FEED_BUCKET,
                "s3_key_dst": final_crm_vendor_feed_key,
                "s3_bucket_dst": NCR_S3_BUCKET,
                "src_aws_conn_id": NCR_S3_CONN_ID,
                "dst_aws_conn_id": NCR_S3_CONN_ID,
            },
            task_id=transform_crm_vendor_feed_task_id,
        )

        truncate_crm_vendor_feed = RedshiftTruncateOperator(
            schema=NCR_SCHEMA,
            table=crm_vendor_feed_table,
            redshift_conn_id=REDSHIFT_CONNECTION,
            task_id="truncate_crm_vendor_feed",
        )

        import_crm_vendor_feed = S3ToRedshiftIamOperator(
            schema=NCR_SCHEMA,
            table=crm_vendor_feed_table,
            s3_bucket=NCR_S3_BUCKET,
            s3_key=final_crm_vendor_feed_key,
            redshift_conn_id=REDSHIFT_CONNECTION,
            copy_options=("FORMAT AS CSV", "IGNOREHEADER 1"),
            task_id="import_crm_vendor_feed",
        )

        (
            check_last_crm_vendor_feed_update
            >> transform_crm_vendor_feed_task
            >> truncate_crm_vendor_feed
            >> import_crm_vendor_feed
        )

        # Import campaign bookings from MIDAS NCR database
        midas_campaign_booking_s3_key = str(s3_brand_prefix).format(
            task_id="midas_campaign_bookings"
        )
        extract_midas_campaign_bookings = PostgresToS3Operator(
            task_id="extract_midas_campaign_bookings",
            sql=str(Path("sql") / Path("extract_midas_campaign_bookings.sql")),
            s3_bucket=NCR_S3_BUCKET,
            s3_key=str(
                PosixPath(
                    midas_campaign_booking_s3_key,
                    "{{ brand_code }}_ncr_midas_campaign_bookings_{}.csv",
                )
            ),
            export_format="csv",
            replace=True,
            gzip=False,
            aws_conn_id=NCR_S3_CONN_ID,
            postgres_conn_id=MIDAS_NCR_CONN_ID,
        )

        st_ncr_midas_campaign_bookings_table_name = (
            "{{ brand_code }}_st_ncr_midas_campaign_bookings"
        )

        truncate_st_ncr_midas_campaign_bookings = RedshiftTruncateOperator(
            task_id="truncate_st_ncr_midas_campaign_bookings",
            schema=NCR_SCHEMA,
            table=st_ncr_midas_campaign_bookings_table_name,
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        load_st_ncr_midas_campaign_bookings = S3ToRedshiftIamOperator(
            task_id="load_st_ncr_midas_campaign_bookings",
            schema=NCR_SCHEMA,
            table=st_ncr_midas_campaign_bookings_table_name,
            s3_key=midas_campaign_booking_s3_key,
            s3_bucket=NCR_S3_BUCKET,
            redshift_conn_id=REDSHIFT_CONNECTION,
            copy_options=["FORMAT AS CSV", "IGNOREHEADER 1"],
        )

        truncate_ncr_midas_campaign_bookings = RedshiftTruncateOperator(
            task_id="truncate_ncr_midas_campaign_bookings",
            schema="{{ ncr_schema }}",
            table="{{ brand_code }}_ncr_midas_campaign_bookings",
            redshift_conn_id=REDSHIFT_CONNECTION,
        )

        import_ncr_midas_campaign_bookings = PostgresOperator(
            task_id="import_ncr_midas_campaign_bookings",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("import_ncr_midas_campaign_bookings.sql")),
        )

        (
            extract_midas_campaign_bookings
            >> truncate_st_ncr_midas_campaign_bookings
            >> load_st_ncr_midas_campaign_bookings
            >> truncate_ncr_midas_campaign_bookings
            >> import_ncr_midas_campaign_bookings
        )

        (start_import, finish_import) = group_airflow_tasks(
            [
                check_last_crm_vendor_feed_update,
                transform_crm_vendor_feed_task,
                truncate_crm_vendor_feed,
                import_crm_vendor_feed,
                extract_midas_campaign_bookings,
                truncate_st_ncr_midas_campaign_bookings,
                load_st_ncr_midas_campaign_bookings,
                truncate_ncr_midas_campaign_bookings,
                import_ncr_midas_campaign_bookings,
            ],
            group_name="import",
        )

        run_ncr_braze_customer = PostgresOperator(
            task_id="run_ncr_braze_customer",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("ncr_braze_customer.sql")),
        )

        run_ncr_vendor_user_campaign_eligibility = PostgresOperator(
            task_id="run_ncr_vendor_user_campaign_eligibility",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("ncr_vendor_user_campaign_eligibility.sql")),
        )

        run_ncr_decision_engine = PostgresOperator(
            task_id="run_ncr_decision_engine",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("ncr_decision_engine.sql")),
        )

        historize_ncr_decision_engine = PostgresOperator(
            task_id="historize_ncr_decision_engine",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("ncr_decision_engine_history.sql")),
        )

        (
            run_ncr_braze_customer
            >> run_ncr_vendor_user_campaign_eligibility
            >> run_ncr_decision_engine
        )
        run_ncr_decision_engine >> historize_ncr_decision_engine

        (start_decision_engine, finish_decision_engine) = group_airflow_tasks(
            [
                run_ncr_braze_customer,
                run_ncr_vendor_user_campaign_eligibility,
                run_ncr_decision_engine,
                historize_ncr_decision_engine,
            ],
            group_name="decision_engine",
        )

        unload_braze_push_task_id = "unload_braze_push"
        s3_braze_prefix = str(s3_brand_prefix).format(task_id=unload_braze_push_task_id)

        truncate_braze_main = RedshiftTruncateOperator(
            task_id="truncate_braze_main",
            schema=NCR_SCHEMA,
            table="{{ brand_code }}_braze_main",
            redshift_conn_id=REDSHIFT_CONNECTION,
        )
        compute_braze_main = PostgresOperator(
            task_id="compute_braze_main",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / "braze_push" / "compute_braze_main.sql"),
        )
        truncate_braze_push = RedshiftTruncateOperator(
            task_id="truncate_braze_push",
            schema=NCR_SCHEMA,
            table="{{ brand_code }}_braze_push",
            redshift_conn_id=REDSHIFT_CONNECTION,
        )
        compute_braze_push = PostgresOperator(
            task_id="compute_braze_push",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / "braze_push" / "compute_braze_push.sql"),
        )
        unload_braze_push = RedshiftToS3IamOperator(
            task_id=unload_braze_push_task_id,
            s3_bucket=NCR_S3_BUCKET,
            s3_prefix=s3_braze_prefix,
            schema=NCR_SCHEMA,
            table_name="{{ brand_code }}_braze_push",
            redshift_conn_id=REDSHIFT_CONNECTION,
            unload_options=("PARALLEL OFF",),
            include_header=False,
        )
        push_braze_ncr_attributes = PythonOperator(
            task_id="push_braze_ncr_attributes",
            python_callable=push_jsonl_to_braze,
            op_kwargs={
                "braze_conn_id": "{{ braze_conn_id }}",
                "s3_conn_id": NCR_S3_CONN_ID,
                "bucket_name": NCR_S3_BUCKET,
                "jsonl_path": str(
                    PosixPath(s3_braze_prefix) / "{{ brand_code }}_braze_push_000"
                ),
            },
        )
        save_braze_history = PostgresOperator(
            task_id="save_braze_history",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / "braze_push" / "save_braze_history.sql"),
        )

        truncate_braze_main >> compute_braze_main
        truncate_braze_push >> compute_braze_main

        (
            compute_braze_main
            >> compute_braze_push
            >> unload_braze_push
            >> push_braze_ncr_attributes
            >> save_braze_history
        )

        (start_push_braze, finish_push_braze) = group_airflow_tasks(
            [
                truncate_braze_main,
                truncate_braze_push,
                compute_braze_main,
                compute_braze_push,
                unload_braze_push,
                push_braze_ncr_attributes,
                save_braze_history,
            ],
            group_name="push_braze",
        )

        braze_campaign_id_mapping = PostgresOperator(
            task_id="braze_campaign_id_mapping",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("braze_campaign_id_mapping.sql")),
        )

        braze_campaigns = PostgresOperator(
            task_id="braze_campaigns",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=str(Path("sql") / Path("braze_campaigns.sql")),
        )

        braze_campaign_id_mapping >> braze_campaigns

        (start_braze_campaigns, finish_braze_campaigns,) = group_airflow_tasks(
            [braze_campaigns, braze_campaign_id_mapping],
            group_name="braze_campaigns",
        )

        datahub_input_tasks = []
        for database in dwh_import_read_replicas.databases.values():
            project = database.extra_params.get("project_name", "")

            if project != "ncr":
                continue

            for table in database.tables:
                if table.name != IMPORT_TABLE_PREFIX + brand_code:
                    continue

                datahub_import_task = (
                    dwh_import_read_replicas.import_from_read_replicas(dag, table)
                )
                datahub_input_tasks.append(datahub_import_task)

        (start_datahub_import, finish_datahub_import,) = group_airflow_tasks(
            get_all_airflow_dependants(datahub_input_tasks, upstream=False),
            group_name="datahub_import",
        )

        # dependencies
        finish_init >> start_import
        finish_import >> start_decision_engine
        finish_decision_engine >> start_push_braze
        finish_push_braze >> start_braze_campaigns
        finish_braze_campaigns >> start_datahub_import

        # Add external dependencies
        wait_for_jenkins_crm_feed = CustomHttpSensor(
            task_id="wait_for_jenkins_crm_feed",
            endpoint="job/{{ dwh_jenkins_crm_feed_job_name }}/lastSuccessfulBuild/api/json",
            http_conn_id="http_jenkins_dwh",
            response_check=check_dwh_jenkins_success,
            poke_interval=60 * 15,  # 15 minutes
            timeout=60 * 60 * 3,  # 3 hours
            mode="reschedule",
        )

        wait_for_jenkins_braze_fetcher = CustomHttpSensor(
            task_id="wait_for_jenkins_braze_fetcher",
            endpoint="job/{{ dwh_jenkins_braze_fetcher_job_name }}/lastSuccessfulBuild/api/json",
            http_conn_id="http_jenkins_dwh",
            response_check=lambda r, **ctx: check_dwh_jenkins_success(
                r, allow_unstable=True, offset=14, **ctx
            ),
            poke_interval=60 * 15,  # 15 minutes
            timeout=60 * 60 * 3,  # 3 hours
            mode="reschedule",
        )

        vendor_user_mapping_table = VENDOR_USER_MAPPING_CONFIG[
            "vendor_user_mapping_output_table"
        ].format(brand_code=brand_code)

        wait_for_vendor_user_mapping = BigQueryCheckMetadataExecutionOperator(
            config=config,
            task_id=f"check-mtdt-{vendor_user_mapping_table}",
            bigquery_conn_id="bigquery_default",
            # Stripping dev suffix because this operators always looks for prod tables
            dataset=f"redshift_{VENDOR_USER_MAPPING_SCHEMA.rstrip('_dev')}",
            table_name=vendor_user_mapping_table,
            metadata_execution_table=MetadataExecutionTable(),
            execution_timeout=timedelta(minutes=15),
            retries=10,
            retry_delay=timedelta(minutes=10),
        )

        wait_for_jenkins_braze_fetcher >> start_braze_campaigns
        wait_for_jenkins_crm_feed >> start_decision_engine
        wait_for_vendor_user_mapping >> start_decision_engine

    return dag


for ncr_brand_code, ncr_brand in NCR_CONFIG.get("ncr_brands").items():
    brand_common_name = ncr_brand.get("brand_common_name")
    version = ncr_brand.get("version")

    dag_id = f"mkt-ncr-{brand_common_name}-v{version}"

    globals()[dag_id] = create_mkt_ncr_dag_for_entity(
        dag_id=dag_id, brand_config=ncr_brand, brand_code=ncr_brand_code
    )
