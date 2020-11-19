"""\
### MKT-Audience

This DAG implements the MKT-Audience project, a proof-of-concept for cross-channel targetting. The
main goal is to orchestrate user campaigns for channels Paid Marketing and CRM.

It consists of 5 basic components

- Initialization boiler plate, running DDL and creating necessary mapping tables
- Audience stage assignments to users based on DWH data
- Gather device information from Adjust
- Push audiences to mParticle (Paid Marketing)
- Push audiences to Braze (CRM)

#### Audience stage assignments

Audience stages are essentially computed from the order history of a user. A user is inherently
identified by `master_id`. When a user enters are new audience (either entirely new user or a user
moving from one audience stage to another) they are assigned a (new) random number upon which the
variant is derived.

#### Adjust device information

TBD.

We only consider users for which we can find reasonable device information in Adjust. Users with
missing or incomplete device information are excluded as we cannot target them meaningfully and
hence

#### mParticle pushes

In mParticle we pay for **active customers**. Customers are considered active when they either are
part of a mParticle campaign or receive an update via a push.

Hence we have to try to achieve two things

- Only push the minimal amount of data needed to mParticle (E.g. only push updates). We do not want
to to touch (e.g. push empty diffs) for a customer that is not part of any running campaign and
hence flag them active.

- Make sure zombie users are excluded from all potential campaigns.

To realise this behavior we compute every day the difference from previous day and only push this
difference.
"""
import os

from airflow import DAG, configuration
import pendulum
from airflow.operators.check_operator import CheckOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator

import mkt_import
import audience
from configuration import config
from datahub.common import alerts
from operators.custom_http_sensor import CustomHttpSensor
from utils.check_dwh_jenkins import check_dwh_jenkins_success
from utils.group_airflow_tasks import group_airflow_tasks
from utils.none_check_operator import NoneCheckOperator
from utils.uploader.braze_push import push_jsonl_to_braze
from utils.uploader.mparticle_push import mparticle_uploader_orchestration

from datahub.operators.redshift.redshift_to_s3_iam_operator import (
    RedshiftToS3IamOperator,
)

CONFIG_VARIABLE = config.get("audience")
ENVIRONMENT = configuration.get("datahub", "environment")

USER_DEFINED_FILTERS = {
    "source_id_filter": lambda input_list: "(" + ", ".join(map(str, input_list)) + ")"
}

AWS_CONNECTION = CONFIG_VARIABLE.get("airflow_aws_conn_id")
REDSHIFT_CONNECTION = CONFIG_VARIABLE.get("airflow_redshift_conn_id")
REDSHIFT_SCHEMA = CONFIG_VARIABLE.get("audience_schema")

# setup for mParticle feed
MPARTICLE_CONFIG = CONFIG_VARIABLE.get("mparticle")
MPARTICLE_BULK_SIZE = MPARTICLE_CONFIG.get("bulk_size")
MPARTICLE_HTTP_LOG_DEBUG = MPARTICLE_CONFIG.get("http_log_debug")
MPARTICLE_DEBUG_MODE = MPARTICLE_CONFIG.get("debug_mode")
MPARTICLE_S3_BUCKET = MPARTICLE_CONFIG.get("unload_s3_bucket", "mkt-audience-dev")
MPARTICLE_S3_PREFIX = os.path.join(
    MPARTICLE_CONFIG.get("unload_s3_prefix", "audience-mparticle-feed"),
    "{{ brand_code }}",
    "{{ ds }}",
)
MPARTICLE_REDSHIFT_TABLE = "{{ brand_code }}_mparticle_pre_push_diff"
MPARTICLE_ENVIRONMENT = "production" if ENVIRONMENT == "production" else "development"

# setup for Braze feed
BRAZE_CONFIG = CONFIG_VARIABLE.get("braze")
BRAZE_S3_BUCKET = BRAZE_CONFIG.get("unload_s3_bucket", "mkt-audience-dev")
BRAZE_S3_FOLDER = os.path.join(
    BRAZE_CONFIG.get("unload_s3_folder", "audience-braze-feed"),
    "{{ brand_code }}",
    "{{ ds }}",
)
BRAZE_REDSHIFT_DIFF_TABLE = "{{ brand_code }}_braze_pre_push_diff_jsonl"


def create_audience_dag_for_entity(dag_id, brand_config, brand_code):
    dag = DAG(
        dag_id=dag_id,
        description=f'Does all the audience magic for {brand_config.get("brand_common_name")}.',
        start_date=pendulum.parse(brand_config.get("dag_start_date")),
        schedule_interval=brand_config.get("schedule_interval"),
        catchup=False,
        user_defined_macros={
            "environment": ENVIRONMENT,
            **brand_config,
            "brand_code": brand_code,
            **CONFIG_VARIABLE,
        },
        user_defined_filters=USER_DEFINED_FILTERS,
        template_searchpath=audience.__path__,
        default_args={
            **mkt_import.DEFAULT_ARGS,
            **{"on_failure_callback": alerts.setup_callback(), "retries": 0},
        },
        tags=[mkt_import.DEFAULT_ARGS["owner"], "audience", brand_code],
    )

    with dag:
        dag.doc_md = __doc__

        #
        # init
        #

        # ddl
        ddl_customer_audience = PostgresOperator(  # noqa: F841 # pylint: disable=W0612
            task_id="ddl_customer_audience",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("ddl", "customer_audience.sql"),
        )

        ddl_customer_audience_history = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="ddl_customer_audience_history",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("ddl", "customer_audience_history.sql"),
            )
        )

        ddl_customer_stage = PostgresOperator(  # noqa: F841 # pylint: disable=W0612
            task_id="ddl_customer_stage",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("ddl", "customer_stage.sql"),
        )

        ddl_customer_stage_history = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="ddl_customer_stage_history",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("ddl", "customer_stage_history.sql"),
            )
        )

        ddl_audience_definition = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="ddl_audience_definition",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("ddl", "audience_definition.sql"),
            )
        )

        ddl_audience_variant_split = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="ddl_audience_variant_split",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("ddl", "audience_variant_split.sql"),
            )
        )

        ddl_customer_device_mapping = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="ddl_customer_device_mapping",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("ddl", "customer_device_mapping.sql"),
            )
        )

        ddl_customer_device_mapping_history = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="ddl_customer_device_mapping_history",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("ddl", "customer_device_mapping_history.sql"),
            )
        )

        ddl_audience_mparticle_feed = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="ddl_audience_mparticle_feed",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("ddl", "audience_mparticle_feed.sql"),
            )
        )

        ddl_audience_braze_feed = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="ddl_audience_braze_feed",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("ddl", "audience_braze_feed.sql"),
            )
        )

        # group DDL operators in the DAG
        current_locals = locals()
        (start_ddl, finish_ddl) = group_airflow_tasks(
            [
                current_locals[task]
                for task in current_locals
                if task.startswith("ddl_")
            ],
            group_name="ddl",
        )

        # udf
        udf_f_audience_to_jsonl = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="udf_f_audience_to_jsonl",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("udf", "f_audience_to_jsonl.sql"),
            )
        )

        udf_f_customer_stage = PostgresOperator(  # noqa: F841 # pylint: disable=W0612
            task_id="udf_f_customer_stage",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("udf", "f_customer_stage.sql"),
        )

        udf_f_customer_stage_age = (
            PostgresOperator(  # noqa: F841 # pylint: disable=W0612
                task_id="udf_f_customer_stage_age",
                postgres_conn_id=REDSHIFT_CONNECTION,
                sql=os.path.join("udf", "f_customer_stage_age.sql"),
            )
        )

        # group UDF operators in the DAG
        current_locals = locals()
        (start_udf, finish_udf) = group_airflow_tasks(
            [
                current_locals[task]
                for task in current_locals
                if task.startswith("udf_")
            ],
            group_name="udf",
        )

        # load
        load_initial_audience_definition = PostgresOperator(
            task_id="load_initial_audience_definition",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join(
                "load",
                f"initial_audience_definition_{brand_code}.sql",
            ),
        )

        load_audience_variant_split = PostgresOperator(
            task_id="load_audience_variant_split",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("load", f"audience_variant_split_{brand_code}.sql"),
        )

        # sanity check
        load_sanity_check_audience_definition = NoneCheckOperator(
            task_id="load_sanity_check_audience_definition",
            conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("load", "sanity_check_audience_definition.sql"),
        )

        load_sanity_check_audience_variant_split = NoneCheckOperator(
            task_id="load_sanity_check_audience_variant_split",
            conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("load", "sanity_check_audience_variant_split.sql"),
        )

        load_initial_audience_definition >> load_sanity_check_audience_definition
        load_audience_variant_split >> load_sanity_check_audience_variant_split

        # group load operators in the DAG
        current_locals = locals()
        (start_load, finish_load) = group_airflow_tasks(
            [
                current_locals[task]
                for task in current_locals
                if task.startswith("load_")
            ],
            group_name="load",
        )

        [finish_ddl, finish_udf] >> start_load

        # group initialization operators in the DAG
        (start_init, finish_init) = group_airflow_tasks(  # pylint: disable=W0612
            [start_ddl, start_udf, finish_load], group_name="init"
        )

        #
        # queries for audience stage assignments
        #
        sql_customer_stage = PostgresOperator(
            task_id="sql_customer_stage",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "customer_stage.sql"),
        )

        sql_customer_stage_history = PostgresOperator(
            task_id="sql_customer_stage_history",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "customer_stage_history.sql"),
        )

        sql_customer_audience = PostgresOperator(
            task_id="sql_customer_audience",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "customer_audience.sql"),
        )

        sql_customer_audience_history = PostgresOperator(
            task_id="sql_customer_audience_history",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "customer_audience_history.sql"),
        )

        sql_customer_stage >> sql_customer_audience
        sql_customer_stage >> sql_customer_stage_history
        [sql_customer_stage, sql_customer_audience] >> sql_customer_audience_history

        # group audience stage assignments in the DAG
        (
            start_customer_audience_stage,
            finish_customer_audience_stage,
        ) = group_airflow_tasks(
            [
                sql_customer_stage,
                sql_customer_stage_history,
                sql_customer_audience,
                sql_customer_audience_history,
            ],
            group_name="customer_audience_stage",
        )

        #
        # queries customer device mapping
        #
        sql_sanity_check_adjust_order_type = CheckOperator(
            task_id="sql_sanity_check_adjust_order_type",
            conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "sanity_check_adjust_order_type.sql"),
        )

        sql_customer_device_mapping = PostgresOperator(
            task_id="sql_customer_device_mapping",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "customer_device_mapping.sql"),
        )

        sql_customer_device_mapping_history = PostgresOperator(
            task_id="sql_customer_device_mapping_history",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "customer_device_mapping_history.sql"),
        )

        (
            sql_sanity_check_adjust_order_type
            >> sql_customer_device_mapping
            >> sql_customer_device_mapping_history
        )

        # group group customer device mapping in the DAG
        (
            start_customer_device_mapping,
            finish_customer_device_mapping,
        ) = group_airflow_tasks(
            [
                sql_sanity_check_adjust_order_type,
                sql_customer_device_mapping,
                sql_customer_device_mapping_history,
            ],
            group_name="customer_device_mapping",
        )

        #
        # queries for customer audience device mapping and mParticle pushes
        #
        sql_audience_mparticle_feed_populate_main = PostgresOperator(
            task_id="sql_audience_mparticle_feed_populate_main",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "audience_mparticle_feed", "populate_main.sql"),
        )

        sql_audience_mparticle_feed_compute_diff = PostgresOperator(
            task_id="sql_audience_mparticle_feed_compute_diff",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "audience_mparticle_feed", "compute_diff.sql"),
        )

        sql_audience_mparticle_feed_record_history = PostgresOperator(
            task_id="sql_audience_mparticle_feed_record_history",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "audience_mparticle_feed", "record_history.sql"),
        )

        audience_mparticle_feed_unload_diff_to_s3 = RedshiftToS3IamOperator(
            task_id="audience_mparticle_feed_unload_diff_to_s3",
            s3_bucket=MPARTICLE_S3_BUCKET,
            s3_prefix=MPARTICLE_S3_PREFIX,
            schema=REDSHIFT_SCHEMA,
            table_name=MPARTICLE_REDSHIFT_TABLE,
            redshift_conn_id=REDSHIFT_CONNECTION,
            unload_options=["FORMAT AS CSV", "GZIP"],
            include_header=True,
        )

        audience_mparticle_feed_mparticle_audience_uploader = PythonVirtualenvOperator(
            task_id="audience_mparticle_feed_mparticle_audience_uploader",
            python_callable=mparticle_uploader_orchestration,
            requirements=["mparticle"],
            op_kwargs={
                "mparticle_conn_id": "{{ mparticle_conn_id }}",
                "aws_conn_id": AWS_CONNECTION,
                "mparticle_s3_bucket": MPARTICLE_S3_BUCKET,
                "mparticle_s3_prefix": MPARTICLE_S3_PREFIX,
                "environment": MPARTICLE_ENVIRONMENT,
            },
            retries=0,
        )

        sql_audience_mparticle_feed_save_snapshot = PostgresOperator(
            task_id="audience_mparticle_feed_save_snapshot",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "audience_mparticle_feed", "save_snapshot.sql"),
        )

        (
            sql_audience_mparticle_feed_populate_main
            >> sql_audience_mparticle_feed_compute_diff
            >> audience_mparticle_feed_unload_diff_to_s3
            >> audience_mparticle_feed_mparticle_audience_uploader
            >> sql_audience_mparticle_feed_save_snapshot
        )
        sql_audience_mparticle_feed_populate_main >> sql_audience_mparticle_feed_record_history

        # group audience device mapping tasks and mParticle pushes in the DAG
        (
            start_audience_mparticle_feed,
            finish_audience_mparticle_feed,
        ) = group_airflow_tasks(
            [
                sql_audience_mparticle_feed_populate_main,
                sql_audience_mparticle_feed_record_history,
                sql_audience_mparticle_feed_compute_diff,
                audience_mparticle_feed_unload_diff_to_s3,
                audience_mparticle_feed_mparticle_audience_uploader,
                sql_audience_mparticle_feed_save_snapshot,
            ],
            group_name="audience_mparticle_feed",
        )

        #
        # queries for braze pushes
        #
        sql_audience_braze_feed_populate_main = PostgresOperator(
            task_id="sql_audience_braze_feed_populate_main",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "audience_braze_feed", "populate_main.sql"),
        )

        sql_audience_braze_feed_compute_diff = PostgresOperator(
            task_id="sql_audience_braze_feed_compute_diff",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "audience_braze_feed", "compute_diff.sql"),
        )

        sql_audience_braze_feed_record_history = PostgresOperator(
            task_id="sql_audience_braze_feed_record_history",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "audience_braze_feed", "record_history.sql"),
        )

        audience_braze_feed_unload_diff_to_s3 = RedshiftToS3IamOperator(
            task_id="audience_braze_feed_unload_diff_to_s3",
            s3_bucket=BRAZE_S3_BUCKET,
            s3_prefix=BRAZE_S3_FOLDER,
            schema=REDSHIFT_SCHEMA,
            table_name=BRAZE_REDSHIFT_DIFF_TABLE,
            redshift_conn_id=REDSHIFT_CONNECTION,
            unload_options=["PARALLEL OFF"],
            include_header=False,
        )

        audience_braze_feed_braze_uploader = PythonOperator(
            task_id="audience_braze_feed_braze_uploader",
            python_callable=push_jsonl_to_braze,
            op_kwargs={
                "braze_conn_id": "{{ braze_conn_id }}",
                "s3_conn_id": AWS_CONNECTION,
                "bucket_name": BRAZE_S3_BUCKET,
                "jsonl_path": os.path.join(
                    BRAZE_S3_FOLDER, BRAZE_REDSHIFT_DIFF_TABLE + "_000"
                ),
            },
            retries=0,
        )

        sql_audience_braze_feed_save_snapshot = PostgresOperator(
            task_id="sql_audience_braze_feed_save_snapshot",
            postgres_conn_id=REDSHIFT_CONNECTION,
            sql=os.path.join("sql", "audience_braze_feed", "save_snapshot.sql"),
        )

        (
            sql_audience_braze_feed_populate_main
            >> sql_audience_braze_feed_compute_diff
            >> audience_braze_feed_unload_diff_to_s3
            >> audience_braze_feed_braze_uploader
            >> sql_audience_braze_feed_save_snapshot
        )
        sql_audience_braze_feed_populate_main >> sql_audience_braze_feed_record_history

        # group audience Braze feed pushes in the DAG
        (start_audience_braze_feed, finish_audience_braze_feed) = group_airflow_tasks(
            [
                sql_audience_braze_feed_populate_main,
                sql_audience_braze_feed_record_history,
                sql_audience_braze_feed_compute_diff,
                audience_braze_feed_unload_diff_to_s3,
                audience_braze_feed_braze_uploader,
                sql_audience_braze_feed_save_snapshot,
            ],
            group_name="audience_braze_feed",
        )

        #
        # external dependencies
        #
        wait_for_jenkins_crm_feed = CustomHttpSensor(
            task_id="wait_for_jenkins_crm_feed",
            endpoint="job/{{ dwh_jenkins_crm_feed_job_name }}/lastSuccessfulBuild/api/json",
            http_conn_id="http_jenkins_dwh",
            response_check=check_dwh_jenkins_success,
            poke_interval=60 * 15,  # 15 minutes
            timeout=60 * 60 * 6,  # 6 hours
            mode="reschedule",
        )

        wait_for_jenkins_adjust_event_computation = CustomHttpSensor(
            task_id="wait_for_jenkins_adjust_event_computation",
            endpoint="job/mkt_adjust_transactions_match/lastSuccessfulBuild/api/json",
            http_conn_id="http_jenkins_dwh",
            response_check=lambda r, **ctx: check_dwh_jenkins_success(
                r, allow_unstable=True, offset=1, **ctx
            ),
            poke_interval=60 * 15,  # 15 minutes
            timeout=60 * 60 * 6,  # 6 hours
            mode="reschedule",
        )

        wait_for_jenkins_crm_feed >> start_customer_audience_stage
        wait_for_jenkins_adjust_event_computation >> start_customer_device_mapping

        #
        # DAG dependencies
        #
        finish_init >> [start_customer_audience_stage, start_customer_device_mapping]

        [
            finish_customer_device_mapping,
            finish_customer_audience_stage,
        ] >> start_audience_mparticle_feed

        finish_customer_audience_stage >> start_audience_braze_feed

    return dag


for brand_code, brand_config in CONFIG_VARIABLE.get("audience_brands").items():
    brand_common_name = brand_config.get("brand_common_name")
    version = brand_config.get("version")

    dag_id = f"mkt_audience.{brand_common_name}-v{version}"

    globals()[dag_id] = create_audience_dag_for_entity(
        dag_id=dag_id, brand_config=brand_config, brand_code=brand_code
    )
