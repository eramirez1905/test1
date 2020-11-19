from datetime import timedelta

from airflow.models import BaseOperator

from datahub.operators.bigquery.big_query_to_big_query_operator import BigQueryToBigQueryOperator
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.common import alerts
from feature_store import PROJECT_ID, FEATURE_STORE_DATASET, FEATURE_STORE_LABELS
from feature_store.feature_store_locator import resolve


def create_daily_pipeline(app_name: str) -> BaseOperator:
    feature_store_app_locator: dict = resolve(app_name)

    feature_store_daily_view_name: str = f'{app_name}_daily'
    feature_store_daily_query_operator: BigQueryOperator = BigQueryOperator(
        task_id=f'create-view-{feature_store_daily_view_name}',
        sql=feature_store_app_locator.get('sql_create_view_daily'),
        params={
            'project_id': PROJECT_ID,
            'feature_store_dataset': FEATURE_STORE_DATASET,
            'view_name': feature_store_daily_view_name,
            'real_time': False,
            'daily': True,
        },
        use_legacy_sql=False,
        on_failure_callback=alerts.setup_callback(),
    )

    feature_store_staging_table_name: str = f'{PROJECT_ID}.staging.{app_name}_stream_table_{{{{ ts_nodash }}}}'
    feature_store_create_staging_operator: BigQueryOperator = BigQueryOperator(
        task_id=f'create-staging-table-{app_name}',
        sql=feature_store_app_locator.get('sql_create_staging_table'),
        destination_dataset_table=feature_store_staging_table_name,
        time_partitioning={
            'field': 'created_date',
            'type': 'DAY',
        },
        params={
            'project_id': PROJECT_ID,
            'feature_store_dataset': FEATURE_STORE_DATASET,
            'view_name': feature_store_daily_view_name,
            'daily': True,
        },
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        execution_timeout=timedelta(minutes=15),
        use_legacy_sql=False,
        on_failure_callback=alerts.setup_callback(),
    )

    partition_name: str = '{{ ds_nodash }}'
    table_name: str = feature_store_app_locator.get('table_name')
    feature_store_copy_to_destination_operator = BigQueryToBigQueryOperator(
        task_id=f'copy-staging-to-destination-{app_name}',
        source_project_dataset_tables=f'{feature_store_staging_table_name}${partition_name}',
        destination_project_dataset_table=f'{PROJECT_ID}.{FEATURE_STORE_DATASET}.{table_name}${partition_name}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        execution_timeout=timedelta(minutes=15),
        on_failure_callback=alerts.setup_callback(),
        labels=FEATURE_STORE_LABELS,
    )

    feature_store_daily_query_operator >> feature_store_create_staging_operator >> feature_store_copy_to_destination_operator
    return feature_store_daily_query_operator


def create_backfill_pipeline(app_name: str) -> BaseOperator:
    feature_store_app_locator: dict = resolve(app_name)
    table_name: str = feature_store_app_locator.get('table_name')

    feature_store_create_table_operator = BigQueryOperator(
        task_id=f'create-table-{app_name}',
        sql=feature_store_app_locator.get('sql_create_table'),
        params={
            'project_id': PROJECT_ID,
            'feature_store_dataset': FEATURE_STORE_DATASET,
            'table_name': table_name,
        },
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        on_failure_callback=alerts.setup_callback(),
        labels=FEATURE_STORE_LABELS,
    )

    feature_store_backfill_view_name: str = f'{app_name}_backfill'
    feature_store_backfill_query_operator: BigQueryOperator = BigQueryOperator(
        task_id=f'create-view-{feature_store_backfill_view_name}',
        sql=feature_store_app_locator.get('sql_create_view_backfill'),
        params={
            'project_id': PROJECT_ID,
            'feature_store_dataset': FEATURE_STORE_DATASET,
            'view_name': feature_store_backfill_view_name,
            'real_time': False,
            'daily': False,
        },
        use_legacy_sql=False,
        on_failure_callback=alerts.setup_callback(),
    )

    feature_store_run_backfill_operator = BigQueryOperator(
        task_id=f'backfill-{app_name}',
        sql=feature_store_app_locator.get('sql_run_backfill'),
        destination_dataset_table=f'{FEATURE_STORE_DATASET}.{table_name}',
        params={
            'project_id': PROJECT_ID,
            'feature_store_dataset': FEATURE_STORE_DATASET,
            'view_name': feature_store_backfill_view_name,
            'daily': False,
            'backfill': True,
        },
        execution_timeout=timedelta(minutes=45),
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        on_failure_callback=alerts.setup_callback(),
        labels=FEATURE_STORE_LABELS,
    )

    feature_store_create_table_operator >> feature_store_backfill_query_operator >> feature_store_run_backfill_operator
    return feature_store_create_table_operator
