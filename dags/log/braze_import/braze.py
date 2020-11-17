from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator

from configuration import config
from datahub.common import alerts
from datahub.operators.bigquery.big_query_to_big_query_operator import BigQueryToBigQueryOperator
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.bq_to_braze_operator import BigQueryToBrazeOperator

BRAZE_DATASET = "braze"
STAGING_DATASET = "staging"
BRAZE_RIDERS_VIEW = "braze_riders"
LAST_PUSHED_RIDERS_TABLE_NAME = "last_pushed_riders"
BRAZE_UI_COLOR = "#d16464"


class Braze:

    def __init__(self,
                 full_import=False):
        self.full_import = full_import
        self.project_id = config.get("braze").get("project_id")

    def braze_create_view(self):
        return BigQueryOperator(
            task_id='braze_create_view',
            sql="riders_view.sql",
            params={
                'braze_dataset': BRAZE_DATASET,
                'braze_riders_view': BRAZE_RIDERS_VIEW,
                'project_id': self.project_id,
            },
            use_legacy_sql=False,
            on_failure_callback=alerts.setup_callback(),
        )

    @staticmethod
    def backfill(start, rider_view):
        # 1. Backfill table using view.
        backfill_table = BigQueryOperator(
            task_id="braze_backfill_table",
            sql='query_last_pushed_table.sql',
            destination_dataset_table=f'{BRAZE_DATASET}.{LAST_PUSHED_RIDERS_TABLE_NAME}',
            params={
                'braze_dataset': BRAZE_DATASET,
                'braze_view': BRAZE_RIDERS_VIEW,
            },
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            use_legacy_sql=False,
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
        )
        # 2. Push riders in 2 to braze.
        push_to_braze_api_operator = BigQueryToBrazeOperator(
            task_id='braze_push_new_and_updated_riders',
            sql='query_to_push_to_braze.sql',
            params={
                'braze_dataset': BRAZE_DATASET,
                'table_name': LAST_PUSHED_RIDERS_TABLE_NAME,
                'staging_table': False,
            },
            executor_config={
                'KubernetesExecutor': {
                    'request_memory': "10000Mi",
                    'limit_memory': "10000Mi",
                    'node_selectors': config.get('braze').get('node_selectors'),
                    'tolerations': config.get('braze').get('tolerations'),
                },
            },
            ui_color=BRAZE_UI_COLOR,
            on_failure_callback=alerts.setup_callback(),
            execution_timeout=timedelta(minutes=60),
        )
        start >> rider_view >> backfill_table >> push_to_braze_api_operator

    @staticmethod
    def daily_import(start, rider_view):
        # 2. Query view and store it into a staging table.
        braze_current_riders_table = 'braze_import_table_current_riders_snapshot'
        staging_riders = BigQueryOperator(
            task_id="braze_create_staging_current_riders",
            sql='query_last_pushed_table.sql',
            # If the ts_nodash is changed, please update braze_create_delta_riders.sql and
            # wherever braze_current_riders_table is used.
            destination_dataset_table=f"{STAGING_DATASET}.{braze_current_riders_table}_{{{{ ts_nodash }}}}",
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            params={
                'braze_dataset': BRAZE_DATASET,
                'braze_view': BRAZE_RIDERS_VIEW,
            },
            use_legacy_sql=False,
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
        )
        # 3. Run Last_Pushed MINUS staging and store into a staging table.
        delta_riders_table = "braze_import_table_delta_riders_"
        delta_riders = BigQueryOperator(
            task_id="braze_create_delta_riders_table",
            sql='braze_create_delta_riders.sql',
            destination_dataset_table=f'{STAGING_DATASET}.{delta_riders_table}{{{{ ts_nodash }}}}',
            params={
                'braze_dataset': BRAZE_DATASET,
                'staging_dataset': STAGING_DATASET,
                'last_pushed_riders': LAST_PUSHED_RIDERS_TABLE_NAME,
                'staging_table': braze_current_riders_table,
            },
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            execution_timeout=timedelta(minutes=15),
            use_legacy_sql=False,
            on_failure_callback=alerts.setup_callback(),
        )
        # 4. Query the table in 3 and push it.
        push_to_braze_api_operator = BigQueryToBrazeOperator(
            task_id='braze_push_new_updated_riders',
            sql='query_to_push_to_braze.sql',
            params={
                'braze_dataset': STAGING_DATASET,
                'table_name': delta_riders_table,
                'staging_table': True,
            },
            executor_config={
                'KubernetesExecutor': {
                    'request_memory': "10000Mi",
                    'limit_memory': "10000Mi",
                    'node_selectors': config.get('braze').get('node_selectors'),
                    'tolerations': config.get('braze').get('tolerations'),
                },
            },
            ui_color=BRAZE_UI_COLOR,
            use_legacy_sql=False,
            on_failure_callback=alerts.setup_callback(),
            execution_timeout=timedelta(minutes=45),
        )
        # 5. Replace last_pushed with 2.
        updated_last_pushed_riders = BigQueryToBigQueryOperator(
            task_id="braze_update_last_pushed_riders",
            source_project_dataset_tables=f"{STAGING_DATASET}.{braze_current_riders_table}_{{{{ ts_nodash }}}}",
            destination_project_dataset_table=f"{BRAZE_DATASET}.{LAST_PUSHED_RIDERS_TABLE_NAME}",
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=alerts.setup_callback(),
        )

        start >> rider_view >> staging_riders >> delta_riders >> push_to_braze_api_operator >> updated_last_pushed_riders

    def render(self):
        start = DummyOperator(task_id='start')
        rider_view = self.braze_create_view()

        if self.full_import:
            self.backfill(start=start, rider_view=rider_view)
        else:
            self.daily_import(start=start, rider_view=rider_view)
