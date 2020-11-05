import os
import unittest
from datetime import datetime

from airflow import DAG

from datahub.common.configuration import Config
from datahub.curated_data.process_curated_data import ProcessCuratedData
from datahub.dwh_import_read_replica import DwhImportReadReplica


def get_dag(type=''):
    return DAG(
        dag_id=f'fake_dag_id{type}',
        start_date=datetime(2019, 2, 25),
        schedule_interval=None,
        max_active_runs=1,
        catchup=False
    )


class ProcessCuratedDataTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.project_id = 'awesome-project'
        self.dataset_id = 'test_dataset'
        self.entities = {}

    def tearDown(self):
        config = Config()
        config.clear_instance()

    def test_sanity_check(self):
        config_object = Config()
        policy_tags = []
        yaml_path = f"{os.path.dirname(__file__)}/fixtures/process_curated_data"
        config_object.add_config('curated_data', yaml_path)
        config_object.add_config('dwh_import', yaml_path)

        dwh_import = DwhImportReadReplica(config_object.config)

        entities = {}
        process_curated_data = ProcessCuratedData(
            dag=get_dag(),
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            config=config_object.config.get('curated_data'),
            entities=entities,
            create_daily_tasks=False,
            policy_tags=policy_tags,
            backfill=False,
            dwh_import=dwh_import,
        )
        tasks = process_curated_data.render()
        self.assertIn("entities", tasks)
        self.assertIn("countries", tasks)

        table_entities = tasks["entities"]
        table_countries = tasks["countries"]

        self.assertIsNotNone(table_entities)
        self.assertIsNotNone(table_countries)

        self.assertEqual('create-table-test_dataset-entities', table_entities.head.task_id)
        self.assertEqual('create-table-test_dataset-countries', table_countries.head.task_id)

        self.assertEqual('end-sc-test_dataset-entities', table_entities.tail.task_id)
        self.assertEqual('create-table-test_dataset-countries', table_countries.tail.task_id)

        # entities
        self.assertEqual({
            'create-table-test_dataset-entities',
            'sc-sanity_check_entities_first',
            'sc-sanity_check_entities_second',
        }, table_entities.tail.upstream_task_ids)
        self.assertEqual({
            'create-table-test_dataset-countries',
        }, table_entities.tail.downstream_task_ids)

        self.assertEqual({
            'start-test_dataset',
        }, table_entities.head.upstream_task_ids)
        self.assertEqual({
            'end-sc-test_dataset-entities',
            'metadata-entities',
            'sc-sanity_check_entities_first',
            'sc-sanity_check_entities_non_blocking',
            'sc-sanity_check_entities_second',
            'policy-tags-entities'
        }, table_entities.head.downstream_task_ids)

        # countries
        self.assertEqual({
            'check-mtdt-hurrier_cities',
            'check-mtdt-iam_countries',
            'end-sc-test_dataset-entities',
            'start-test_dataset'
        }, table_countries.head.upstream_task_ids)
        self.assertEqual({
            'metadata-countries',
            'policy-tags-countries'
        }, table_countries.head.downstream_task_ids)

    def test_with_only_non_blocking_sanity_check(self):
        config_object = Config()
        policy_tags = []
        yaml_path = f"{os.path.dirname(__file__)}/fixtures/process_curated_data"
        config_object.add_config('curated_data_with_only_non_blocking_sanity_check', yaml_path)
        config_object.add_config('dwh_import', yaml_path)

        dwh_import = DwhImportReadReplica(config_object.config)

        process_curated_data = ProcessCuratedData(
            dag=get_dag(),
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            config=config_object.config.get('curated_data'),
            entities=self.entities,
            create_daily_tasks=False,
            policy_tags=policy_tags,
            backfill=False,
            dwh_import=dwh_import,
        )
        tasks = process_curated_data.render()
        self.assertIn("entities", tasks)
        self.assertIn("countries", tasks)

        table_entities = tasks["entities"]
        table_countries = tasks["countries"]

        self.assertIsNotNone(table_entities)
        self.assertIsNotNone(table_countries)

        self.assertEqual('create-table-test_dataset-entities', table_entities.head.task_id)
        self.assertEqual('create-table-test_dataset-countries', table_countries.head.task_id)

        self.assertEqual('end-sc-test_dataset-entities', table_entities.tail.task_id)
        self.assertEqual('create-table-test_dataset-countries', table_countries.tail.task_id)

        # entities
        self.assertEqual({
            'create-table-test_dataset-entities'
        }, table_entities.tail.upstream_task_ids)
        self.assertEqual({
            'create-table-test_dataset-countries',
        }, table_entities.tail.downstream_task_ids)

        self.assertEqual({
            'start-test_dataset',
        }, table_entities.head.upstream_task_ids)
        self.assertEqual({
            'end-sc-test_dataset-entities',
            'metadata-entities',
            'sc-sanity_check_entities_non_blocking',
            'policy-tags-entities'
        }, table_entities.head.downstream_task_ids)

        # countries
        self.assertEqual({
            'check-mtdt-hurrier_cities',
            'check-mtdt-iam_countries',
            'end-sc-test_dataset-entities',
            'start-test_dataset'
        }, table_countries.head.upstream_task_ids)
        self.assertEqual({
            'metadata-countries',
            'policy-tags-countries'
        }, table_countries.head.downstream_task_ids)

    def test_require_partiton_filter(self):
        config_object = Config()
        policy_tags = []
        yaml_path = f"{os.path.dirname(__file__)}/fixtures/process_curated_data"
        config_object.add_config('curated_data_daily', yaml_path)
        config_object.add_config('dwh_import', yaml_path)

        dwh_import = DwhImportReadReplica(config_object.config)

        entities = {}
        process_curated_data = ProcessCuratedData(
            dag=get_dag(type='_daily'),
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            config=config_object.config.get('curated_data'),
            entities=entities,
            create_daily_tasks=True,
            policy_tags=policy_tags,
            backfill=False,
            dwh_import=dwh_import,
        )
        tasks = process_curated_data.render()
        self.assertIn("streaming_shared", tasks)
        self.assertIn("streaming_non_shared", tasks)

        streaming_shared = tasks["streaming_shared"]
        streaming_non_shared = tasks["streaming_non_shared"]

        self.assertIsNotNone(streaming_shared)
        self.assertIsNotNone(streaming_non_shared)

        self.assertEqual('start-daily-tasks-streaming_shared', streaming_shared.head.task_id)
        self.assertEqual('start-daily-tasks-streaming_non_shared', streaming_non_shared.head.task_id)

        self.assertEqual('end-daily-tasks-streaming_shared', streaming_shared.tail.task_id)
        self.assertEqual('end-daily-tasks-streaming_non_shared', streaming_non_shared.tail.task_id)

        # streaming_shared
        self.assertEqual({
            'auth-view-acl-streaming_shared',
            'auth-view-curated_data_filtered-streaming_shared',
            'auth-view-curated_data_shared-streaming_shared',
            'set-partition-filter-streaming_shared',
        }, streaming_shared.tail.upstream_task_ids)

        # streaming_non_shared
        self.assertEqual({
            'set-partition-filter-streaming_non_shared',
        }, streaming_non_shared.tail.upstream_task_ids)
