import os
import unittest
from datetime import datetime

import hiyapyco
from airflow import DAG, AirflowException

from datahub.curated_data.entities_config import EntitiesConfig


def _load_fixture(expected_fixture_path):
    return hiyapyco.load(expected_fixture_path,
                         interpolate=True,
                         failonmissingfiles=True,
                         method=hiyapyco.METHOD_MERGE)


def _get_mocked_dag():
    return DAG(
        dag_id=f'curated_data',
        description=f'Create Curated Data on BigQuery',
        start_date=datetime(2019, 2, 25),
        schedule_interval=None,
        max_active_runs=1,
        catchup=False)


class CuratedDataBigQueryOperatorTest(unittest.TestCase):
    _entity_instance = None

    def setUp(self) -> None:
        if self._entity_instance:
            self._entity_instance.clear_instance()

    def tearDown(self):
        self._entity_instance.clear_instance()

    def test_entities(self):
        yaml_file = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures/entities.yaml'
        self._entity_instance = EntitiesConfig(yaml_file)
        self.assertEqual(self._entity_instance.yaml_file, yaml_file)

        entities = self._entity_instance.entities
        expected_fixture_path = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures/expected_entities.yaml'
        expected_entities = _load_fixture(expected_fixture_path)

        self.assertEqual(expected_entities.get('entities'), entities)

    def test_missing_hurrier_platform_in_entities(self):
        yaml_file = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures/missing_hurrier_platform_entities.yaml'
        self._entity_instance = EntitiesConfig(yaml_file)
        self.assertEqual(self._entity_instance.yaml_file, yaml_file)

        entities = self._entity_instance.entities
        expected_fixture_path = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures/expected_missing_hurrier_platform_entities.yaml'
        expected_entities = _load_fixture(expected_fixture_path)

        self.assertEqual(expected_entities.get('entities'), entities)

    def test_duplicate_hurrier_platforms(self):
        yaml_file = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures/entities_double_hurrier_platforms.yaml'
        print(yaml_file)
        self._entity_instance = EntitiesConfig(yaml_file)
        self.assertEqual(self._entity_instance.yaml_file, yaml_file)

        with self.assertRaises(AirflowException) as e:
            self._entity_instance.entities
        self.assertEqual('Found duplicated hurrier_platforms: {}'.format([('de', 'pizza_de')]), str(e.exception))

    def test_duplicate_rps_platforms(self):
        yaml_file = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures/entities_double_rps_platforms.yaml'
        self._entity_instance = EntitiesConfig(yaml_file)
        self.assertEqual(self._entity_instance.yaml_file, yaml_file)

        with self.assertRaises(AirflowException) as e:
            self._entity_instance.entities
        self.assertEqual('Found duplicated rps_platforms: {}'.format([('DE', 'pizza_de')]), str(e.exception))
