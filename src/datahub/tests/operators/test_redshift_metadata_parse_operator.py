import json
import os
import unittest
from datetime import datetime

from airflow import DAG

from datahub.operators.redshift_metadata_parse_operator import RedshiftMetadataParseOperator


def _get_mocked_dag():
    return DAG(
        dag_id=f'curated_data',
        description=f'Create Curated Data on BigQuery',
        start_date=datetime(2019, 2, 25),
        schedule_interval=None,
        max_active_runs=1,
        catchup=False)


class RedshiftMetadataParseOperatorTest(unittest.TestCase):
    def test_entities(self):
        expected_schema_filename = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures/schema_expected.json'
        with open(expected_schema_filename) as file:
            expected_schema = json.load(file)

        fixture_filename = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures/metadata.json'
        with open(fixture_filename) as file:
            metadata_json = file.read()
            actual = RedshiftMetadataParseOperator.parse_schema(metadata_json)

            self.assertEqual(actual, expected_schema)
