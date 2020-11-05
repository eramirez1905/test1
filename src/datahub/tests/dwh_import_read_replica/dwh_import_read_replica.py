import os
import unittest
from datetime import datetime

from datahub.common.configuration import Config
from datahub.dwh_import_read_replica import DwhImportReadReplica


def init_config():
    config_object = Config()
    yaml_path = f"{os.path.dirname(__file__)}/fixtures/dwh_import_read_replica"
    config_object.add_config('config', yaml_path)
    config = config_object.config
    return config


class DwhImportReadReplicaTest(unittest.TestCase):
    def tearDown(self):
        config = Config()
        config.clear_instance()

    def test_next_day(self):
        context = {
            'execution_date': datetime(2019, 7, 7, 23, 0, 0),
            'next_execution_date': datetime(2019, 7, 8, 1, 0, 0)
        }
        dwh_import_read_replica = DwhImportReadReplica(init_config())
        actual = dwh_import_read_replica.is_execution_date_next_day(**context)
        self.assertEqual(actual, True)

    def test_same_day(self):
        context = {
            'execution_date': datetime(2019, 7, 7, 11, 0, 0),
            'next_execution_date': datetime(2019, 7, 7, 15, 0, 0)
        }
        dwh_import_read_replica = DwhImportReadReplica(init_config())
        actual = dwh_import_read_replica.is_execution_date_next_day(**context)
        self.assertEqual(actual, False)

    def test_init(self):
        dwh_import_read_replica = DwhImportReadReplica(init_config())
        self.assertIn("test_enabled", dwh_import_read_replica.databases)
        self.assertIn("test_table_disabled", dwh_import_read_replica.databases)
        self.assertIn("test_database_disabled", dwh_import_read_replica.databases)
        self.assertEqual(3, len(dwh_import_read_replica.databases.keys()))
        self.assertEqual(1, len(dwh_import_read_replica.databases["test_enabled"].tables))
