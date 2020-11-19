import unittest

from datahub.common.helpers import DatabaseSettings, CuratedDataTable


class HelpersTest(unittest.TestCase):
    def test_database_settings(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'app_version': "1",
            'tables': [
                {
                    'name': 'test',
                }
            ],
        }
        database = DatabaseSettings('test', database_params)
        self.assertEqual(database.name, 'test')
        self.assertEqual(database.extra_params, {
            "app_version": "1"
        })

    def test_database_wrong_source(self):
        database_params = {
            'is_regional': False,
            'source': 'foo',
            'tables': [
                {
                    'name': 'test',
                }
            ],
        }
        with self.assertRaises(Exception) as context:
            DatabaseSettings('test', database_params)

        self.assertTrue(
            "'foo' is not a valid DataSource for database 'test', available DataSources:" in str(context.exception),
            context.exception)

    def test_table_wrong_source(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'tables': [
                {
                    'name': 'test',
                    'source': 'foo',
                }
            ],
        }
        with self.assertRaises(Exception) as context:
            DatabaseSettings('test', database_params)

        self.assertTrue(
            "'foo' is not a valid DataSource for table 'test_test', available DataSources:" in str(context.exception),
            context.exception)

    def test_duplicate_columns_in_curated_layer(self):
        params = {
            'name': 'duplicate_test',
            'columns': [
                'region',
                'country',
                'region'
            ]
        }

        with self.assertRaises(Exception) as context:
            CuratedDataTable(source="", params=params, dataset="")

        self.assertTrue('There are duplicated columns in: duplicate_test' in str(context.exception), context.exception)

    def test_require_partition_filter_without_time_partitioning(self):
        params = {
            'name': 'duplicate_test',
            'require_partition_filter': 'true'
        }

        with self.assertRaises(Exception) as context:
            CuratedDataTable(source="", params=params, dataset="")

        self.assertTrue(
            'Parameter require_partition_filter set for table: duplicate_test without time_partitioning defined' in str(
                context.exception), context.exception)

    def test_sql_template_in_curated_layer(self):
        params = {
            'name': 'template_test',
            'sql_template': 'template.sql'
        }

        curated_data_table = CuratedDataTable(source="", params=params, dataset="")

        self.assertEqual(curated_data_table.sql_filename, 'template.sql')

    def test_no_sql_template_in_curated_layer(self):
        params = {
            'name': 'no_template_test',
        }

        curated_data_table = CuratedDataTable(source="", params=params, dataset="")

        self.assertEqual(curated_data_table.sql_filename, 'no_template_test.sql')

    def test_table_name_with_namespace(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'tables': [
                {
                    'name': 'table',
                    'namespace': 'namespace',
                }
            ],
        }

        dbsetting = DatabaseSettings('db', database_params)
        self.assertEqual(dbsetting.tables[0].table_name, 'db_namespace_table')

    def test_table_name_without_namespace(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'tables': [
                {
                    'name': 'table',
                }
            ],
        }

        dbsetting = DatabaseSettings('db', database_params)
        self.assertEqual(dbsetting.tables[0].table_name, 'db_table')

    def test_database_enable_opsgenie_alert(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'opsgenie': {
                'enabled': True,
                'priority': 'P2',
            },
            'tables': [
                {
                    'name': 'table',
                }
            ],
        }

        dbsetting = DatabaseSettings('db', database_params)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('enabled'), True)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('priority'), 'P2')

    def test_table_enable_opsgenie_alert(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'opsgenie': {
                'enabled': True,
            },
            'tables': [
                {
                    'name': 'table',
                    'opsgenie': {
                        'priority': 'P1',
                    },
                }
            ],
        }

        dbsetting = DatabaseSettings('db', database_params)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('enabled'), True)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('priority'), 'P1')

    def test_table_opsgenie_alert_default_priority(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'opsgenie': {
                'enabled': True,
            },
            'tables': [
                {
                    'name': 'table',
                }
            ],
        }

        dbsetting = DatabaseSettings('db', database_params)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('enabled'), True)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('priority'), 'P3')

    def test_default_enable_opsgenie_alert(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'tables': [
                {
                    'name': 'table',
                    'opsgenie': {
                        'enabled': True,
                        'priority': 'P1',
                    },
                }
            ],
        }

        dbsetting = DatabaseSettings('db', database_params)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('enabled'), True)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('priority'), 'P1')

    def test_table_override_opsgenie_alert(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'opsgenie': {
                'enabled': True,
            },
            'tables': [
                {
                    'name': 'table',
                    'opsgenie': {
                        'enabled': False,
                    },
                }
            ],
        }

        dbsetting = DatabaseSettings('db', database_params)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('enabled'), False)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('priority'), 'P3')

    def test_default_opsgenie_alert(self):
        database_params = {
            'is_regional': False,
            'source': 'RDS',
            'tables': [
                {
                    'name': 'table',
                }
            ],
        }

        dbsetting = DatabaseSettings('db', database_params)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('enabled'), False)
        self.assertEqual(dbsetting.tables[0].opsgenie.get('priority'), 'P3')

    def test_write_disposition(self):
        database_params = {
            'is_regional': False,
            'source': 'RedShift_S3',
            'tables': [
                {
                    'name': 'table',
                    'write_disposition': 'WRITE_APPEND'
                }
            ],
        }
        db_setting = DatabaseSettings('db', database_params)
        self.assertEqual(db_setting.tables[0].write_disposition, 'WRITE_APPEND')

    def test_write_disposition_validation(self):
        database_params = {
            'is_regional': False,
            'source': 'RedShift_S3',
            'tables': [
                {
                    'name': 'table',
                    'write_disposition': 'WRITE'
                }
            ],
        }
        with self.assertRaises(Exception) as context:
            DatabaseSettings('db', database_params)

        self.assertEqual(f'The write disposition WRITE is not valid. Possible values are: WRITE_APPEND WRITE_TRUNCATE', str(context.exception))
