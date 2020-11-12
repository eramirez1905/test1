import unittest
from datetime import date, datetime

import pandas as pd

from operators.utils.validation import normalize_schema, validate_schema, coerce_to_datetime, coerce_to_date, \
    validate_data, summarize_validation


class ValidationTest(unittest.TestCase):
    @staticmethod
    def __load_test_data_json(name):
        if name == 'schema':
            j = {
                'orders': {
                    'type': 'number',
                    'max': 1000,
                },
                'date': {
                    'type': 'date',
                    'required': True,
                    'min': '2018-01-01',
                }
            }
        elif name == 'validation':
            j = {
                'primary_key': ['id'],
                'schema': {
                    'orders': {
                        'type': 'number',
                        'max': 1000,
                    },
                    'datetime': {
                        'type': 'datetime',
                        'min': '2018-02-01 00:00:00',
                    }
                }
            }
        else:
            raise ValueError(f'Not a valid name: {name}')

        return j

    def test_normalize_schema(self):
        schema = self.__load_test_data_json('schema')
        schema_normalized = normalize_schema(schema)

        # date rules like 'min' should be parsed from str to date
        self.assertEqual(schema_normalized['date']['min'], date(2018, 1, 1))

        # date coerce function should be appended
        to_date_fun = schema_normalized['date']['coerce']
        self.assertEqual(to_date_fun('2020-06-01'), date(2020, 6, 1))

    def test_validate_schema_none_valid(self):
        data = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'orders': [10, 20, 30, 40]
        })
        schema = self.__load_test_data_json('schema')
        validation_summary = validate_schema(data, schema)
        self.assertFalse(validation_summary['is_valid'])
        self.assertEqual(validation_summary['rows']['is_valid'], 0)
        self.assertEqual(validation_summary['rows']['is_not_valid'], 4)
        self.assertEqual(validation_summary['summary'], ["date: ['required field'] (4 times)"])

    def test_validate_schema_all_valid(self):
        data = pd.DataFrame({
            'id': [1, 2],
            'orders': [10, 20],
            'date': ['2018-01-01', '2019-01-01']
        })
        schema = self.__load_test_data_json('schema')
        validation_summary = validate_schema(data, schema)
        self.assertTrue(validation_summary['is_valid'])
        self.assertEqual(validation_summary['rows']['is_valid'], 2)
        self.assertEqual(validation_summary['rows']['is_not_valid'], 0)
        self.assertEqual(validation_summary['summary'], [])

    def test_validate_schema_some_valid(self):
        data = pd.DataFrame({
            'id': [1, 2],
            'orders': [10, 20],
            'date': ['2018-02-01 00:30:00', '2019-03-01']
        })
        schema = self.__load_test_data_json('schema')
        validation_summary = validate_schema(data, schema)
        self.assertFalse(validation_summary['is_valid'])
        self.assertEqual(validation_summary['rows']['is_valid'], 1)
        self.assertEqual(validation_summary['rows']['is_not_valid'], 1)

    def test_coerce_to_datetime(self):
        self.assertEqual(coerce_to_datetime("2018-05-01 15:00:00"), datetime(2018, 5, 1, 15))
        with self.assertRaises(ValueError):
            coerce_to_datetime("2018-05-10 15:xx:00")

    def test_coerce_to_date(self):
        self.assertEqual(coerce_to_date("2018-05-02"), date(2018, 5, 2))
        with self.assertRaises(ValueError):
            coerce_to_date("2018-05-02 00:00:00")

    def test_validate_data(self):
        schema = self.__load_test_data_json('validation')
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'orders': [10, 20, 30],
            'datetime': ['2018-02-01 00:00:00', '2018-02-01 00:30:00', '2018-02-01 01:00:00']
        })
        validation_summary = validate_data(data, schema)
        self.assertTrue(validation_summary['is_valid'])
        self.assertTrue(validation_summary['primary_key'])
        self.assertTrue(validation_summary['schema']['is_valid'])

    def test_validate_data_id_duplicated(self):
        schema = self.__load_test_data_json('validation')
        data = pd.DataFrame({
            'id': [1, 2, 2],
            'orders': [10, 20, 30],
            'datetime': ['2018-02-01 00:00:00', '2018-02-01 00:30:00', '2018-02-01 01:00:00']
        })
        validation_summary = validate_data(data, schema)
        self.assertFalse(validation_summary['is_valid'])
        self.assertFalse(validation_summary['primary_key'])
        self.assertTrue(validation_summary['schema']['is_valid'])

    def test_validate_data_max_exceeded(self):
        schema = self.__load_test_data_json('validation')
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'orders': [10000, 20, 30],
            'datetime': ['2018-02-01 00:00:00', '2018-02-01 00:30:00', '2018-02-01 01:00:00']
        })
        validation_summary = validate_data(data, schema)
        self.assertFalse(validation_summary['is_valid'])
        self.assertTrue(validation_summary['primary_key'])
        self.assertFalse(validation_summary['schema']['is_valid'])

    def test_validate_data_datetime_missing(self):
        schema = self.__load_test_data_json('validation')
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'orders': [10000, 20, 30],
            'datetime': ['2018-02-01 00:00:00', None, '2018-02-01 01:00:00']
        })
        validation_summary = validate_data(data, schema)
        self.assertFalse(validation_summary['is_valid'])
        self.assertTrue(validation_summary['primary_key'])
        self.assertFalse(validation_summary['schema']['is_valid'])
        self.assertEqual(validation_summary['schema']['rows']['is_valid'], 1)
        self.assertEqual(validation_summary['schema']['rows']['is_not_valid'], 2)

    def test_validate_data_column_missing(self):
        schema = self.__load_test_data_json('validation')
        data = pd.DataFrame({
            'orders': [10, 20, 30],
            'datetime': ['2018-02-01 00:00:00', '2018-02-01 00:30:00', '2018-02-01 01:00:00']
        })
        validation_summary = validate_data(data, schema)
        self.assertFalse(validation_summary['is_valid'])
        self.assertFalse(validation_summary['primary_key'])
        self.assertTrue(validation_summary['schema']['is_valid'])
        self.assertEqual(validation_summary['schema']['rows']['is_valid'], 3)
        self.assertEqual(validation_summary['schema']['rows']['is_not_valid'], 0)

    def test_summarize_validation_successful(self):
        schema = self.__load_test_data_json('validation')
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'orders': [10, 20, 30],
            'datetime': ['2018-02-01 00:00:00', '2018-02-01 00:30:00', '2018-02-01 01:00:00']
        })
        validation = validate_data(data, schema)
        validation_summary = summarize_validation(validation)
        self.assertEqual(validation_summary,
                         'Primary key validation successful.\nSchema validation successful for 3 row(s).\n')

    def test_summarize_validation_primary_key_not_valid(self):
        schema = self.__load_test_data_json('validation')
        data = pd.DataFrame({
            'id': [1, 2, 2],
            'orders': [10, 20, 30],
            'datetime': ['2018-02-01 00:00:00', '2018-02-01 00:30:00', '2018-02-01 01:00:00']
        })
        validation = validate_data(data, schema)
        validation_summary = summarize_validation(validation)
        self.assertEqual(validation_summary,
                         'Primary key validation failed.\nSchema validation successful for 3 row(s).\n')

    def test_summarize_validation_single_row_invalid(self):
        schema = self.__load_test_data_json('validation')
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'orders': [10, 20, 10000],
            'datetime': ['2018-02-01 00:00:00', '2018-02-01 00:30:00', '2018-02-01 01:00:00']
        })
        validation = validate_data(data, schema)
        validation_summary = summarize_validation(validation)
        self.assertEqual(validation_summary,
                         "Primary key validation successful.\n"
                         + "There were schema validation errors in 1 row(s):\n"
                         + "orders: ['max value is 1000'] (1 times)\n")
