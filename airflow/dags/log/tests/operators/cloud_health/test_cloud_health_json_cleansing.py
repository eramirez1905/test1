import json
import os
import unittest

from airflow import AirflowException
from freezegun import freeze_time

from operators.cloud_health.cloud_health_json_cleansing import CloudHealthJsonCleansing


class CloudHealthJsonCleansingTest(unittest.TestCase):
    maxDiff = None

    def test_report(self):
        self.maxDiff = None
        fixture_path = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures'
        freezer = freeze_time("2020-08-28 19:15:46.212643")
        freezer.start()

        with open(f'{fixture_path}/test_report.json', 'r') as file:
            input_raw = json.loads(file.read())

        report_type = 'cost'
        cloud_health_json_cleanser = CloudHealthJsonCleansing(report_type, '2020-08-20 00:00:00')

        actual_output = cloud_health_json_cleanser.prepare(input_raw)

        with open(f'{fixture_path}/test_result.json', 'r') as file:
            expected_result = file.read()

        self.assertEqual(expected_result, actual_output)
        freezer.stop()

    def test_report_with_data_error(self):
        fixture_path = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures'

        with open(f'{fixture_path}/report_with_data_error.json', 'r') as file:
            input_raw = json.loads(file.read())

        report_type = 'cost'
        cloud_health_json_cleanser = CloudHealthJsonCleansing(report_type, '2020-08-20 00:00:00')

        with self.assertRaises(AirflowException) as context:
            cloud_health_json_cleanser.prepare(input_raw)

        error_message = "This filter combination has no members to find at this time interval (i.e. empty set). " \
                        "Please try a different filter set or time interval"
        self.assertEqual(f"CloudHealth API response error message: {error_message}", str(context.exception))
