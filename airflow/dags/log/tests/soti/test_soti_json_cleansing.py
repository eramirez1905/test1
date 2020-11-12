import unittest

from freezegun import freeze_time

from soti.soti_json_cleansing import SotiJsonCleansing


class SotiJsonCleansingTest(unittest.TestCase):

    def setUp(self):
        self.basic_dict = {
            'region': 'eu',
            '_ingested_at': '2019-09-09 05:05:05.000000',
            'created_date': '2019-09-09',
            'merge_layer_run_from': '2019-09-09 05:00:00'
        }

    def test_prepare(self):
        freezer = freeze_time("2019-09-09 05:05:05.000000")
        freezer.start()

        soti_json_cleansing = SotiJsonCleansing('eu', '2019-09-09 05:00:00')
        input_dict = [
            {'$type': 'my_test'},
            {'$type': '1'}
        ]

        expected_result = '{"type": "my_test", "region": "eu", "_ingested_at": "2019-09-09 05:05:05.000000", ' \
                          '"created_at": "2019-09-09 05:05:05.000000", "updated_at": "2019-09-09 05:05:05.000000", ' \
                          '"created_date": "2019-09-09", "merge_layer_run_from": "2019-09-09 05:00:00"}\n' \
                          '{"type": "1", "region": "eu", "_ingested_at": "2019-09-09 05:05:05.000000", ' \
                          '"created_at": "2019-09-09 05:05:05.000000", "updated_at": "2019-09-09 05:05:05.000000", ' \
                          '"created_date": "2019-09-09", "merge_layer_run_from": "2019-09-09 05:00:00"}'

        str_result = soti_json_cleansing.prepare(input_dict)
        self.assertEqual(expected_result, str_result)
        freezer.stop()
