import os
import unittest

from datahub.common.configuration import IamConfig


class IamConfigTest(unittest.TestCase):

    def tearDown(self):
        iam_config = IamConfig()
        iam_config.clear_instance()

    def test_singleton_object(self):
        iam_config1 = IamConfig()
        iam_config2 = IamConfig()

        self.assertEqual(iam_config1, iam_config2)

    def test_singleton_object_iam_config(self):
        iam_config1 = IamConfig()
        iam_config1.add_config('google_iam', f'{format(os.getenv("AIRFLOW_HOME"))}/src/datahub/access_control/google_iam/yaml')
        iam_config2 = IamConfig()

        self.assertEqual(iam_config1.config, iam_config2.config)
