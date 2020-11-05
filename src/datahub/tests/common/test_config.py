import unittest

from airflow import configuration as conf

from datahub.common.configuration import Config


class ConfigTest(unittest.TestCase):

    def tearDown(self):
        config1 = Config()
        config1.clear_instance()

    def test_singleton_object(self):
        config1 = Config()
        config2 = Config()

        self.assertEqual(config1, config2)

    def test_singleton_object_config(self):
        config1 = Config()
        config1.add_config("config", f"{conf.get('core', 'DAGS_FOLDER')}/configuration/yaml")
        config2 = Config()

        self.assertEqual(config1.config, config2.config)
