import os
import unittest

from configuration import Config
from configuration.transformations.clarisights import transform_clarisights_config
from datahub.common.configuration import ConfigTransformation


class ClarisightsTransformTest(unittest.TestCase):
    maxDiff = None
    fixture_path = f'{os.path.dirname(os.path.realpath(__file__))}/fixtures'
    _config_instance = None

    def setUp(self):
        self._config_instance = Config()
        self._config_instance.clear_instance()

    def tearDown(self):
        self._config_instance.clear_instance()

    def test_transform(self):
        self._config_instance = Config()
        self._config_instance.add_config('cs_config', self.fixture_path)
        transformation = ConfigTransformation('clarisights', 'dwh_merge_layer_databases', transform_clarisights_config)
        self._config_instance.add_transformation(transformation)

        cs_config = self._config_instance.config

        self._config_instance.clear_instance()

        self._config_instance = Config()
        self._config_instance.add_config('dhub_config', self.fixture_path)
        dhub_config = self._config_instance.config

        del cs_config['clarisights']

        self.assertDictEqual(dhub_config, cs_config)
