import unittest

from datahub.curated_data.entities_config import EntitiesConfig


class EntitiesConfigTest(unittest.TestCase):
    def test_singleton_object(self):
        conf1 = EntitiesConfig()
        conf2 = EntitiesConfig()
        self.assertEqual(conf1, conf2)
        conf1.clear_instance()

    def test_singleton_object_entities(self):
        conf1 = EntitiesConfig()
        conf2 = EntitiesConfig()
        self.assertEqual(conf1.entities, conf2.entities)
        conf1.clear_instance()
