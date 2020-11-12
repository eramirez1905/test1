import unittest

from airflow.models import DagBag


class DagBagTest(unittest.TestCase):
    def test_validate_dags(self):
        dag_bag = DagBag()
        self.assertEqual(len(dag_bag.import_errors), 0)
