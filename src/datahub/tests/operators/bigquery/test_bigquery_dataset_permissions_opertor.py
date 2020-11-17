import unittest
from datetime import datetime
from unittest.mock import MagicMock

from airflow import DAG, AirflowException

from datahub.operators.bigquery.bigquery_dataset_permissions_operator import BigQueryDatasetPermissionsOperator


def get_dag():
    return DAG(
        dag_id=f'fake_dag_id',
        start_date=datetime(2019, 2, 25),
        schedule_interval=None,
        max_active_runs=1,
        catchup=False)


class CuratedDataBigQueryOperatorTest(unittest.TestCase):
    def setUp(self):
        self.project_id = 'awesome-project'

    def test_operator(self):
        dataset_id = 'test_dataset'
        access_entries = {
            "READER": {
                "userByEmail": [
                    "faisal.farouk@deliveryhero.com",
                    "vishnu.viswanathan@deliveryhero.com"
                ],
                "groupByEmail": [
                    "bi_team@deliveryhero.com"
                ]
            },
            "WRITER": {
                "userByEmail": [
                    "riccardo.bini@deliveryhero.com"
                ]
            }
        }

        operator = BigQueryDatasetPermissionsOperator(
            dag=get_dag(),
            task_id="task_id_fake",
            project_id=self.project_id,
            dataset_id=dataset_id,
            access_entries=access_entries,
        )
        operator._hook = MagicMock()
        operator._hook.update_google_iam_dataset_permissions = MagicMock()

        operator.execute(context={})

        operator._hook.update_google_iam_dataset_permissions.assert_called_once_with(
            dataset_id=dataset_id,
            project_id=self.project_id,
            blacklist_roles=[],
            access_entries=[
                {
                    'role': 'READER',
                    'userByEmail': 'faisal.farouk@deliveryhero.com',
                },
                {
                    'role': 'READER',
                    'userByEmail': 'vishnu.viswanathan@deliveryhero.com',
                },
                {
                    'role': 'READER',
                    'groupByEmail': 'bi_team@deliveryhero.com',
                },
                {
                    'role': 'WRITER',
                    'userByEmail': 'riccardo.bini@deliveryhero.com',
                },
            ],
        )

    def test_curated_data_dataset_with_reader_role(self):
        dataset_id = 'curated_data_shared_datahub'

        access_entries = {
            "READER": {
                "groupByEmail": [
                    "bi_team@deliveryhero.com"
                ]
            },
        }

        operator = BigQueryDatasetPermissionsOperator(
            dag=get_dag(),
            task_id="task_id_fake",
            project_id=self.project_id,
            dataset_id=dataset_id,
            access_entries=access_entries,
        )
        operator._hook = MagicMock()
        operator._hook.update_google_iam_dataset_permissions = MagicMock()

        with self.assertRaisesRegex(AirflowException, f'Roles READER are not allowed for the dataset {dataset_id}'):
            operator.execute(context={})
