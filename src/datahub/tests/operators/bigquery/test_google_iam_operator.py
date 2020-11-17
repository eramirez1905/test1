import unittest
from datetime import datetime

from airflow import DAG, AirflowException

from datahub.access_control import iam_config
from datahub.operators.bigquery.google_iam_opertor import GoogleIamOperator


def _get_dag():
    dag = DAG(
        dag_id=f'test_dag',
        start_date=datetime(2019, 2, 25),
        schedule_interval=None)
    return dag


class GoogleIamOperatorTest(unittest.TestCase):
    project_id = "fulfillment-dwh-staging"

    def test_build_iam_bindings(self):
        role_members = iam_config.get('google_cloud_platform').get(self.project_id).get("role_members")
        update_iam_permissions = GoogleIamOperator(
            dag_id=_get_dag(),
            task_id='update_google_iam_permissions',
            role_members=role_members,
            project_id=self.project_id,
        )

        expected_iam_bindings = [
            {
                'members': [
                    'serviceAccount:databricks@fulfillment-dwh-staging.iam.gserviceaccount.com'
                ],
                'role': 'roles/bigquery.admin'
            },
            {
                'members': [
                    'serviceAccount:databricks@fulfillment-dwh-staging.iam.gserviceaccount.com',
                    'group:tech.logisticsdata@deliveryhero.com',
                    'user:riccardo.bini@deliveryhero.com'
                ],
                'role': 'roles/bigquery.dataEditor'
            },
            {
                'members': [
                    'group:tech.logisticsdata@deliveryhero.com'
                ],
                'role': 'roles/bigquery.user'
            },
            {
                'members': [
                    'group:tech.logisticsdata@deliveryhero.com'
                ],
                'role': 'roles/storage.admin'
            }
        ]

        iam_bindings = update_iam_permissions._build_role_members()
        self.assertEqual(expected_iam_bindings, iam_bindings)

    def test_available_types(self):
        invalid_user = "foo@example.com"
        invalid_type = 'foo'
        role_members = [
            {
                "name": invalid_user,
                "type": invalid_type,
                "roles": []
            }
        ]
        update_iam_permissions = GoogleIamOperator(
            dag_id=_get_dag(),
            task_id='update_google_iam_permissions',
            role_members=role_members,
            project_id=self.project_id,
        )

        with self.assertRaises(AirflowException) as e:
            update_iam_permissions._build_role_members()
        self.assertEqual(f'Invalid type: {invalid_type} for user: {invalid_user}', str(e.exception))
