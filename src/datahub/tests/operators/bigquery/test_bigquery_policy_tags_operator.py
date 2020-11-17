import unittest
from datetime import datetime
from unittest.mock import MagicMock

from airflow import DAG

from datahub.operators.bigquery.bigquery_policy_tags_operator import BigQueryPolicyTagsOperator


def get_dag():
    return DAG(
        dag_id=f'fake_dag_id',
        start_date=datetime(2019, 2, 25),
        schedule_interval=None,
        max_active_runs=1,
        catchup=False
    )


class TestBigQueryPolicyTagsOperator(unittest.TestCase):
    def setUp(self):
        self.project_id = 'awesome-project'
        self.dataset_id = 'test_dataset'
        self.table_id = 'test_table'

    def test_operator(self):
        policy_tags = [
            {
                "name": "projects/fulfillment-dwh-staging/locations/us/taxonomies/2424367705306853562/policyTags/4276815434084361614",
                "display_name": "rider_name_tag",
            },
            {
                "name": "projects/fulfillment-dwh-staging/locations/us/taxonomies/2424367705306853562/policyTags/5058014474188488723",
                "display_name": "email_tag",
            }
        ]
        columns = [
            {
                "tag": "rider_name_tag",
                "column": "rider_name"
            },
            {
                "tag": "email_tag",
                "column": "email"
            }
        ]
        operator = BigQueryPolicyTagsOperator(
            dag=get_dag(),
            task_id="task_id_fake",
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            policy_tags=policy_tags,
            columns=columns,
        )

        operator._hook = MagicMock()
        operator._hook.update_policy_tags = MagicMock()

        context = {}
        operator.execute(context)

        expected_policy_tags = {
            "rider_name_tag": "projects/fulfillment-dwh-staging/locations/us/taxonomies/2424367705306853562/policyTags/4276815434084361614",
            "email_tag": "projects/fulfillment-dwh-staging/locations/us/taxonomies/2424367705306853562/policyTags/5058014474188488723",
        }
        expected_columns = {
            'rider_name': 'rider_name_tag',
            'email': 'email_tag'
        }
        operator._hook.update_policy_tags.assert_called_once_with(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            project_id=self.project_id,
            policy_tags=expected_policy_tags,
            columns=expected_columns,
        )
