from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from datahub.access_control import iam_config
from datahub.common import alerts
from datahub.operators.bigquery.bigquery_create_empty_dataset_operator import BigQueryCreateEmptyDatasetOperator
from datahub.operators.bigquery.bigquery_dataset_permissions_operator import BigQueryDatasetPermissionsOperator
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator
from datahub.operators.bigquery.bigquery_permissions_opertor import BigQueryPermissionsOperator
from datahub.operators.bigquery.google_iam_opertor import GoogleIamOperator


class DwhAccessControlBigQuery:
    def __init__(self, config: dict):
        self.config = config
        self.acl_dataset = self.config.get('bigquery').get('dataset').get('acl')
        self.cl_dataset = self.config.get('bigquery').get('dataset').get('cl')
        self.curated_data_shared = self.config.get('bigquery').get('dataset').get('curated_data_shared')

    def render_acl_tasks(self, dag: DAG):

        ventures = []
        for venture in self.config.get('acl'):
            users = []
            for user in venture.get('users', []):
                users.append({
                    'email': user.get('email'),
                    'countries': user.get('countries', venture.get('countries')),
                    'entities': user.get('entities', venture.get('entities', []))
                })
            ventures.append({
                'name': venture.get('name'),
                'domain': venture.get('domain'),
                'countries': venture.get('countries'),
                'entities': venture.get('entities', []),
                'users': users
            })

        project_id = self.config.get('bigquery').get('project_id')
        create_dataset_permissions_task = BigQueryCreateEmptyDatasetOperator(
            dag=dag,
            task_id='create-dataset-acl',
            dataset_id=self.acl_dataset,
            project_id=project_id,
            on_failure_callback=alerts.setup_callback(),
        )

        create_users_task = BigQueryOperator(
            dag=dag,
            task_id='create-table-users',
            sql=f'users.sql',
            use_legacy_sql=False,
            params={
                'project_id': project_id,
                'ventures': ventures,
                'acl_dataset': self.acl_dataset,
                'cl_dataset': self.cl_dataset,
            },
            on_failure_callback=alerts.setup_callback(),
        )

        create_access_control_task = BigQueryOperator(
            dag=dag,
            task_id='create-table-acl',
            sql=f'access_control.sql',
            use_legacy_sql=False,
            params={
                'project_id': project_id,
                'acl_dataset': self.acl_dataset,
            },
            on_failure_callback=alerts.setup_callback(),
        )

        update_bigquery_permissions_task = BigQueryPermissionsOperator(
            dag=dag,
            task_id='update_bigquery_shared_dataset_permissions',
            dataset_id=self.curated_data_shared,
            acl=self.config.get('acl', []),
            on_failure_callback=alerts.setup_callback(),
        )

        create_dataset_permissions_task >> create_users_task
        create_users_task >> create_access_control_task
        create_dataset_permissions_task >> update_bigquery_permissions_task

    @staticmethod
    def render_iam_tasks(dag: DAG):

        google_dataset_iam = iam_config.get('google_dataset_iam')
        for project_id, datasets in google_dataset_iam.items():
            start_iam_dataset = DummyOperator(
                dag=dag,
                task_id=f"start-iam-dataset-permissions-{project_id}"
            )
            for dataset_id, access_entries in datasets.items():
                update_bigquery_dataset_permissions_task = BigQueryDatasetPermissionsOperator(
                    dag=dag,
                    task_id=f"ds_{project_id}_{dataset_id}",
                    project_id=project_id,
                    dataset_id=dataset_id,
                    access_entries=access_entries,
                    on_failure_callback=alerts.setup_callback(),
                )
                start_iam_dataset >> update_bigquery_dataset_permissions_task

        start_iam_project = DummyOperator(
            dag=dag,
            task_id=f"start-iam-project-permissions"
        )
        google_cloud_platform = iam_config.get('google_cloud_platform')
        for project_id in google_cloud_platform.keys():
            update_iam_permissions = GoogleIamOperator(
                dag=dag,
                task_id=f"update_iam_{project_id}",
                role_members=google_cloud_platform[project_id]["role_members"],
                project_id=project_id,
                on_failure_callback=alerts.setup_callback(),
            )
            start_iam_project >> update_iam_permissions
