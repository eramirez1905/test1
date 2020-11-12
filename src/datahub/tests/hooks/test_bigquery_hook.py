import unittest
from unittest.mock import MagicMock

from datahub.hooks.bigquery_hook import BigQueryHook


class GoogleBigQueryHookTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'awesome-project'
        self.dataset_id = 'test_dataset'

        self.dataset = {
            'kind': 'bigquery#dataset',
            'etag': 'gxx5kCIIywKz3a0sykC++g==',
            'id': f'{self.project_id}:{self.dataset_id}',
            'selfLink': f'https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/datasets/{self.dataset_id}',
            'datasetReference': {
                'datasetId': self.dataset_id,
                'projectId': self.project_id
            },
            'access': [
                {
                    'role': 'WRITER',
                    'specialGroup': 'projectWriters'
                },
                {
                    'role': 'OWNER',
                    'specialGroup': 'projectOwners'
                },
                {
                    'role': 'OWNER',
                    'userByEmail': 'god@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'specialGroup': 'projectReaders'
                },
                {
                    'role': 'READER',
                    'userByEmail': 'service@project.iam.gserviceaccount.com'
                },
                {
                    'role': 'READER',
                    'userByEmail': 'john.doe@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'groupByEmail': 'bi-team@deliveryhero.com'
                },
            ],
            'creationTime': '1593700633136',
            'lastModifiedTime': '1597413150145',
            'location': 'US'
        }

        schema = {
            "fields": [
                {
                    "name": "rider_name",
                    "type": "STRING",
                    "mode": "REQUIRED",
                },
                {
                    "name": "email",
                    "type": "STRING",
                    "mode": "REQUIRED",
                },
            ]
        }
        self.hook = BigQueryHook()
        self.hook.get_schema = MagicMock(return_value=schema)

        self.patch_dataset = MagicMock()
        self.patch_table = MagicMock()

        get_dataset_mock = MagicMock()
        get_dataset_mock.get_dataset = MagicMock(return_value=self.dataset)
        get_dataset_mock.patch_dataset = self.patch_dataset
        get_cursor = MagicMock(return_value=get_dataset_mock)

        get_dataset_mock.patch_table = self.patch_table
        self.hook.get_cursor = get_cursor

    def test_update_dataset_permissions_update_role_for_existing_user(self):
        users = [
            {
                'role': 'OWNER',
                'userByEmail': 'god@deliveryhero.com'
            },
            {
                'role': 'READER',
                'userByEmail': 'service@project.iam.gserviceaccount.com'
            },
            {
                'role': 'WRITER',
                'userByEmail': 'john.doe@deliveryhero.com'
            },
            {
                'role': 'READER',
                'groupByEmail': 'bi-team@deliveryhero.com'
            },
        ]
        expected = {
            'etag': 'gxx5kCIIywKz3a0sykC++g==',
            'access': [
                {
                    'role': 'WRITER',
                    'specialGroup': 'projectWriters'
                },
                {
                    'role': 'OWNER',
                    'specialGroup': 'projectOwners'
                },
                {
                    'role': 'READER',
                    'specialGroup': 'projectReaders'
                },
                {
                    'role': 'OWNER',
                    'userByEmail': 'god@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'userByEmail': 'service@project.iam.gserviceaccount.com'
                },
                {
                    'role': 'WRITER',
                    'userByEmail': 'john.doe@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'groupByEmail': 'bi-team@deliveryhero.com'
                },
            ],
        }

        self.hook.update_google_iam_dataset_permissions(self.dataset_id, self.project_id, users)

        self.patch_dataset.assert_called_once_with(
            self.dataset_id,
            expected,
            self.project_id
        )

    def test_update_dataset_permissions_add_user(self):
        users = [
            {
                'role': 'OWNER',
                'userByEmail': 'god@deliveryhero.com'
            },
            {
                'role': 'READER',
                'userByEmail': 'service@project.iam.gserviceaccount.com'
            },
            {
                'role': 'READER',
                'userByEmail': 'john.doe@deliveryhero.com'
            },
            {
                'role': 'READER',
                'groupByEmail': 'bi-team@deliveryhero.com'
            },
            {
                'role': 'OWNER',
                'groupByEmail': 'tech-team@deliveryhero.com'
            },
        ]
        expected = {
            'etag': 'gxx5kCIIywKz3a0sykC++g==',
            'access': [
                {
                    'role': 'WRITER',
                    'specialGroup': 'projectWriters'
                },
                {
                    'role': 'OWNER',
                    'specialGroup': 'projectOwners'
                },
                {
                    'role': 'READER',
                    'specialGroup': 'projectReaders'
                },
                {
                    'role': 'OWNER',
                    'userByEmail': 'god@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'userByEmail': 'service@project.iam.gserviceaccount.com'
                },
                {
                    'role': 'READER',
                    'userByEmail': 'john.doe@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'groupByEmail': 'bi-team@deliveryhero.com'
                },
                {
                    'role': 'OWNER',
                    'groupByEmail': 'tech-team@deliveryhero.com'
                },
            ],
        }

        self.hook.update_google_iam_dataset_permissions(self.dataset_id, self.project_id, users)

        self.patch_dataset.assert_called_once_with(
            self.dataset_id,
            expected,
            self.project_id
        )

    def test_update_dataset_permissions_remove_user(self):
        users = [
            {
                'role': 'OWNER',
                'userByEmail': 'god@deliveryhero.com'
            },
            {
                'role': 'READER',
                'userByEmail': 'service@project.iam.gserviceaccount.com'
            },
            {
                'role': 'READER',
                'groupByEmail': 'bi-team@deliveryhero.com'
            }
        ]
        expected = {
            'etag': 'gxx5kCIIywKz3a0sykC++g==',
            'access': [
                {
                    'role': 'WRITER',
                    'specialGroup': 'projectWriters'
                },
                {
                    'role': 'OWNER',
                    'specialGroup': 'projectOwners'
                },
                {
                    'role': 'READER',
                    'specialGroup': 'projectReaders'
                },
                {
                    'role': 'OWNER',
                    'userByEmail': 'god@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'userByEmail': 'service@project.iam.gserviceaccount.com'
                },
                {
                    'role': 'READER',
                    'groupByEmail': 'bi-team@deliveryhero.com'
                }
            ],
        }

        self.hook.update_google_iam_dataset_permissions(self.dataset_id, self.project_id, users)

        self.patch_dataset.assert_called_once_with(
            self.dataset_id,
            expected,
            self.project_id
        )

    def test_update_dataset_curated_data_permissions_add_user(self):
        users = [
            {
                'role': 'OWNER',
                'groupByEmail': 'tech-team@deliveryhero.com'
            },
        ]
        expected = {
            'etag': 'gxx5kCIIywKz3a0sykC++g==',
            'access': [
                {
                    'role': 'WRITER',
                    'specialGroup': 'projectWriters'
                },
                {
                    'role': 'OWNER',
                    'specialGroup': 'projectOwners'
                },
                {
                    'role': 'READER',
                    'specialGroup': 'projectReaders'
                },
                {
                    'role': 'OWNER',
                    'groupByEmail': 'tech-team@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'userByEmail': 'service@project.iam.gserviceaccount.com'
                },
                {
                    'role': 'READER',
                    'userByEmail': 'john.doe@deliveryhero.com'
                },
                {
                    'role': 'READER',
                    'groupByEmail': 'bi-team@deliveryhero.com'
                },
            ],
        }

        self.hook.update_google_iam_dataset_permissions(self.dataset_id, self.project_id, users, blacklist_roles=['READER'])

        self.patch_dataset.assert_called_once_with(
            self.dataset_id,
            expected,
            self.project_id
        )

    def test_update_policy_tags(self):
        policy_tags = {
            'rider_name_tag': 'projects/fulfillment-dwh-staging/locations/us/taxonomies/2424367705306853562/policyTags/4276815434084361614',
            'email_tag': 'projects/fulfillment-dwh-staging/locations/us/taxonomies/2424367705306853562/policyTags/5058014474188488723'
        }
        columns = {
            'rider_name': 'rider_name_tag',
            'email': 'email_tag'
        }
        table_id = "table_name"
        self.hook.update_policy_tags(self.dataset_id, table_id, self.project_id, policy_tags, columns)

        expected_schema = [
            {
                "name": "rider_name",
                "type": "STRING",
                "mode": "REQUIRED",
                "policyTags": {
                    "names": [
                        "projects/fulfillment-dwh-staging/locations/us/taxonomies/2424367705306853562/policyTags/4276815434084361614"
                    ]
                }
            },
            {
                "name": "email",
                "type": "STRING",
                "mode": "REQUIRED",
                "policyTags": {
                    "names": [
                        "projects/fulfillment-dwh-staging/locations/us/taxonomies/2424367705306853562/policyTags/5058014474188488723"
                    ]
                }
            },
        ]
        self.patch_table.assert_called_once_with(
            self.dataset_id,
            table_id,
            self.project_id,
            schema=expected_schema,
        )
