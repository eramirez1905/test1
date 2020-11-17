import unittest
from unittest.mock import MagicMock

from airflow import AirflowException

from datahub.hooks.google_iam_hook import GoogleIamHook


class GoogleIamHookTest(unittest.TestCase):

    def setUp(self):
        self.google_iam_policy = {
            "version": 1,
            "etag": "BwWg9Onj8jE=",
            "bindings": [
                {
                    "role": "roles/bigquery.admin",
                    "members": [
                        "user:alessandro.la@deliveryhero.com",
                        "user:faisal.farouk@deliveryhero.com",
                    ]
                },
                {
                    "role": "roles/bigquery.dataEditor",
                    "members": [
                        "user:riccardo.bini@deliveryhero.com",
                        "serviceAccount:780393426212@cloudservices.gserviceaccount.com",
                        "serviceAccount:service-123456789123@containerregistry.iam.gserviceaccount.com",
                        "serviceAccount:service-780393426212@dataproc-accounts.iam.gserviceaccount.com",
                    ]
                },
                {
                    "role": "roles/bigquery.dataOwner",
                    "members": [
                        "serviceAccount:12345678912-compute@developer.gserviceaccount.com",
                    ]
                },
                {
                    "role": "roles/storage.objectViewer",
                    "members": [
                        "serviceAccount:databricks@fulfillment-dwh-staging.iam.gserviceaccount.com"
                    ]
                },
                {
                    "role": "roles/editor",
                    "members": [
                        "serviceAccount:fulfillment-dwh-staging@appspot.gserviceaccount.com"
                    ]
                },
            ]
        }
        self.hook = GoogleIamHook()
        self.hook.get_conn = MagicMock(return_value=None)
        self.hook.get_policy = MagicMock(return_value=self.google_iam_policy)
        self.hook.set_policy = MagicMock(side_effect=self.set_policy_response)

    @staticmethod
    def set_policy_response(project_id, policy):
        bindings = policy.get("bindings").copy()
        return {
            "version": 1,
            "etag": "BwWs3l2K8VM=",
            "bindings": bindings
        }

    def test_modify_policy_add_member(self):
        role_members = [
            {
                "role": "roles/bigquery.admin",
                "members": [
                    "user:matias.pacelli@deliveryhero.com",
                    "user:riccardo.bini@deliveryhero.com",
                    "user:alexander.kagoshima@deliveryhero.com",
                    "user:alessandro.la@deliveryhero.com",
                ],
            },
            {
                "role": "roles/bigquery.dataEditor",
                "members": [
                    "user:riccardo.bini@deliveryhero.com",
                ]
            },
        ]

        expected = {
            "version": 1,
            "etag": "BwWg9Onj8jE=",
            "bindings": [
                {
                    "role": "roles/bigquery.admin",
                    "members": [
                        "user:alessandro.la@deliveryhero.com",
                        "user:alexander.kagoshima@deliveryhero.com",
                        "user:matias.pacelli@deliveryhero.com",
                        "user:riccardo.bini@deliveryhero.com",
                    ]
                },
                {
                    "role": "roles/bigquery.dataEditor",
                    "members": [
                        "serviceAccount:780393426212@cloudservices.gserviceaccount.com",
                        "serviceAccount:service-123456789123@containerregistry.iam.gserviceaccount.com",
                        "serviceAccount:service-780393426212@dataproc-accounts.iam.gserviceaccount.com",
                        "user:riccardo.bini@deliveryhero.com",
                    ]
                },
                {
                    "role": "roles/bigquery.dataOwner",
                    "members": [
                        "serviceAccount:12345678912-compute@developer.gserviceaccount.com"
                    ]
                },
                {
                    "role": "roles/editor",
                    "members": [
                        "serviceAccount:fulfillment-dwh-staging@appspot.gserviceaccount.com"
                    ]
                },
            ]
        }

        self.hook.update_google_iam_project_permissions(project_id="staging", role_members=role_members)
        self.hook.set_policy.assert_called_once_with(
            project_id='staging',
            policy=expected
        )

    def test_modify_policy_add_member_of_role_that_does_not_exist(self):
        role_members = [
            {
                "role": "roles/bigquery.admin",
                "members": [
                    "user:alessandro.la@deliveryhero.com",
                    "user:faisal.farouk@deliveryhero.com",
                ]
            },
            {
                "role": "roles/bigquery.dataEditor",
                "members": [
                    "user:riccardo.bini@deliveryhero.com",
                    "serviceAccount:service-780393426212@dataproc-accounts.iam.gserviceaccount.com",
                ]
            },
            {
                "role": "roles/bigquery.owner",
                "members": [
                    "user:riccardo.bini@deliveryhero.com",
                    "user:alexander.kagoshima@deliveryhero.com",
                    "user:alessandro.la@deliveryhero.com"
                ],
            },
            {
                "role": "roles/editor",
                "members": [
                    "serviceAccount:fulfillment-dwh-staging@appspot.gserviceaccount.com"
                ]
            },
        ]

        self.hook.update_google_iam_project_permissions(project_id="staging", role_members=role_members)
        self.hook.set_policy.assert_called_once_with(
            project_id='staging',
            policy={
                "version": 1,
                "etag": "BwWg9Onj8jE=",
                "bindings": [
                    {
                        "role": "roles/bigquery.admin",
                        "members": [
                            "user:alessandro.la@deliveryhero.com",
                            "user:faisal.farouk@deliveryhero.com",
                        ]
                    },
                    {
                        "role": "roles/bigquery.dataEditor",
                        "members": [
                            "serviceAccount:780393426212@cloudservices.gserviceaccount.com",
                            "serviceAccount:service-123456789123@containerregistry.iam.gserviceaccount.com",
                            "serviceAccount:service-780393426212@dataproc-accounts.iam.gserviceaccount.com",
                            "user:riccardo.bini@deliveryhero.com",
                        ]
                    },
                    {
                        "role": "roles/bigquery.dataOwner",
                        "members": [
                            "serviceAccount:12345678912-compute@developer.gserviceaccount.com",
                        ]
                    },
                    {
                        "role": "roles/bigquery.owner",
                        "members": [
                            "user:alessandro.la@deliveryhero.com",
                            "user:alexander.kagoshima@deliveryhero.com",
                            "user:riccardo.bini@deliveryhero.com",
                        ],
                    },
                    {
                        "role": "roles/editor",
                        "members": [
                            "serviceAccount:fulfillment-dwh-staging@appspot.gserviceaccount.com"
                        ]
                    },
                ]
            }
        )

    def test_modify_policy_remove_all_member(self):
        role_members = [
            {
                "role": "roles/bigquery.dataEditor",
                "members": [
                    "user:riccardo.bini@deliveryhero.com"
                ]
            }
        ]

        self.hook.update_google_iam_project_permissions(project_id="staging", role_members=role_members)
        self.hook.set_policy.assert_called_once_with(
            project_id='staging',
            policy={
                "version": 1,
                "etag": "BwWg9Onj8jE=",
                "bindings": [
                    # {
                    #     "role": "roles/bigquery.admin",
                    #     "members": []
                    # },
                    {
                        "role": "roles/bigquery.dataEditor",
                        "members": [
                            "serviceAccount:780393426212@cloudservices.gserviceaccount.com",
                            "serviceAccount:service-123456789123@containerregistry.iam.gserviceaccount.com",
                            "serviceAccount:service-780393426212@dataproc-accounts.iam.gserviceaccount.com",
                            "user:riccardo.bini@deliveryhero.com",
                        ]
                    },
                    {
                        "role": "roles/bigquery.dataOwner",
                        "members": [
                            "serviceAccount:12345678912-compute@developer.gserviceaccount.com",
                        ]
                    },
                    {
                        "role": "roles/editor",
                        "members": [
                            "serviceAccount:fulfillment-dwh-staging@appspot.gserviceaccount.com"
                        ]
                    },
                ]
            }
        )

    def test_modify_policy_remove_a_member(self):
        """The variable members does not indicate which members to remove, but which should be present."""
        role_members = [
            {
                "role": "roles/bigquery.admin",
                "members": [
                    "user:faisal.farouk@deliveryhero.com"
                ],
            },
            {
                "role": "roles/bigquery.dataEditor",
                "members": [
                    "user:riccardo.bini@deliveryhero.com"
                ],
            },
        ]
        self.hook.update_google_iam_project_permissions(project_id="staging", role_members=role_members)
        self.hook.set_policy.assert_called_once_with(
            project_id='staging',
            policy={
                "version": 1,
                "etag": "BwWg9Onj8jE=",
                "bindings": [
                    {
                        "role": "roles/bigquery.admin",
                        "members": [
                            "user:faisal.farouk@deliveryhero.com"
                        ],
                    },
                    {
                        "role": "roles/bigquery.dataEditor",
                        "members": [
                            "serviceAccount:780393426212@cloudservices.gserviceaccount.com",
                            "serviceAccount:service-123456789123@containerregistry.iam.gserviceaccount.com",
                            "serviceAccount:service-780393426212@dataproc-accounts.iam.gserviceaccount.com",
                            "user:riccardo.bini@deliveryhero.com",
                        ],
                    },
                    {
                        "role": "roles/bigquery.dataOwner",
                        "members": [
                            "serviceAccount:12345678912-compute@developer.gserviceaccount.com",
                        ]
                    },
                    {
                        "role": "roles/editor",
                        "members": [
                            "serviceAccount:fulfillment-dwh-staging@appspot.gserviceaccount.com"
                        ]
                    },
                ]
            }
        )

    def test_update_google_iam_permissions(self):
        role_members = [
            {
                "role": "roles/bigquery.admin",
                "members": [
                    "user:alessandro.la@deliveryhero.com",
                    "user:matias.pacelli@deliveryhero.com",
                    "user:riccardo.bini@deliveryhero.com"
                ]
            },
            {
                "role": "roles/bigquery.dataEditor",
                "members": [
                    "user:riccardo.bini@deliveryhero.com",
                    "user:alexander.kagoshima@deliveryhero.com"
                ]
            }
        ]

        self.hook.update_google_iam_project_permissions(project_id="staging", role_members=role_members)
        self.hook.set_policy.assert_called_once_with(
            project_id='staging',
            policy={
                "version": 1,
                "etag": "BwWg9Onj8jE=",
                "bindings": [
                    {
                        "role": "roles/bigquery.admin",
                        "members": [
                            "user:alessandro.la@deliveryhero.com",
                            "user:matias.pacelli@deliveryhero.com",
                            "user:riccardo.bini@deliveryhero.com",
                        ],
                    },
                    {
                        "role": "roles/bigquery.dataEditor",
                        "members": [
                            "serviceAccount:780393426212@cloudservices.gserviceaccount.com",
                            "serviceAccount:service-123456789123@containerregistry.iam.gserviceaccount.com",
                            "serviceAccount:service-780393426212@dataproc-accounts.iam.gserviceaccount.com",
                            "user:alexander.kagoshima@deliveryhero.com",
                            "user:riccardo.bini@deliveryhero.com",
                        ],
                    },
                    {
                        "role": "roles/bigquery.dataOwner",
                        "members": [
                            "serviceAccount:12345678912-compute@developer.gserviceaccount.com",
                        ],
                    },
                    {
                        "role": "roles/editor",
                        "members": [
                            "serviceAccount:fulfillment-dwh-staging@appspot.gserviceaccount.com"
                        ]
                    },
                ]
            }
        )

    def test_update_google_iam_request_response_mismatch(self):
        role_members = [
            {
                "role": "roles/bigquery.admin",
                "members": [
                    "user:alessandro.la@deliveryhero.com",
                    "user:matias.pacelli@deliveryhero.com",
                    "user:riccardo.bini@deliveryhero.com"
                ]
            },
            {
                "role": "roles/bigquery.dataEditor",
                "members": [
                    "user:riccardo.bini@deliveryhero.com",
                    "user:alexander.kagoshima@deliveryhero.com"
                ]
            }
        ]

        set_iam_response = {
            "version": 1,
            "etag": "BwWg9Onj8jE=",
            "bindings": [
                {
                    "role": "roles/bigquery.admin",
                    "members": [
                        "user:alessandro.la@deliveryhero.com",
                        "user:matias.pacelli@deliveryhero.com",
                        "user:riccardo.bini-alias@deliveryhero.com",
                    ],
                },
                {
                    "role": "roles/bigquery.dataEditor",
                    "members": [
                        "serviceAccount:780393426212@cloudservices.gserviceaccount.com",
                        "serviceAccount:service-780393426212@dataproc-accounts.iam.gserviceaccount.com",
                        "user:riccardo.bini@deliveryhero.com",
                        "user:alexander.kagoshima@deliveryhero.com"
                    ],
                },
                {
                    "role": "roles/bigquery.dataOwner",
                    "members": [
                        "serviceAccount:12345678912-compute@developer.gserviceaccount.com",
                    ],
                },
                {
                    "role": "roles/editor",
                    "members": [
                        "serviceAccount:fulfillment-dwh-staging@appspot.gserviceaccount.com"
                    ]
                },
            ]
        }
        self.hook.set_policy = MagicMock(return_value=set_iam_response)

        error_message = f"Iam Policy not applied correctly in project_id=staging. Please check the logs for details."
        with self.assertRaisesRegex(AirflowException, error_message):
            self.hook.update_google_iam_project_permissions(project_id="staging", role_members=role_members)
