from typing import List, Dict

from airflow import AirflowException
from airflow.contrib.hooks.bigquery_hook import BigQueryHook as BigQueryHookBase
from googleapiclient.errors import HttpError
from pandas_gbq import read_gbq


class BigQueryHook(BigQueryHookBase):
    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        pass

    def bulk_dump(self, table, tmp_file):
        pass

    def bulk_load(self, table, tmp_file):
        pass

    def get_table_schema(self, dataset_id, table_id):
        return self.get_service().tables() \
            .get(projectId=self.project_id, datasetId=dataset_id, tableId=table_id) \
            .execute()

    def create_empty_table(self,
                           dataset_id,
                           table_id,
                           schema_fields,
                           time_partitioning=None,
                           clustering=None,
                           labels=None):
        """
        This function is a copy of its parent with in addition "clustering".
        Airflow master branch contains something similar, with Airflow 2.0 this function is not needed anymore
        """
        table_resource = {
            'tableReference': {
                'tableId': table_id
            }
        }

        if schema_fields:
            table_resource['schema'] = {'fields': schema_fields}

        if time_partitioning:
            table_resource['timePartitioning'] = time_partitioning

        if clustering:
            table_resource['clustering'] = clustering

        if labels:
            table_resource['labels'] = labels

        self.log.info('Creating Table %s:%s.%s',
                      self.project_id, dataset_id, table_id)
        try:
            self.get_service().tables().insert(
                projectId=self.project_id,
                datasetId=dataset_id,
                body=table_resource
            ).execute()

            self.log.info('Table created successfully: %s:%s.%s',
                          self.project_id, dataset_id, table_id)

        except HttpError as err:
            raise AirflowException('BigQuery job failed. Error was: {}'.format(err.content))

    def update_data_viewer_permissions(self, dataset_id: str, email_addresses: [], group_addresses: []):
        if len(email_addresses) == 0 and len(group_addresses) == 0:
            self.log.info(f"No permissions to update in the dataset '{dataset_id}'")
            return

        cursor = self.get_cursor()
        service = self.get_service()

        shared_dataset = cursor.get_dataset(dataset_id)

        self.log.info(f"Allow users {email_addresses} to access to the dataset {dataset_id}")

        shared_dataset = self._get_dataset_without_custom_roles(email_addresses, group_addresses, shared_dataset)

        try:
            self.log.info(f"Update dataset '{dataset_id}' access list to {shared_dataset['access']}")
            dataset_resource = service.datasets() \
                .update(datasetId=dataset_id,
                        projectId=self.project_id,
                        body=shared_dataset) \
                .execute(num_retries=2)
            self.log.info("Dataset Resource: {}".format(dataset_resource))
        except HttpError as err:
            raise AirflowException(
                'Failed to update permissions for the dataset {}. Error was: {}'.format(dataset_id, err.content))

    def dataset_exists(self, dataset_id, project_id=None):
        """
        Checks for the existence of a dataset in Google BigQuery.

        :param project_id:
        :param dataset_id: The BigQuery Dataset ID
        :type dataset_id: str
        :param dataset_id: The name of the dataset in which to look for the
            table.
        :type dataset_id: string
        """
        service = self.get_service()
        dataset_project_id = project_id if project_id else self.project_id
        try:
            dataset_resource = service.datasets().get(
                datasetId=dataset_id, projectId=dataset_project_id).execute()
            self.log.info("Dataset Resource: {}".format(dataset_resource))
            return True
        except HttpError as e:
            if e.resp['status'] == '404':
                return False
            raise

    def update_google_iam_dataset_permissions(self, dataset_id, project_id, access_entries: List[Dict[str, str]], blacklist_roles=None):
        if blacklist_roles is None:
            blacklist_roles = []
        dataset = self.get_cursor().get_dataset(dataset_id, project_id)

        access = dataset.get('access')
        special_groups = [access_entry for access_entry in access if self._is_special_group(access_entry=access_entry)]
        whitelisted_access_entry = [access_entry for access_entry in access if self._is_whitelisted(access_entry, blacklist_roles)]

        access_entries_request = special_groups + access_entries + whitelisted_access_entry

        self.__log_access_entry_delta(access_entries_request, dataset.get("access"), project_id, dataset_id)

        dataset_resource = {
            "etag": dataset.get("etag"),
            "access": access_entries_request
        }
        self.log.info("dataset_resource %s", dataset_resource)
        self.get_cursor().patch_dataset(dataset_id, dataset_resource, project_id)

    @staticmethod
    def _is_special_group(access_entry: dict):
        return not any(name in access_entry for name in ['userByEmail', 'groupByEmail'])

    def _is_whitelisted(self, access_entry, blacklist_roles):
        return access_entry.get('role') in blacklist_roles and not self._is_special_group(access_entry=access_entry)

    def __log_access_entry_delta(self, new_policy, old_policy, project_id, dataset_id):
        new, old = self.__access_entry_delta(new_policy, old_policy)
        if len(old) > 0 or len(new) > 0:
            [self.log.info(f"Members to add in {project_id}.{dataset_id}: {email}") for email in new]
            [self.log.info(f"Members to remove in {project_id}.{dataset_id}: {email}") for email in old]

    def __access_entry_delta(self, new_policy, old_policy):
        old_flat = self.__flat_access_entry(old_policy)
        new_flat = self.__flat_access_entry(new_policy)
        return sorted(list(new_flat - old_flat)), sorted(list(old_flat - new_flat))

    @staticmethod
    def __flat_access_entry(access_entries):
        members = []
        for access_entry in access_entries:
            entry = []
            for key, value in access_entry.items():
                entry.append(f"{key}={value}")
            members.append(', '.join(entry))
        return set(members)

    @staticmethod
    def _get_dataset_without_custom_roles(email_addresses, group_addresses, shared_dataset):
        not_reader_roles = [a for a in shared_dataset.get('access') if a.get('role') != 'READER']
        reader_roles = [a for a in shared_dataset.get('access') if
                        (a.get('role') == 'READER' and 'groupByEmail' not in a and 'userByEmail' not in a)]
        shared_dataset['access'] = not_reader_roles + reader_roles
        shared_dataset['access'].extend(
            [{'role': 'READER', 'userByEmail': email_address} for email_address in email_addresses]
        )
        shared_dataset['access'].extend(
            [{'role': 'READER', 'groupByEmail': group_address} for group_address in group_addresses]
        )
        return shared_dataset

    def get_pandas_df(self, sql, parameters=None, dialect=None):
        """
        Returns a Pandas DataFrame for the results produced by a BigQuery
        query. The DbApiHook method must be overridden because Pandas
        doesn't support PEP 249 connections, except for SQLite. See:

        https://github.com/pydata/pandas/blob/master/pandas/io/sql.py#L447
        https://github.com/pydata/pandas/issues/6900

        :param sql: The BigQuery SQL to execute.
        :type sql: string
        :param parameters: The parameters to render the SQL query with (not
            used, leave to override superclass method)
        :type parameters: mapping or iterable
        :param dialect: Dialect of BigQuery SQL – legacy SQL or standard SQL
            defaults to use `self.use_legacy_sql` if not specified
        :type dialect: string in {'legacy', 'standard'}
        """
        if dialect is None:
            dialect = 'legacy' if self.use_legacy_sql else 'standard'

        return read_gbq(sql,
                        project_id=self._get_field('project'),
                        dialect=dialect,
                        credentials=self._get_credentials())

    def update_policy_tags(self, dataset_id: str, table_id: str, project_id: str, policy_tags: dict, columns: dict):
        schema = self.get_schema(project_id, dataset_id, table_id)
        self.log.debug(f"Schema for {project_id}.{dataset_id}.{table_id}: %s", schema)

        fields = []
        for field in schema['fields']:
            if field["type"] not in ["RECORD"]:
                if "policyTags" in field:
                    # cleanup policyTags in case we want to remove previous set tags
                    field["policyTags"] = {
                        "names": []
                    }
                if field["name"] in columns:
                    field["policyTags"] = {
                        "names": [
                            policy_tags[columns[field["name"]]]
                        ]
                    }
            fields.append(field)

        self.log.info(f"Updated schema for {project_id}.{dataset_id}.{table_id}: %s", fields)

        self.get_cursor().patch_table(dataset_id, table_id, project_id, schema=fields)

    def get_schema(self, project_id: str, dataset_id: str, table_id: str):
        """
        self.get_cursor().get_schema() is buggy, the project_id used to patch the table is
        the one defined in the hook

        :param project_id:  the project ID of the requested table
        :param dataset_id: the dataset ID of the requested table
        :param table_id: the table ID of the requested table
        :return: a table schema
        """
        tables_resource = self.get_service().tables() \
            .get(projectId=project_id, datasetId=dataset_id, tableId=table_id) \
            .execute(num_retries=self.num_retries)
        schema = tables_resource['schema']
        return schema
