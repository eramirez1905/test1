import re

import googleapiclient.discovery
from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class GoogleIamHook(GoogleCloudBaseHook):

    def __init__(self,
                 gcp_conn_id="google_cloud_default",
                 delegate_to=None):
        super().__init__(gcp_conn_id=gcp_conn_id,
                         delegate_to=delegate_to)
        self._conn = None

    def get_conn(self):
        if not self._conn:
            self._conn = googleapiclient.discovery.build(
                "cloudresourcemanager", "v1", credentials=self._get_credentials(), cache_discovery=False
            )
        return self._conn

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass

    def update_google_iam_project_permissions(self, project_id, role_members):
        get_policy_response = self.get_policy(project_id=project_id)
        policy = get_policy_response.copy()

        policy["bindings"] = self.__merge_bindings(role_members, policy["bindings"])

        self.__log_bindings_delta(policy["bindings"], get_policy_response["bindings"], project_id)

        set_policy_response = self.set_policy(project_id=project_id, policy=policy)

        new, old = self.__bindings_delta(set_policy_response["bindings"], policy["bindings"])
        if len(old) > 0 or len(new) > 0:
            self.log.error(f"Policies present in the response but not in the request in project_id={project_id}: {', '.join(old)}")
            self.log.error(f"Policies present in the request but not in the response in project_id={project_id}: {', '.join(new)}")
            raise AirflowException(f"Iam Policy not applied correctly in project_id={project_id}. Please check the logs for details.")

        return set_policy_response

    def __log_bindings_delta(self, new_policy, old_policy, project_id):
        new, old = self.__bindings_delta(new_policy, old_policy)
        if len(old) > 0 or len(new) > 0:
            [self.log.info(f"Members to add in project_id={project_id}: {email}") for email in new]
            [self.log.info(f"Members to remove in project_id={project_id}: {email}") for email in old]

    def __merge_bindings(self, new_policy, old_policy):
        members_new = {}
        members_old = {}
        for binding in old_policy:
            # Extract SA created by Google
            members_old[binding["role"]] = [member for member in binding["members"] if self.__whitelist_google_service_accounts(member)]
        for member in new_policy:
            members_new[member["role"]] = member["members"]

        bindings = []
        all_roles = set(list(members_old.keys()) + list(members_new.keys()))
        for role in all_roles:
            actual_old_members = self.__extract_members_by_role(role, members_old)
            actual_new_members = self.__extract_members_by_role(role, members_new)
            members = list(set(actual_old_members + actual_new_members))
            if len(members) > 0:
                bindings.append({
                    "role": role,
                    "members": sorted(members)
                })
        return sorted(bindings, key=lambda b: b['role'])

    @staticmethod
    def __extract_members_by_role(role, members_old):
        members = []
        if role in members_old.keys():
            members = members_old[role]
        return members

    def __bindings_delta(self, new_policy, old_policy):
        old_flat = self.__flat_bindings(old_policy)
        new_flat = self.__flat_bindings(new_policy)
        return sorted(list(new_flat - old_flat)), sorted(list(old_flat - new_flat))

    @staticmethod
    def __whitelist_google_service_accounts(member):
        if re.match(r'^serviceAccount:service-[0-9]+@[a-z0-z-]+\.iam\.gserviceaccount\.com', member) is not None:
            return True
        if re.match(r'^serviceAccount:[0-9]+-compute@developer\.gserviceaccount\.com', member) is not None:
            return True
        if re.match(r'^serviceAccount:[0-9]+@cloudservices\.gserviceaccount\.com', member) is not None:
            return True
        if re.match(r'^serviceAccount:[a-z0-9-_]+@appspot\.gserviceaccount\.com', member) is not None:
            return True
        return False

    @staticmethod
    def __flat_bindings(bindings):
        members = []
        for binding in bindings:
            for member in binding['members']:
                members.append(f"{member} - {binding['role']}")
        return set(members)

    def get_policy(self, project_id, version=1):
        """Gets IAM policy for a project."""
        policy = (
            self.get_conn().projects().getIamPolicy(
                resource=project_id,
                body={"options": {"requestedPolicyVersion": version}},
            ).execute()
        )
        self.log.debug(f"Google IAM policy in project_id={project_id} {policy}")
        return policy

    def set_policy(self, project_id, policy):
        """Sets IAM policy for a project."""
        self.log.debug(f"The updated policy in project_id={project_id}: {policy}")

        return self.get_conn().projects().setIamPolicy(
            resource=project_id,
            body={"policy": policy}
        ).execute()
