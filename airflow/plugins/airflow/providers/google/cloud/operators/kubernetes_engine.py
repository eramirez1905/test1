#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This module contains Google Kubernetes Engine operators.
"""

import os
import tempfile
from typing import Dict, Optional, Union

from google.cloud.container_v1.types import Cluster

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.common.utils.process_utils import execute_in_subprocess, patch_environ


class GKEDeleteClusterOperator(BaseOperator):
    """
    Deletes the cluster, including the Kubernetes endpoint and all worker nodes.

    To delete a certain cluster, you must specify the ``project_id``, the ``name``
    of the cluster, the ``location`` that the cluster is in, and the ``task_id``.

    **Operator Creation**: ::

        operator = GKEClusterDeleteOperator(
                    task_id='cluster_delete',
                    project_id='my-project',
                    location='cluster-location'
                    name='cluster-name')

    .. seealso::
        For more detail about deleting clusters have a look at the reference:
        https://google-cloud-python.readthedocs.io/en/latest/container/gapic/v1/api.html#google.cloud.container_v1.ClusterManagerClient.delete_cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEDeleteClusterOperator`

    :param project_id: The Google Developers Console [project ID or project number]
    :type project_id: str
    :param name: The name of the resource to delete, in this case cluster name
    :type name: str
    :param location: The name of the Google Compute Engine zone in which the cluster
        resides.
    :type location: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: The api version to use
    :type api_version: str
    """
    template_fields = ['project_id', 'gcp_conn_id', 'name', 'location', 'api_version']

    @apply_defaults
    def __init__(self,
                 name: str,
                 location: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v2',
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.api_version = api_version
        self.name = name
        self._check_input()

    def _check_input(self):
        if not all([self.project_id, self.name, self.location]):
            self.log.error(
                'One of (project_id, name, location) is missing or incorrect')
            raise AirflowException('Operator has incorrect or missing input.')

    def execute(self, context):
        hook = GKEHook(gcp_conn_id=self.gcp_conn_id, location=self.location)
        delete_result = hook.delete_cluster(name=self.name, project_id=self.project_id)
        return delete_result


class GKECreateClusterOperator(BaseOperator):
    """
    Create a Google Kubernetes Engine Cluster of specified dimensions
    The operator will wait until the cluster is created.

    The **minimum** required to define a cluster to create is:

    ``dict()`` ::
        cluster_def = {'name': 'my-cluster-name',
                       'initial_node_count': 1}

    or

    ``Cluster`` proto ::
        from google.cloud.container_v1.types import Cluster

        cluster_def = Cluster(name='my-cluster-name', initial_node_count=1)

    **Operator Creation**: ::

        operator = GKEClusterCreateOperator(
                    task_id='cluster_create',
                    project_id='my-project',
                    location='my-location'
                    body=cluster_def)

    .. seealso::
        For more detail on about creating clusters have a look at the reference:
        :class:`google.cloud.container_v1.types.Cluster`

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKECreateClusterOperator`

    :param project_id: The Google Developers Console [project ID or project number]
    :type project_id: str
    :param location: The name of the Google Compute Engine zone in which the cluster
        resides.
    :type location: str
    :param body: The Cluster definition to create, can be protobuf or python dict, if
        dict it must match protobuf message Cluster
    :type body: dict or google.cloud.container_v1.types.Cluster
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: The api version to use
    :type api_version: str
    """
    template_fields = ['project_id', 'gcp_conn_id', 'location', 'api_version', 'body']

    @apply_defaults
    def __init__(self,
                 location: str,
                 body: Optional[Union[Dict, Cluster]],
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 api_version: str = 'v2',
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.api_version = api_version
        self.body = body
        self._check_input()

    def _check_input(self):
        if not all([self.project_id, self.location, self.body]) or not (
            (isinstance(self.body, dict) and "name" in self.body and "initial_node_count" in self.body) or
            (getattr(self.body, "name", None) and getattr(self.body, "initial_node_count", None))
        ):
            self.log.error(
                "One of (project_id, location, body, body['name'], "
                "body['initial_node_count']) is missing or incorrect"
            )
            raise AirflowException("Operator has incorrect or missing input.")

    def execute(self, context):
        hook = GKEHook(gcp_conn_id=self.gcp_conn_id, location=self.location)
        create_op = hook.create_cluster(cluster=self.body, project_id=self.project_id)
        return create_op


KUBE_CONFIG_ENV_VAR = "KUBECONFIG"
