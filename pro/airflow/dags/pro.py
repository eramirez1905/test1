from datetime import datetime, timedelta
import json
import os
import tempfile
from urllib.parse import urlparse

from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from google.protobuf.duration_pb2 import Duration
import pytz

# FIXME: make a PR to airflow repo to add these fields
DataprocDeleteClusterOperator.template_fields = ['cluster_name',
                                                 'project_id',
                                                 'region',
                                                 'impersonation_chain']

###############################################################################
# VARIABLES
###############################################################################

#  ----------------------------------------------------------------------------
#  DEFAULT ARGS FOR DAGS
#  ----------------------------------------------------------------------------

default_args = {
    'owner': 'data_analytics_factory',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['analytics-factory@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

#  ----------------------------------------------------------------------------
#  CONNECTIONS | CREDENTIALS - VARIABLES
#  ----------------------------------------------------------------------------

conn_id_gcp = 'conn_id_gcp'
conn_id_gcp_cros = 'conn_id_gcp_cros'
sa_cross = Variable.get("config_sa-cross")

slack_token_variable_name = "slack_token"
slack_user_name_daf_airflow = "slack_user_name_to_users"
slack_channel_daf_airflow = "slack_channel_daf_airflow"

github_auth = Variable.get("github_auth", deserialize_json=True)
#  ----------------------------------------------------------------------------
#  HOOKS - VARIABLES
#  ----------------------------------------------------------------------------

bq_hook = BigQueryHook(bigquery_conn_id=conn_id_gcp_cros, use_legacy_sql=False)

#  ----------------------------------------------------------------------------
#  DATE - VARIABLES
#  ----------------------------------------------------------------------------

macro_yesterday_date = '{{ ds }}'
macro_today_date = '{{ next_ds }}'

#  ----------------------------------------------------------------------------
#  CLUSTER CONFIG - VARIABLES
#  ----------------------------------------------------------------------------
predict_config_script = \
    Variable.get("var-food_ontology-predict-configuration",
                 deserialize_json=True)
predict_properties_daily = \
    Variable.get("var-food_ontology-predict-config-daily")
predict_properties_initial_load = \
    Variable.get("var-food_ontology-predict-config-initial-load")

cluster_config_var = json.loads(
    Variable.get("var-food_ontology-predict_dataproc-cluster-config"))

REGION = os.environ.get("GCP_LOCATION", cluster_config_var['region'])
ZONE = os.environ.get("GCP_REGION", cluster_config_var['zone'])
CLUSTER_NAME_DAILY = cluster_config_var['cluster_pre_name'] \
                     + "{{ execution_date.format('%Y%m%d%H%M') }}"
CLUSTER_NAME_INITIAL_LOAD = \
    f"{cluster_config_var['cluster_pre_name']}initial-" \
    + "{{ execution_date.format('%Y%m%d%H%M') }}"
PROJECT_ID = Variable.get("config_project-id")
PIG_JAR_FILE_URIS = Variable.get("ab_test_jar_file_uris")

machine_type_uri = cluster_config_var['machine_type_uri']
num_instances = cluster_config_var['num_instances']
boot_disk_size_gb = cluster_config_var['boot_disk_size_gb']
boot_disk_type = cluster_config_var['boot_disk_type']
image_version = cluster_config_var['image_version']
timeIdle = Duration()
timeIdle.seconds = cluster_config_var['idle_delete_ttl_seconds']
subnetwork_uri = cluster_config_var['subnetwork_uri']
initialization_actions_execution_timeout = Duration()
initialization_actions_execution_timeout.seconds = \
    cluster_config_var['initialization_actions_execution_timeout_seconds']
initialization_actions_execution = \
    "gs://dataproc-initialization-actions/python/pip-install.sh"
git_branch = cluster_config_var['git_branch']
label_team = cluster_config_var['label_team']
label_task = cluster_config_var['label_task']
pip_install_pkg = \
    f"-egit+https://{github_auth['user']}:{github_auth['token']}@github.com/" \
    f"pedidosya/data-analytics-factory-food-ontology-model.git@{git_branch}#" \
    f"egg=food_ontology&subdirectory=src"


#  ----------------------------------------------------------------------------
#  CLUSTER CONFIG - VARIABLES
#  ----------------------------------------------------------------------------
#
CLUSTER_CONFIGURATION = {
    "master_config": {
        "machine_type_uri": machine_type_uri,
        "num_instances": num_instances,
        "accelerators": [],
        "disk_config": {
            "boot_disk_size_gb": boot_disk_size_gb,
            "boot_disk_type": boot_disk_type
        }
    },
    "software_config": {
        "image_version": image_version,
        "properties": {
            "dataproc:dataproc.allow.zero.workers": "true"
        }
    },
    'lifecycle_config': {
        "idle_delete_ttl": timeIdle
    },
    'gce_cluster_config': {
        'metadata': {
            "PIP_PACKAGES": pip_install_pkg,
        },
        'service_account': sa_cross,
        'subnetwork_uri': subnetwork_uri
    },
    'initialization_actions': [
        {
            "executable_file": initialization_actions_execution,
            "execution_timeout": initialization_actions_execution_timeout
        }
    ]
}


###############################################################################
# FUNCTIONS
###############################################################################

#  ----------------------------------------------------------------------------
#  GENERAL - FUNCTIONS
#  ----------------------------------------------------------------------------

def f_get_execution_date_from_last_dag_run(dag):
    last_dag_run = dag.get_last_dagrun(include_externally_triggered=True)
    if last_dag_run is None:
        return str(datetime.now().replace(tzinfo=pytz.UTC))
    else:
        return str(last_dag_run.execution_date)


#  ----------------------------------------------------------------------------
#  DATAPROC CONFIG - FUNCTIONS
#  ----------------------------------------------------------------------------
def get_cluster_config():
    """
    https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/ClusterConfig
    """
    pass


def get_pyspark_job_config(python_file, config_file, cluster_name):
    """
    https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/PySparkJob
    """
    config_file_name = config_file.split('/')[-1]
    pyspark_job = {
        "reference": {
            "project_id": PROJECT_ID
        },
        "placement": {
            "cluster_name": cluster_name
        },
        "pyspark_job": {
            "main_python_file_uri": python_file,
            "args": ["--config_file={}".format(config_file_name)],
            "file_uris": [config_file]
         },
        "labels": {
            "team": label_team,
            "task": label_task
        }
    }

    return pyspark_job


def get_pig_job_config(query, cluster_name):
    """
    https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/PigJob
    """
    pig_job = {
        "reference": {
            "project_id": PROJECT_ID
        },
        "placement": {
            "cluster_name": cluster_name
        },
        "pig_job": {
            "jar_file_uris": [
                PIG_JAR_FILE_URIS
            ],
            "query_list": {
                "queries": [
                    query
                ]
            }
        },
        "labels": {
            "team": label_team,
            "task": label_task
        }
    }

    return pig_job


def write_str_to_gcp(string: str,
                     gcp_path: str,
                     conn_id: str = 'google_cloud_default'):
    """Dump a string into a file in google bucket"""
    storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=conn_id)
    destination_uri = urlparse(gcp_path)
    with tempfile.TemporaryDirectory() as tmp_folder:
        temp_path_abs = os.path.join(tmp_folder, 'config_file')
        with open(temp_path_abs, 'w') as f:
            f.write(string)

        if destination_uri.path.startswith('/'):
            destination_path = destination_uri.path[1:]
        else:
            destination_path = destination_uri.path

        storage_hook.upload(
            destination_uri.netloc,
            destination_path,
            temp_path_abs
        )


delete_current_daily_execution = """
DELETE FROM food_ontology.product_ontology_historical
WHERE DATE(last_update) = "{{ next_ds }}"
"""

delete_query = """
DELETE FROM food_ontology.product_ontology
WHERE product_id in (
SELECT  po.product_id
    FROM food_ontology.product_ontology po
    LEFT JOIN food_ontology.product_ontology_historical poh
    ON poh.product_id = po.product_id
    WHERE DATE(po.last_update) < "{{ next_ds }}"
    AND DATE(poh.last_update) = "{{ next_ds }}"
)
"""

insert_query = """
INSERT food_ontology.product_ontology
SELECT  *
FROM food_ontology.product_ontology_historical
WHERE DATE(last_update) = "{{ next_ds }}"
"""
###############################################################################
# DAG
###############################################################################

with DAG(
    dag_id="DAF_PIPELINE_FOOD_ONTOLOGY_PREDICT_DAG",
    catchup=True,
    schedule_interval='00 6 * * *',
    max_active_runs=1,
    default_args=default_args
) as dag_daily:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=CLUSTER_NAME_DAILY,
        region=REGION,
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIGURATION,
    )

    pig_job_nltk_stopwords = DataprocSubmitJobOperator(
        task_id="pig_job_nltk_stopwords",
        job=get_pig_job_config("sh python -m nltk.downloader stopwords",
                               CLUSTER_NAME_DAILY),
        location=REGION,
        project_id=PROJECT_ID
    )

    pig_job_spacy_vocabulary = DataprocSubmitJobOperator(
        task_id="pig_job_spacy_vocabulary",
        job=get_pig_job_config("sh python -m spacy download es_core_news_lg",
                               CLUSTER_NAME_DAILY),
        location=REGION,
        project_id=PROJECT_ID
    )

    python_task_config_file = PythonOperator(
        task_id='python_task_config_file',
        python_callable=write_str_to_gcp,
        op_args=[predict_properties_daily,
                 predict_config_script['config_path_daily'],
                 conn_id_gcp, ]
    )

    bq_delete_current_execution = BigQueryOperator(
        task_id="bq_delete_current_execution",
        sql=delete_current_daily_execution,
        use_legacy_sql=False,
        bigquery_conn_id=conn_id_gcp_cros,
    )

    pyspark_job_task_predict = DataprocSubmitJobOperator(
        task_id="pyspark_job_task",
        job=get_pyspark_job_config(
            predict_config_script['script_path'],
            predict_config_script['config_path_daily'],
            CLUSTER_NAME_DAILY
        ),
        location=REGION,
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME_DAILY,
        region=REGION
    )

    delete_cluster_on_failure = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME_DAILY,
        region=REGION,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    bq_delete = BigQueryOperator(
        task_id="delete_record_of_execution_date",
        sql=delete_query,
        use_legacy_sql=False,
        bigquery_conn_id=conn_id_gcp_cros,
    )

    bq_insert = BigQueryOperator(
        task_id="insert_record_of_execution_date",
        sql=insert_query,
        use_legacy_sql=False,
        bigquery_conn_id=conn_id_gcp_cros,
    )

    task_slack_message_fail_to_us = SlackAPIPostOperator(
        task_id='slack_message_fail_to_us',
        channel=Variable.get(slack_channel_daf_airflow),
        token=Variable.get(slack_token_variable_name),
        text=f":error_dot: FOOD ONTOLOGY: something failed in the dag."
             f" {dag_daily}",
        username=Variable.get(slack_user_name_daf_airflow),
        trigger_rule=TriggerRule.ONE_FAILED
    )

    chain(
        create_cluster,
        [
            pig_job_nltk_stopwords,
            pig_job_spacy_vocabulary,
            python_task_config_file,
        ],
        bq_delete_current_execution,
        pyspark_job_task_predict,
        delete_cluster,
        bq_delete,
        bq_insert,
        [task_slack_message_fail_to_us, delete_cluster_on_failure],
    )

with DAG(
        dag_id="PROS_DAF_PIPELINE_FOOD_ONTOLOGY_PREDICT_INITIAL_LOAD_DAG",
        max_active_runs=1,
        schedule_interval=None,
        default_args=default_args
) as dag_historical:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=CLUSTER_NAME_INITIAL_LOAD,
        region=REGION,
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIGURATION,
    )

    pig_job_nltk_stopwords = DataprocSubmitJobOperator(
        task_id="pig_job_nltk_stopwords",
        job=get_pig_job_config("sh python -m nltk.downloader stopwords",
                               CLUSTER_NAME_INITIAL_LOAD),
        location=REGION,
        project_id=PROJECT_ID
    )

    pig_job_spacy_vocabulary = DataprocSubmitJobOperator(
        task_id="pig_job_spacy_vocabulary",
        job=get_pig_job_config("sh python -m spacy download es_core_news_lg",
                               CLUSTER_NAME_INITIAL_LOAD),
        location=REGION,
        project_id=PROJECT_ID
    )

    python_task_config_file = PythonOperator(
        task_id='python_task_config_file',
        python_callable=write_str_to_gcp,
        op_args=[predict_properties_initial_load,
                 predict_config_script['config_path_initial_load'],
                 conn_id_gcp, ]
    )

    pyspark_job_task_predict = DataprocSubmitJobOperator(
        task_id="pyspark_job_task",
        job=get_pyspark_job_config(
            predict_config_script['script_path'],
            predict_config_script['config_path_initial_load'],
            CLUSTER_NAME_INITIAL_LOAD
        ),
        location=REGION,
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME_INITIAL_LOAD,
        region=REGION
    )

    task_slack_message_fail_to_us = SlackAPIPostOperator(
        task_id='slack_message_fail_to_us',
        channel=Variable.get(slack_channel_daf_airflow),
        token=Variable.get(slack_token_variable_name),
        text=f":error_dot: FOOD ONTOLOGY: something failed in the dag. "
             f"{dag_historical}",
        username=Variable.get(slack_user_name_daf_airflow),
        trigger_rule=TriggerRule.ONE_FAILED
    )

    chain(
        create_cluster,
        [
            pig_job_nltk_stopwords,
            pig_job_spacy_vocabulary,
            python_task_config_file,
        ],
        pyspark_job_task_predict,
        delete_cluster,
        task_slack_message_fail_to_us,
    )
