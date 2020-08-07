# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, date
from google.protobuf.duration_pb2 import Duration
import os
import stat
import airflow
import json
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks import S3Hook

#Importamos gcp
from airflow import models

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator
)
from airflow.utils.dates import days_ago


# SET DEFALT ARGUMENTS
default_args = {
    'owner': 'bi_partners',
    'depends_on_past': False,
    'email': ['guillermo.hernandez@pedidosya.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=4),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# GET VARIABLES FROM AIRFLOW ENVIRONMENT
rute = 'airflow/dags'
project_name = "DATA_ORIGINS_SQS_PROCESS_DAG"
dag_name = 'DATA_ORIGINS_SQS_PROCESS_{0}_DAG'

dag_path = os.path.dirname(os.path.realpath(__file__))
config_path = "{0}/config".format(dag_path)

###################################   START CONFIG   ###################################
path_script = "gs://data-ori-test-poc-storage/data-proc-test/scripts/{}".format(project_name)
path_py = "{}/py".format(path_script)

#variables para manejar los cluster
REGION = os.environ.get("GCP_LOCATION", "us-central1")
ZONE = os.environ.get("GCP_REGION", "us-central1b")

#conexiones
emr_ssh=json.loads(S3Hook.get_connection('aws_s3_conn').extra)

#recuperamos la info del cluster a crear 
CLUSTER = json.loads(Variable.get("gcp-dataproc-n4"))

#se ingresan los tiempos 
tiempoIdle = Duration()
tiempoUp = Duration()
#Generamos el tiempo sin hacer nada y el tiempo maximo del cluster arriba
tiempoIdle.seconds=999
tiempoUp.seconds=7200

#preparamos la variable cluster 
CLUSTER['config']['lifecycle_config']={
      "idle_delete_ttl": tiempoIdle,
      "auto_delete_ttl": tiempoUp
    }
#se agregan las claves de s3, configuracion extra para conectar 
CLUSTER['config']['software_config']['properties']['spark:spark.hadoop.fs.s3a.access.key']=emr_ssh['aws_access_key_id']
CLUSTER['config']['software_config']['properties']['spark:spark.hadoop.fs.s3a.secret.key']=emr_ssh['aws_secret_access_key']

#definimos el nombre del cluster
CLUSTER_NAME='data-origins-dataproc-orders'
CLUSTER['cluster_name']=CLUSTER_NAME
#cargamos el nombre del proyecto en una variable
PROJECT_ID=CLUSTER['project_id']

#colocamos las etiquetas especificas al cluster
CLUSTER['labels']['owner']='guillermo-hernandez'

####################################   END CONFIG   ####################################

# Config file load (json)
with open('{0}/DATA_ORIGINS_Config.json'.format(config_path)) as json_file:
    dag_bag = json.load(json_file)

########################################################################################

def createSparkStep(pDConfig):
    return {
        "reference":{
            "project_id":PROJECT_ID
        },
        "placement":{
            "cluster_name":CLUSTER_NAME
        },    
        "labels": {
            "team": "data-originis",
            "owner": "guillermo-hernandez",
            "task": 'load_{0}'.format(pDConfig['model'])
            },   
        "pyspark_job":{
            "main_python_file_uri":"{}/{}.py".format(path_py, pDConfig['model']),
                "properties": {
                "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark.driver.memory":"5g",
                "spark.executor.memory":"5g",  
                "spark.driver.cores": "2",
                "spark.executor.cores": "2"
            },
            "args": [
                "-m", pDConfig["model"],
                "-source", pDConfig["origin"],
                "-destination", pDConfig["destination"],  
                "-history" ,pDConfig["data_history"],
                "-path" , path_script
            ]
        }
    }    



def create_dag(dagName, d):
    try:
        start_date = datetime.strptime(d["start_date"], "%Y-%m-%d")
    except:
        start_date = None
    config = {"start_date": start_date }

    dag = DAG(dagName, default_args=dict(default_args, **config), max_active_runs=4, schedule_interval=d['scheduler'], catchup = False)

    with dag:
        checking_process = DummyOperator(task_id='checking_process', dag=dag)
        ending_process = DummyOperator(task_id='ending_process', dag=dag)

        #creamos el cluster en dataproc
        create_cluster = DataprocCreateClusterOperator(
            dag=dag,
            task_id="create_cluster", 
            project_id=PROJECT_ID, 
            cluster=CLUSTER, 
            region=REGION
        )

        generate_last_sqs = DataprocSubmitJobOperator(
            dag=dag,
            task_id="Generate_Last_SQS_{}".format(d['model']), 
            job=createSparkStep(d), 
            location=REGION, 
            project_id=PROJECT_ID
        )

        delete_cluster = DataprocDeleteClusterOperator(
            dag=dag,
            task_id="delete_cluster",
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_NAME,
            region=REGION,
        )

        # # # Pasos 
        checking_process>>create_cluster>>generate_last_sqs>>delete_cluster>>ending_process
        
        return dag

for d in dag_bag["dags"]:
    if d["active"]:
        dag_id = dag_name.format(d["model"])
        d['destination'] = dag_bag['destination'].format(d['model'])
        d['origin'] = dag_bag['origin'].format(d['origin_s3_bucket'], d['origin_s3_folder'])

        globals()[dag_id] = create_dag(dag_id, d)