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
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.sensors import SqlSensor
from airflow.contrib.sensors.python_sensor import PythonSensor

#Importamos gcp
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator
)
from airflow.utils.dates import days_ago

default_args = {
    'owner':  'data-origins',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 1),
    'email': 'asnioby.hernandez@pedidosya.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

#variables para manejar los cluster
REGION = os.environ.get("GCP_LOCATION", "us-central1")
ZONE = os.environ.get("GCP_REGION", "us-central1b")
dag_name = "DATA_ORIGINS_SQS_{0}_DAG"
dag_name_query = "DATA_ORIGINS_QUERY_{0}_DAG"

#conexiones
emr_ssh = json.loads(S3Hook.get_connection('aws_s3_conn').extra)
bq_conn = "bigquery_default"
http_conn = "http_service_audit"
airflow_conn = "airflow_db"

#recuperamos la info del cluster a crear
CLUSTER = json.loads(Variable.get("gcp-dataproc-n4"))

#se ingresan los tiempos
tiempoIdle = Duration()
tiempoUp = Duration()
#Generamos el tiempo sin hacer nada y el tiempo maximo del cluster arriba
tiempoIdle.seconds=999
tiempoUp.seconds=7200

# definimos el nombre del cluster
CLUSTER_NAME = 'data-origins-dataproc-{0}'

# preparamos la variable cluster
CLUSTER['config']['lifecycle_config'] = {
    "idle_delete_ttl": tiempoIdle,
    "auto_delete_ttl": tiempoUp
}

# se agregan las claves de s3, configuracion extra para conectar
CLUSTER['config']['software_config']['properties']['spark:spark.hadoop.fs.s3a.access.key'] = emr_ssh[
    'aws_access_key_id']
CLUSTER['config']['software_config']['properties']['spark:spark.hadoop.fs.s3a.secret.key'] = emr_ssh[
    'aws_secret_access_key']

# cargamos el nombre del proyecto en una variable
PROJECT_ID = CLUSTER['project_id']

def getPySparkJobTemplate():
    return {
        "reference":{
          "project_id": PROJECT_ID
       },
        "placement":{
          "cluster_name": CLUSTER_NAME
       },
        "labels": {
          "team": "data-originis",
          "owner": "asnioby-hernandez",
          "task": "partner"
        },
        "pyspark_job":{
            "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/@script",
            "properties": {
                "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark.driver.memory":"20g",
                "spark.executor.memory":"20g",
                "spark.driver.cores": "8",
                "spark.executor.cores": "8",
                "spark.sql.sources.partitionOverwriteMode":"dynamic"
              },
            "args": [
                 "-source_path","@source",
                 "-target_path",  "@target",
                 "-partition",  "@date_id",
                 "-days_back",  "@days_back",
                 "-type",  "@type",
                 "-model",  "@model",
                 "-schema", "@schema"
              ]
       }
    }

def getPySparkEnumJobTemplate():
    return {
        "reference":{
          "project_id": PROJECT_ID
       },
        "placement":{
          "cluster_name": CLUSTER_NAME
       },
        "labels": {
          "team": "data-originis",
          "owner": "asnioby-hernandez",
          "task": "partner"
        },
        "pyspark_job":{
            "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/@script",
            "properties": {
                "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark.driver.memory":"2g",
                "spark.executor.memory":"2g",
                "spark.driver.cores": "2",
                "spark.executor.cores": "2",
                "spark.sql.sources.partitionOverwriteMode":"dynamic"
              },
            "args": [
                 "-source","@source",
                 "-name",  "@name",
                 "-type",  "@type",
                 "-format",  "@format",
                 "-schema", "@schema"
              ]
       }
    }

def getPySparkDataLakeJobTemplate():
    return {
        "reference":{
          "project_id": PROJECT_ID
       },
        "placement":{
          "cluster_name": CLUSTER_NAME
       },
        "labels": {
          "team": "data-originis",
          "owner": "asnioby-hernandez",
          "task": "partner"
        },
        "pyspark_job":{
            "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/@script",
            "properties": {
                "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark.driver.memory":"4g",
                "spark.executor.memory":"4g",
                "spark.driver.cores": "4",
                "spark.executor.cores": "4",
                "spark.sql.sources.partitionOverwriteMode":"dynamic"
              },
            "args": [
                 "-source","@source",
                 "-name",  "@name",
                 "-type",  "@type",
                 "-table",  "@table"
              ]
       }
    }

def createNewGoogleCloudSDKCommand(pDag, pName, pCommand):
    return BashOperator(
        task_id=pName,
        bash_command=pCommand,
        dag=pDag
    )

def createNewDummyOperator(pDag, pName):
    return DummyOperator(task_id=pName, dag=pDag)

def createNewCreateClusterOperator(pDag, pModel, pClusterName, pClusterType):
    configCluster = {}
    if pClusterType:
        configCluster = json.loads(Variable.get(pClusterType))

        # preparamos la variable cluster
        configCluster['config']['lifecycle_config'] = {
            "idle_delete_ttl": tiempoIdle,
            "auto_delete_ttl": tiempoUp
        }

        # se agregan las claves de s3, configuracion extra para conectar
        configCluster['config']['software_config']['properties']['spark:spark.hadoop.fs.s3a.access.key'] = emr_ssh[
            'aws_access_key_id']
        configCluster['config']['software_config']['properties']['spark:spark.hadoop.fs.s3a.secret.key'] = emr_ssh[
            'aws_secret_access_key']

    configCluster['cluster_name'] = pClusterName
    configCluster['labels'] = {'owner': 'asnioby-hernandez', 'task':  pModel}
    configDag = dict(CLUSTER, **configCluster)

    return DataprocCreateClusterOperator(
        dag=pDag,
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster=configDag,
        region=REGION
    )

def createNewDeleteClusterOperator(pDag, pModel, pClusterName):
    return DataprocDeleteClusterOperator(
        dag=pDag,
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=pClusterName,
        region=REGION,
    )

def createNewSubmitJobOperator(pDag, pPySparkJob, pType):
    return DataprocSubmitJobOperator(
        dag=pDag,
        task_id="pyspark_task_{0}".format(pType),
        job=pPySparkJob,
        location=REGION,
        project_id=PROJECT_ID
    )

def createNewMergeBigQueryOperator(pDag, pModel, pSqsId, pSqsOrder, pDateStr, pMainId):
    entityId = pSqsId
    sqlAddId = ""
    if not pMainId:
        entityId = 'id'
        sqlAddId = ", {0} as id".format(pSqsId)

    return BigQueryOperator(
        task_id='merge_entity',
        sql="""
        DELETE FROM `peya-data-origins-stg.origins_data_stg.sqs_{0}`
        WHERE {3} IN (SELECT {1} 
                          FROM `peya-data-origins-stg.origins_data_message.sqs_{0}`
                          where yyyymmdd = date('{5}')
                    )
        ;

        insert into `peya-data-origins-stg.origins_data_stg.sqs_{0}`
        WITH entity_sqs as (
          select *, row_number() over (partition by {1} order by {2} desc) as message_number
          from `peya-data-origins-stg.origins_data_message.sqs_{0}`
          where yyyymmdd = date('{5}')
        )
        select * {4}
        from entity_sqs
        where message_number = 1;
        """.format(pModel, pSqsId, pSqsOrder, entityId, sqlAddId, pDateStr),
        use_legacy_sql=False,
        bigquery_conn_id=bq_conn,
        dag=pDag
    )

def createNewBigQueryOperator(pDag, pModule, pName, pDateId, pDateStr, pConnId):
    return BigQueryOperator(
        task_id='query_{0}'.format(pName),
        sql=open("{0}/sql/{1}/{2}.sql".format(home_path, pModule, pName), 'r').read().replace('@dateId', pDateId).replace('@dateStr', pDateStr),
        use_legacy_sql=False,
        bigquery_conn_id=pConnId,
        dag=pDag
    )

def createSensorTask(pDag,pProceso,pPeriodo):
  return HttpSensor(
    task_id='sensor_ok_{0}'.format(pProceso),
    http_conn_id=http_conn,
    endpoint='api/redshift/validate/talendJobStatus',
    request_params={"job": "{0}".format(pProceso), "fecha": "{0}".format(pPeriodo)},
    response_check=lambda response: True if "true" in response.text else False,
    mode='reschedule',
    poke_interval=60*20,
    dag=pDag,
  )

def createNewTriggerDagOperator(pDag, pDagName, pExecDate='{{ execution_date }}'):
    return TriggerDagRunOperator(
        task_id="trigger_dag_{}".format(pDagName),
        trigger_dag_id=pDagName,
        execution_date=pExecDate,
        dag=pDag,
    )

def createExternalSensorTask(pDag, pDagId, pTaskId=None, pDelta=0):
    return ExternalTaskSensor(
            task_id='sensor_ok_{0}'.format(pDagId),
            external_dag_id=pDagId,
            external_task_id=pTaskId,
            execution_delta=timedelta(minutes=pDelta),
            allowed_states=['success'],
            mode="reschedule",
            poke_interval=60*20,
            timeout=60*60*8,
            dag=pDag
    )

def createSqlTaskSensor(pDag, pDagId, pTaskId, pDate):
    task_id = 'sensor_ok_{0}'.format(pDagId)
    if pTaskId:
        task_id += '_' + pTaskId
    return SqlSensor(
        task_id=task_id,
        conn_id=airflow_conn,
        sql="""select coalesce((dr.state = 'success' or (t.state = 'success' and '{1}' != '') ), false)::int as status
               from dag d
                    left join dag_run dr on d.dag_id = dr.dag_id
                                                -- and not dr.external_trigger
                                                and date(dr.execution_date) = date('@exec_date')
                    left join task_instance t on d.dag_id = t.dag_id
                                                    and dr.execution_date = t.execution_date
                                                    and t.task_id = '{1}'
               where d.dag_id = '{0}'""".format(pDagId, pTaskId).replace('@exec_date', pDate),
        mode='reschedule',
        poke_interval=5*60,
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=4*60),
        dag=pDag
    )

def createNewS3ModifiedSensor(pDag, pName, pBucket, pKey):
    return PythonSensor(
        task_id="check_s3_{}".format(pName),
        dag=pDag,
        python_callable=checkS3ModifiedDate,
        op_kwargs={'pBucket': pBucket, 'pKey': pKey},
        mode='reschedule',
        poke_interval=60*20,
    )

def checkS3ModifiedDate(pBucket, pKey):
    command = 'aws2 s3api head-object --bucket {0} --key {1} --profile bigdata'.format(pBucket, pKey)
    print(command)
    stream = os.popen(command)
    description = json.loads(stream.read())
    print(json.dumps(description, indent=4, sort_keys=True))
    last_modified = description['LastModified']
    last_modified_datetime = datetime.strptime(last_modified[:19], '%Y-%m-%dT%H:%M:%S')
    print('last_modified ' + str(last_modified_datetime) + ' UTC')
    modified_date = last_modified_datetime.date()
    today_date = datetime.utcnow().date()
    print('if today is ' + str(today_date) + ' then')
    answer = modified_date == today_date
    if answer:
        print('GO EXTRACT')
    else:
        print('WAIT')
    return answer

def createSqsDAG(dConfig, dagName):
    try:
        start_date = datetime.strptime(d["start_date"], "%Y-%m-%d")
    except:
        start_date = None

    config = {"retries": dConfig['retries'],
              "start_date": start_date}
    new_dag = DAG(dagName, schedule_interval=dConfig['scheduler'], catchup=dConfig['catchup'],
                  default_args=dict(default_args, **config), max_active_runs=1)

    # checking_process = createNewDummyOperator(new_dag, 'checking_process')
    # ending_process = createNewDummyOperator(new_dag, 'ending_process')

    cluster_name = 'data-origins-dataproc-{0}'.format(dConfig['name'])
    # creamos el cluster en dataproc
    create_cluster = createNewCreateClusterOperator(new_dag, dConfig['model'], cluster_name,  dConfig.get('cluster_type'))
    delete_cluster = createNewDeleteClusterOperator(new_dag, dConfig['model'], cluster_name)

    for job in dConfig['pyspark_jobs']:
        jobTemplate = getPySparkJobTemplate()
        jobTemplate["placement"]["cluster_name"] = cluster_name
        jobTemplate["labels"]["task"] = dConfig['model']
        jobTemplate["pyspark_job"]["main_python_file_uri"] = jobTemplate["pyspark_job"]["main_python_file_uri"]\
            .replace('@model', dConfig['model'])\
            .replace('@script', dConfig['spark_script'])

        jobTemplate['pyspark_job']['args'] = [arg.replace('@source', dConfig['source_bucket'] + job['source_dir'])
                                   .replace('@target',  dConfig['target_bucket'] + job['target_dir'])
                                   .replace('@date_id', dConfig['date_id'])
                                   .replace('@days_back', str(dConfig['days_back']))
                                   .replace('@type', job['type'])
                                   .replace('@model', dConfig['model'])
                                   .replace('@schema', dConfig['schema'])
                               for arg in jobTemplate['pyspark_job']['args']
                              ]

        pyspark_task = createNewSubmitJobOperator(new_dag, jobTemplate, job['type'])
        airflow.utils.helpers.chain(*[create_cluster, pyspark_task, delete_cluster])

        if dConfig['entity'].get('merge'):
            merge_entity = createNewMergeBigQueryOperator(new_dag
                                                 , dConfig['model']
                                                 , dConfig['entity']['sqs_message_id']
                                                 , dConfig['entity']['sqs_message_order']
                                                 , dConfig['date_str']
                                                 , dConfig['entity']['main_id']
                                                )
            airflow.utils.helpers.chain(*[delete_cluster, merge_entity])

            for trigger in dConfig['triggers']:
                trigger_dag = createNewTriggerDagOperator(new_dag, trigger['name'])
                airflow.utils.helpers.chain(*[merge_entity, trigger_dag])

    return new_dag

def createEnumDAG(dConfig, dagName):
    try:
        start_date = datetime.strptime(d["start_date"], "%Y-%m-%d")
    except:
        start_date = None

    config = {"retries": dConfig['retries'],
              "start_date": start_date}
    new_dag = DAG(dagName, schedule_interval=dConfig['scheduler'], catchup=dConfig['catchup'],
                  default_args=dict(default_args, **config), max_active_runs=1)

    # checking_process = createNewDummyOperator(new_dag, 'checking_process')
    # ending_process = createNewDummyOperator(new_dag, 'ending_process')

    cluster_name = 'data-origins-dataproc-{0}'.format(dConfig['name'])
    # creamos el cluster en dataproc
    create_cluster = createNewCreateClusterOperator(new_dag, dConfig['model'], cluster_name,  dConfig.get('cluster_type'))
    delete_cluster = createNewDeleteClusterOperator(new_dag, dConfig['model'], cluster_name)

    for job in dConfig['pyspark_jobs']:
        if job.get('active'):
            jobTemplate = getPySparkEnumJobTemplate()
            jobTemplate["placement"]["cluster_name"] = cluster_name
            jobTemplate["labels"]["task"] = dConfig['model']
            jobTemplate["pyspark_job"]["main_python_file_uri"] = jobTemplate["pyspark_job"]["main_python_file_uri"]\
                .replace('@model', dConfig['model'])\
                .replace('@script', dConfig['spark_script'])

            jobTemplate['pyspark_job']['args'] = [arg.replace('@source', dConfig['source_bucket'] + (job.get('source_dir') or dConfig['source_dir']))
                                       .replace('@name', job['name'])
                                       .replace('@type', job['type'])
                                       .replace('@format', (job.get('format') or dConfig['format']))
                                       .replace('@schema', (job.get('schema') or dConfig['schema']))
                                   for arg in jobTemplate['pyspark_job']['args']
                                  ]

            pyspark_task = createNewSubmitJobOperator(new_dag, jobTemplate, job['name'])
            airflow.utils.helpers.chain(*[create_cluster, pyspark_task, delete_cluster])

    # airflow.utils.helpers.chain(*[checking_process, create_cluster])
    # airflow.utils.helpers.chain(*[delete_cluster, ending_process])

    return new_dag

def createQueryDAG(dConfig, dagName):
    try:
        start_date = datetime.strptime(d["start_date"], "%Y-%m-%d")
    except:
        start_date = None

    config = {"retries": dConfig['retries'],
              "start_date": start_date}
    new_dag = DAG(dagName, schedule_interval=dConfig['scheduler'], catchup=dConfig['catchup'],
                  default_args=dict(default_args, **config), max_active_runs=1)

    # checking_process = createNewDummyOperator(new_dag, 'checking_process')
    # ending_process = createNewDummyOperator(new_dag, 'ending_process')

    sensor_list = []
    for sensor in dConfig['sensors']:
        if sensor['active']:
            if sensor['type'] == 'AIRFLOW':
                new_sensor = createExternalSensorTask(new_dag, sensor['name'])
                sensor_list.append(new_sensor)

    query_list = []
    first_query = True
    for query in dConfig['queries']:
        if query['active']:
            if query['type'] == 'BQ':
                new_query = createNewBigQueryOperator(new_dag
                                                      , query['module']
                                                      , query['name']
                                                      , query.get('date_id') or dConfig['date_id']
                                                      , query.get('date_str') or dConfig['date_str']
                                                      , query.get('conn_id') or bq_conn
                                                      )
                if first_query:
                    first_query = False
                    for sensor in sensor_list:
                        airflow.utils.helpers.chain(*[sensor, new_query])
                else:
                    airflow.utils.helpers.chain(*[prev_query, new_query])
                prev_query = new_query

    # airflow.utils.helpers.chain(*[checking_process, create_cluster])
    # airflow.utils.helpers.chain(*[delete_cluster, ending_process])

    return new_dag

def createDataLakeDAG(dConfig, dagName):
    try:
        start_date = datetime.strptime(d["start_date"], "%Y-%m-%d")
    except:
        start_date = None

    config = {"retries": dConfig['retries'],
              "start_date": start_date}
    new_dag = DAG(dagName, schedule_interval=dConfig['scheduler'], catchup=dConfig['catchup'],
                  default_args=dict(default_args, **config), max_active_runs=1)

    checking_process = createNewDummyOperator(new_dag, 'checking_process')
    ending_process = createNewDummyOperator(new_dag, 'ending_process')

    cluster_name = 'data-origins-dataproc-{0}'.format(dConfig['name'])
    # creamos el cluster en dataproc
    create_cluster = createNewCreateClusterOperator(new_dag, dConfig['model'], cluster_name,  dConfig.get('cluster_type'))
    delete_cluster = createNewDeleteClusterOperator(new_dag, dConfig['model'], cluster_name)

    for job in dConfig['pyspark_jobs']:
        jobTemplate = getPySparkDataLakeJobTemplate()
        jobTemplate["placement"]["cluster_name"] = cluster_name
        jobTemplate["labels"]["task"] = dConfig['model']
        jobTemplate["pyspark_job"]["main_python_file_uri"] = jobTemplate["pyspark_job"]["main_python_file_uri"]\
            .replace('@model', dConfig['model'])\
            .replace('@script', dConfig['spark_script'])

        jobTemplate['pyspark_job']['args'] = [arg.replace('@source', job['source_bucket'] + job.get('source_dir'))
                                                   .replace('@name', job['name'])
                                                   .replace('@type', job['type'])
                                                   .replace('@table', job.get('target_table'))
                                                   .replace('@schema', (job.get('schema') or dConfig['schema']))
                                               for arg in jobTemplate['pyspark_job']['args']
                                              ]

        pyspark_task = createNewSubmitJobOperator(new_dag, jobTemplate, job['name'])
        airflow.utils.helpers.chain(*[create_cluster, pyspark_task, delete_cluster])

    airflow.utils.helpers.chain(*[checking_process, create_cluster])
    airflow.utils.helpers.chain(*[delete_cluster, ending_process])

    return new_dag

home_path = os.path.dirname(os.path.realpath(__file__))

#SQS DAGS
with open(home_path + '/config/dag_bag_sqs.json') as json_file:
    dag_bag = json.load(json_file)

for this_dag in dag_bag:
    d = dag_bag[this_dag]
    if d['active'] == True:
        dag_id = dag_name.format(d['model'])
        print("--->Generating DAG {0}".format(dag_id))
        globals()[dag_id] = createSqsDAG(d, dag_id)

#Enums DAGs
with open(home_path + '/config/dag_bag_enum.json') as json_file_enum:
    dag_bag_enum = json.load(json_file_enum)

for this_dag in dag_bag_enum:
    d = dag_bag_enum[this_dag]
    if d['active'] == True:
        dag_id = dag_name.format(d['model'])
        print("--->Generating DAG {0}".format(dag_id))
        globals()[dag_id] = createEnumDAG(d, dag_id)

#Datalake DAGs
with open(home_path + '/config/dag_bag_datalake.json') as json_file_enum:
    dag_bag_datalake = json.load(json_file_enum)

for this_dag in dag_bag_datalake:
    d = dag_bag_datalake[this_dag]
    if d['active'] == True:
        dag_id = dag_name.format(d['model'])
        print("--->Generating DAG {0}".format(dag_id))
        globals()[dag_id] = createDataLakeDAG(d, dag_id)

#Query DAGS
with open(home_path + '/config/dag_bag_query.json') as json_file:
    dag_bag_query = json.load(json_file)

for this_dag in dag_bag_query:
    d = dag_bag_query[this_dag]
    if d['active'] == True:
        dag_id = dag_name_query.format(d['model'])
        print("--->Generating DAG {0}".format(dag_id))
        globals()[dag_id] = createQueryDAG(d, dag_id)