from datetime import datetime, timedelta, date
from google.protobuf.duration_pb2 import Duration
import os
import stat
import airflow
import json
from airflow import macros
# from airflow.contrib.operators.ssh_operator import SSHOperator # paramiko
from airflow.hooks import S3Hook
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
# from airflow.operators.slack_operator import SlackAPIPostOperator # slackclient
from airflow.utils.trigger_rule import TriggerRule

#Importamos gcp
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator
)

# #BOTO3
import boto3

# currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
# sys.path.append(currentdir)
# from OPSLOG_PANDORA_S3_TO_DL_DWH_CONFIG import *

# SET VARIABLES FOR EXECUTION
dag_name = "DATA_ORIGINS_DH_S3_TO_BQ_{0}_DAG_2"
dag_folder = "DATA_ORIGINS_DH_S3_TO_BQ"

# SSH
ssh_conn = 'ssh_hadoop_datanode1_bi'

# S3
pandora_s3_bucket = "peyabi.deliveryhero.eu"
# pandora_source_dir = "dh-export/pandora/il_region_ccn_la_external"
pandora_source_dir = "dh-export/pandora/il_region_ccn_la_export"
pandora_target_dir = "refined/pandora"
data_file = 'data.gzmanifest'

aws_conn = "aws_s3_conn"
s3_live_bucket = "peyabi.datalake.live"
s3_utils_bucket = "peyabi.code.live"
home_dir = "bi-operations"
refined_dir = "refined/pandora"

# SSH
ssh_conn = 'ssh_hadoop_datanode1_ti'

# HDFS
hdfs_node = "hadoop-namenode-bi:9000"
hdfs_refined = "pandora"

# Redshift
redshift_conn = "dwh_redshift_prod"

actual_moth_id = '{{ execution_date.strftime("%Y%m") }}'
prev_moth_id = '{{ execution_date.replace(day=1).ds_add(ds, -10).strftime("%Y%m") }}'

daily_schedule = "0 11 * * *"
monthly_schedule = "0 11 1 * *"

our_team = "bi_operations" #Equipo al que pertenece el responsable del DAG
my_mail = "bryan.grill@pedidosya.com" #El mail del responsable del DAG
team_mail = "bi-ops-log@pedidosya.com" #Mail del equipo por si el owner no esta disponible
# extraInfo = ""  #Nada o algo que quieran destacar

#########################################################################
# CONFIGS DE GCP
#########################################################################

gcs_bucket = "data-origins-storage"

#conexiones
emr_ssh = json.loads(S3Hook.get_connection('aws_s3_conn').extra)
bq_conn = "bigquery_default"
http_conn = "http_service_audit"
airflow_conn = "airflow_db"

REGION = os.environ.get("GCP_LOCATION", "us-central1")
ZONE = os.environ.get("GCP_REGION", "us-central1b")
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
          "owner": "bryan-grill",
          "task": "partner"
        },
        "pyspark_job":{
            "main_python_file_uri":"gs://data-origins-storage/dmarts/scripts/@script",
            "properties": {
                "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                "spark.sql.sources.partitionOverwriteMode":"dynamic"
              },
            "args": [
                "-source_path", 's3a://@source_dir',
                "-live_path", 'gs://@target_dir',
                "-partition", '@date_id',
                "-model", '@model',
                "-conversions", "@conversions",
                "-partition_col", "@partition_col",
                "-country_col", "@country_col",
                "-overwrite_mode", "@overwrite_mode"
              ]
       }
    }
#########################################################################
# FIN
#########################################################################

# SET DEFALT ARGUMENTS
default_args = {
    'owner': 'bi_operations',
    'email': ['fernanda.gonzalez@pedidosya.com', 'asnioby.hernandez@pedidosya.com', 'bryan.grill@pedidosya.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

default_args_subdag = {
    'owner': 'bi_operations',
    'email': ['bryan.grill@pedidosya.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(minutes=40),
    'catchup': False,
    'retries': 0
}

# GET VARIABLES FROM AIRFLOW ENV
try:
    git_repo_path = Variable.get('git_bi_operations_path')
except:
    git_repo_path = "/root/airflow_extra/bi-operations"

dag_path = "{0}/DATA_ORIGINS_DH_S3_TO_BQ".format(git_repo_path)
py_path = "{0}/py".format(dag_path)
sql_path = "{0}/sql".format(dag_path)
sh_path = "{0}/sh".format(dag_path)
config_path = "{0}/config".format(dag_path)

# API VALIDATION
try:
    API_HOST = Variable.get('bi_api_service_host')
    API_PORT = Variable.get('bi_api_service_port')
except:
    # En caso de fallos, seteamos valores por defecto
    API_HOST = "10.0.91.124"
    API_PORT = "9003"

PROTOCOLO = "http://"
API_ENDPOINT = "{0}:{1}".format(API_HOST, API_PORT)

s3 = S3Hook(aws_conn_id=aws_conn)
s3b = boto3.client(
    's3',
    aws_access_key_id=s3.get_credentials().access_key,
    aws_secret_access_key=s3.get_credentials().secret_key
)

redshift_hook = PostgresHook(postgres_conn_id=redshift_conn)

# UTILS
def getScpComand(pBacket, pKey):
    theobjects = s3b.list_objects_v2(Bucket=pBacket, Prefix=pKey)
    distcpAll = "hadoop distcp -Dfs.s3a.access.key='{{ params.access_key }}' -Dfs.s3a.secret.key='{{ params.secret_key }}' 's3a://{{ params.source }}/*' 'hdfs://{{ params.target }}/'"
    cmdStr = ""

    try:
        listKey = []
        for object in theobjects['Contents']:
            key = object['Key']
            if key.find('yyyymm=') != -1 and key.endswith('.parquet'):
                listKey.append(key.split('/')[-2])

        listKey = list(set(listKey))
        distcpTemplate = "hadoop distcp -Dfs.s3a.access.key='{{ params.access_key }}' -Dfs.s3a.secret.key='{{ params.secret_key }}' 's3a://{{ params.source }}/@keyValue/*' 'hdfs://{{ params.target }}/@dateId'"
        for key in listKey:
            keyValue = key
            dateId = key.replace('yyyymmdd=','').replace('yyyymm=','')
            cmdStr += '\n' + distcpTemplate.replace('@keyValue', keyValue).replace('@dateId', dateId)
    except:
        pass
    print_stuff(cmdStr or distcpAll)
    return cmdStr or distcpAll

def print_stuff(text):
    print("-------- HERE'S YOUR STUFF --------")
    print(text)
    print("-------- HERE IT ENDS --------")

def validatedModelTotalRows(pSourceFolder, pDwhTable, pDwhFilter, **kwargs):
    try:
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn)
        sql_count = """select COALESCE(count(*), 0) as total from {0} where {1};""".format(pDwhTable, pDwhFilter or 'true')

        source_key = pandora_source_dir + '/' + pSourceFolder + '/' + data_file
        obj = s3b.get_object(Bucket=pandora_s3_bucket, Key=source_key)
        manisfest = json.loads(obj['Body'].read())

        row_count = manisfest['meta']['record_count']
        print("Total Rows Pandora: " + str(row_count))
        print("-------- PANDORA TABLE SCHEMA --------")
        for total in redshift_hook.get_first(sql_count):
            print("Total Rows Redshift: " + str(total))
            if int(row_count) == int(total):
                return 'slack_validation_passed'
        return 'slack_validation_failed'
    except:
        raise ValueError('Validation error!')

def createNewBranchValidated(pDag, pSourceFolder, pTableName, pDwhFilter):
    return BranchPythonOperator(
        task_id='model_validation_total',
        provide_context=True,
        python_callable=validatedModelTotalRows,
        op_args=[pSourceFolder, pTableName, pDwhFilter],
        dag=pDag
    )

def upload_files_to_s3(connection, bucket):
    for filename in os.listdir(git_repo_path + '/airflow/dags/' + dag_folder + '/emr/'):
        connection.load_file(git_repo_path + '/airflow/dags/' + dag_folder + '/emr/' + filename,
                             home_dir + '/' + dag_folder + '/spark_scripts/' + filename,
                             bucket_name=bucket,
                             replace=True,
                             encrypt=False)


def createSyncS3Bucket(pDag, pS3Bucket):
    return PythonOperator(
        task_id='sync_s3_bucket',
        python_callable=upload_files_to_s3,
        op_args=[s3, pS3Bucket],
        dag=pDag
    )


def s3_delete_all_objects(connection, bucket, folder):
    lista = connection.list_keys(bucket_name=bucket, prefix=folder)
    if lista:
        connection.delete_objects(bucket, lista)


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

# VALIDATION
def doTheValidation(pType, pSourceOrigen, pProjectOrigen, pSchemaOrigen
                    , pTableNameOrigen, pSourceDestino, pSchemaDestino, pTableNameDestino
                    , pDateFieldOrigen, pDateFieldDestino, pDateValueOrigen, pDateValueDestino
                    , pBranchOk, pBranchError, **kwargs):
    dim_req = "http://localhost:9003/api/count?operator=eq&sourceOrigen={sourceOrigen}&projectOrigen={projectOrigen}&schemaOrigen={schemaOrigen}&tableNameOrigen={tableNameOrigen}&sourceDestino={sourceDestino}&schemaDestino={schemaDestino}&tableNameDestino={tableNameDestino}&cluster={cluster}&isParticionedByDateDestino=false"
    fact_req = "http://localhost:9003/api/count?operator=eq&sourceOrigen={sourceOrigen}&projectOrigen={projectOrigen}&schemaOrigen={schemaOrigen}&tableNameOrigen={tableNameOrigen}&dateFieldOrigen={dateFieldOrigen}&dateOrigen={dateValueOrigen}&sourceDestino={sourceDestino}&schemaDestino={schemaDestino}&tableNameDestino={tableNameDestino}&dateFieldDestino={dateFieldDestino}&dateDestino={dateValueDestino}&useDateFunctionDestino=true&useDateFunctionOrigen=false&cluster={cluster}"
    req = ""
    if pType == "DIM":
        req = dim_req.format(sourceOrigen=pSourceOrigen,
                             projectOrigen=pProjectOrigen,
                             schemaOrigen=pSchemaOrigen,
                             tableNameOrigen=pTableNameOrigen,
                             sourceDestino=pSourceDestino,
                             schemaDestino=pSchemaDestino,
                             tableNameDestino=pTableNameDestino,
                             cluster='hadoop-namenode-ti')
    else:
        req = fact_req.format(sourceOrigen=pSourceOrigen,
                              projectOrigen=pProjectOrigen,
                              schemaOrigen=pSchemaOrigen,
                              tableNameOrigen=pTableNameOrigen,
                              sourceDestino=pSourceDestino,
                              schemaDestino=pSchemaDestino,
                              tableNameDestino=pTableNameDestino,
                              dateFieldOrigen=pDateFieldOrigen,
                              dateFieldDestino=pDateFieldDestino,
                              dateValueOrigen=pDateValueOrigen,
                              dateValueDestino=pDateValueDestino,
                              cluster='hadoop-namenode-ti'
                              )
    print("--->Requesting:{0}".format(req))
    response = requests.get(req)
    print('---->Response:{0}'.format(response.text))
    if response.status_code == 200:
        json_res = json.loads(response.text)
        if json_res['cantRegistrosOrigen'] == -1 or json_res['cantRegistrosDestino'] == -1:
            raise ValueError('API error - return -1')
        if json_res['validacion']:
            return pBranchOk  # 'slack_validation_passed'
        else:
            return pBranchError  # 'slack_validation_failed'
    else:
        raise ValueError('API error - no response')

def getVarSql(pConnId, pSql):
    try:
        redshift_hook = PostgresHook(postgres_conn_id=pConnId)
        total = redshift_hook.get_first(pSql)
        print("Total Rows Redshift: " + str(total[0]))
        return total[0] or 0
    except:
        raise ValueError('count error!')

# New DAGs
def toDeleteOrNotToDelete(pDelete, **kwargs):
    if pDelete != "":
        return "bq_delete_previous_data"
    else:
        return "not_doing_anything"


def createNewRedshiftToS3(pDag, pQuery, pS3Bucket, pS3Prefix):
    return PostgresOperator(
        task_id='rds_to_s3',
        sql="""
            UNLOAD ('{{ params.query }}')
            TO '{{ params.target }}'
            HEADER
            FORMAT CSV
            PARALLEL OFF
            ACCESS_KEY_ID '{{ params.access_key }}'
            SECRET_ACCESS_KEY '{{ params.secret_key }}'
            ALLOWOVERWRITE;
            """,
        postgres_conn_id="redshift_testing_db",
        params={
            'target': 's3://{0}/{1}'.format(pS3Bucket, pS3Prefix),
            'query': pQuery,
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
        },
        dag=pDag)

# def createNewAuditInsert(pDag, pSource, pTable):
#     return BigQueryOperator(
#         task_id='bq_update_audit_table',
#         sql='sql/Update_Audit_Table.sql',
#         use_legacy_sql=False,
#         bigquery_conn_id='peya_bigquery',
#         params={'updated_dataset': '\'{0}\''.format(pTable.split(".")[1]),
#                 'updated_table': '\'{0}\''.format(pTable.split(".")[2]),
#                 'updated_source': '\'{0}\''.format(pSource),
#                 'today_date': today_date
#                 },
#         dag=pDag
#     )

def createNewDummyOperator(pDag, pName):
    return DummyOperator(task_id='dummy_{0}'.format(pName),
                         pool='default_pool',
                         dag=pDag)


def createNewBranchDeleterPythonOperator(pDag, pDelete):
    return BranchPythonOperator(
        task_id='delete_or_not_delete',
        provide_context=True,
        python_callable=toDeleteOrNotToDelete,
        op_kwargs={'pDelete': pDelete},
        dag=pDag
    )


def createNewEmptyS3Bucket(pModel, pDag, pS3Bucket, pS3Folder, pPostfix, pDateId):
    return PythonOperator(
        task_id='empty_s3_bucket_sf_{0}_{1}'.format(pModel, pPostfix),
        python_callable=s3_delete_all_objects,
        op_args=[s3, pS3Bucket, '{0}/{1}/{2}/'.format(pS3Folder, pModel, pDateId)],
        dag=pDag
    )

def createNewS3toRedshiftFull(pDag, pRedshiftConn, pImportTable, pDwhTable, pModel, pS3Key, dwhColumns, pDwhFilter):
    return PostgresOperator(
        task_id="s3_to_redshift_{0}".format(pModel),
        sql="""
            DELETE FROM {{ params.import_table }};

            COPY {{ params.import_table }}
            FROM 's3://{{ params.source }}'
            IAM_ROLE  'arn:aws:iam::860782241405:role/biRDSRole'
            FORMAT AS PARQUET;

            DELETE FROM {{ params.dwh_table }}
            WHERE {{ params.filter }};

            INSERT INTO {{ params.dwh_table }}{{ params.column_list }} 
            SELECT * FROM {{ params.import_table }}
            WHERE {{ params.filter }};

            DELETE FROM {{ params.import_table }};
            """,
        postgres_conn_id=pRedshiftConn,
        params={
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
            'source': pS3Key,
            'import_table': pImportTable,
            'dwh_table': pDwhTable,
            'column_list': dwhColumns,
            'filter': pDwhFilter or 'true'
        },
        dag=pDag
    )


def createNewS3toRedshiftMonth(pDag, pRedshiftConn, pImportTable, pDwhTable, pDwhDateField, pModel, pS3Key, dwhColumns,
                               pDateId, pDwhFilter='true'):
    return PostgresOperator(
        task_id="s3_to_redshift_{0}".format(pModel),
        sql="""
            DELETE FROM {{ params.import_table }};
            
            COPY {{ params.import_table }}
            FROM 's3://{{ params.source }}/yyyymm=@date_id'
            IAM_ROLE  'arn:aws:iam::860782241405:role/biRDSRole'
            FORMAT AS PARQUET;

            DELETE FROM {{ params.dwh_table }} 
            WHERE TO_CHAR({{ params.dwh_date_field }}, 'YYYYMM') = '@date_id'
                    and {{ params.filter }};

            INSERT INTO {{ params.dwh_table }}({{ params.column_list }}) 
            SELECT * FROM {{ params.import_table }} 
            WHERE TO_CHAR({{ params.dwh_date_field }}, 'YYYYMM') = '@date_id'
                    and {{ params.filter }};
            
            DELETE FROM {{ params.import_table }};
            """.replace('@date_id', pDateId),
        postgres_conn_id=pRedshiftConn,
        params={
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
            'source': pS3Key,
            'import_table': pImportTable,
            'dwh_table': pDwhTable,
            'dwh_date_field': pDwhDateField,
            'column_list': dwhColumns,
            'filter': pDwhFilter or 'true'
        },
        dag=pDag
    )

def createNewS3toRedshiftMonthExternal(pDag, pRedshiftConn, pExternalTable, pDwhTable, pDwhDateField, pModel, pS3Key, dwhColumns,
                                       extColumns, pDateId, dwhFilter='true'):
    return PostgresOperator(
        task_id="s3_to_redshift_{0}".format(pModel),
        sql="""
            alter table {{ params.external_table }} add if not exists partition (yyyymm='@date_id') location 's3://{{ params.source }}/yyyymm=@date_id';
            
            BEGIN;
                DELETE FROM {{ params.dwh_table }} 
                WHERE TO_CHAR({{ params.dwh_date_field }}, 'YYYYMM') = '@date_id'
                        and {{ params.filter }};
    
                INSERT INTO {{ params.dwh_table }}({{ params.dwh_columns }}) 
                SELECT {{ params.ext_columns }} 
                FROM {{ params.external_table }} 
                WHERE yyyymm = '@date_id' 
                        and {{ params.filter }};
            COMMIT;
            END;
            """.replace('@date_id', pDateId),
        postgres_conn_id=pRedshiftConn,
        autocommit=True,
        params={
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
            'source': pS3Key,
            'external_table': pExternalTable,
            'dwh_table': pDwhTable,
            'dwh_date_field': pDwhDateField,
            'dwh_columns': dwhColumns,
            'ext_columns': extColumns or dwhColumns,
            'filter': dwhFilter or 'true'
        },
        dag=pDag
    )

def createNewS3toRedshiftFullExternal(pDag, pRedshiftConn, pExternalTable, pDwhTable, pModel, dwhColumns, extColumns, dwhFilter='true'):
    return PostgresOperator(
        task_id="s3_to_redshift_{0}".format(pModel),
        sql="""
            DELETE FROM {{ params.dwh_table }} 
            WHERE {{ params.filter }} ;

            INSERT INTO {{ params.dwh_table }}({{ params.dwh_columns }}) 
            SELECT {{ params.ext_columns }} 
            FROM {{ params.external_table }} 
            where {{ params.filter }}
            """,
        postgres_conn_id=pRedshiftConn,
        params={
            'external_table': pExternalTable,
            'dwh_table': pDwhTable,
            'dwh_columns': dwhColumns,
            'ext_columns': extColumns or dwhColumns,
            'filter': dwhFilter or 'true'
        },
        dag=pDag
    )

def createNewS3toRedshiftCustom(pDag, pRedshiftConn, pImportTable, pDwhTable, pDwhFilter, pDwhDateField, pModel,
                                pS3Bucket,
                                pS3Folder, pS3Prefix, dwhCustomInsert):
    return PostgresOperator(
        task_id="s3_to_redshift_{0}".format(pModel),
        sql="""
            DELETE FROM {{ params.import_table }};

            COPY {{ params.import_table }}
            FROM 's3://{{ params.source }}/{{ ds_nodash }}'
            IAM_ROLE  'arn:aws:iam::860782241405:role/biRDSRole'
            FORMAT AS PARQUET;

            DELETE FROM {{ params.dwh_table }} 
            WHERE date({{ params.dwh_date_field }}) = date('{{ ds }}');

            {{ params.custom_insert }}
             WHERE date({{ params.dwh_date_field }}) = date('{{ ds }}');
            """,
        postgres_conn_id=pRedshiftConn,
        params={
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
            'source': '{0}/{1}/{2}_parquet'.format(pS3Bucket, pS3Folder, pS3Prefix),
            'import_table': pImportTable,
            'dwh_table': pDwhTable,
            'dwh_filter': pDwhFilter,
            'dwh_date_field': pDwhDateField,
            'custom_insert': dwhCustomInsert
        },
        dag=pDag
    )


def createNewS3toRedshiftDim(pDag, pRedshiftConn, pImportTable, pDwhTable, pDwhColumns, pModel, pS3Bucket, pS3Folder,
                             pS3Prefix,
                             pDateId):
    return PostgresOperator(
        task_id="s3_to_redshift_{0}".format(pModel),
        sql="""
            DELETE FROM {{ params.import_table }};
            
            COPY {{ params.import_table }}
            FROM '{{ params.source }}/'
            IAM_ROLE  'arn:aws:iam::860782241405:role/biRDSRole'
            FORMAT AS PARQUET;

            DELETE FROM {{ params.dwh_table }};

            INSERT INTO {{ params.dwh_table }} 
            SELECT {{ params.insert_columns }} FROM {{ params.import_table }};
            
            DELETE FROM {{ params.import_table }};
            """.replace('@date_id', pDateId),
        postgres_conn_id=pRedshiftConn,
        params={
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
            'source': '{0}/{1}/{2}'.format(pS3Bucket, pS3Folder, pS3Prefix),
            'import_table': pImportTable,
            'dwh_table': pDwhTable,
            'insert_columns': pDwhColumns
        },
        dag=pDag
    )


def createNewS3toOldHDFSDim(pDag, pSource, pTarget):
    return SSHOperator(
        task_id='s3_to_hdfs',
        command="""
                pwd
                hdfs dfs -rm -r hdfs://{{ params.target }}
                hadoop distcp -Dfs.s3a.access.key='{{ params.access_key }}' -Dfs.s3a.secret.key='{{ params.secret_key }}' 's3a://{{ params.source }}/*' 'hdfs://{{ params.target }}/'
                """,
        timeout=20,
        ssh_conn_id=ssh_conn,
        params={
            'target': pTarget,
            'source': pSource,
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
        },
        dag=pDag
    )

def createNewS3toOldHDFSFull(pDag, pSource, pTarget):
    return SSHOperator(
        task_id='s3_to_hdfs',
        command="""
                pwd
                hdfs dfs -rm -r hdfs://{{ params.target }}/*
                @cmd""".replace('@cmd', getScpComand(s3_live_bucket, pSource.replace(s3_live_bucket + '/', ''))),
        timeout=20,
        ssh_conn_id=ssh_conn,
        params={
            'target': pTarget,
            'source': pSource,
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
        },
        dag=pDag
    )


def createNewS3toOldHDFSMonth(pDag, pSource, pTarget, pParitionFolder=''):
    return SSHOperator(
        task_id='s3_to_hdfs',
        command="""
                pwd
                hdfs dfs -rm -r hdfs://{{ params.target }}/@partition_folder/*
                hadoop distcp -Dfs.s3a.access.key='{{ params.access_key }}' -Dfs.s3a.secret.key='{{ params.secret_key }}' 's3a://{{ params.source }}/yyyymm=@partition_folder/*' 'hdfs://{{ params.target }}/@partition_folder/'
                """.replace('@partition_folder', pParitionFolder),
        timeout=20,
        ssh_conn_id=ssh_conn,
        params={
            'target': pTarget,
            'source': pSource,
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
        },
        dag=pDag
    )


def createNewS3toOldHDFSDay(pDag, pSource, pTarget, pParitionFolder=''):
    return SSHOperator(
        task_id='s3_to_hdfs_{0}'.format(pParitionFolder),
        command="""
                pwd
                hdfs dfs -rm -r {{ params.target }}/@partition_folder/*
                hadoop distcp -Dfs.s3a.access.key='{{ params.access_key }}' -Dfs.s3a.secret.key='{{ params.secret_key }}' '{{ params.source }}/yyyymmdd=@partition_folder/*' '{{ params.target }}@partition_folder/'
                """.replace('@partition_folder', pParitionFolder),
        timeout=20,
        ssh_conn_id=ssh_conn,
        params={
            'target': pTarget,
            'source': pSource,
            'access_key': s3.get_credentials().access_key,
            'secret_key': s3.get_credentials().secret_key,
        },
        dag=pDag
    )


def createNewSuccessSlackOperator(pDag, pDagName):
    return SlackAPIPostOperator(
        dag=pDag,
        task_id='slack_validation_passed',
        channel=Variable.get('slack_delivery_partners'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':package::heavy_check_mark: - {time} - {dag} has validated data'.format(
            dag='dag --> {}'.format(pDagName),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S')
        ),
        retries=0
    )


def createNewErrorSlackOperator(pDag, pDagName):
    return SlackAPIPostOperator(
        dag=pDag,
        task_id='slack_validation_failed',
        channel=Variable.get('slack_delivery_partners'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':package::failed: - {time} - {dag} has not completed due to validation error'.format(
            dag='dag --> {}'.format(pDagName),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S'),
        ),
        retries=0
    )


def createNewSuccessSlackTrOperator(pDag, pDagName, pModel):
    return SlackAPIPostOperator(
        dag=pDag,
        task_id='slack_process_success_{}'.format(pModel),
        trigger_rule=TriggerRule.ALL_SUCCESS,
        channel=Variable.get('slack_delivery_partners'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':package::heavy_check_mark: - {time} - {dag} has completed'.format(
            dag='dag --> {0}_{1}'.format(pDagName, pModel),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S')
        ),
        retries=0
    )


def createNewErrorSlackTrOperator(pDag, pDagName, pModel):
    return SlackAPIPostOperator(
        dag=pDag,
        task_id='slack_process_error_{}'.format(pModel),
        trigger_rule=TriggerRule.ONE_FAILED,
        channel=Variable.get('slack_delivery_partners'),
        icon_url=Variable.get('slack_icon_url'),
        token=Variable.get('slack_token'),
        text=':package::failed: - {time} - {dag} has not completed'.format(
            dag='dag --> {0}_{1}'.format(pDagName, pModel),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S'),
        ),
        retries=0
    )


def createNewBranchValidatorBqRedshiftPythonOperator(pDag, pType, pSourceOrigen, pProjectOrigen, pSchemaOrigen,
                                                     pTableNameOrigen, pSourceDestino, pSchemaDestino,
                                                     pTableNameDestino, pDateFieldOrigen, pDateFieldDestino,
                                                     pDateValue):
    return BranchPythonOperator(
        task_id='validation_bq_to_redshift',
        provide_context=True,
        python_callable=doTheValidation,
        op_kwargs={'pType': pType,
                   'pSourceOrigen': pSourceOrigen,
                   'pProjectOrigen': pProjectOrigen,
                   'pSchemaOrigen': pSchemaOrigen,
                   'pTableNameOrigen': pTableNameOrigen,
                   'pSourceDestino': pSourceDestino,
                   'pSchemaDestino': pSchemaDestino,
                   'pTableNameDestino': pTableNameDestino,
                   'pDateFieldOrigen': pDateFieldOrigen,
                   'pDateFieldDestino': pDateFieldDestino,
                   'pDateValueOrigen': pDateValue,
                   'pDateValueDestino': pDateValue,
                   'pBranchOk': 'slack_validation_passed',
                   'pBranchError': 'slack_validation_failed'
                   },
        dag=pDag
    )


def createNewBranchValidatorBqHDFSPythonOperator(pDag, pModel, pType, pSourceOrigen, pProjectOrigen, pSchemaOrigen,
                                                 pTableNameOrigen, pSourceDestino, pSchemaDestino, pTableNameDestino,
                                                 pDateFieldOrigen, pDateFieldDestino, pDateValueOrigen,
                                                 pDateValueDestino):
    return BranchPythonOperator(
        task_id='validation_bq_to_hdfs',
        provide_context=True,
        python_callable=doTheValidation,
        op_kwargs={'pType': pType,
                   'pSourceOrigen': pSourceOrigen,
                   'pProjectOrigen': pProjectOrigen,
                   'pSchemaOrigen': pSchemaOrigen,
                   'pTableNameOrigen': pTableNameOrigen,
                   'pSourceDestino': pSourceDestino,
                   'pSchemaDestino': pSchemaDestino,
                   'pTableNameDestino': pTableNameDestino,
                   'pDateFieldOrigen': pDateFieldOrigen,
                   'pDateFieldDestino': pDateFieldDestino,
                   'pDateValueOrigen': pDateValueOrigen,
                   'pDateValueDestino': pDateValueDestino,
                   'pBranchOk': 'hdfs_to_s3_' + pModel,
                   'pBranchError': 'slack_validation_failed'
                   },
        dag=pDag
    )


def createNewS3ModifiedSensor(pDag, pBucket, pKey):
    return PythonSensor(
        task_id="check_modified_s3",
        dag=pDag,
        python_callable=checkS3ModifiedDate,
        op_kwargs={'pBucket': pBucket, 'pKey': pKey},
        mode='reschedule',
        poke_interval=60 * 20,
        # retries = 0,
    )


def createNewCreateClusterOperator(pDag, pModel, pClusterName, pClusterType):
    configCluster = {}
    configCluster['cluster_name'] = pClusterName
    configCluster['labels'] = {'owner': 'bryan-grill', 'task':  pModel}
    configDag = dict(CLUSTER, **configCluster)

    return DataprocCreateClusterOperator(
        dag=pDag,
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster=configDag,
        pool='default_pool',
        region=REGION
    )

def createNewDeleteClusterOperator(pDag, pModel, pClusterName):
    return DataprocDeleteClusterOperator(
        dag=pDag,
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=pClusterName,
        pool='default_pool',
        region=REGION,
    )

def createNewSubmitJobOperator(pDag, pPySparkJob, pType):
    return DataprocSubmitJobOperator(
        dag=pDag,
        task_id="pyspark_task_{0}".format(pType),
        job=pPySparkJob,
        pool="default_pool",
        location=REGION,
        project_id=PROJECT_ID
    )

def hello_world():
    print("hello world")


def createDAGFull(dConfig, dagName):
    config = {"retries": dConfig['retries'],
              "start_date": datetime(dConfig['start_year'], dConfig['start_month'], dConfig['start_day'])}
    new_dag = DAG(dagName, schedule_interval=dConfig['scheduler'], catchup=dConfig['catchup'],
                  default_args=dict(default_args, **config), max_active_runs=4)

    # Notificaction
    # if dConfig['send_slack']:
    #     slack_success = createNewSuccessSlackTrOperator(new_dag, dagName, dConfig['model'])
    #     slack_error = createNewErrorSlackTrOperator(new_dag, dagName, dConfig['model'])
    # else:
    #     slack_success = createNewDummyOperator(new_dag, 'success')
    #     slack_error = createNewDummyOperator(new_dag, 'error')

    # SPARK PROCESSING
    # sync_s3 = createSyncS3Bucket(new_dag, s3_utils_bucket)
    cluster_name = 'data-origins-dataproc-{0}'.format(dConfig['name'])
    # create_cluster = createNewCreateClusterOperator(new_dag, dConfig['name'], cluster_name,  dConfig.get('cluster_type'))
    # delete_cluster = createNewDeleteClusterOperator(new_dag, dConfig['name'], cluster_name)


    print_2 = PythonOperator(
        task_id='print_2',
        python_callable=hello_world,
        dag=new_dag
    )
    create_cluster = createNewDummyOperator(new_dag, 'create_cluster')
    delete_cluster = createNewDummyOperator(new_dag, 'delete_cluster')

    for sub_job in dConfig['tables']:
        table = dConfig['tables'][str(sub_job)]
        if table['active']:
            if table['types_conversion']:
                aux = []
                separator = ","
                for key, value in table['types_conversion'].items():
                    aux.append(key + ':' + separator.join(value))
                separator = ";"
                conversion_string = separator.join(aux)
            else:
                conversion_string = 'False'
            # PARTITION COLUMN
            if table['filter_column']:
                partition_col = table['filter_column']
            else:
                partition_col = 'False'

            # COUNTRY ID COLUMN
            if table['country_col']:
                country_col = table['country_col']
            else:
                country_col = 'False'

            jobTemplate = getPySparkJobTemplate()
            jobTemplate["placement"]["cluster_name"] = cluster_name
            jobTemplate["labels"]["task"] = table['model']
            jobTemplate["pyspark_job"]["main_python_file_uri"] = jobTemplate["pyspark_job"]["main_python_file_uri"] \
                .replace('@model', table['model']) \
                .replace('@script', table['spark_script'])

            jobTemplate['pyspark_job']['args'] = [arg.replace('@script', table['spark_script'])
                                                         .replace('@s3_utils_bucket', s3_utils_bucket)
                                                         .replace('@home_dir', home_dir).replace('@dag_folder', dag_folder)
                                                         .replace('@source_dir',
                                                                  table['source_bucket'] + '/' + table[
                                                                      'source_dir'] + '/' +
                                                                  table['source_folder'])
                                                         .replace('@target_dir',
                                                                  gcs_bucket + '/' + table['target_dir'] + '/' +
                                                                  table['target_folder'])
                                                         .replace('@date_id', table['date_id'])
                                                         .replace('@model', table['model'])
                                                         .replace('@conversions', conversion_string)
                                                         .replace('@partition_col', partition_col)
                                                         .replace('@country_col', country_col)
                                                         .replace('@overwrite_mode', table.get('overwrite_mode') or 'static')
                                                     for arg in jobTemplate['pyspark_job']['args']]

            # pyspark_task = createNewSubmitJobOperator(new_dag, jobTemplate, table['model'])
            pyspark_task = createNewDummyOperator(new_dag, table['model'])
            airflow.utils.helpers.chain(*[create_cluster, pyspark_task, delete_cluster])

    # CHECK UPDATE STATUS
    # check_s3_modified = createNewS3ModifiedSensor(new_dag, pandora_s3_bucket, dConfig['source_dir'] + '/' + dConfig['source_folder'] + '/' + data_file)

    # airflow.utils.helpers.chain(*[check_s3_modified, emr_processing])
    # airflow.utils.helpers.chain(*[sync_s3, emr_processing])

    # s3_to_redshift = createNewDummyOperator(new_dag, 'redshift')

    # airflow.utils.helpers.chain(*[create_cluster, add_tasks])
    # airflow.utils.helpers.chain(*[add_tasks, [slack_success,slack_error]])
    return new_dag

home_path = os.path.dirname(os.path.realpath(__file__))

# CONSTRUCCION DEL DAG GENERAL
with open(home_path + '/config/dag_bag_config.json') as json_file:
    dag_bag = json.load(json_file)

for this_dag in dag_bag:
    d = dag_bag[this_dag]
    if d['active'] == True:
        dag_id = dag_name.format(d['name'])
        print("--->Generating DAG {0}".format(dag_id))
        globals()[dag_id] = createDAGFull(d, dag_id)