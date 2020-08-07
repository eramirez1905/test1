# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, date
import airflow
import json

from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.models import TaskInstance

from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

# EMR OPERATORS
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.hooks import SSHHook

import requests

################################################################################################
# DAG Genera entidades de Ordenes, con data NRT
# fact_orders
# fact_orders_discount
# fact_orders_details
# fact_orders_details_options
# fact_orders_data
################################################################################################


# SLACK
slack_channel = Variable.get('slack_channel_core_tech')
slack_token = Variable.get('slack_token')
slack_ok_emoji = ':bi-icon-ok:'
slack_error_emoji = ':bi-icon-alert:'

# SET DEFALT ARGUMENTS
default_args = {
    'owner': 'bi_core_tech',
    'depends_on_past': False,
    'email': ['carlos.jaime@pedidosya.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15),
    'catchup': False,
    'scheduler': "30 6 * * *",
    'start_date': datetime(2020, 6, 20),
}

# GET VARIABLES FROM AIRFLOW ENVIRONMENT
try:
    git_repo_path = Variable.get('git_bi_core_path')
except:
    git_repo_path = '/root/airflow_extra/bi_core_path'

try:
    git_emr_path = Variable.get('git_bi_core_emr')
except:
    git_emr_path = "/home/hadoop/repo/bi-core-tech/airflow/dags"

# SET DAG CUSTOM CONFIG
dag_name = 'CORE_Daily_Orders'
dag_config = 'core_daily_orders_config.json'
dag_base_config = 'core_daily_orders_base.json'

dag_path = "{0}/airflow/dags/{1}".format(git_repo_path, dag_name)
config_path = "{0}/config".format(dag_path)

emr_path = "{0}/{1}".format(git_emr_path, dag_name)

# DATETIME VARIABLES
today_date = '\'' + datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S") + '\''
today_date_short = datetime.strftime(datetime.now(), "%Y-%m-%d")
today_date_short_yyyymmdd = datetime.strftime(datetime.now(), "%Y%m%d")
yesterday_date_short = datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d")

# hooks
emr_info = EmrHook('aws_emr').get_conn()
emr_ssh = SSHHook.get_connection('emr_ssh')

# Config file load Base (json)
# Clean orders data
with open('{0}/{1}'.format(config_path, dag_base_config)) as json_file:
    dag_bag_base = json.load(json_file)

# Config file load (json)
# Entities (Facts)
with open('{0}/{1}'.format(config_path, dag_config)) as json_file:
    dag_bag = json.load(json_file)

# S3
s3_bucket = ''
s3_folder = ''

########################################################################################################################
## Audit Procesos
########################################################################################################################

# GET VARIABLES FROM AIRFLOW ENVIRONMENT
try:
    API_HOST = "10.0.90.144"
    API_PORT = "8406"
    API_SECRET_KEY = Variable.get('bi_api_service_hive_secret_key')
except:
    # En caso de fallos, seteamos valores por defecto
    API_HOST = "10.0.90.144"
    API_PORT = "8406"
    API_SECRET_KEY = ""

# Parametros de servicio de logging
PROTOCOLO = "http://"
API_URL = "{0}:{1}".format(API_HOST, API_PORT)
headers = {'Authorization': API_SECRET_KEY, 'content-type': 'application/json'}

# Procedimientos de registro/actualización de log de ejecución
def insertRecord(**kwargs):
    try:
        params = {"processName": str(kwargs['process']), "systemName": "Airflow", "status": "ON_PROCESS"}
        endpoint = "{0}{1}/api/process/start".format(PROTOCOLO, API_URL)
        r = requests.post(url=endpoint, data=json.dumps(params), headers=headers)
        print(r.status_code)

        if r.status_code == 200:
            print('Insert audit record ok')
            data = r.json()
            idRecord = data['id']
            print('** idRecord: {}'.format(idRecord))
            kwargs['ti'].xcom_push(key='id', value=idRecord)
        else:
            raise ValueError('Error insert audit record')
    except Exception as ex:
        print(ex)

def updateRecord(**kwargs):
    try:

        # Verifica stado actual de la task pasada por parametro
        dag_instance = kwargs['dag']
        operator_instance = dag_instance.get_task(kwargs['check_task_id'])
        task_status = TaskInstance(task=operator_instance, execution_date=kwargs['execution_date']).current_state()

        task_status = task_status.upper()

        print('** UPDATE {0}').format(task_status)

        idRecord = kwargs['ti'].xcom_pull(key='id', task_ids=kwargs['logging_task_id'])

        print('***** xcom_pull result {0}'.format(idRecord))

        params = {"id": idRecord, "status": task_status}
        endpoint = "{0}{1}/api/process/finalize".format(PROTOCOLO, API_URL)
        r = requests.post(url=endpoint, data=json.dumps(params), headers=headers)
        if r.status_code == 200:
            print('Insert audit record ok')
        else:
            raise ValueError('Error insert audit record')

    except Exception as ex:
        print(ex)


def loggingProcess(pDag, **kwargs):
    print('** Logging {0}'.format(kwargs['log']))
    if kwargs['log'].upper() == 'INSERT':
        logging = PythonOperator(
            dag=pDag,
            task_id=kwargs['task_id'],
            python_callable=insertRecord,
            provide_context=True,
            op_kwargs={"process": kwargs['process']}
        )
    else:
        # UPDATE Logging, with ERROR or SUCCESS
        logging = PythonOperator(
            task_id=kwargs['task_id'],
            python_callable=updateRecord,
            provide_context=True,
            op_kwargs={"dag": pDag,
                       "check_task_id": kwargs['check_task_id'],
                       "logging_task_id": kwargs['logging_task_id']},
            dag=pDag
        )

    return logging


########################################################################################################################

def createDummyOperator(pDag, pName):
    return DummyOperator(task_id='Dummy_{0}'.format(pName), trigger_rule='none_failed', dag=pDag)

def create_spark_steps_dag(**kwargs):
    # bucket = 's3://{}/{}'.format(s3_bucket, s3_folder)

    return {
        'Name': '{0}'.format(kwargs.get("step")),
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--master", "local[*]",
                "--conf", "spark.local.dir=/mnt/test",
                "--driver-memory", "18G",
                "--executor-memory", "18G",
                "--driver-cores", "4",
                "--executor-cores", "4",
                "{0}{1}".format(emr_path, kwargs.get("py_spark", "")),
                "--s3_source_path", kwargs.get("s3_source_path", ""),
                "--s3_destination_path", kwargs.get("s3_destination_path", ""),
                "--data_history", kwargs.get("data_history", "")
            ]
        }
    }


def create_sub_dag(p_owner, p_email, p_start_date):
    return {
        'owner': p_owner,
        'depends_on_past': False,
        'email': p_email,
        'email_on_failure': True,
        'email_on_retry': True,
        'start_date': p_start_date,
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }


def create_job_flow(dag_name, extra_info=""):
    executionDay = '{{ execution_date.strftime("%Y%m%d")}}'
    dagName = dag_name  # El nombre del DAG al que pertenece
    extraInfo = extra_info  # Nada o algo que quieran destacar, por ejemplo para diferenciar que corre cada una hora, una vez al dia, corrida historica, etc

    logFolderInS3 = 's3://peyabi.datalake.live/bi-core-tech/logs/'
    bootstrapLocationInS3 = 's3://peyabi.code.live/bi-core-tech/bootstrap/bootstrap_git_sync-customs_lib.sh'
    our_team = 'bi-core-tech'  # Equipo al que pertenece el responsable del DAG
    my_mail = 'carlos.jaime@pedidosya.com'  # El mail del responsable del DAG
    team_mail = 'bi-core-tech@pedidosya.com'

    JOB_FLOW_INITIAL_VALUES = {
        'Name': '{0}_{1}'.format(dagName, executionDay),
        'LogUri': logFolderInS3,
        'Tags': [
            {
                "Key": "team",
                "Value": our_team
            },
            {
                "Key": "owner_email",
                "Value": my_mail
            },
            {
                "Key": "team_email",
                "Value": team_mail
            }
        ],
        'BootstrapActions': [
            {
                'Name': 'sync_git',
                'ScriptBootstrapAction': {
                    'Path': bootstrapLocationInS3
                }
            }
        ]
    }
    return JOB_FLOW_INITIAL_VALUES


def createNewStepChecker(pDag, task_ids, stepNumber, stepObj):
    return EmrStepSensor(
        task_id='watch_step_' + stepObj['Name'],
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='@task_ids', key='return_value')[@index] }}".replace('@index', str(
            stepNumber)).replace('@task_ids', task_ids),
        aws_conn_id='aws_emr',
        mode='reschedule',
        poke_interval=60,
        dag=pDag
    )


def createSubDag(dagName, pSubDagName, job_flow, args, spark_steps_base, spark_steps):

    dag = DAG(dag_id=dagName + '.' + pSubDagName,
              default_args=args,
              schedule_interval=None)

    checking_process = DummyOperator(task_id='checking_process', dag=dag)

    ending_process = DummyOperator(task_id='ending_process', dag=dag)

    #step_checker_list = [checking_process]

    #####
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=job_flow,
        aws_conn_id='aws_emr',
        emr_conn_id='emr_transient_medium_disk',
        dag=dag
    )

    #####
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_emr',
        dag=dag
    )

    #
    # Base Steps ################################################################################################
    base_steps = EmrAddStepsOperator(
        task_id='base_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_emr',
        steps=spark_steps_base,
        dag=dag
    )

    #
    # Facts Steps ################################################################################################

    facts_steps = EmrAddStepsOperator(
        task_id='facts_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_emr',
        steps=spark_steps,
        dag=dag
    )


    for num, step in enumerate(spark_steps_base, start=0):

        step_log = [cluster_creator, base_steps, facts_steps, checking_process]

        ## Insert inicio de ejecución en auditoria de procesos
        insert_audit = loggingProcess(dag,
                                      task_id='insert-logging-{0}'.format(step['Name']),
                                      process=step['Name'],
                                      log='INSERT')

        step_to_watch = createNewStepChecker(pDag=dag,
                                             task_ids='base_steps',
                                             stepNumber=num,
                                             stepObj=step)

        update_audit = loggingProcess(dag,
                                      task_id='update-logging-{0}'.format(step['Name']),
                                      logging_task_id='insert-logging-{0}'.format(step['Name']),
                                      check_task_id='watch_step_' + step['Name'],
                                      log='UPDATE')

        #step_log.extend([insert_audit, step_to_watch, update_audit])
        #airflow.utils.helpers.chain(*step_log)

        ## Agrega pasos a la secuencia
        step_log.extend([insert_audit, step_to_watch, update_audit, ending_process])

        airflow.utils.helpers.chain(*step_log)

    for num, step in enumerate(spark_steps, start=0):

        step_log = [cluster_creator, base_steps, facts_steps, checking_process]

        ## Insert inicio de ejecución en auditoria de procesos
        insert_audit = loggingProcess(dag,
                                      task_id='insert-logging-{0}'.format(step['Name']),
                                      process=step['Name'],
                                      log='INSERT')

        step_to_watch = createNewStepChecker(pDag=dag,
                                             task_ids='facts_steps',
                                             stepNumber=num,
                                             stepObj=step)

        update_audit = loggingProcess(dag,
                                      task_id='update-logging-{0}'.format(step['Name']),
                                      logging_task_id='insert-logging-{0}'.format(step['Name']),
                                      check_task_id='watch_step_' + step['Name'],
                                      log='UPDATE')

        ## Agrega pasos a la secuencia
        step_log.extend([insert_audit, step_to_watch, update_audit, ending_process])

        airflow.utils.helpers.chain(*step_log)

    airflow.utils.helpers.chain(*[ending_process, cluster_remover])

    #cluster_creator >> base_steps >> facts_steps >> step_checker_list >> cluster_remover

    return dag


def createSubDagOperator(pParentDag, pDagName):

    tid_subdag= 'Generate_Orders'

    spark_steps_base = []
    spark_steps = []

    # Add steps to generate base data. Clean Orders
    for b in dag_bag_base:
        # Is enabled? config file
        if (b['enabled']=="1"):
            # step_name, dag_path, py_spark
            steps_base = create_spark_steps_dag(step=b['step'],
                                           py_spark=b['py_spark'],
                                           s3_source_path=b['s3_source_path'],
                                           s3_destination_path=b['s3_destination_path'],
                                           data_history=b['data_history'])

            spark_steps_base.append(steps_base)

    # Add steps to generate facts data
    for i in dag_bag:
        # Is enabled? config file
        if (i['enabled']=="1"):
            # step_name, dag_path, py_spark
            steps = create_spark_steps_dag(step=i['step'],
                                           py_spark=i['py_spark'],
                                           s3_source_path=i['s3_source_path'],
                                           s3_destination_path=i['s3_destination_path'],
                                           data_history=i['data_history'])

            spark_steps.append(steps)

    args = create_sub_dag(default_args['owner'], default_args['email'], default_args['start_date'])

    job_flow = create_job_flow(pDagName)

    new_subdag = createSubDag(pDagName, tid_subdag, job_flow, args, spark_steps_base, spark_steps)

    sd_op = SubDagOperator(task_id=tid_subdag,
                           dag=pParentDag,
                           subdag=new_subdag,
                           retries=1,
                           retry_delay=timedelta(minutes=30))
    return sd_op


############################

def createNewSuccessSlackTrOperator(pDag, pDagName):
    return SlackAPIPostOperator(
        dag=pDag,
        task_id='slack_process_success',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        channel=slack_channel,
        icon_url=Variable.get('slack_icon_url'),
        token=slack_token,
        text='{emoji} | {time} | {dag} *has completed*'.format(
            dag='dag --> {}'.format(pDagName),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S'),
            emoji=slack_ok_emoji,
        ),
        retries=0
    )


def createNewErrorSlackTrOperator(pDag, pDagName):
    return SlackAPIPostOperator(
        dag=pDag,
        task_id='slack_process_error',
        trigger_rule=TriggerRule.ONE_FAILED,
        channel=slack_channel,
        icon_url=Variable.get('slack_icon_url'),
        token=slack_token,
        text='{emoji} | {time} | {dag} *has not completed*'.format(
            dag='dag --> {}'.format(pDagName),
            time=datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S'),
            emoji=slack_error_emoji,
        ),
        retries=0
    )


#with DAG(dag_name, default_args=dict(default_args, **config), max_active_runs=4,
with DAG(dag_name, default_args=default_args, max_active_runs=4,
         schedule_interval=default_args['scheduler'], catchup=default_args["catchup"]) as dag:

    # Notificaction Process
    slack_success_tr = createNewSuccessSlackTrOperator(dag, dag_name)
    slack_error_tr = createNewErrorSlackTrOperator(dag, dag_name)

    run_this_first = DummyOperator(task_id='Inicio', dag=dag)



    processing = createSubDagOperator(dag, dag_name)

    # # # Paso 2
    airflow.utils.helpers.chain(*[run_this_first, processing, slack_success_tr])
    airflow.utils.helpers.chain(*[run_this_first, processing, slack_error_tr])
