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

#BIGQUERY Operators
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


#Importamos gcp
from airflow import models

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator
)
from airflow.utils.dates import days_ago


dagName = "DATA_ORGINS_ORDERS_EVENT"
default_args = {
    'owner':  'data-origins',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 1),
    'email': 'nicolas.alvarezderon@pedidosya.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

#variables para manejar los cluster
REGION = os.environ.get("GCP_LOCATION", "us-central1")
ZONE = os.environ.get("GCP_REGION", "us-central1b")
dag_name = dagName


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
CLUSTER['labels']['owner']='nicolas-alvarezderon'
CLUSTER['labels']['task']='orders'

base_date = datetime.now()
date_string = datetime.strftime(base_date,'%Y-%m-%d_%H-%S')

PYSPARK_JOBS = [{
   "reference":{
      "project_id":PROJECT_ID,
      "job_id":"Last_event_order_{}".format(date_string)
   },
   "placement":{
      "cluster_name":CLUSTER_NAME
   },    
  "labels": {
      "team": "data-originis",
      "owner": "nicolas-alvarezderon",
      "task": "last_orders_raw_event"
    },   
   "pyspark_job":{
      "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/Orders/Last_Order_Event_Raw_Data.py",
        "properties": {
        "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
        "spark.driver.memory":"20g",
        "spark.executor.memory":"20g",  
        "spark.driver.cores": "8",
        "spark.executor.cores": "8",
        "spark.sql.sources.partitionOverwriteMode":"dynamic"   
      },
     "args": [
         "-source","s3a://peyabi.datalake.live/bi-core-tech/raw/kinesis/orders",
         "-destination",  "gs://data-origins-storage/data-proc-test/temporal/last_order_events",  
         "-history" ,"3",
         "-load_type","HOURLY"
      ]
   }
},
{
   "reference":{
      "project_id":PROJECT_ID,
      "job_id":"Orders_Upsert_{}".format(date_string)
   },
   "placement":{
      "cluster_name":CLUSTER_NAME
   },    
  "labels": {
      "team": "data-originis",
      "owner": "nicolas-alvarezderon",
      "task": "orders_upsert"
    },   
   "pyspark_job":{
      "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/Orders/5_Generate_Fact_Orders_Events_Upserts.py",
        "properties": {
        "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
        "spark.driver.memory":"5g",
        "spark.executor.memory":"5g",  
        "spark.driver.cores": "2",
        "spark.executor.cores": "2"
      },
     "args": [
         "-source","gs://data-origins-storage/data-proc-test/temporal/last_order_events",

      ]
   }
},
{
   "reference":{
      "project_id":PROJECT_ID,
      "job_id":"Discounts_Upsert_{}".format(date_string)
   },
   "placement":{
      "cluster_name":CLUSTER_NAME
   },    
  "labels": {
      "team": "data-origins",
      "owner": "nicolas-alvarezderon",
      "task": "discounts_upsert"
    },   
   "pyspark_job":{
      "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/Orders/2_Generate_Fact_Orders_Discounts.py",
        "properties": {
        "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
        "spark.driver.memory":"5g",
        "spark.executor.memory":"5g",  
        "spark.driver.cores": "2",
        "spark.executor.cores": "2"
      },
     "args": [
         "-source","gs://data-origins-storage/data-proc-test/temporal/last_order_events",

      ]
   }
},
{
   "reference":{
      "project_id":PROJECT_ID,
      "job_id":"Details_Upsert_{}".format(date_string)
   },
   "placement":{
      "cluster_name":CLUSTER_NAME
   },    
  "labels": {
      "team": "data-origins",
      "owner": "nicolas-alvarezderon",
      "task": "details_upsert"
    },   
   "pyspark_job":{
      "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/Orders/3_Generate_Fact_Orders_Details.py",
        "properties": {
        "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
        "spark.driver.memory":"5g",
        "spark.executor.memory":"5g",  
        "spark.driver.cores": "2",
        "spark.executor.cores": "2"
      },
     "args": [
         "-source","gs://data-origins-storage/data-proc-test/temporal/last_order_events",

      ]
   }
},
{
   "reference":{
      "project_id":PROJECT_ID,
      "job_id":"Orders_Data_Upsert_{}".format(date_string)
   },
   "placement":{
      "cluster_name":CLUSTER_NAME
   },    
  "labels": {
      "team": "data-origins",
      "owner": "nicolas-alvarezderon",
      "task": "data_upsert"
    },   
   "pyspark_job":{
      "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/Orders/1_Generate_Fact_Order_Data.py",
        "properties": {
        "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
        "spark.driver.memory":"5g",
        "spark.executor.memory":"5g",  
        "spark.driver.cores": "2",
        "spark.executor.cores": "2"
      },
     "args": [
         "-source","gs://data-origins-storage/data-proc-test/temporal/last_order_events",

      ]
   }
},
{
   "reference":{
      "project_id":PROJECT_ID,
      "job_id":"Details_Options_Upsert_{}".format(date_string)
   },
   "placement":{
      "cluster_name":CLUSTER_NAME
   },    
  "labels": {
      "team": "data-originis",
      "owner": "nicolas-alvarezderon",
      "task": "details_option_upsert"
    },   
   "pyspark_job":{
      "main_python_file_uri":"gs://data-origins-storage/data-proc-test/scripts/Orders/4_Generate_Fact_Orders_Details_Options.py",
        "properties": {
        "spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
        "spark.driver.memory":"5g",
        "spark.executor.memory":"5g",  
        "spark.driver.cores": "2",
        "spark.executor.cores": "2"
      },
     "args": [
         "-source","gs://data-origins-storage/data-proc-test/temporal/last_order_events",

      ]
   }
}]


with DAG(dagName, schedule_interval="0 */2 * * *", catchup=False, default_args=default_args) as dag:

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

    generate_Last_Order_Event = DataprocSubmitJobOperator(
        dag=dag,
        task_id="Generate_Last_Order_Event", 
        job=PYSPARK_JOBS[0], 
        location=REGION, 
        project_id=PROJECT_ID
    )

    genereta_Upsert_Orders = DataprocSubmitJobOperator(
        dag=dag,
        task_id="genereta_Upsert_Orders", 
        job=PYSPARK_JOBS[1], 
        location=REGION, 
        project_id=PROJECT_ID
    )

    genereta_Upsert_Discounts = DataprocSubmitJobOperator(
        dag=dag,
        task_id="genereta_Upsert_Discounts", 
        job=PYSPARK_JOBS[2], 
        location=REGION, 
        project_id=PROJECT_ID
    )

    genereta_Upsert_Details = DataprocSubmitJobOperator(
        dag=dag,
        task_id="genereta_Upsert_Details", 
        job=PYSPARK_JOBS[3], 
        location=REGION, 
        project_id=PROJECT_ID
    )
    genereta_Upsert_Data = DataprocSubmitJobOperator(
        dag=dag,
        task_id="genereta_Upsert_Data", 
        job=PYSPARK_JOBS[4], 
        location=REGION, 
        project_id=PROJECT_ID
    )
    genereta_Upsert_Details_Options = DataprocSubmitJobOperator(
        dag=dag,
        task_id="genereta_Upsert_Details_Options", 
        job=PYSPARK_JOBS[5], 
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


    #################### UPSERT   ##################################

    select_partitions_orders =BigQueryOperator(
        task_id='select_partitions_orders',
        sql="""
            select string_agg(distinct '\\'' || cast(parse_date('%Y%m%d',cast(yyyymmdd as string)) as string)||'\\'',',') from origins_data_stg.orders_data_to_upsert
        """,
        use_legacy_sql=False,
        destination_dataset_table='origins_data_stg.temp_partitions_to_upsert',
        write_disposition='WRITE_TRUNCATE' 
    )

    push_xcom_value_orders = BigQueryGetDataOperator(
        task_id='push_xcom_value_orders',
        dataset_id='origins_data_stg',
        table_id='temp_partitions_to_upsert')


    update_orders = BigQueryOperator(
        task_id="update_orders",
        sql="""
            delete from origin_data_refined.origin_order o 
            where o.order_id in (select stg.order_id from origins_data_stg.orders_to_upsert stg)
            and yyyymmdd in ({{task_instance.xcom_pull(task_ids='push_xcom_value_orders')[0][0]}});
            insert into origin_data_refined.origin_order(order_id ,restaurant_id ,area_id ,delivery_date ,delivery_date_id ,registered_date ,registered_date_id ,order_hour ,is_pre_order ,is_take_out ,coupon_used ,user_id ,response_user_id ,restaurant_user_id ,response_date ,response_date_id ,reject_message_id ,delivery_time_id ,reception_system_id ,first_successful ,payment_method_id ,amount_no_discount ,commission ,discount ,shipping_amount ,total_amount ,payment_amount ,tax_amount ,online_payment ,responded_system_id ,cop_user_id ,user_address ,user_phone ,white_label_id ,address_id ,with_logistics ,dispatch_date ,logistics_commission ,promised_delivery_time_id ,secondary_reception_system_id ,client_guid ,latitude ,longitude ,delivery_zone_id ,business_type ,user_identity_card ,application_version ,shipping_amount_no_discount ,credit_card_commission ,country_id ,city_id ,has_notes ,restaurant_name ,order_notes ,has_final_user_documents ,distance_kilometers ,distance_meters ,state_id ,application_id,yyyymmdd)
            select order_id ,restaurant_id ,area_id ,delivery_date ,delivery_date_id ,registered_date ,registered_date_id ,order_hour ,is_pre_order ,is_take_out ,coupon_used ,user_id ,response_user_id ,restaurant_user_id ,response_date ,response_date_id ,reject_message_id ,delivery_time_id ,reception_system_id ,first_successful ,payment_method_id ,cast (amount_no_discount as NUMERIC) ,cast(commission as NUMERIC),cast(discount  as NUMERIC),cast(shipping_amount  as NUMERIC),cast(total_amount  as NUMERIC),cast(payment_amount  as NUMERIC),cast(tax_amount  as NUMERIC),online_payment ,responded_system_id ,cop_user_id ,user_address ,user_phone ,white_label_id ,address_id ,with_logistics ,dispatch_date ,cast(logistics_commission  as NUMERIC),promised_delivery_time_id ,secondary_reception_system_id ,client_guid ,latitude ,longitude ,delivery_zone_id ,business_type_id ,user_identity_card ,application_version ,shipping_amount_no_discount ,credit_card_commission ,country_id ,city_id ,has_notes ,restaurant_name  ,order_notes ,has_final_user_documents ,distance_kilometers ,distance_meters ,state_id ,application_id,cast (registered_date as date)
            from origins_data_stg.orders_to_upsert;
            """,
            #.format(",".join(a for a in list(map(lambda x: "'"+x[0]+"'","{{task_instance.xcom_pull(task_ids='push_xcom_value')}}")))),
        use_legacy_sql=False
    )

    checking_process>>create_cluster>>generate_Last_Order_Event>>[genereta_Upsert_Orders,genereta_Upsert_Discounts,genereta_Upsert_Details,genereta_Upsert_Data,genereta_Upsert_Details_Options]>>delete_cluster>>ending_process
    genereta_Upsert_Orders >> select_partitions_orders >> push_xcom_value_orders >> update_orders >>ending_process
    
