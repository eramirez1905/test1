from datetime import datetime, timedelta, date
import airflow
import json
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator



dagName = "TEST_XCOM_PARTITION"
default_args = {
    'owner':  'data-origins',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 1),
    'email': 'nicolas.alvarezderon@pedidosya.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

partitions=''
def process_data_from_bq(**kwargs):
    ti = kwargs['ti']
    bq_data = ti.xcom_pull(task_ids='push_xcom_value')
    partitions= ",".join(a for a in list(map(lambda x: "'"+x[0]+"'",bq_data)))

def createUpsertOperator(task_id):
    upsertOrder = BigQueryOperator(
        task_id=task_id,
        bql="""
            delete from origin_data_refined.origin_order o 
            where o.order_id in (select stg.order_id from origins_data_stg.orders_to_upsert stg)
            and yyyymmdd in ({0});
            insert into origin_data_refined.origin_order(order_id ,restaurant_id ,area_id ,delivery_date ,delivery_date_id ,registered_date ,registered_date_id ,order_hour ,is_pre_order ,is_take_out ,coupon_used ,user_id ,response_user_id ,restaurant_user_id ,response_date ,response_date_id ,reject_message_id ,delivery_time_id ,reception_system_id ,first_successful ,payment_method_id ,amount_no_discount ,commission ,discount ,shipping_amount ,total_amount ,payment_amount ,tax_amount ,online_payment ,responded_system_id ,cop_user_id ,user_address ,user_phone ,white_label_id ,address_id ,with_logistics ,dispatch_date ,logistics_commission ,promised_delivery_time_id ,secondary_reception_system_id ,client_guid ,latitude ,longitude ,delivery_zone_id ,business_type ,user_identity_card ,application_version ,shipping_amount_no_discount ,credit_card_commission ,country_id ,city_id ,has_notes ,restaurant_name ,order_notes ,has_final_user_documents ,distance_kilometers ,distance_meters ,state_id ,application_id,yyyymmdd)
            select order_id ,restaurant_id ,area_id ,delivery_date ,delivery_date_id ,registered_date ,registered_date_id ,order_hour ,is_pre_order ,is_take_out ,coupon_used ,user_id ,response_user_id ,restaurant_user_id ,response_date ,response_date_id ,reject_message_id ,delivery_time_id ,reception_system_id ,first_successful ,payment_method_id ,cast (amount_no_discount as NUMERIC) ,cast(commission as NUMERIC),cast(discount  as NUMERIC),cast(shipping_amount  as NUMERIC),cast(total_amount  as NUMERIC),cast(payment_amount  as NUMERIC),cast(tax_amount  as NUMERIC),online_payment ,responded_system_id ,cop_user_id ,user_address ,user_phone ,white_label_id ,address_id ,with_logistics ,dispatch_date ,cast(logistics_commission  as NUMERIC),promised_delivery_time_id ,secondary_reception_system_id ,client_guid ,latitude ,longitude ,delivery_zone_id ,business_type_id ,user_identity_card ,application_version ,shipping_amount_no_discount ,credit_card_commission ,country_id ,city_id ,has_notes ,restaurant_name  ,order_notes ,has_final_user_documents ,distance_kilometers ,distance_meters ,state_id ,application_id,cast (registered_date as date)
            from origins_data_stg.orders_to_upsert
            """.format(",".join(a for a in list(map(lambda x: "'"+x[0]+"'","{{task_instance.xcom_pull(task_ids='push_xcom_value')}}")))),
        use_legacy_sql=False
    )
    return upsertOrder


with DAG(dagName, schedule_interval=None, catchup=False, default_args=default_args) as dag:

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

    select_partitions_orders>>push_xcom_value_orders>> update_orders
