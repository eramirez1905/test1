#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import date,timedelta,datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import min, max, col, countDistinct, count, date_format

from pyspark.sql.types import IntegerType

import argparse

def generateFailRateOwnerInhouseOwner(app_args):

    s3_fact_orders = app_args.s3_source_path
    s3_fail_rate_owner_inhouse_owner = app_args.s3_destination_path
    s3_fact_orders_paths = []

    # Dependencias
    s3_dim_historical_restaurant = 's3://peyabi.datalake.live/bi-core-tech/refined/dim_historical_restaurant'
    s3_logistics_orders = 's3://peyabi.datalake.live/bi-logistics/refined/logistic_orders'
    s3_raw_logistics_orders_events = 's3://peyabi.datalake.live/bi-core-tech/raw/nrt/orders/logistic_event'

    # bucket/folder temporal
    s3_temporary_logistics_orders_events = 's3://peyabi.datalake.live/bi-core-tech/temp/logistic_event'


    filter_days = int(app_args.data_history)  # Cantidad de dias para filtros de ordenes y particion

    # Partition by country date (utc-3 for Uruguay)
    to_registered_date = datetime.now() - timedelta(days=1)  # Fecha Filtro para registered_date de la orden
    partitions = [datetime.strftime(to_registered_date - timedelta(days=x), "%Y%m%d") for x in range(filter_days)]
    print('** Particiones por Registered_date: {}'.format(partitions))

    # Partition UTC, added days after and before
    to_date_utc = datetime.now()  # Fecha Filtro para registered_date de la orden
    partitions_utc = [datetime.strftime(to_date_utc - timedelta(days=x), "%Y%m%d") for x in range(filter_days + 2)]
    print('** Particiones por UTC: {}'.format(partitions_utc))

    ##

    # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
    conf = SparkConf().setAppName("ETL__Orders_Fail_Rate_Owner_Inhouse_Owner")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    sqlcontext = SQLContext(spark)


    ## Data Preparation

    #
    # FACT_ORDERS DATA ################################################################################################
    #

    for day in partitions:
        s3_fact_orders_paths.append(
            '{0}/yyyymmdd={1}'.format(s3_fact_orders, day))
        print('<<-------- Orders raw data source: {0}/yyyymmdd={1}'.format(s3_fact_orders, day))

    # Lectura de toda la data de ordenes
    fact_orders = spark.read.parquet(*s3_fact_orders_paths)

    fact_orders = fact_orders.withColumn('yyyymmdd', date_format(col('registered_date'), 'yyyMMdd').cast(IntegerType()))

    fact_orders.createOrReplaceTempView('fact_orders')

    sqlcontext.cacheTable("fact_orders")

    #
    # DIM HISTORICAL RESTAURANT ########################################################################################
    #

    s3_dim_historical_restaurant_paths = []

    # Itera sobre las particiones necesarias
    for day in partitions:
        s3_dim_historical_restaurant_paths.append(
            '{0}/yyyymmdd={1}'.format(s3_dim_historical_restaurant, day))
        print('<<-------- Orders raw data source: {0}/yyyymmdd={1}'.format(s3_dim_historical_restaurant, day))

    dim_historical_restaurant = spark.read.parquet(*s3_dim_historical_restaurant_paths)

    dim_historical_restaurant.createOrReplaceTempView('dim_historical_restaurant')

    sqlcontext.cacheTable("dim_historical_restaurant")

    print('** Generada vista --> dim_historical_restaurant **')

    #
    # LOGISTICS ORDERS ################################################################################################
    #

    s3_logistics_orders_paths = []

    # Itera sobre las particiones necesarias
    for day in partitions_utc:
        s3_logistics_orders_paths.append(
            '{0}/yyyymmdd={1}'.format(s3_logistics_orders, day))
        print('<<-------- Orders raw data source: {0}/yyyymmdd={1}'.format(s3_logistics_orders, day))

    logistics_orders = spark.read.parquet(*s3_logistics_orders_paths)

    #logistics_orders = logistics_orders.cache()

    logistics_orders.createOrReplaceTempView('logistics_orders')

    sqlcontext.cacheTable("logistics_orders")

    print('** Generada vista --> logistics_orders **')

    #
    # FACT_ORDERS_LOGISTIC_EVENTS #####################################################################################
    #

    s3_raw_logistics_orders_events_paths = []
    s3_temporary_logistics_orders_events_paths = []

    # Itera sobre las particiones necesarias
    for day in partitions_utc:
        s3_raw_logistics_orders_events_paths.append(
            '{0}/yyyymmdd={1}'.format(s3_raw_logistics_orders_events, day))
        print('<<-------- Orders raw data source: {0}/yyyymmdd={1}'.format(s3_raw_logistics_orders_events, day))

    # Copia archivos parquets a carpeta temporal. Evita la eliminación por carga NRT
    spark.read.parquet(*s3_raw_logistics_orders_events_paths).write.parquet(s3_temporary_logistics_orders_events,
                                                                            mode='overwrite')

    raw_logistics_orders_events = spark.read.parquet(s3_temporary_logistics_orders_events)

    raw_logistics_orders_events.createOrReplaceTempView('raw_logistics_orders_events')

    sqlcontext.cacheTable("raw_logistics_orders_events")

    # filtramos por event = 'MISSED' or event = 'EXPIRED'
    # Estos eventos no aparecen desde marzo 2020

    raw_logistics_orders_events_filtered = spark.sql("SELECT order_id, min(event) as event from raw_logistics_orders_events as l where l.event = 'MISSED' or l.event = 'EXPIRED' group by l.order_id")

    # Tabla STG raw_logistics_orders_events_filtered

    raw_logistics_orders_events_filtered.createOrReplaceTempView('raw_logistics_orders_events_filtered')

    sqlcontext.cacheTable("raw_logistics_orders_events_filtered")

    #
    # PROCESO ORDER_FAIL_RATE_IN_HOUSE_OWNER ###########################################################################
    #

    # Query generada por equipo de Operaciones para el calculo de las columnas fail_rate e in_house_owner
    # Ultima revisión: 2020-06-19
    # Carlos Jaime (Data Engineer) y Andres Ramirez (CDX Analyst)

    final_query = """
        select l.order_id,
        l.registered_date,
        l.country_id,
        l.state_id,
    
            case when l.state_id = 1 then 'Confirmed'
    
            when l.reject_message_id = 9 and l.registered_date < '2019-10-09' and ((l.reception_system_id = 65 and p.event = 'EXPIRED')
            or (l.reception_system_id in (20,26,30,34,42,44,49,51,54,55,57,59,60,61,66,70,71,72,73,74,79,82,83,85,87,93,99,125,127,129,130,131,132,133,134,135,137,139,140) and l.reception_date is not null)) then 'Restaurant'
            when l.reject_message_id = 9 and l.registered_date < '2019-10-09' and ((l.reception_system_id = 65 and p.event = 'MISSED') or (l.reception_system_id <> 65)) then 'PedidosYa'
            when l.reject_message_id = 9 then 'PedidosYa'
    
            when l.reject_message_id = 12 and l.registered_date < '2019-10-25' and ( l.responded_system_id = 2 or (l.responded_system_id = 65 and l.responded_system_id <> l.reception_system_id )) then 'PedidosYa'
            when l.reject_message_id = 12 and l.registered_date < '2019-10-25' then 'Restaurant'
            when l.reject_message_id = 12 then 'PedidosYa'
    
            when l.reject_message_id = 27 and s.vendor_late <= 600 then 'PedidosYa'
            when l.reject_message_id = 27 then 'Restaurant'
    
            when l.with_logistics = 0 and l.reject_message_id in (1,2,3,7,10,11,13,15,16,17,21,25,26,29,32,34,36,37,39,42,43,46) then 'Restaurant'
            when l.with_logistics = 0 and l.reject_message_id in (8,22,33,41,44,45) then 'PedidosYa'
            when l.with_logistics = 0 and l.reject_message_id in (4,5,6,14,18,19,20,23,24,28,30,31,35,38) then 'User'
    
            when l.with_logistics = 1 and l.reject_message_id in (2,3,7,10,11,13,15,21,26,32,37,39,46) then 'Restaurant'
            when l.with_logistics = 1 and l.reject_message_id in (1,8,16,17,22,33,34,40,41,43,44,45) then 'PedidosYa'
            when l.with_logistics = 1 and l.reject_message_id in (4,5,6,14,18,19,20,23,24,28,30,31,35,38) then 'User'
            when l.with_logistics = 1 and l.reject_message_id in (25,29,36,42) then 'Rider'
    
            else 'PedidosYa'
            end as fail_rate_owner,
    
            case when l.state_id = 1 then 'Confirmed'
    
            when l.reject_message_id = 9 and l.registered_date < '2019-10-09' and h.is_important_account = true and ((l.reception_system_id = 65 and p.event = 'EXPIRED')
            or (l.reception_system_id in (20,26,30,34,42,44,49,51,54,55,57,59,60,61,66,70,71,72,73,74,79,82,83,85,87,93,99,125,127,129,130,131,132,133,134,135,137,139,140) and l.reception_date is not null)) then 'PQP Local'
            when l.reject_message_id = 9 and l.registered_date < '2019-10-09' and ((l.reception_system_id = 65 and p.event = 'EXPIRED')
            or (l.reception_system_id in (20,26,30,34,42,44,49,51,54,55,57,59,60,61,66,70,71,72,73,74,79,82,83,85,87,93,99,125,127,129,130,131,132,133,134,135,137,139,140) and l.reception_date is not null)) then 'PQP HQ'
            when l.reject_message_id = 9 and l.registered_date < '2019-10-09' and ((l.reception_system_id = 65 and p.event = 'MISSED') or (l.reception_system_id <> 65)) then 'Partners'
            when l.reject_message_id = 9 then 'Partners'
    
            when l.reject_message_id = 12 and l.registered_date < '2019-10-25' and ( l.responded_system_id = 2 or (l.responded_system_id = 65 and l.responded_system_id <> l.reception_system_id )) then 'Partners'
            when l.reject_message_id = 12 and l.registered_date < '2019-10-25' then 'PQP HQ'
            when l.reject_message_id = 12 then 'Partners'
    
            when l.reject_message_id = 27 and s.vendor_late <= 600 then 'Logistics Performance'
            when l.reject_message_id = 27 and h.is_important_account = true then 'PQP Local'
            when l.reject_message_id = 27 then 'PQP HQ'
    
            when l.with_logistics = 0 and l.reject_message_id in (1,2,3,39) then 'Contenido'
            when l.with_logistics = 0 and l.reject_message_id in (14,18,20,24,31,33) then 'Delivery'
            when l.with_logistics = 0 and l.reject_message_id in (43) then 'New Verticals'
            when l.with_logistics = 0 and l.reject_message_id in (8,38) then 'Partners'
            when l.with_logistics = 0 and l.reject_message_id in (19,30) then 'Payments'
            when l.with_logistics = 0 and l.reject_message_id in (45) then 'PQP Local'
            when l.with_logistics = 0 and h.is_important_account = true and l.reject_message_id in (6,7,10,11,13,15,16,17,21,25,26,29,32,34,35,36,37,42,46) then 'PQP Local'
            when l.with_logistics = 0 and l.reject_message_id in (6,7,10,11,13,15,16,17,21,25,26,29,32,34,35,36,37,42,46) then 'PQP HQ'
            when l.with_logistics = 0 and l.reject_message_id in (4,5,22,23,28,40,41,44) then 'Shopping'
    
            when l.with_logistics = 1 and l.reject_message_id in (2,3,39) then 'Contenido'
            when l.with_logistics = 1 and l.reject_message_id in (14,18,20,24,31,33) then 'Delivery'
            when l.with_logistics = 1 and l.reject_message_id in (16,17) then 'Logistics Performance'
            when l.with_logistics = 1 and l.reject_message_id in (25,29,36,42) then 'Logistics Rider Performance'
            when l.with_logistics = 1 and l.reject_message_id in (43) then 'New Verticals'
            when l.with_logistics = 1 and l.reject_message_id in (8,38) then 'Partners'
            when l.with_logistics = 1 and l.reject_message_id in (19,30) then 'Payments'
            when l.with_logistics = 1 and l.reject_message_id in (45) then 'PQP Local'
            when l.with_logistics = 1 and h.is_important_account = true and l.reject_message_id in (6,7,10,11,13,15,21,26,32,34,35,37,46) then 'PQP Local'
            when l.with_logistics = 1 and l.reject_message_id in (6,7,10,11,13,15,21,26,32,34,35,37,46) then 'PQP HQ'
            when l.with_logistics = 1 and l.reject_message_id in (1,4,5,22,23,28,40,41,44) then 'Shopping'
    
            else 'Partners'
            end as inhouse_owner,
            yyyymmdd
    
        from fact_orders as l
        left join dim_historical_restaurant as h on l.restaurant_id = h.restaurant_id and l.registered_date_id = h.date_id
        left join logistics_orders as s on l.order_id = s.order_id
        left join raw_logistics_orders_events_filtered as p on p.order_id = l.order_id
        """

    print('** Query ** \n {0}'.format(final_query))

    fact_order_fail_rate_in_house_owner = spark.sql(final_query).cache()

    # Ordena data por order_id
    fact_order_fail_rate_in_house_owner = fact_order_fail_rate_in_house_owner.orderBy('order_id', ascending=True)

    # Elimina repetidos
    fact_order_fail_rate_in_house_owner = fact_order_fail_rate_in_house_owner.distinct()

    # Generamos parquet de ordenes, particionado por yyyymmdd
    fact_order_fail_rate_in_house_owner = fact_order_fail_rate_in_house_owner.repartition(
        fact_order_fail_rate_in_house_owner.yyyymmdd)

    print('---->> order_fail_rate_in_house_owner: {0}'.format(s3_fail_rate_owner_inhouse_owner))

    # Generación de parquets por partición
    for day in partitions:
        to_parquet = '{0}/yyyymmdd={1}'.format(s3_fail_rate_owner_inhouse_owner, day)

        partition = fact_order_fail_rate_in_house_owner.filter(col('yyyymmdd') == day)

        print('** Escribiendo parquet particion yyyymmdd={0}'.format(day))

        partition.write.parquet(to_parquet, mode='overwrite')

    print('** ORDER_FAIL_RATE_IN_HOUSE_OWNER Generado **')

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", "--s3_source_path",
                        help="S3 Bucket Path de origen de los datos de ordenes")
    parser.add_argument("-destination", "--s3_destination_path",
                        help="S3 Bucket Path de escritura de datos procesados")
    parser.add_argument("-history", "--data_history",
                        help="Cantidad de dias a procesar, contando del actual para atrás. Default 3 dias", default=3)
    return parser.parse_args()


if __name__ == '__main__':
    app_args = get_app_args()
    generateFailRateOwnerInhouseOwner(app_args)




