#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import date,timedelta,datetime
import json
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import min, max, col, countDistinct, count, date_format
from pyspark.sql.functions import when, split, lower

from pyspark.sql.types import *

import argparse

def generateFactOrdersData(app_args):

        # Esquema ordenes
    s3_order_schema_path = "gs://data-origins-storage/data-proc-test/schema/or_orders_schema.json"

    # Rutas de lectura y escritura
    s3_orders_last_event_path = app_args.s3_source_path

    # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
    conf = SparkConf().setAppName("ETL__Fact_Orders_Details")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    
    # Lectura de toda la data de ordenes
     ###  Se obtiene el último esquema de las ordenes
    query = spark.sparkContext.wholeTextFiles(s3_order_schema_path).collect()
    auxEsquema=json.loads(query[0][1])
    schema=StructType.fromJson(auxEsquema)
    ########

    orders = spark.read.schema(schema).parquet(s3_orders_last_event_path)

    orders = orders.withColumn('yyyymmdd', date_format(col('registeredDate'), 'yyyMMdd').cast(IntegerType()))


    ##
    ### Campos Calculados
    ##

    # click_date
    orders = orders.withColumn('_click_date', when((orders.data.clickDate == None) | orders.data.clickDate.isNull() | (orders.data.clickDate == 'None'), orders.registeredDate)
                               .when(orders.data.clickDate > orders.registeredDate, orders.registeredDate)
                               .otherwise(orders.data.clickDate))
    print('** Calculo click_date **')

    # consumes_stamps
    orders = orders.withColumn('_consumes_stamps', orders.stampsDiscount > 0)
    print('** Calculo consumes_stamps **')

    # campaignCategory 1 y 2
    orders = orders.withColumn('_campaignCategory', split(split(lower(orders.data.parsedTracking),'campaigncategory=').getItem(1),'&').getItem(0))
    print('** Calculo campaignCategory **')

    # campaignName 1 y 2
    orders = orders.withColumn('_campaignName', split(split(lower(orders.data.parsedTracking),'campaignname=').getItem(1),'&').getItem(0))
    print('** Calculo campaignName **')

    # campaignSource 1 y 2
    orders = orders.withColumn('_campaignSource', split(split(lower(orders.data.parsedTracking),'campaignsource=').getItem(1),'&').getItem(0))
    print('** Calculo campaignSource **')


    ### Generación de la Fact_Orders_Data

    # Consulta de las columnas finales
    fact_orders_data = orders.select(col('id').alias('order_id'),
                                     col('registereddate').alias('registered_date'),
                                     col('state').alias('order_state_name'),
                                     col('data.id').alias('orders_data_id'),
                                     col('data.first').alias('first'),
                                     col('data.respondedReceptionSystem.id').alias('response_reception_system'),
                                     col('data.receptionSystem.id').alias('reception_system_id'),
                                     col('data.gclid').alias('gclid'),
                                     col('data.matUser').alias('mat_user'),
                                     col('data.onlinePaymentId').alias('online_payment_id'),
                                     col('data.installDate').alias('install_date'),
                                     col('data.respondedReceptionSystem.id').alias('responded_reception_system_id'),
                                     col('_click_date').alias('click_date'), # Calculada
                                     col('data.parsedTracking').alias('parsed_tracking'),
                                     col('data.rawTracking').alias('raw_tracking'),
                                     col('data.creditCard.id').alias('credit_card_id'),
                                     col('data.firstSuccessful').alias('first_successful'),
                                     col('data.ipAddress').alias('ip_address'),
                                     col('_consumes_stamps').alias('consumes_stamps'), # Calculada
                                     col('data.searchGuid').alias('search_guid'),
                                     col('data.receptionSystemEnabled').alias('reception_system_enabled'),
                                     col('data.parallelReceptionSystem').alias('parallel_reception_system'),
                                     col('data.secondaryReceptionSystem.id').alias('secondary_reception_system_id'),
                                     col('data.isExpress').alias('is_express'),
                                     col('_campaignCategory').alias('campaignCategory1'),
                                     col('_campaignCategory').alias('campaignCategory2'),
                                     col('_campaignName').alias('campaignName1'),
                                     col('_campaignName').alias('campaignName2'),
                                     col('_campaignSource').alias('campaignSource1'),
                                     col('_campaignSource').alias('campaignSource2'),
                                     col('yyyymmdd')
                                     )



    fact_orders_data.write.mode('overwrite').format("bigquery")\
    .option("temporaryGcsBucket","data-origins-storage").option("intermediateFormat","orc")\
    .save("peya-data-origins-stg.origins_data_stg.orders_data_to_upsert")

    print('** fact_orders_data generado **')

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", "--s3_source_path",
                        help="S3 Bucket Path de origen de los datos de ordenes, NRT")
    parser.add_argument("-destination", "--s3_destination_path",
                        help="S3 Bucket Path de escritura de datos de ordenes procesadas")
    parser.add_argument("-history", "--data_history",
                        help="Cantidad de dias a procesar, contando del actual para atrás. Default 3 dias", default=3)
    return parser.parse_args()


if __name__ == '__main__':
    app_args = get_app_args()
    generateFactOrdersData(app_args)




