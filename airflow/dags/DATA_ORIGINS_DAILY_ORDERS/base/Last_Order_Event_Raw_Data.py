#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import date,timedelta,datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import json


import argparse

def cleanOrdersRawData(app_args):

    s3_nrw_raw_orders_data = app_args.s3_source_path
    s3_orders_data_cleaned_path = app_args.destination_path
    order_schema_path = "gs://data-origins-storage/data-proc-test/schema/or_orders_schema.json"
    s3_orders_data_paths = []


    # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
    conf = SparkConf().setAppName("ETL_Last_Order_Event_Raw_Data")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

  
    filter_history = int(app_args.data_history) # Cantidad de dias para filtros de ordenes y particion

    # Partition UTC, added days after and before
    base_date = datetime.now()  # Fecha Filtro para registered_date de la orden
    
    if (app_args.load_type.upper()=='HOURLY'):
        message_start = base_date - timedelta(hours=(filter_history))
        #Tomo una particion mas (filter_history + 1) porque los mensajes pueden estar desordenados en las particiones
        partitons_datetimes= [base_date - timedelta(hours=x) for x in range(filter_history + 1)]
        for part_datetime in partitons_datetimes:
            s3_orders_data_paths.append('{0}/{1}/{2}/*'.format(s3_nrw_raw_orders_data, datetime.strftime(part_datetime,"%Y%m%d"),datetime.strftime(part_datetime,"%H")))
            print('<<-------- Orders json data source: {0}/{1}/{2}/*'.format(s3_nrw_raw_orders_data, datetime.strftime(part_datetime,"%Y%m%d"),datetime.strftime(part_datetime,"%H")))
    else:
        message_start = base_date - timedelta(days=(filter_history))
        #Tomo una particion mas (filter_history + 1) porque los mensajes pueden estar desordenados en las particiones
        partitions_to_loads = [datetime.strftime(base_date - timedelta(days=x), "%Y%m%d") for x in range(filter_history + 1)]
        for day in partitions_to_loads:
            s3_orders_data_paths.append('{0}/{1}/*/*'.format(s3_nrw_raw_orders_data, day))
            print('<<-------- Orders json data source: {0}/{1}/*/*'.format(s3_nrw_raw_orders_data, day))
    

    # Lectura de toda la data NRT de ordenes

    ###  Se obtiene el último esquema de las ordenes
    query = spark.sparkContext.wholeTextFiles(order_schema_path).collect()
    auxEsquema=json.loads(query[0][1])
    schema=StructType.fromJson(auxEsquema)
    ########

    ## Tomo una particion mas


    
    
    orders_raw_data = spark.read.schema(schema).load(s3_orders_data_paths, format='json', enconding='UTF-8')
    

    orders_raw_data = orders_raw_data.filter(to_timestamp(orders_raw_data._message_timestamp/1000)>=message_start)
    
    
    ## Filtra las ordenes de "hoy" segun la fecha de registro
    #orders_raw_data = orders_raw_data.filter(col('registeredDate')<base_date)

## Obtien ultima version de las ordenes NRT, por _message_timestamp
    orders_raw_data = orders_raw_data.withColumn('sort_column',when(col('state') =='PENDING',0).otherwise(col('_message_timestamp')))

    window = Window.partitionBy(col('id')).orderBy(col('sort_column').desc())

    
    orders_raw_data = orders_raw_data.withColumn('last',row_number().over(window))
    orders = orders_raw_data.filter(col('last')== 1)

    #orders.cache()

## FILTROS DE ORDENES DE PRUEBA

    ### 1 Filtro de ordenes con restaurant.id = 42
    orders = orders.filter(orders.restaurant.id != 42)
    print('** Filtradas ordenes con restaurant.id = 42 **')

    ### 2 Filtro de ordenes con User.type = 'AUTOMATION' id = 15 Usuarios de AUTOMATION
    orders = orders.filter(orders.user.type != 'AUTOMATION')
    print('** Filtradas ordenes con user.type = AUTOMATION **')

    ### 3 Filtro usuario de prueba user_id = 14003043

    orders = orders.filter(orders.user.id != 14003043)
    print('** Filtradas ordenes con user.id = 14003043 (Test User) **')

    orders.cache()

## Genera el parquet con la raw data de ordenes filtradas, para ser usada en la geberación de las entidades

    print('---->> Orders clean data Destination: {0}'.format(s3_orders_data_cleaned_path))



    orders.write.parquet(s3_orders_data_cleaned_path, mode='overwrite', compression='snappy')


    print('** Ordenes Generadas **')

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", "--s3_source_path",
                        help="S3 Bucket Path de origen de los datos de ordenes, NRT")
    parser.add_argument("-destination","--destination_path",
                        help="Gs Bucket Path de escritura de datos de ordenes procesadas")
    parser.add_argument("-history","--data_history",
                        help="Cantidad de dias/horas a procesar, contando del actual para atrás. Default 3 dias", default=2)
    parser.add_argument("-load_type","--load_type",
                        help="Valores aceptado Hourly/Daily, define si el proceso correra por hora o por dia, la cantidad se define en el argumento history", default="Daily")                            
    return parser.parse_args()


if __name__ == '__main__':
    app_args = get_app_args()
    cleanOrdersRawData(app_args)




