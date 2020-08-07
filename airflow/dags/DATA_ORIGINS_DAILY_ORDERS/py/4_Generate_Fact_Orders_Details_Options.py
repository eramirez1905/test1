#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import date,timedelta,datetime
import json
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import min, max, col, countDistinct, count, date_format
from pyspark.sql.functions import when, split, lower, upper

from pyspark.sql.functions import explode_outer, lit, size
from pyspark.sql.types import *

import argparse

def generateFactOrdersDetailsOptions(app_args):


    # Esquema ordenes
    s3_order_schema_path = "gs://data-origins-storage/data-proc-test/schema/or_orders_schema.json"

    # Rutas de lectura y escritura
    s3_orders_last_event_path = app_args.s3_source_path


    # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
    conf = SparkConf().setAppName("ETL_fact_orders_detail_options_Upsert")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    


    ###  Se obtiene el último esquema de las ordenes
    query = spark.sparkContext.wholeTextFiles(s3_order_schema_path).collect()
    auxEsquema=json.loads(query[0][1])
    schema=StructType.fromJson(auxEsquema)
    ########

    orders = spark.read.schema(schema).parquet(s3_orders_last_event_path)
    orders = orders.withColumn('yyyymmdd', date_format(col('registeredDate'), 'yyyMMdd').cast(IntegerType()))

 ## Procesamiento fact_orders_details_options

    # Genera columnas por arrays de details
    pre_fact_orders_details_options = orders.select(
        col('id').alias('order_id'),
        col('registeredDate').alias('registered_date'),
        col('state').alias('order_state_name'),
        explode_outer(col('details')).alias('_details_struct'),  # Genera registro por elemento del array
        col('yyyymmdd')
    )

    # Genera columnas por arrays de details options
    pre_fact_orders_details_options = pre_fact_orders_details_options.select(
        col('order_id'),
        col('registered_date'),
        col('order_state_name'),
        col('_details_struct.id').alias('order_detail_id'),
        col('_details_struct.product.id').alias('product_id'),
        col('_details_struct.productName').alias('product_name'),
        explode_outer(col('_details_struct.optionGroups')).alias('_optionGroups_struct'),
        # Genera registro por elemento del array
        col('yyyymmdd')
    )

    # Genera columnas por arrays de details options groups
    pre_fact_orders_details_options = pre_fact_orders_details_options.select(
        col('order_id'),
        col('order_detail_id'),
        col('registered_date'),
        col('order_state_name'),
        col('product_id'),
        col('product_name'),
        col('_optionGroups_struct.id').alias('group_id'),
        col('_optionGroups_struct.name').alias('group_name'),
        explode_outer(col('_optionGroups_struct.options')).alias('_option_struct'),
        # Genera registro por elemento del array
        col('yyyymmdd')
    )

    # Genera columnas por arrays de details options
    pre_fact_orders_details_options = pre_fact_orders_details_options.select(col('order_id'),
                                                                             col('order_detail_id'),
                                                                             col('registered_date'),
                                                                             col('order_state_name'),
                                                                             col('_option_struct.id').alias(
                                                                                 'order_detail_option_id'),
                                                                             col('product_id'),
                                                                             col('product_name'),
                                                                             col('group_id'),
                                                                             col('group_name'),
                                                                             col('_option_struct.id').alias(
                                                                                 'option_id'),
                                                                             col('_option_struct.name').alias(
                                                                                 'option_name'),
                                                                             col('_option_struct.amount').alias(
                                                                                 'amount'),
                                                                             col('yyyymmdd')
                                                                             )

    # Filtro ordenes sin options
    fact_orders_details_options = pre_fact_orders_details_options.filter(col('order_detail_option_id').isNotNull())

    fact_orders_details_options.write.mode('overwrite').format("bigquery")\
    .option("temporaryGcsBucket","data-origins-storage").option("intermediateFormat","orc")\
    .save("peya-data-origins-stg.origins_data_stg.orders_details_options_to_upsert")

    print('** fact_orders_details_options generado **')

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", "--s3_source_path",
                        help="S3 Bucket Path de origen de los datos de ordenes, NRT")
    parser.add_argument("-destination", "--s3_destination_path",
                        help="S3 Bucket Path de escritura de datos de ordenes procesadas")
    return parser.parse_args()


if __name__ == '__main__':
    app_args = get_app_args()
    generateFactOrdersDetailsOptions(app_args)




