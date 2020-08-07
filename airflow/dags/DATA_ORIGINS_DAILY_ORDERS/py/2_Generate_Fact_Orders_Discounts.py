#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import date,timedelta,datetime
import json
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import min, max, col, countDistinct, count, date_format
from pyspark.sql.functions import when, split, lower, upper

from pyspark.sql.functions import explode_outer, lit, size
from pyspark.sql.functions import regexp_replace

from pyspark.sql.types import *

import argparse

def generateFactOrdersDiscounts(app_args):

    # Rutas de lectura y escritura
    s3_order_schema_path = "gs://data-origins-storage/data-proc-test/schema/or_orders_schema.json"
    s3_orders_last_event_path = app_args.s3_source_path


    s3_refined_discount_type_path = 's3a://peyabi.datalake.live/bi-core-tech/refined/dim_discount_type/'
    s3_refined_paid_by = 's3a://peyabi.datalake.live/bi-core-tech/refined/dim_discount_paid_by/'

     

    # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
    conf = SparkConf().setAppName("ETL_fact_orders_Upsert")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()


    # Lectura de toda la data de ordenes
     ###  Se obtiene el último esquema de las ordenes
    query = spark.sparkContext.wholeTextFiles(s3_order_schema_path).collect()
    auxEsquema=json.loads(query[0][1])
    schema=StructType.fromJson(auxEsquema)
    ########

    orders = spark.read.schema(schema).parquet(s3_orders_last_event_path)
    orders = orders.withColumn('yyyymmdd', date_format(col('registeredDate'), 'yyyMMdd').cast(IntegerType()))


## Procesamiento fact_orders_discounts

    # Filtra ordenes con descuentos
    orders = orders.filter((orders.discounts.isNotNull()) & (size(orders.discounts) > 0))

    print('** Cantidad de ordenes con descuentos: {0} **'.format(orders.count()))

##
### Campos Calculados
##

    # Genera columna "version" con value static
    orders = orders.withColumn('_version', lit(0))

    # Genera columnas por arrays de discounts
    pre_fact_orders_discounts = orders.select(col('id').alias('order_id'),col('registeredDate').alias('registered_date'),
                                            col('state').alias('order_state_name'),	
                                              explode_outer(col('discounts')).alias('_discount_struct'),
                                              # Genera registro por elemento del array
                                              col('_version').alias('version'),
                                              col('yyyymmdd')
                                              )

    ## ORIGINAL VALUE ID

    pre_fact_orders_discounts = pre_fact_orders_discounts.withColumn('_original_type_id',
                                                                     when(
                                                                         pre_fact_orders_discounts._discount_struct.originalType == 'VALUE',
                                                                         2)
                                                                     .otherwise(1))

    print('** ORIGINAL VALUE ID **')

    ## VOUCHER_VALUE

    pre_fact_orders_discounts = pre_fact_orders_discounts.withColumn('_voucher_value',
                                                                     regexp_replace(
                                                                         regexp_replace(
                                                                             pre_fact_orders_discounts._discount_struct.notes,
                                                                             'voucher:', '')
                                                                         , '\\\\', ''
                                                                     )
                                                                     )

    print('** VOUCHER_VALUE **')

#
## JOINS
#

    ## DISCOUNT TYPE ID

    dim_discount_type = spark.read.parquet(s3_refined_discount_type_path).select('discount_type_id', 'discount_type')

    join_discount_type = [
        upper(dim_discount_type.discount_type) == upper(pre_fact_orders_discounts._discount_struct.type)]

    pre_fact_orders_discounts = pre_fact_orders_discounts.join(dim_discount_type, join_discount_type, 'left')

    ## PAID BY ID

    dim_discount_paid_by = spark.read.parquet(s3_refined_paid_by).select('paid_by_id', 'paid_by')

    join_paid_by = [upper(dim_discount_paid_by.paid_by) == upper(pre_fact_orders_discounts._discount_struct.paidBy)]

    pre_fact_orders_discounts = pre_fact_orders_discounts.join(dim_discount_paid_by, join_paid_by, 'left')

    print('** JOIN - PAID BY ID **')

#
## GENERACIÓN DE FACT_ORDERS_DISCOUNTS
#

    # Genera columnas por arrays de discounts
    fact_orders_discounts = pre_fact_orders_discounts.select(col('order_id'),
                                                             col('_discount_struct.id').alias('order_discount_id'),
                                                             col('registered_date'),
                                                             col('order_state_name'),
                                                             col('version'),
                                                             col('_discount_struct.amount').alias('amount'),
                                                             col('_discount_struct.notes').alias('notes'),
                                                             col('_original_type_id').alias('original_type_id'),
                                                             col('_discount_struct.originalValue').alias(
                                                                 'original_value'),
                                                             col('paid_by_id'),
                                                             col('_discount_struct.priority').alias('priority'),
                                                             col('discount_type_id'),
                                                             (when(pre_fact_orders_discounts.discount_type_id == 40,
                                                                   col('_voucher_value')).otherwise(None)).alias(
                                                                 'voucher_value'),  # Voucher Code formateado
                                                             col('yyyymmdd')
                                                             )

    
    fact_orders_discounts.write.mode('overwrite').format("bigquery")\
    .option("temporaryGcsBucket","data-origins-storage").option("intermediateFormat","orc")\
    .save("peya-data-origins-stg.origins_data_stg.orders_discounts_to_upsert")

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", "--s3_source_path",
                        help="GS Bucket Path de origen de los datos de ordenes, NRT")
    parser.add_argument("-destination", "--s3_destination_path",
                        help="Tabla temporal Big Query")
    return parser.parse_args()


if __name__ == '__main__':
    app_args = get_app_args()
    generateFactOrdersDiscounts(app_args)




