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

def generateFactOrdersDetails(app_args):

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

#
## Procesamiento fact_orders_details
#

    pre_fact_orders_details = orders.select(col('id').alias('order_id'),
                                            col('registeredDate').alias('registered_date'),
                                            col('address.area.id').alias('area_id'),
                                            col('restaurant.id').alias('restaurant_id'),
                                            col('state').alias('order_state_name'),
                                            explode_outer(col('details')).alias('_details_struct'),
                                            # Genera registro por elemento del array
                                            col('yyyymmdd')
                                            )



    ## GENERACION DE FACT_ORDERS_DETAILS

    # Genera columnas por arrays de details
    fact_orders_details = pre_fact_orders_details.select(col('order_id'),
                                                         col('_details_struct.id').alias('order_detail_id'),
                                                         col('restaurant_id'),
                                                         col('_details_struct.product.id').alias('product_id'),
                                                         col('registered_date'),
                                                         date_format(col('registered_date'), 'yyyMMdd').cast(
                                                             IntegerType()).alias('registered_date_id'),
                                                         col('_details_struct.quantity').alias('prod_quantity'),
                                                         col('_details_struct.subtotal').alias('prod_subtotal'),
                                                         col('_details_struct.unitPrice').alias('prod_unit_price'),
                                                         col('area_id'),
                                                         col('order_state_name').alias('order_state_name'),
                                                         col('_details_struct.product.foodCategoryTag.id').alias(
                                                             'category_tag_id'),
                                                         col('_details_struct.product.foodCategoryTag.name').alias(
                                                             'category_tag_name'),
                                                         col(
                                                             '_details_struct.product.foodCategoryTag.foodCategory.id').alias(
                                                             'category_id'),
                                                         col(
                                                             '_details_struct.product.foodCategoryTag.foodCategory.name').alias(
                                                             'category_name'),
                                                         col('_details_struct.productName').alias('product_name'),
                                                         col('_details_struct.integrationCode').alias(
                                                             'product_integration_code'),
                                                         # is_subsidized
                                                         # subsidized_subtotal
                                                         # subsidized_unit_price
                                                         col('yyyymmdd')
                                                         )


    fact_orders_details.write.mode('overwrite').format("bigquery")\
    .option("temporaryGcsBucket","data-origins-storage").option("intermediateFormat","orc")\
    .save("peya-data-origins-stg.origins_data_stg.orders_details_to_upsert")

    
def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", "--s3_source_path",
                        help="S3 Bucket Path de origen de los datos de ordenes, NRT")
    parser.add_argument("-destination", "--s3_destination_path",
                        help="S3 Bucket Path de escritura de datos de ordenes procesadas")
    return parser.parse_args()


if __name__ == '__main__':
    app_args = get_app_args()
    generateFactOrdersDetails(app_args)




