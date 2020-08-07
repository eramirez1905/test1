#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import date,timedelta,datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import to_timestamp

# Campos calculados
from pyspark.sql.functions import when, split, lower, sum, udf, hour, length, lit, upper, expr, min, max, round
from pyspark.sql.functions import explode_outer, date_format, unix_timestamp, from_unixtime, array_contains
from pyspark.sql.types import *
import json
from datetime import date,timedelta,datetime

from pyspark.sql.functions import when, col, udf

from pyspark.sql.functions import acos, cos, sin, toRadians

import argparse

# Convierte fecha en formato YYYYMMDD
def convert_date_id(date_value):
    return date_format(date_value, 'yyyyMMdd').cast(IntegerType())

# Suma timeoffset a fecha
def add_timeOffset(date_value, timeOffset):
    return from_unixtime(unix_timestamp(date_value) + (timeOffset * 60), format="yyyy-MM-dd HH:mm:ss")

def f_geo_calculate_distance(long_x, lat_x, long_y, lat_y):
    return round(acos(
        sin(toRadians(lat_x)) * sin(toRadians(lat_y)) +
        cos(toRadians(lat_x)) * cos(toRadians(lat_y)) *
            cos(toRadians(long_x) - toRadians(long_y))
    ) * lit(6371.0),6)


def generateFactOrdersUpserts(app_args):

    # Esquema ordenes
    s3_order_schema_path = "gs://data-origins-storage/data-proc-test/schema/or_orders_schema.json"

    # Rutas de lectura y escritura
    s3_orders_last_event_path = app_args.s3_source_path


    # s3_raw_reception_offset = 's3://peyabi.datalake.live/bi-core-tech/raw/nrt/reception_event/reception_offset'
    # s3_raw_acknowledgement_offset = 's3://peyabi.datalake.live/bi-core-tech/raw/nrt/reception_event/acknowledgement_offset'
    # s3_raw_logistic_event = "s3://peyabi.datalake.live/bi-core-tech/raw/nrt/orders/logistic_event"

    s3_refined_order_state = 's3a://peyabi.datalake.live/bi-core-tech/refined/dim_order_state/'
    s3_refined_application = 's3a://peyabi.datalake.live/bi-core-tech/refined/dim_application/'
    s3_refined_business_type = 's3a://peyabi.datalake.live/bi-core-tech/refined/dim_business_type/'

    ##

    # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
    conf = SparkConf().setAppName("ETL_fact_orders_Upsert")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sqlcontext = SQLContext(spark)


    ###  Se obtiene el último esquema de las ordenes
    query = spark.sparkContext.wholeTextFiles(s3_order_schema_path).collect()
    auxEsquema=json.loads(query[0][1])
    schema=StructType.fromJson(auxEsquema)
    ########

    orders = spark.read.schema(schema).parquet(s3_orders_last_event_path)
    orders = orders.withColumn('yyyymmdd', date_format(col('registeredDate'), 'yyyMMdd').cast(IntegerType()))

    ####################################################################################################################
    ## FORMAT DATA DE JSON SQS #########################################################################################

    pre_fact_orders = orders.select(col('id').alias('order_id'),
                                    lower(col('application')).alias('_application'),
                                    col('restaurant.id').alias('restaurant_id'),
                                    col('address.id').alias('area_id'),
                                    col('deliveryDate').alias('delivery_date'),
                                    convert_date_id(col('deliveryDate')).alias('delivery_date_id'),
                                    col('registeredDate').alias('registered_date'),
                                    convert_date_id(col('registeredDate')).alias('registered_date_id'),
                                    hour(col('registeredDate')).alias('order_hour'),
                                    when(col('registeredDate') == col('deliveryDate'), False).otherwise(True).alias(
                                        'is_pre_order'),
                                    col('pickup').alias('is_take_out'),
                                    when(col('amountNoDiscount') != col('totalAmount'), True).otherwise(False).alias(
                                        'coupon_used'),
                                    lower(col('state')).alias('_state'),
                                    col('user.id').alias('user_id'),

                                    col('responseUser.id').alias('response_user_id'),
                                    col('restaurantUser.id').alias('restaurant_user_id'),
                                    col('responseDate').alias('response_date'),
                                    convert_date_id(col('responseDate')).alias('response_date_id'),
                                    col('rejectMessage.id').alias('reject_message_id'),
                                    col('deliveryTime.id').alias('delivery_time_id'),
                                    col('data.receptionSystem.id').alias('reception_system_id'),
                                    col('data.firstSuccessful').alias('first_successful'),
                                    col('paymentMethod.id').alias('payment_method_id'),
                                    col('amountNoDiscount').alias('amount_no_discount'),
                                    col('commission').alias('commission'),
                                    col('discount'),
                                    col('shippingAmount').alias('shipping_amount'),
                                    col('totalAmount').alias('total_amount'),
                                    col('paymentAmount').alias('payment_amount'),
                                    col('taxAmount').alias('tax_amount'),
                                    (col('totalAmount') + col('shippingAmount')).alias('final_amount'),

                                    col('paymentMethod.online').alias('online_payment'),
                                    col('data.respondedReceptionSystem.id').alias('responded_system_id'),

                                    col('copUser.id').alias('cop_user_id'),

                                    col('addressDescription').alias('address_description'),
                                    col('addressPhone').alias('address_phone'),

                                    col('whiteLabel.id').alias('white_label_id'),

                                    col('address.id').alias('address_id'),

                                    col('withLogistics').alias('with_logistics'),
                                    col('dispatchDate').alias('dispatch_date'),

                                    col('logisticsCommission').alias('logistics_commission'),
                                    col('promisedDeliveryTime.id').alias('promised_delivery_time_id'),
                                    col('data.secondaryReceptionSystem.id').alias('secondary_reception_system_id'),
                                    col('clientGuid').alias('client_guid'),

                                    split(col('addressCoordinates'), ',')[0].alias('order_latitude').cast('float'),
                                    split(col('addressCoordinates'), ',')[1].alias('order_longitude').cast('float'),

                                    col('deliveryZoneId').alias('delivery_zone_id'),
                                    col('restaurant.businessType').alias('_business_type'),
                                    col('userIdentityCard').alias('user_identity_card'),
                                    col('applicationVersion').alias('application_version'),
                                    col('shippingAmountNoDiscount').alias('shipping_amount_no_discount'),
                                    col('creditCardCommission').alias('credit_card_commission'),
                                    col('restaurant.country.id').alias('country_id'),
                                    col('address.area.city.id').alias('city_id'),
                                    (when(length(col('notes')) > 1, True).otherwise(False)).alias('has_notes'),
                                    col('restaurant.name').alias('restaurant_name'),
                                    col('restaurant.address.latitude').alias('restaurant_latitude'),
                                    col('restaurant.address.longitude').alias('restaurant_longitude'),
                                    col('notes').alias('notes'),
                                    col('restaurant.restaurantBillingInfo.generateFinalUserDocuments').alias(
                                        'has_final_user_documents'),

                                    when(array_contains(col('discounts.type'), 'VOUCHER'), True).otherwise(False).alias(
                                        'has_voucher'),

                                    when((array_contains(col('fundings.type'), 'WALLET_CREDIT') | (array_contains(col('fundings.type'), 'WALLET_DEBIT'))), True).otherwise(False).alias('has_wallet')
                                    )

    ## Calculo de distancias

    pre_fact_orders = pre_fact_orders.withColumn('distance_kilometers',
                                                 f_geo_calculate_distance(col('order_longitude'), col('order_latitude'),
                                                                          col('restaurant_longitude'),
                                                                          col('restaurant_latitude')))

    pre_fact_orders = pre_fact_orders.withColumn('distance_meters', col('distance_kilometers') * 1000) # Convierte en mts


    # Genera vista temporal
    pre_fact_orders.createOrReplaceTempView("pre_fact_orders")

    #sqlcontext.cacheTable("pre_fact_orders")

    ### ORDER STATE ID ##################################################################################################

    dim_order_state = spark.read.parquet(s3_refined_order_state).select(col('order_state_id').alias('state_id'),
                                                                        lower(col('state_name')).alias('state_name')).distinct()

    dim_order_state.createOrReplaceTempView("stg_dim_order_state")

    #sqlcontext.cacheTable("stg_dim_order_state")

    print('** LEFT JOIN --> dim_order_state ({})'.format(s3_refined_order_state))

    ### BUSINESS_TYPE ID ##################################################################################################

    dim_business_type = spark.read.parquet(s3_refined_business_type).select(col('business_type_id').alias('business_type_id'),
                                                                        lower(col('business_type_name')).alias('business_type_name')).distinct()

    dim_business_type.createOrReplaceTempView("stg_dim_business_type")

    #sqlcontext.cacheTable("stg_dim_business_type")

    print('** LEFT JOIN --> dim_business_type ({})'.format(s3_refined_business_type))

    ### APPLICATION ID ##################################################################################################

    dim_application = spark.read.parquet(s3_refined_application).select(col('application_id'),
                                                                        lower(col('application_name')).alias('application_name')).distinct()

    dim_application.createOrReplaceTempView("stg_dim_application")

    #sqlcontext.cacheTable("stg_dim_application")

    print('** LEFT JOIN --> stg_dim_application ({})'.format(s3_refined_application))

    # ### VISUALIZATION DATE ##############################################################################################

    # # Itera sobre las particiones necesarias
    # s3_raw_acknowledgement_offset_paths = []
    # for day in partitions:
    #     s3_raw_acknowledgement_offset_paths.append(
    #         '{0}/yyyymmdd={1}'.format(s3_raw_acknowledgement_offset, day))

    #     print('<<-------- Orders raw data source: {0}/yyyymmdd={1}'.format(s3_raw_acknowledgement_offset, day))

    # data_acknowledgement_offset = spark.read.parquet(*s3_raw_acknowledgement_offset_paths).cache()

    # # Elimina duplicados y se queda con menor fecha de visualización por order_id
    # data_acknowledgement_offset = (data_acknowledgement_offset.groupBy(col('order').alias('order_id'))
    #                                .agg(min(col('timestamp')).alias('timestamp'),
    #                                     min(col('countryId')).alias('country_id'),
    #                                     min('timeOffset').alias('timeOffset')))

    # # Gener la fecha de visualización a partir de timestamp, convirtiendo de UTC a la tz del pais por la columna timeOffset
    # data_acknowledgement_offset = data_acknowledgement_offset.withColumn('visualization_date',
    #                                                                      add_timeOffset(col('timestamp'),
    #                                                                                     col('timeOffset'))).withColumn(
    #     'visualization_date_id', convert_date_id(col('visualization_date')))

    # # Tabla temporal
    # data_acknowledgement_offset.createOrReplaceTempView("stg_visualization_date")

    # sqlcontext.cacheTable("stg_visualization_date")

    # ### RECEPTION DATE #################################################################################################

    # # Itera sobre las particiones necesarias
    # s3_raw_reception_offset_paths = []
    # for day in partitions:
    #     s3_raw_reception_offset_paths.append(
    #         '{0}/yyyymmdd={1}'.format(s3_raw_reception_offset, day))

    #     print('<<-------- Orders raw data source: {0}/yyyymmdd={1}'.format(s3_raw_reception_offset, day))

    # data_reception_offset = spark.read.parquet(*s3_raw_reception_offset_paths).cache()

    # # Elimina duplicados y se queda con menor fecha de visualización por order_id
    # data_reception_offset = (data_reception_offset.groupBy(col('order').alias('order_id'))
    #                          .agg(min(col('timestamp')).alias('timestamp'),
    #                               min(col('countryId')).alias('country_id'),
    #                               min('timeOffset').alias('timeOffset')))

    # # Genera la fecha de visualización a partir de timestamp, convirtiendo de UTC a la tz del pais por la columna timeOffset
    # data_reception_offset = data_reception_offset.withColumn('reception_date', add_timeOffset(col('timestamp'), col(
    #     'timeOffset'))).withColumn('reception_date_id', convert_date_id(col('reception_date')))

    # # Tabla temporal
    # data_reception_offset.createOrReplaceTempView("stg_reception_offset")

    # sqlcontext.cacheTable("stg_reception_offset")

    # ### TRANSMITTING TIME ###############################################################################################
    
    # s3_raw_logistic_event_paths = []
    # # Itera sobre las particiones necesarias
    # for day in partitions:
    #     s3_raw_logistic_event_paths.append(
    #         '{0}/yyyymmdd={1}'.format(s3_raw_logistic_event, day))
    #     print('<<-------- Orders raw data source: {0}/yyyymmdd={1}'.format(s3_raw_logistic_event, day))

    # data_logistic_event = spark.read.parquet(*s3_raw_logistic_event_paths).cache()

    # data_transmitting_date = data_logistic_event.filter(col('event') == 'TRANSMITTING').groupBy(col('order_id')).agg(
    #     min(col('tracking_date')).alias('transmitting_date')).withColumn('transmitting_date_id', convert_date_id(col('transmitting_date')))

    # # Tabla temporal
    # data_transmitting_date.createOrReplaceTempView("stg_transmitting_date")

    # sqlcontext.cacheTable("stg_transmitting_date")

    #####################################################################################################################
    ## JOINS + Calculo de comisiones ####################################################################################

    pre_fact_orders_joins = sqlcontext.sql("""
                select 
                    pre_fact_orders.order_id,
                    IFNULL(pre_fact_orders.restaurant_id,0) as restaurant_id,
                    IFNULL(pre_fact_orders.area_id,0) as area_id,
                    cast(pre_fact_orders.delivery_date AS Timestamp) as delivery_date,
                    pre_fact_orders.delivery_date_id,
                    cast(pre_fact_orders.registered_date AS Timestamp) as registered_date,
                    pre_fact_orders.registered_date_id,
                    pre_fact_orders.order_hour,
                    pre_fact_orders.is_pre_order,
                    pre_fact_orders.is_take_out,
                    pre_fact_orders.coupon_used,
                    IFNULL(pre_fact_orders.user_id,0) as user_id,
                    IFNULL(pre_fact_orders.response_user_id,0) as response_user_id,
                    IFNULL(pre_fact_orders.restaurant_user_id,0) as restaurant_user_id,
                    pre_fact_orders.response_date,
                    pre_fact_orders.response_date_id,
                    IFNULL(pre_fact_orders.reject_message_id,0) as reject_message_id,
                    IFNULL(pre_fact_orders.delivery_time_id,0) as delivery_time_id,
                    IFNULL(pre_fact_orders.reception_system_id,0) as reception_system_id,
                    pre_fact_orders.first_successful,
                    IFNULL(pre_fact_orders.payment_method_id,0) as payment_method_id,
                    IFNULL(pre_fact_orders.amount_no_discount,0) as amount_no_discount,
                    IFNULL(pre_fact_orders.commission,0) as commission,
                    IFNULL(pre_fact_orders.discount,0) as discount,
                    IFNULL(pre_fact_orders.shipping_amount,0) as shipping_amount,
                    IFNULL(pre_fact_orders.total_amount,0) as total_amount,
                    IFNULL(pre_fact_orders.payment_amount,0) as payment_amount,
                    pre_fact_orders.tax_amount,
                    pre_fact_orders.final_amount,
                    pre_fact_orders.online_payment,
                    IFNULL(pre_fact_orders.responded_system_id,0) as responded_system_id,
                    IFNULL(pre_fact_orders.cop_user_id,0) as cop_user_id,
                    pre_fact_orders.address_description as user_address,
                    pre_fact_orders.address_phone as user_phone,
                    IFNULL(pre_fact_orders.white_label_id,0) as white_label_id,
                    IFNULL(pre_fact_orders.address_id,0) as address_id,
                    pre_fact_orders.with_logistics,
                    pre_fact_orders.dispatch_date,
                    pre_fact_orders.logistics_commission,
                    pre_fact_orders.promised_delivery_time_id,
                    IFNULL(pre_fact_orders.secondary_reception_system_id,0) as secondary_reception_system_id,
                    pre_fact_orders.client_guid,
                    pre_fact_orders.order_latitude as latitude,
                    pre_fact_orders.order_longitude as longitude,
                    IFNULL(pre_fact_orders.delivery_zone_id,0) as delivery_zone_id,
                    
                    IFNULL(stg_dim_business_type.business_type_id,0) as business_type_id,
                    
                    pre_fact_orders.user_identity_card,
                    pre_fact_orders.application_version,
                    pre_fact_orders.shipping_amount_no_discount,
                    pre_fact_orders.credit_card_commission,
                    IFNULL(pre_fact_orders.country_id,0) as country_id,
                    IFNULL(pre_fact_orders.city_id,0) as city_id,
                    pre_fact_orders.has_notes,
                    pre_fact_orders.restaurant_name,
                    pre_fact_orders.restaurant_latitude,
                    pre_fact_orders.restaurant_longitude,
                    pre_fact_orders.notes as order_notes,
                    pre_fact_orders.has_final_user_documents,
                    pre_fact_orders.has_voucher,
                    pre_fact_orders.has_wallet as has_wallet_credit,
                    
                    IFNULL(pre_fact_orders.distance_kilometers,0) AS distance_kilometers,
                    IFNULL(pre_fact_orders.distance_meters,0) AS distance_meters,




                    stg_dim_order_state.state_id as state_id,
                    stg_dim_application.application_id

                FROM pre_fact_orders

                    LEFT JOIN stg_dim_order_state as stg_dim_order_state 
                        ON stg_dim_order_state.state_name = lower(pre_fact_orders._state)
                    
                    LEFT JOIN stg_dim_application as stg_dim_application 
                        ON stg_dim_application.application_name = lower(pre_fact_orders._application)
                        
                    LEFT JOIN stg_dim_business_type as stg_dim_business_type
                        ON stg_dim_business_type.business_type_name = lower(pre_fact_orders._business_type)
                  """)


    pre_fact_orders_joins.createOrReplaceTempView("pre_fact_orders_joins")

    #sqlcontext.cacheTable("pre_fact_orders_joins")

    #####################################################################################################################
    ### GENERACION DE PARQUETS FINAL ###################################################################################

    #fact_orders = pre_fact_orders_joins.cache().distinct()

    fact_orders = pre_fact_orders_joins.orderBy('order_id', ascending=True)


    fact_orders.write.mode('overwrite').format("bigquery")\
    .option("temporaryGcsBucket","data-origins-storage").option("intermediateFormat","orc")\
    .save("peya-data-origins-stg.origins_data_stg.orders_to_upsert")


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
    generateFactOrdersUpserts(app_args)