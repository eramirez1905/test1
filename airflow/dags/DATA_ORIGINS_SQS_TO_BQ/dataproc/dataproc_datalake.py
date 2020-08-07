import argparse, configparser
import traceback
import time
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import json
import requests
from pyspark.sql.window import Window
from pyspark.sql.types import StructType

def caster(i):
    switcher = {
        'timestamp': TimestampType(),
        'boolean': BooleanType(),
        'double': DoubleType(),
        'integer':IntegerType(),
        'string': StringType(),
    }
    return switcher.get(i, "Invalid conversion key, you moron")

def datalake_partners(pSourceDir, pName, pTable):
    try:
        conf = SparkConf().setAppName("ETL_ENUM_PROCESS_{0}".format(pName))
        # conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # conf.set("spark.hive.mapred.supports.subdirectories", "true")
        # conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        print('--->START READING DATA FROM S3')
        df = sqlContext.read.load(pSourceDir, format='parquet', enconding='UTF-8')
        df = df.select(col('restaurant_id'),
                        col('restaurant_name'),
                        col('restaurant_state_id'),
                        col('restaurant_type_id'),
                        col('area_id'),
                        col('country_id'),
                        col('registered_date_id'),
                        col('is_vip'),
                        col('is_gold_vip'),
                        col('is_important_account'),
                        col('registered_date'),
                        col('created_date_id'),
                        col('created_date'),
                        col('last_order_date'),
                        col('first_order_date'),
                        col('rut'),
                        col('has_online_payment'),
                        col('has_stamps'),
                        col('has_pos'),
                        col('restaurant_street'),
                        col('restaurant_door_number'),
                        col('restaurant_phone'),
                        col('restaurant_owner_contact_name'),
                        col('restaurant_owner_contact_last_name'),
                        col('restaurant_owner_contact_phones'),
                        col('reception_system_name'),
                        col('commission'),
                        col('max_shipping_amount'),
                        col('min_delivery_amount'),
                        col('publicity_cost'),
                        col('automation_cost'),
                        col('avg_rating'),
                        col('main_cousine'),
                        col('qty_comments'),
                        col('is_talent'),
                        col('has_banner'),
                        col('has_discount'),
                        col('delivery_time'),
                        col('max_delivery_amount'),
                        col('min_shipping_amount'),
                        col('qty_zones'),
                        col('is_express'),
                        col('is_debtor'),
                        col('contract_number'),
                        col('has_mov'),
                        col('qty_current_gold_vip'),
                        col('has_featured_product'),
                        col('qty_current_featured_product'),
                        col('is_logistic'),
                        col('account_owner'),
                        col('address_id'),
                        col('avg_speed'),
                        col('is_premium'),
                        col('main_cousine_category_id'),
                        col('previous_state_id'),
                        col('change_state_date_id'),
                        col('link'),
                        col('logo'),
                        col('reception_system_enabled'),
                        col('is_day'),
                        col('is_night'),
                        col('city_id'),
                        col('disabled_reason'),
                        col('avg_food'),
                        col('avg_service'),
                        col('has_custom_photo_menu'),
                        col('disabled_motive'),
                        col('has_shipping_amount'),
                        col('orders_reception_system_id'),
                        col('orders_secondary_reception_system_id'),
                        col('logistics_commission'),
                        col('has_restaurant_portal'),
                        col('dispatch_current_month'),
                        col('sap_id'),
                        col('commission_restaurant'),
                        col('first_date_online'),
                        col('delivery_type'),
                        col('email'),
                        col('business_type'),
                        col('business_name'),
                        col('billing_info_id'),
                        col('is_online'),
                        col('is_new_online'),
                        col('is_offline'),
                        col('accepts_vouchers'),
                        col('is_active'),
                        col('is_pending'),
                        col('is_new_online_logistic'),
                        col('is_chain'),
                        col('is_new_registered'),
                        col('url_site'),
                        col('salesforce_id'),
                        col('centralized_reception_partner_id'),
                        col('affected_by_porygon_events'),
                        col('affected_by_porygon_optimizations'),
                        col('public_phone'),
                        col('automatic_phone'),
                        col('last_updated'),
                        col('qty_products'),
                        col('qty_picts'),
                        col('parallel_reception_system'),
                        col('qty_custom_picts'),
                        col('accepts_and_supports_vouchers'),
                        col('qty_picts_portal'),
                        col('menu_id'),
                        col('declines_but_supports_vouchers'),
                        col('concept_id'),
                        col('kitchen_id'),
                        col('is_darkstore'),
                        col('business_category_id'),
                        col('avg_main_cuisine_product_price'),
                        col('accepts_pre_order'),
                        col('requires_proof_of_delivery'),
                        col('qty_products_portal'),
                        col('last_heartbeat_primary_rs'),
                        col('last_heartbeat_secondary_rs'),
                        col('identity_card_behaviour'),
                        col('single_commission'),
                        col('shopper_type_id'),
                        col('audi_load_date')
        )
        print('<---END READING DATA FROM s3')

        df.write.mode('overwrite').format("bigquery")\
            .option("temporaryGcsBucket", "data-origins-temporal")\
            .option("intermediateFormat", "orc") \
            .save(pTable)

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

def datalake_entity(pSourceDir, pName, pTable):
    try:
        conf = SparkConf().setAppName("ETL_DATALAKE_PROCESS_{0}".format(pName))
        # conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # conf.set("spark.hive.mapred.supports.subdirectories", "true")
        # conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        print('--->START READING DATA ENUM FROM S3')
        df = sqlContext.read.load(pSourceDir, format='parquet', enconding='UTF-8')
        print('<---END READING DATA ENUM FROM s3')

        df.write.mode('overwrite').format("bigquery")\
            .option("temporaryGcsBucket", "data-origins-temporal")\
            .option("intermediateFormat", "orc") \
            .save(pTable)

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", type=str, help="S3 PATH")
    parser.add_argument("-name", type=str, help="Model Name")
    parser.add_argument("-table", type=str, help="BQ Table")
    parser.add_argument("-type", type=str, help="Table Type")

    return parser.parse_args()

if __name__ == '__main__':
    app_args = get_app_args()
    print(app_args.name)
    if app_args.name == 'partners':
        datalake_partners(app_args.source
                            , app_args.name
                            , app_args.table
                            )
    else:
        datalake_entity(app_args.source
                          , app_args.name
                          , app_args.table
                          )