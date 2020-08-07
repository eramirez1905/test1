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

schema_path = 's3a://peyabi.code.live/bi-core-tech/schemas/{}'
enum_path = 's3a://peyabi.datalake.live/bi-core-tech/raw/nrt/entidades/enums/{0}.csv'

def caster(i):
    switcher = {
        'timestamp': TimestampType(),
        'boolean': BooleanType(),
        'double': DoubleType(),
        'integer':IntegerType(),
        'string': StringType(),
    }
    return switcher.get(i, "Invalid conversion key, you moron")

def add_date_id(pDfInit, pCol):
    result_df = pDfInit.withColumn("date_id", from_unixtime(unix_timestamp(col(pCol)), "yyyyMMdd").cast(dataType=IntegerType()))
    return result_df

# Convierte fecha en formato YYYYMMDD
def convert_date_id(date_value):
    return date_format(date_value, 'yyyyMMdd').cast(IntegerType())

def add_enum_id(pDf, pEnumName, pCol, sqlContext):
    dfEnum = sqlContext.read.load(enum_path.format(pEnumName), format="csv", header=True, encoding="utf8")
    dfEnum = dfEnum.withColumn('id', dfEnum['id'].cast(IntegerType()))
    dfEnum = dfEnum.withColumnRenamed('id', pCol + '_id')
    dfEnum = dfEnum.alias('enum')
    df = pDf.alias('df')
    df = df.join(dfEnum, df[pCol] == dfEnum['name'], how='left').select('df.*', 'enum.{0}_id'.format(pCol))
    return df

def sqs_stg_process(pSourceDir, pTargetDir, pPartition, pLastDays, pModel, pSchema):
    try:
        conf = SparkConf().setAppName("ETL_SQS_PROCESS_{0}_{1}".format(pPartition, pModel))
        conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        conf.set("spark.hive.mapred.supports.subdirectories", "true")
        conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        # Load JSON Schema
        f = sc.textFile(schema_path.format(pSchema)).collect()
        jsonData = ''.join(f)
        d = json.loads(jsonData)
        modelSchema = StructType.fromJson(d)

        #Filter Days
        filter_days = int(pLastDays)  # Cantidad de dias para filtros de ordenes y particion
        base_date = datetime.now().date()  # Fecha Filtro para registered_date de la orden
        partition_start = datetime.strftime(base_date - timedelta(days=filter_days), "%Y%m%d")
        print('Particion inicial: {}'.format(partition_start))

        sourceDir = pSourceDir + '/*/*'.format(pPartition)

        print('--->START READING DATA NRT FROM S3')
        dfSqs = sqlContext.read.option("mergeSchema", "true")\
            .option("basePath", pSourceDir)\
            .schema(modelSchema)\
            .load(sourceDir, format="parquet", encoding="utf8")\
            .filter(col('yyyymmdd') > partition_start)
        print('<---END READING DATA NRT FROM s3')

        print('---------------------- RAW SCHEMA ----------------------')
        dfSqs.printSchema()
        dfFilter = filter_last_message(dfSqs)
        print('<---END READING ENUM FROM s3')

        # ###########################################
        # PARTNER
        # ###########################################
        print("PARTNER")
        dfPartner = dfFilter.select(
                                    col('acceptsPreOrder').alias('accepts_pre_order'),
                                    col('acceptsVouchers').alias('accepts_vouchers'),
                                    col('address.area.city.id').alias('city_id'),
                                    col('address.area.id').alias('area_id'),
                                    col('address.doorNumber').alias('restaurant_door_number'),
                                    col('address.id').alias('address_id'),
                                    col('address.phone').alias('restaurant_phone'),
                                    col('address.street').alias('restaurant_street'),
                                    col('affectedByPorygonEvents').alias('affected_by_porygon_events'),
                                    col('affectedByPorygonOptimizations').alias('affected_by_porygon_optimizations'),
                                    col('automaticPhone').alias('automatic_phone'),
                                    col('automaticPhoneEnabled').alias('automatic_phone_enabled'),
                                    col('billingInfo.id').alias('billing_info_id'),
                                    col('branchParent.id').alias('branch_id'),
                                    col('branchParent.name').alias('branch_name'),
                                    col('businessType').alias('business_name'),
                                    col('businessType').alias('business_type'),
                                    col('capacityCheck').alias('capacity_check'),
                                    col('country.id').alias('country_id'),
                                    col('country.url').alias('country_url_site'),
                                    convert_date_id(col('dateCreated')).alias('created_date_id'),
                                    col('dateCreated').alias('created_date'),
                                    col('deliveryTime.description').alias('delivery_time_description'),
                                    col('deliveryTime.id').alias('delivery_time_id'),
                                    col('deliveryTime.maxMinutes').alias('delivery_time_max_minutes'),
                                    col('deliveryTime.minMinutes').alias('delivery_time_min_minutes'),
                                    col('deliveryTime.name').alias('delivery_time'),
                                    col('deliveryTime.order').alias('delivery_time_order'),
                                    col('deliveryType').alias('delivery_type'),
                                    col('description').alias('description'),
                                    col('disabledReason').alias('disabled_reason'),
                                    # col('foodCategories[].id').alias('main_cousine_category_id'),
                                    col('headerImage').alias('header_image'),
                                    col('homeVip').alias('home_vip'),
                                    col('id').alias('restaurant_id'),
                                    col('identityCardBehaviour').alias('identity_card_behaviour'),
                                    col('integrationCode').alias('integration_code'),
                                    col('integrationName').alias('integration_name'),
                                    coalesce(col('isDarkstore'), lit('false').cast(BooleanType())).alias('is_darkstore'),
                                    col('isImportantAccount').alias('is_important_account'),
                                    col('isVip').alias('is_vip'),
                                    col('lastUpdated').alias('last_updated'),
                                    col('link').alias('link'),
                                    col('logo').alias('logo'),
                                    col('mandatoryAddressConfirmation').alias('mandatory_address_confirmation'),
                                    col('mandatoryIdentityCard').alias('mandatory_identity_card'),
                                    col('mandatoryPaymentAmount').alias('mandatory_payment_amount'),
                                    col('maxShippingAmount').alias('max_shipping_amount'),
                                    # col('').alias('has_banner'),
                                    col('menu.id').alias('menu_id'),
                                    col('menu.name').alias('menu_name'),
                                    col('messageId').alias('message_id'),
                                    col('messageTimestamp').alias('message_timestamp'),
                                    col('migrationId').alias('backend_id'),
                                    col('minDeliveryAmount').alias('min_delivery_amount'),
                                    col('name').alias('restaurant_name'),
                                    col('noIndex').alias('restaurant_noindex'),
                                    col('noIndexGooglePlaces').alias('restaurant_noindex_google_places'),
                                    col('ordersReceptionSystem.id').alias('orders_reception_system_id'),
                                    col('ordersReceptionSystem.isPos').alias('has_pos'),
                                    col('ordersReceptionSystem.name').alias('reception_system_name'),
                                    col('ordersSecondaryReceptionSystem.id').alias('orders_secondary_reception_system_id'),
                                    col('ordersSecondaryReceptionSystem.isPos').alias('orders_secondary_reception_system_ispos'),
                                    col('ordersSecondaryReceptionSystem.name').alias('orders_secondary_reception_system_name'),
                                    coalesce(col('parallelReceptionSystem'), lit('false').cast(BooleanType())).alias('parallel_reception_system'),
                                    when(array_contains(col('paymentMethods.online'), True), True).otherwise(False).alias('has_online_payment'),
                                    # col('paymentMethods[].online = true').alias('has_online_payment'),
                                    col('privatePhone').alias('private_phone'),
                                    col('publicPhone').alias('public_phone'),
                                    coalesce(col('receptionSystemEnabled'), lit('false').cast(BooleanType())).alias('reception_system_enabled'),
                                    col('registeredDate').alias('registered_date'),
                                    convert_date_id(col('registeredDate')).alias('registered_date_id'),
                                    # col('registeredDate').alias('is_new_registered'),
                                    when(convert_date_id(col('registeredDate')) == convert_date_id(date_add(current_date(), -1)), True).otherwise(False).alias('is_new_registered'),
                                    # col('restaurantBrand.id is not null').alias('is_chain'),
                                    when(col('restaurantBrand.id') > 0, True).otherwise(False).alias('is_chain'),
                                    col('restaurantBrand.id').alias('centralized_reception_partner_id'),
                                    col('restaurantBrand.name').alias('restaurant_brand_name'),
                                    col('restaurantTrustScore.id').alias('restaurant_trust_score_id'),
                                    col('salesforceId').alias('salesforce_id'),
                                    col('shippingAmount').alias('shipping_amount'),
                                    when(col('shippingAmount') > 0, True).otherwise(False).alias('has_shipping_amount'),
                                    col('shippingAmountIsPercentage').alias('shipping_amount_is_percentage'),
                                    col('stampsNeeded').alias('stamps_needed'),
                                    when((col('stampsNeeded') > 0) & (col('stampsState') == 'ACTIVE'), True).otherwise(False).alias('has_stamps'),
                                    col('stampsState').alias('stamps_state'),
                                    # 1 ON_LINE | 2 RETENTION |3 PENDING | 4	UPDATING | 5	DELETED | 6	CLOSED
                                    col('state').alias('restaurant_state'),
                                    when(col('state') == "PENDING", True).otherwise(False).alias('is_pending'),
                                    # col('state not in ("1", "4") && type <> "3"').alias('is_offline'),
                                    when((((col('state') != "ON_LINE") & (col('state') != "UPDATING")) & (col('type') != "DELIVERY")),
                                         True).otherwise(False).alias('is_offline'),
                                    # col('state in ("1", "4") && type = 3').alias('is_online'),
                                    when(((col('state') == "ON_LINE") | (col('state') == "UPDATING")) & (col('type') == "DELIVERY"),
                                         True).otherwise(False).alias('is_online'),
                                    when(array_contains(col('tags'), '##DELIVERY_PREMIUM'), True).otherwise(False).alias('is_premium'),
                                    when(array_contains(col('tags'), '##delivery-express'), True).otherwise(False).alias('is_express'),
                                    coalesce(col('talent'), lit('false').cast(BooleanType())).alias('is_talent'),
                                    col('type').alias('restaurant_type'),
                                    coalesce(col('useLiveOrderTracking'), lit('false').cast(BooleanType())).alias('is_logistic'),
                                    col('businessCategory.id').alias('business_category_id'),

        )
        # # CONVERT THROUGH CONVERSION MATRIX
        # if pConversionList != 'False':
        #     conversions = pConversionList.split(";")
        #     for sub_conv in conversions:
        #         pos = sub_conv.find(":")
        #         key = sub_conv[0:pos]
        #         conv_list = sub_conv[pos + 1:]
        #         listed_conv = conv_list.split(",")
        #         for this_col in listed_conv:
        #             print('CONVERTING ' + this_col + ' to ' + key)
        #             df = df.withColumn(this_col, df[this_col].cast(dataType=caster(key)))
        #     print('---------------------- CONVERTED SCHEMA ----------------------')
        #     df.printSchema()
        #     # df.show(5)
        # else:
        #     print('------------ NO CONVERSION TO DO HERE ------------')

        ##STATE
        ##BUSINESS STATE
        dfPartner = add_enum_id(dfPartner, 'partner_state', 'restaurant_state', sqlContext)
        ##BUSINESS TYPE
        dfPartner = add_enum_id(dfPartner, 'partner_type', 'business_type', sqlContext)
        ##DELIVERY TYPE
        dfPartner = add_enum_id(dfPartner, 'partner_delivery_type', 'delivery_type', sqlContext)
        ##STAMPS STATE
        dfPartner = add_enum_id(dfPartner, 'partner_stamps_state', 'stamps_state', sqlContext)
        dfPartner.printSchema()
        # dfPartner
        dfPartner.write.parquet(pTargetDir + '/partner', mode='overwrite', compression='snappy')
        dfPartner.write.mode('overwrite').format("bigquery").option("temporaryGcsBucket", "data-ori-test-poc-storage") \
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_stg.partner")

        # ###########################################
        # TAGS
        # ###########################################
        print("TAGS")
        dfTags = dfFilter.select(col('id').alias('restaurant_id'),
                                 explode_outer(col('tags')).alias('tag')
                                ).filter(col('tag').isNotNull())
        dfTags.printSchema()
        # dfTags
        dfTags.write.parquet(pTargetDir + '/tags', mode='overwrite', compression='snappy')
        dfTags.write.mode('overwrite').format("bigquery").option("temporaryGcsBucket", "data-origins-temporal") \
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_stg.partner_tags")

        # ###########################################
        # CHANNEL
        # ###########################################
        print("channels")
        dfChannels = dfFilter.select(col('id').alias('restaurant_id'),
                                  explode_outer(col('channels')).alias('channel')
                                  ).filter(col('channel.id').isNotNull())

        dfChannels = dfChannels.select(col('restaurant_id'),
                                          col('channel.id').alias('channel_id'),
                                          col('channel.slug').alias('channel_slug')
                                          )
        dfChannels.printSchema()
        # dfChannels
        dfChannels.write.parquet(pTargetDir + '/channels', mode='overwrite', compression='snappy')
        dfChannels.write.mode('overwrite').format("bigquery").option("temporaryGcsBucket", "data-origins-temporal") \
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_stg.partner_channels")

        # ###########################################
        # AREAS
        # ###########################################
        print("AREAS")
        dfAreas = dfFilter.select(col('id').alias('restaurant_id'),
                                    explode_outer(col('areas')).alias('area')
                                  ).filter(col('area.id').isNotNull())

        dfAreas = dfAreas.select(col('restaurant_id'),
                                 col('area.id').alias('area_id'),
                                )
        dfAreas.printSchema()
        # dfAreas
        dfAreas.write.parquet(pTargetDir + '/areas', mode='overwrite', compression='snappy')
        dfAreas.write.mode('overwrite').format("bigquery").option("temporaryGcsBucket", "data-origins-temporal") \
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_stg.partner_areas")

        # ###########################################
        # foodCategories
        # ###########################################
        print("foodCategories")
        dfFoodCategories = dfFilter.select(col('id').alias('restaurant_id'),
                                            explode_outer(col('foodCategories')).alias('food')
                                          ).filter(col('food.id').isNotNull())

        dfFoodCategories = dfFoodCategories.select(col('restaurant_id'),
                                                    col('food.id').alias('restaurant_food_category_id'),
                                                    col('food.enabled').alias('enabled'),
                                                    col('food.foodCategory.id').alias('food_category_id'),
                                                    col('food.foodCategory.name').alias('food_category_name'),
                                                    col('food.foodCategory.isDeleted').alias('food_category_isdeleted'),
                                                    col('food.foodCategory.visible').alias('food_category_visible'),
                                                    col('food.foodCategory.country.id').alias('food_category_country_id'),
                                                    col('food.manuallySorted').alias('manually_sorted'),
                                                    col('food.percentage').alias('percentage'),
                                                    col('food.quantity').alias('quantity'),
                                                    col('food.sortingIndex').alias('sorting_index'),
                                                    col('food.state').alias('state'),
                                          )

        dfFoodCategories = add_enum_id(dfFoodCategories, 'food_category_state', 'state', sqlContext)

        dfFoodCategories.printSchema()
        # dfFoodCategories
        dfFoodCategories.write.parquet(pTargetDir + '/food_categories', mode='overwrite', compression='snappy')
        dfFoodCategories.write.mode('overwrite').format("bigquery").option("temporaryGcsBucket", "data-origins-temporal") \
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_stg.partner_food_categories")

        # ###########################################
        # paymentMethods
        # ###########################################
        print("paymentMethods")
        dfPaymentMethods = dfFilter.select(col('id').alias('restaurant_id'),
                                            explode_outer(col('paymentMethods')).alias('paymentMethod')
                                          ).filter(col('paymentMethod.id').isNotNull())

        dfPaymentMethods = dfPaymentMethods.select(col('restaurant_id'),
                                                    col('paymentMethod.id').alias('payment_method_id'),
                                                    col('paymentMethod.online').alias('payment_method_online'),
                                                  )
        dfPaymentMethods.printSchema()
        # dfPaymentMethods
        dfPaymentMethods.write.parquet(pTargetDir + '/payment_methods', mode='overwrite', compression='snappy')
        dfPaymentMethods.write.mode('overwrite').format("bigquery").option("temporaryGcsBucket",
                                                                           "data-origins-temporal") \
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_stg.partner_payment_methods")

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

def sqs_raw_process(pSourceDir, pPartition, pModel, pSchema):
    try:
        conf = SparkConf().setAppName("ETL_KINESIS_PROCESS_{0}_{1}".format(pPartition, pModel))
        conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        conf.set("spark.hive.mapred.supports.subdirectories", "true")
        conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        # Load JSON Schema
        f = sc.textFile(schema_path.format(pSchema)).collect()
        jsonData = ''.join(f)
        d = json.loads(jsonData)
        modelSchema = StructType.fromJson(d)

        print("Base Dir:" + pSourceDir)
        sourceDir = pSourceDir + '/yyyymmdd={0}/hh=*/*'.format(pPartition)
        print("Source Dir:" + sourceDir)

        print('--->START READING DATA KINESIS FROM S3')
        df = sqlContext.read.option("mergeSchema", "true")\
            .option("basePath", pSourceDir)\
            .schema(modelSchema).load(sourceDir, format="json", enconding='UTF-8')
        print('<---END READING DATA KINESIS FROM s3')

        df = df.withColumn('yyyymmdd_str', df['yyyymmdd'].cast(dataType=caster('string')))
        df = df.withColumn('yyyymmdd', to_date(df['yyyymmdd_str'], "yyyyMMdd"))
        df = df.withColumn('hh', df['hh'].cast(dataType=caster('integer')))
        df = df.drop('yyyymmdd_str')

        df.write.mode('overwrite').format("bigquery")\
            .option("temporaryGcsBucket", "data-origins-temporal")\
            .option("intermediateFormat", "orc") \
            .option("partitionField", "yyyymmdd")\
            .option("datePartition", pPartition)\
            .save("peya-data-origins-stg.origins_data_message.sqs_{0}".format(pModel))

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

def filter_last_message(raw_data, pColId="id", pColOrder="messageTimestamp"):
    window = Window.partitionBy(col(pColId)).orderBy(col(pColOrder).desc())
    raw_data = raw_data.withColumn('last', row_number().over(window))
    raw_data = raw_data.filter(col('last') == 1)
    raw_data = raw_data.drop('last')
    raw_data.cache()
    return raw_data

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source_path", type=str, help="S3 ODS PATH")
    parser.add_argument("-target_path", type=str, help="S3 Target PATH")
    parser.add_argument("-partition", type=str, help="PARTITION YYYYMM")
    parser.add_argument("-model", type=str, help="SF MODEL")
    parser.add_argument("-days_back", type=str, help="Last Days")
    parser.add_argument("-type", type=str, help="Type of Process (STG or RAW)")
    parser.add_argument("-schema", type=str, help="Schema PATH")

    return parser.parse_args()

if __name__ == '__main__':
    app_args = get_app_args()
    print(app_args.model)
    if app_args.type == 'stg':
        sqs_stg_process(app_args.source_path
                        , app_args.target_path
                        , app_args.partition
                        , app_args.days_back
                        , app_args.model
                        , app_args.schema
                        )
    elif app_args.type == 'raw':
        sqs_raw_process(app_args.source_path
                        , app_args.partition
                        , app_args.model
                        , app_args.schema
                        )