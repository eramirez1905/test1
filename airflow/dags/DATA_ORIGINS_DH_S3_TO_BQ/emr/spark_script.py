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

dir_countries = 's3a://peyabi.datalake.live/bi-core-tech/refined/dim_country_details'

api_url = "http://10.0.90.144:8405/api/hive/create/partitions"
api_param = "environment=PROD&schema=datalake&name={0}&initDate={1}&endDate={2}&createTable=false"
api_header = {"Authorization": "QzM5e!BZ$ASrd0EE$qUQTM2Vg#jfSyn3"}

def caster(i):
    switcher = {
        'timestamp': TimestampType(),
        'boolean': BooleanType(),
        'double': DoubleType(),
        'integer':IntegerType(),
        'string': StringType(),
    }
    return switcher.get(i, "Invalid conversion key, you moron")

def date_partitioning(i):
    switcher = {
        'day': [(year, "created_year"), (month, "created_month"), (dayofmonth, "created_day")],
        'month': [(year, "created_year"), (month, "created_month")],
        'year': [(year, "created_year")],
    }
    return switcher.get(i, "Invalid partitioning level, you moron")

def add_country_id(pDfInit, pCol, pSqlContext):
    df_countries = pSqlContext.read.load(dir_countries, format="parquet", encoding="utf8", multiLine='true')
    pDfInit = pDfInit.alias('pDfInit')
    df_countries = df_countries.alias('df_countries')
    joined_df = pDfInit.join(df_countries, pDfInit[pCol] == df_countries['country_name_en'], how='left').select('pDfInit.*', 'df_countries.country_id')
    return joined_df

def add_partition_range(pDf, pModel, pSqlContext, pPartition, pCol='yyyymm'):
    datalake_table = datalake_map.get(pModel) or pModel
    # dfPartition = pDf.groupBy().max(pCol).min(pCol).select(col(min_exp).alias('min_partition'),
    #                                                        col(max_exp).alias('max_partition'))
    pDf.createOrReplaceTempView('tmp_view')
    dfPartition = pSqlContext.sql("""
                                select 
                                          min({0}) as min_partition
                                        , max({0}) as max_partition
                                 from tmp_view
                                 where {0} >= '{1}'
                                 """.format(pCol, pPartition))
    print('--->ADD PARTITION')
    for row in dfPartition.rdd.collect():
        print('--->CREATE PARTITION {0} TO {1}'.format(row['min_partition'], row['max_partition']))
        result = createDatalakePartition(datalake_table, row['min_partition'], row['max_partition'])
        print('--->CREATE PARTITION RESULT {0}'.format(str(result)))
    print('--->END PARTITION')

def add_partition_data(pDf, pModel, pSqlContext, pCol='yyyymm'):
    datalake_table = datalake_map.get(pModel) or pModel
    pDf.createOrReplaceTempView('tmp_view')
    dfPartition = pSqlContext.sql("select distinct {0} as partition from tmp_view".format(pCol))
    print('--->ADD PARTITION')
    for row in dfPartition.rdd.collect():
        print('--->CREATE PARTITION {0} TO {1}'.format(row['partition'], row['partition']))
        result = createDatalakePartition(datalake_table, row['partition'], row['partition'])
        print('--->CREATE PARTITION RESULT {0}'.format(str(result)))
    print('--->END PARTITION')

def add_date_id(pDfInit, pCol):
    result_df = pDfInit.withColumn("date_id", from_unixtime(unix_timestamp(col(pCol)), "yyyyMMdd").cast(dataType=IntegerType()))
    return result_df

def move_files(pSourceDir, pTargetDir, pPartition, pPartitioningCol, pConversionList, pCountryCol, pModel, pOverwriteMode='static'):
    try:
        conf = SparkConf().setAppName("ETL_PROCESS_PANDORA_{0}_{1}".format(pPartition, pModel))
        conf.set("spark.sql.sources.partitionOverwriteMode", pOverwriteMode)
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING DATA FROM S3')
        # df = sqlContext.read.load(pSourceDir + '/*/*.parquet', format="parquet", encoding="utf8", multiLine='true')
        print("""df = sqlContext.read.option("mergeSchema", "true").load(pSourceDir, format="parquet", encoding="utf8", multiLine='true')""")
        df = sqlContext.read.option("mergeSchema", "true").load(pSourceDir, format="parquet", encoding="utf8", multiLine='true')
        # print(str(df.count()) + ' lineas')
        print('<---END READING DATA FROM s3')
        print('---------------------- RAW SCHEMA ----------------------')
        df.printSchema()

        # CONVERT THROUGH CONVERSION MATRIX
        if pConversionList != 'False':
            conversions = pConversionList.split(";")
            for sub_conv in conversions:
                pos = sub_conv.find(":")
                key = sub_conv[0:pos]
                conv_list = sub_conv[pos + 1:]
                listed_conv = conv_list.split(",")
                for this_col in listed_conv:
                    print('CONVERTING ' + this_col + ' to ' + key)
                    df = df.withColumn(this_col, df[this_col].cast(dataType=caster(key)))
            print('---------------------- CONVERTED SCHEMA ----------------------')
            df.printSchema()
            # df.show(5)
        else:
            print('------------ NO CONVERSION TO DO HERE ------------')

        if pCountryCol != 'False':
            df = add_country_id(df, pCountryCol, sqlContext)

        # if pPartitioningCol != 'False':
        #     df = add_date_id(df, pPartitioningCol)
        print('SAVING TO ' + pTargetDir)

        if pPartitioningCol != 'False':
            df.write.mode('overwrite').format("bigquery") \
                .option("temporaryGcsBucket", "data-origins-temporal") \
                .option("intermediateFormat", "orc") \
                .option("partitionField", pPartitioningCol) \
                .save("peya-data-origins-stg.origin_data_refined.dmarts_{0}".format(pModel))
                # .option("datePartition", pPartition) \
            #Create Partition
            # add_partition_range(df, pModel, sqlContext, pPartition)
        else:
            df.write.mode('overwrite').format("bigquery") \
                .option("temporaryGcsBucket", "data-origins-temporal") \
                .option("intermediateFormat", "orc") \
                .save("peya-data-origins-stg.origin_data_refined.dmarts_{0}".format(pModel))

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source_path", type=str, help="S3 ODS PATH")
    parser.add_argument("-live_path", type=str, help="S3 LIVE PATH")
    parser.add_argument("-partition", type=str, help="PARTITION YYYYMM")
    parser.add_argument("-model", type=str, help="SF MODEL")
    parser.add_argument("-conversions", type=str, help="COLUMNS TO CONVERT")
    parser.add_argument("-partition_col", type=str, help="PARTITIONING INFO")
    parser.add_argument("-country_col", type=str, help="COUNTRY MAPPING")
    parser.add_argument("-overwrite_mode", type=str, help="Overwrite Mode")

    return parser.parse_args()


if __name__ == '__main__':
    app_args = get_app_args()
    print(app_args.model)
    # move_to_dl_live(app_args.source_path, app_args.live_path, app_args.partition, app_args.partition_col, app_args.conversions, 'False', 'False', app_args.model)
    move_files(app_args.source_path
                    , app_args.live_path
                    , app_args.partition
                    , app_args.partition_col
                    , app_args.conversions
                    , app_args.country_col
                    , app_args.model
                    , app_args.overwrite_mode
                    )
    # else:
    #     print(app_args.model)
    #     print('--------------------- ERROR NO MODEL FOUND ---------------------')
    #     raise ValueError('object not listed in conditions, schmock')
