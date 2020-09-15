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

dir_countries = 's3://peyabi.datalake.live/bi-core-tech/refined/dim_country_details'

api_url = "http://10.0.90.144:8405/api/hive/create/partitions"
api_param = "environment=PROD&schema=datalake&name={0}&initDate={1}&endDate={2}&createTable=false"
api_header = {"Authorization": "QzM5e!BZ$ASrd0EE$qUQTM2Vg#jfSyn3"}

datalake_map = {
    "salesforce_tickets": "gcc_salesforce_tickets",
    "salesforce_chats": "gcc_salesforce_chats",
    "navigation_history": "help_center_navigation_history",
    "session_events": "help_center_session_events",
    "sessions": "help_center_sessions",
    "flow_session": "help_center_flow_session"
}

def createDatalakePartition(pTableName, pPartitionStart, pPartitionEnd):
    params = api_param.format(pTableName, pPartitionStart, pPartitionEnd)
    for i in range(0, 3):
        try:
            response = requests.post("{0}?{1}".format(api_url, params)
                                        , headers=api_header)
            if response.status_code != 200:
                raise "API Status error!"
            else:
                return True
        except:
            print("API ERROR: DATALAKE TABLE: {0} PARTITION: {1} TO {2}".format(pTableName, pPartitionStart, pPartitionEnd))
            print(traceback.format_exc())
            time.sleep(5)
            continue
        break
    return False

def caster(i):
    switcher = {
        'timestamp': TimestampType(),
        'boolean': BooleanType(),
        'double': DoubleType(),
        'integer':IntegerType(),
    }
    return switcher.get(i, "Invalid conversion key, you moron")

def country_col_map(i):
    switcher = {
        'nps_new_riders': "iso_code_2",
        'nps_existing_riders': "iso_code_2"
    }
    return switcher.get(i, "country_name_en")

def date_partitioning(i):
    switcher = {
        'day': [(year, "created_year"), (month, "created_month"), (dayofmonth, "created_day")],
        'month': [(year, "created_year"), (month, "created_month")],
        'year': [(year, "created_year")],
    }
    return switcher.get(i, "Invalid partitioning level, you moron")

def add_country_id(pDfInit, pCol, pSqlContext, pModel):
    df_countries = pSqlContext.read.load(dir_countries, format="parquet", encoding="utf8", multiLine='true')
    pDfInit = pDfInit.alias('pDfInit')
    df_countries = df_countries.alias('df_countries')
    joined_df = pDfInit.join(df_countries, lower(pDfInit[pCol]) == lower(df_countries[country_col_map(pModel)]), how='left').select('pDfInit.*', 'df_countries.country_id')
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

def move_to_dl_live(pSourceDir, pTargetDir, pPartition, pPartitioningCol, pConversionList, pCountryCol, pModel, pOverwriteMode='static'):
    try:
        conf = SparkConf().setAppName("ETL_PROCESS_PANDORA_{0}_{1}".format(pPartition, pModel))
        conf.set("spark.sql.sources.partitionOverwriteMode", pOverwriteMode)
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING DATA FROM S3')
        df = sqlContext.read.option("header", "true").csv(pSourceDir, sep=',', multiLine='true')
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
            df = add_country_id(df, pCountryCol, sqlContext, pModel)

        if pPartitioningCol != 'False':
            df = add_date_id(df, pPartitioningCol)

        if pPartitioningCol != 'False':
            print('SAVING TO '+ pTargetDir)
            df = df.withColumn("yyyymm", from_unixtime(unix_timestamp(col(pPartitioningCol)), "yyyyMM"))
            df.write.partitionBy("yyyymm").parquet(pTargetDir + '/', mode='overwrite', compression='snappy')
            #Create Partition
            add_partition_range(df, pModel, sqlContext, pPartition)
        else:
            df.write.parquet(pTargetDir, mode='overwrite', compression='snappy')

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

# ** NOT USED **
def simple_obj_to_hdfs(pOdsDir, pTargetDir, pStgDir, pJsonPath, pPartition, pConversionList, pPartitioning, pModel):
    try:
        conf = SparkConf().setAppName("ETL_PROCESS_SALESFORCE_{0}_{1}".format(pPartition, pModel))
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING DATA FROM S3')
        df = sqlContext.read.json(pJsonPath, encoding="utf8")

        # print(str(df.count()) + ' lineas')
        print('<---END READING DATA FROM s3')
        # print('---------------------- RAW SCHEMA ----------------------')

        # CONVERT THROUGH CONVERSION MATRIX
        conversions = pConversionList.split(";")
        for sub_conv in conversions:
            pos = sub_conv.find(":")
            key = sub_conv[0:pos]
            conv_list = sub_conv[pos + 1:]
            listed_conv = conv_list.split(",")
            for this_col in listed_conv:
                print('CONVERTING ' + this_col + 'to ' + key)
                df = df.withColumn(this_col, df[this_col].cast(dataType=caster(key)))

        print('---------------------- CONVERTED SCHEMA ----------------------')
        df.printSchema()
        # df.show(5)
        print('SAVING INTO -> ' + pOdsDir)
        print(str(len(df.columns)) + ' COLUMNS')
        df.write.parquet(pOdsDir + '/', mode='overwrite', compression='snappy')

        # INITIALIZE PARTITION INFORMATION
        partition_list = []
        exprs = []
        if pPartitioning != "None":
            parting = json.loads(pPartitioning.replace('\'', "\""))
            conf = parting[parting['col_name']]
            if conf['type'] == "timestamp":
                dt = col("createddate").cast("date")
                fname = date_partitioning(conf["level"])
                exprs = [col("*")] + [f(dt).alias(name) for f, name in fname]
                partition_list = [name for _, name in fname]

        try:
            df1 = sqlContext.read.load(pOdsDir, format="parquet", encoding="utf8", multiLine='true')
            df1.createOrReplaceTempView('ods')
            print('DATA FRAME ODS')
            print(str(len(df1.columns)) + ' COLUMNS')
            # print(str(df1.count()) + ' REGISTROS')
            # df1.show(10)
        except AnalysisException:
            print('ODS DIRECTORY IS EMPTY')
        try:
            df2 = sqlContext.read.load(pTargetDir, format="parquet", encoding="utf8", multiLine='true')
            # DROP PARTITION COLUMNS TO AVOID UNMATCHED UNION
            if partition_list:
                print('DO THE DROP')
                for col_to_drop in partition_list:
                    df2 = df2.drop(col_to_drop)
            df2.createOrReplaceTempView('live')
            print('DATA FRAME LVE')
            print(str(len(df2.columns)) + ' COLUMNS')
            # print(str(df2.count()) + ' REGISTROS')
            # df2.show(10)
            df3 = sqlContext.sql("""
                SELECT * FROM
                ods
                UNION
                SELECT * FROM
                live""")
        except AnalysisException as xError:
            print(xError)
            df3 = sqlContext.sql("""
                SELECT * FROM
                ods""")
            print('LIVE DIRECTORY IS EMPTY')

        print('DATA FRAME STG')
        print(str(len(df3.columns)) + ' COLUMNS')
        # print(str(df3.count()) + ' REGISTROS')
        # df3.printSchema()
        df4 = df3.groupBy('id').max("time_fetched_from_salesforce").select('id', col(
            'max(time_fetched_from_salesforce)').alias('max_date'))
        df4.createOrReplaceTempView('maxes')

        # FOR VERIFICATION ONLY
        query = """SELECT MAX(max_date) , MIN(max_date)
                    FROM maxes"""
        df_posta = sqlContext.sql(query)
        print('---PRUEBA---')
        # df_posta.show(10)
        # FOR VERIFICATION ONLY

        df3 = df3.alias('df3')
        df_final = df3.join(df4, (df3.id == df4.id) & (df3.time_fetched_from_salesforce == df4.max_date),
                            'inner').select('df3.*')
        # print(str(df_final.count()) + ' REGISTROS DATA FRAME FINAL\n ' + pStgDir)
        print(str(len(df_final.columns)) + ' COLUMNS')
        df_final.write.parquet(pStgDir, mode='overwrite', compression='snappy')

        df_live = sqlContext.read.load(pStgDir, format="parquet", encoding="utf8", multiLine='true')

        # ULTIMATE FINAL WRITE
        # PARTITIONING THROUGH CONVERSION JSON
        if partition_list:
            df_live.select(*exprs).write.partitionBy(*partition_list).parquet(pTargetDir + '/', mode='overwrite',
                                                                              compression='snappy')
        else:
            df_live.write.parquet(pTargetDir, mode='overwrite', compression='snappy')

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

# ** NOT USED **
def partitioned_obj_to_hdfs(pHdfsStgPath, pHdfsOdsPath, pHdfsLivePath, pJsonPath, pPartition, pEpochList, pModel):
    try:
        conf = SparkConf().setAppName("ETL_PROCESS_SALESFORCE_{0}_{1}".format(pPartition, pModel))
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        print('--->START READING DATA FROM S3')
        df = sqlContext.read.json(pJsonPath, encoding="utf8")

        # print(str(df.count()) + ' lineas')
        print('<---END READING DATA FROM s3')
        # print('---------------------- RAW SCHEMA ----------------------')
        print(pEpochList)
        list = pEpochList.strip('][').split(', ')
        for col in list:
            print('CONVERTING ' + col)
            df = df.withColumn(col, df[col].cast(dataType=TimestampType()))
        # df = df.withColumn('createddate', df["createddate"].cast(dataType=TimestampType()))
        print('---------------------- CONVERTED SCHEMA ----------------------')
        df.printSchema()
        # df.show(5)
        print('SAVING INTO -> ' + pHdfsLivePath + pPartition + '/')
        # df.select(*exprs).write.partitionBy(*(name for _, name in fname)).parquet(pHdfsOdsPath + '/', mode='append', compression='snappy')
        df.write.parquet(pHdfsLivePath + pPartition + '/', mode='overwrite', compression='snappy')

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)


def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_path", type=str, help="S3 ODS PATH")
    parser.add_argument("--live_path", type=str, help="S3 LIVE PATH")
    parser.add_argument("--partition", type=str, help="PARTITION YYYYMM")
    parser.add_argument("--model", type=str, help="SF MODEL")
    parser.add_argument("--conversions", type=str, help="COLUMNS TO CONVERT")
    parser.add_argument("--partition_col", type=str, help="PARTITIONING INFO")
    parser.add_argument("--country_col", type=str, help="COUNTRY MAPPING")
    parser.add_argument("--overwrite_mode", type=str, help="Overwrite Mode")

    return parser.parse_args()


if __name__ == '__main__':
    app_args = get_app_args()
    print(app_args.model)
    # move_to_dl_live(app_args.source_path, app_args.live_path, app_args.partition, app_args.partition_col, app_args.conversions, 'False', 'False', app_args.model)
    move_to_dl_live(app_args.source_path
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
