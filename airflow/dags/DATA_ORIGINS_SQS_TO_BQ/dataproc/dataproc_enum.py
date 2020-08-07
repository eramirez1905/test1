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
from pyspark.sql.types import StructType, ArrayType

schema_path = 's3a://peyabi.code.live/bi-core-tech/schemas/{}'
schema_struct_str = '{"fields": [{"metadata": {},"name": "id","nullable": true,"type": "integer"}],"type": "struct"}'

def caster(i):
    switcher = {
        'timestamp': TimestampType(),
        'boolean': BooleanType(),
        'double': DoubleType(),
        'integer':IntegerType(),
        'string': StringType(),
    }
    return switcher.get(i, "Invalid conversion key, you moron")

def sqs_raw_process(pSourceDir, pName, pFormat, pSchema):
    try:
        conf = SparkConf().setAppName("ETL_ENUM_PROCESS_{0}".format(pName))
        # conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # conf.set("spark.hive.mapred.supports.subdirectories", "true")
        # conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        # Load JSON Schema
        f = sc.textFile(schema_path.format(pSchema)).collect()
        jsonData = ''.join(f)
        d = json.loads(jsonData)
        modelSchema = StructType.fromJson(d)

        sourceDir = pSourceDir + '{0}.{1}'.format(pName, pFormat)
        print("Source Dir:" + sourceDir)

        print('--->START READING DATA ENUM FROM S3')
        df = sqlContext.read.schema(modelSchema).load(sourceDir, format='CSV', header=True, enconding='UTF-8')
        print('<---END READING DATA ENUM FROM s3')

        df.write.mode('overwrite').format("bigquery")\
            .option("temporaryGcsBucket", "data-origins-temporal")\
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_raw.enum_{0}".format(pName))

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

def sqs_raw_country(pSourceDir, pName, pFormat, pSchema):
    try:
        conf = SparkConf().setAppName("ETL_ENUM_PROCESS_{0}".format(pName))

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        # Load JSON Schema
        f = sc.textFile(schema_path.format(pSchema)).collect()
        jsonData = ''.join(f)
        d = json.loads(jsonData)
        modelSchema = StructType.fromJson(d)

        d = json.loads(schema_struct_str)
        structSchema = StructType.fromJson(d)

        sourceDir = pSourceDir + '{0}.{1}'.format(pName, pFormat)
        print("Source Dir:" + sourceDir)

        print('--->START READING DATA ENUM FROM S3')
        df = sqlContext.read.schema(modelSchema).load(sourceDir, format='CSV', header=True, enconding='UTF-8')
        print('<---END READING DATA ENUM FROM s3')

        dfFinal = df.select(col("id")
                  , col("culture")
                  , col("isEnabled")
                  , col("timeFormat")
                  , col("name")
                  , col("identityCardBehaviour")
                  , col("timeOffset")
                  , col("shortName")
                  , from_json(regexp_replace(col("platforms"), '-', ','), ArrayType(StringType())).alias("platforms")
                  , from_json(col("defaultCity"), structSchema).alias("defaultCity")
                  , from_json(col("currency"), structSchema).alias("currency")
        )

        dfFinal.write.mode('overwrite').format("bigquery")\
            .option("temporaryGcsBucket", "data-origins-temporal")\
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_raw.enum_{0}".format(pName))

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

def sqs_json_country(pSourceDir, pName, pFormat, pSchema):
    try:
        conf = SparkConf().setAppName("ETL_ENUM_PROCESS_{0}".format(pName))

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        sourceDir = pSourceDir + '{0}.{1}'.format(pName, pFormat)
        print("Source Dir:" + sourceDir)

        print('--->START READING DATA ENUM FROM S3')
        df = sqlContext.read.load(sourceDir, format='JSON', enconding='UTF-8')
        print('<---END READING DATA ENUM FROM s3')

        df.write.mode('overwrite').format("bigquery")\
            .option("temporaryGcsBucket", "data-origins-temporal")\
            .option("intermediateFormat", "orc") \
            .save("peya-data-origins-stg.origins_data_raw.enum_{0}".format(pName))

    except:
        print('---------------------- FALLA ----------------------')
        print(traceback.format_exc())
        time.sleep(1)  # workaround para el bug del thread shutdown
        exit(1)

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-source", type=str, help="S3 ODS PATH")
    parser.add_argument("-name", type=str, help="PARTITION YYYYMM")
    parser.add_argument("-format", type=str, help="SF MODEL")
    parser.add_argument("-type", type=str, help="Type of Process (STG or RAW)")
    parser.add_argument("-schema", type=str, help="Schema PATH")

    return parser.parse_args()

if __name__ == '__main__':
    app_args = get_app_args()
    print(app_args.name)
    # if app_args.name == 'country':
    #     sqs_raw_country(app_args.source
    #                     , app_args.name
    #                     , app_args.format
    #                     , app_args.schema
    #                     )
    if app_args.name == 'country':
        sqs_json_country(app_args.source
                        , app_args.name
                        , app_args.format
                        , app_args.schema
                        )
    else:
        sqs_raw_process(app_args.source
                        , app_args.name
                        , app_args.format
                        , app_args.schema
                        )