# -*- coding: utf-8 -*-
# Se importan las librerias argparse y ConfigParser para recibir parametros en las funciones
from datetime import date,timedelta,datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import json


def return_sql_file(sql_path, sql_name):
    with open("{}/{}.sql".format(sql_path, sql_name)) as fr:
        sql_file = fr.read()

    return sql_file

def load(app_args):
    sql_path = '{}/sql/park'.format(app_args.path)

    # asigna a la variable con el nombre de la aplicacion que se ejecutara en spark
    conf = SparkConf().setAppName("ETL_PEYA-{}".format(app_args.model))
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    filter_days = int(app_args.data_history) # Cantidad de dias para filtros de ordenes y particion

    print('--->START READING DATA S3')
    
    # SE CREA EL DATAFRAME  
    df = spark.read.parquet(app_args.bucket_input)
    yesterday_date_short = datetime.strftime(datetime.now() - timedelta(days=filter_days), "%Y%m%d")

    df=df.filter(col('yyyymmdd') > yesterday_date_short)
    window = Window.partitionBy(col('id')).orderBy(col('messageTimestamp').desc())
    df = df.withColumn('last',row_number().over(window))

    dfFin=df.filter(col('last')== 1)

    print("Información obtenida")
    dfFin.printSchema()
    dfFin.show(10)


    # SE CREAN LA TABLA TEMPORAL
    dfFin.createOrReplaceTempView(app_args.model)

    # SE APLICA TIPO DE DATOS A LAS COLUMNAS POR QUERY
    sql_model = return_sql_file(sql_path, app_args.model)
    dfFin = sqlContext.sql(sql_model)
    
    dfFin.printSchema()
    dfFin.show(10, False)
    
    dfFin.write.mode('overwrite').format("bigquery")\
    .option("temporaryGcsBucket","data-origins-storage").option("intermediateFormat","orc")\
    .save(app_args.destination)

    print("Escrito en S3 ({})".format(app_args.destination))

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--model", help="Nombre del Model")
    parser.add_argument("-source", "--source", help="Bucket Input S3")
    parser.add_argument("-destination", "--destination", help="BQ Output")
    parser.add_argument("-history", "--data_history", help="Cantidad de dias a procesar, contando del actual para atrás. Default 3 dias", default=3)
    parser.add_argument("-path", "--path", help="Path")
    return parser.parse_args()

if __name__ == '__main__':
    app_args = get_app_args()
    load(app_args)
