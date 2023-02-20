from pyspark.sql import SparkSession

def get_spark_session(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark