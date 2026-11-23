from pyspark.sql import SparkSession

def get_spark_session():
    spark = (SparkSession.builder.
            appName("HMS-Analytics").
            master("local[4]").
            getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")

    return spark

