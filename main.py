# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, sum

# spark = (SparkSession.builder.
#          appName("HMS-Analytics").
#          master("local[4]").
#          getOrCreate())

# spark.sparkContext.setLogLevel("ERROR")

# # print(spark.version)

# df = spark.read.csv("data/appointments.csv", header=True, inferSchema=True)

# # df.printSchema()
# # print(df.show(5))

# # filter_df = df.filter(col("status") == "CONFIRMED")

# # selected_df = filter_df.select("appointment_id", "doctor_id", "fees")

# # high_fees_df = selected_df.filter(col("fees") >= 1000)

# # high_fees_df.show(100)
# # print("Total matching appointments:", high_fees_df.count())

# # input("Check Spark UI again, then press Enter...")
# df = df.repartition(4)

# print("Partitions:", df.rdd.getNumPartitions())
# result = (
#     df.filter(col("status") == "CONFIRMED")
#       .groupBy("doctor_id")
#       .agg(
#           count("*").alias("total_appointments"),
#           sum("fees").alias("total_fees")
#       )
#       .orderBy(col("total_appointments").desc())
# )

# print(result)
# result.show(1500, truncate=False)

# print("Input partitions:", df.rdd.getNumPartitions())
# print("Result partitions:", result.rdd.getNumPartitions())

# input("Inspect Spark UI, then press Enter...")
import csv
from src.spark_session import get_spark_session

from src.rdd.appointment_analytics import count_confirmed_appointments
from src.rdd.revenue_analytics import calculate_total_fees
from src.rdd.doctor_enrichment import enrich_appointments
from src.rdd.data_validation import validate_appointments
from src.dataframes.dataframe_main import main as datafarme_main

def parse_csv(line):
    return next(csv.reader([line]))

def main():
    spark = get_spark_session()
    sc = spark.sparkContext

    try:
        raw_rdd = sc.textFile("data/raw/appointments.csv")
        header = raw_rdd.first()

        appointments_rdd = (
            raw_rdd
            .filter(lambda line: line != header)
            .map(parse_csv)
        )

        # Part A - RDD Basics
        print("\n--- Appointment Analytics ---")
        count_confirmed_appointments(appointments_rdd)


        print("\n--- Revenue Analytics ---")
        total_fees = calculate_total_fees(appointments_rdd)
        print(f"Total fees: ₹{total_fees:,.2f}")

        # Part B - Advanced RDD
        print("\n--- Doctor Enrichment ---")
        enriched_rdd = enrich_appointments(
            sc, appointments_rdd
        )

        for row in enriched_rdd.take(10):
            print(row)

        print("\n--- Data Validation ---")
        validate_appointments(sc, appointments_rdd)

        print("\n------------ dataframe-----------")
        datafarme_main()
        
    finally:
        # Stop Spark only after all jobs finish
        spark.stop()


if __name__ == "__main__":
    main()