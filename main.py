from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = (SparkSession.builder.
         appName("HMS-Analytics").
         master("local[4]").
         getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# print(spark.version)

df = spark.read.csv("data/appointments.csv", header=True, inferSchema=True)

# df.printSchema()
# print(df.show(5))

# filter_df = df.filter(col("status") == "CONFIRMED")

# selected_df = filter_df.select("appointment_id", "doctor_id", "fees")

# high_fees_df = selected_df.filter(col("fees") >= 1000)

# high_fees_df.show(100)
# print("Total matching appointments:", high_fees_df.count())

# input("Check Spark UI again, then press Enter...")
df = df.repartition(4)

print("Partitions:", df.rdd.getNumPartitions())
result = (
    df.filter(col("status") == "CONFIRMED")
      .groupBy("doctor_id")
      .agg(
          count("*").alias("total_appointments"),
          sum("fees").alias("total_fees")
      )
      .orderBy(col("total_appointments").desc())
)

print(result)
result.show(1500, truncate=False)

print("Input partitions:", df.rdd.getNumPartitions())
print("Result partitions:", result.rdd.getNumPartitions())

input("Inspect Spark UI, then press Enter...")