
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, sum as spark_sum

# spark = (
#     SparkSession.builder
#     .appName("HMS-Analytics")
#     .master("local[*]")
#     .getOrCreate()
# )

# df = spark.read.csv(
#     "appointments.csv",
#     header=True,
#     inferSchema=True
# )

# df.printSchema()

# # Transformations: lazy
# completed = df.filter(
#     col("status") == "COMPLETED"
# )

# revenue = completed.groupBy(
#     "department_id"
# ).agg(
#     count("*").alias("appointments"),
#     spark_sum("fees").alias("total_fees")
# )

# # Action: triggers execution
# revenue.show()

# # Inspect physical execution plan
# revenue.explain()

# input("Press Enter to stop Spark...")
# spark.stop()

from pyspark.sql import SparkSession

spark = (SparkSession.builder.
         appName("HMS-Analytics").
         master("local[4]").
         getOrCreate())

print(spark.version)