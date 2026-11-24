from src.spark_session import get_spark_session

spark = get_spark_session()

sales_df = spark.read.csv("data/module4/sales_4.csv", header=True, inferSchema=True)

# sales_df.printSchema()

sales_df.createOrReplaceTempView("sales")

result_df = spark.sql("""
    SELECT
        product_category,
        SUM(amount) AS total_amount,
        COUNT(transaction_id) AS transaction_count
    FROM sales
    GROUP BY product_category
""")
# result_df.show()

result_df.write.mode("overwrite").partitionBy("product_category").parquet("data/module4/sales_summary")

display_result = [(row.product_category, row.total_amount, row.transaction_count) for row in result_df.orderBy("product_category").collect()]

print(display_result)