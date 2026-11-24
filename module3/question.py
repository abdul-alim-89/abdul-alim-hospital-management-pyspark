from src.spark_session import get_spark_session
from pyspark.sql.types import StructField, StructType, IntegerType, DecimalType, StringType
from pyspark.sql.functions import col, sum, avg

spark = get_spark_session()

sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
    StructField("discount", DecimalType(10, 2), True)

])

products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("category", StringType(), False),
    StructField("product_name", StringType(), False)
])

sales_df = spark.read.csv("data/module3/sales.csv", header=True, schema=sales_schema)

product_df = spark.read.csv("data/module3/products.csv", header=True, schema=products_schema)

# sales_df.printSchema()
# product_df.printSchema()

valid_sales_df = sales_df.filter(
    (col("quantity") >= 0) &
    (col("unit_price") >= 0) &
    (col("discount") >= 0) &
    (col("discount") < 1)

)

# valid_sales_df.show()

revenue_df = valid_sales_df.withColumn(
    "revenue",
    col("quantity") * col("unit_price") * (1 - col("discount"))
)

# revenue_df.show()

join_df = revenue_df.join(
    product_df,
    on="product_id",
    how="inner"
)

# join_df.show()

cat_summary = (join_df.groupBy("category").agg(
    sum("revenue").alias("total_revenue"),
    avg("revenue").alias("avg_revenue")
).orderBy(col("total_revenue").desc(), col("category").desc())
.select("category", "total_revenue", "avg_revenue"))

cat_summary.show()

cat_summary.explain("formatted")
cat_summary.explain("extended")
for row in cat_summary.toLocalIterator():
    total = round(row["total_revenue"], 2)
    average = round(row["avg_revenue"], 2)

    print(f'{row["category"]}, {total:.2f}, {average:.2f}')

input()