from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
)

DOCTOR_SCHEMA = StructType([
    StructField("doctor_id", LongType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("department_id", IntegerType(), True),
])