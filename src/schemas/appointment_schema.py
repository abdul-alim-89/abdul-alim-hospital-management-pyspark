from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DateType,
    DecimalType,
    TimestampType,
)

APPOINTMENT_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("patient_id", LongType(), False),
    StructField("doctor_id", LongType(), False),
    StructField("fees", DecimalType(10, 2), True),
    StructField("status", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("appointment_date", DateType(), False),
    StructField("appointment_time", StringType(), True),
    StructField("note", StringType(), True),
])