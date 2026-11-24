from pyspark.sql import DataFrame
from pyspark.sql.functions import col, substring, when, lit


def derive_appointment_columns(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "confirmed_revenue",
            when(
                col("status") == "CONFIRMED",
                col("fees")
            ).otherwise(
                lit(0).cast("decimal(10,2)")
            )
        )
        .withColumn(
            "fee_category",
            when(col("fees").isNull(), "UNKNOWN")
            .when(col("fees") <= 0, "INVALID")
            .when(col("fees") < 500, "LOW")
            .when(col("fees") < 1000, "MEDIUM")
            .otherwise("HIGH")
        )
        .withColumn(
            "appointment_hour",
            substring("appointment_time", 1, 2)
            .cast("int")
        )
    )