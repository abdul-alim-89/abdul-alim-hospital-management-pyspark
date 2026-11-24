from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, count, avg, min, max, concat_ws

def get_doctor_performance(
    appointment_df: DataFrame,
    doctor_df: DataFrame
) -> DataFrame:
    """""the most valid confirmed appointments, and how much expected revenue do they generate"""
    doctors = doctor_df.select(
    "doctor_id",
    concat_ws(" ", "first_name", "last_name").alias("doctor_name"),
    "department_id",
)
    return (
        appointment_df
        .join(
            doctors,
            on="doctor_id",
            how="inner"
        )
        .filter(
            (col("status") == "CONFIRMED")
            & (col("fees") > 0)
        )
        .groupBy(
            "doctor_id",
            "doctor_name",
            "department_id"
        )
        .agg(
            count("*").alias("total_appointments"),

            sum("fees").alias("total_revenue"),

            avg("fees").alias("average_fees"),

            min("fees").alias("minimum_fees"),

            max("fees").alias("maximum_fees")
        )
        .orderBy(
            col("total_revenue").desc(),
            col("doctor_name").asc(),
            col("doctor_id").asc()
        )
    )