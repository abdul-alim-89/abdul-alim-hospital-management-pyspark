from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_high_value_appointments(
    df: DataFrame
) -> DataFrame:

    return (
        df
        .filter(
            (col("status") == "CONFIRMED")
            & (col("fees") >= 1000)
        )
        .select(
            "id",
            "doctor_id",
            "appointment_date",
            "fees",
            "fee_category",
            "confirmed_revenue"
        )
    )