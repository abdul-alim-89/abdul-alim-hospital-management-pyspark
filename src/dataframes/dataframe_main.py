from src.spark_session import get_spark_session

from src.schemas.appointment_schema import APPOINTMENT_SCHEMA

from src.dataframes.appointment_transformations import (
    derive_appointment_columns
)

from src.dataframes.appointment_analytics import (
    get_high_value_appointments
)


def main():
    spark = get_spark_session()

    try:
        appointments_df = (
            spark.read
            .option("header", True)
            .schema(APPOINTMENT_SCHEMA)
            .csv("data/raw/appointments.csv")
        )

        # print("\n--- Appointment Schema ---")
        # appointments_df.printSchema()

        # print("\n--- Derived Columns ---")

        transformed_df = derive_appointment_columns(
            appointments_df
        )

        # transformed_df.show(10, truncate=False)

        print("\n--- High Value Appointments ---")

        high_value_df = get_high_value_appointments(
            transformed_df
        )

        high_value_df.show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()