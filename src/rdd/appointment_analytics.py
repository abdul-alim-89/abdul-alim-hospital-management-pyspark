
from src.spark_session import get_spark_session
import csv


def parse_appointment(line):
    return next(csv.reader([line]))



def count_confirmed_appointments(appointments_rdd):

    confirmed = appointments_rdd.filter(
        lambda row: row[4].strip().upper() == "CONFIRMED"
    )

    doctor_counts = (
        confirmed
        .map(lambda row: (int(row[2]), 1))
        .reduceByKey(lambda a, b: a + b)
    )

    top_doctors = doctor_counts.takeOrdered(
        10,
        key=lambda item: (-item[1], item[0])
    )

    for doctor_id, count in top_doctors:
        print(
            f"Doctor ID: {doctor_id} | "
            f"Confirmed Appointments: {count}"
        )

    return top_doctors



if __name__ == "__main__":
    count_confirmed_appointments()