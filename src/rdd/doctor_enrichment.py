
import csv


def parse_csv(line):
    return next(csv.reader([line]))


def enrich_appointments(sc, appointments_rdd):

    # 1. Read departments CSV
    departments_rdd = sc.textFile(
        "data/raw/departments.csv"
    )

    department_header = departments_rdd.first()

    departments = (
        departments_rdd
        .filter(lambda line: line != department_header)
        .map(parse_csv)
        .map(lambda row: (
            int(row[0]),  # department_id
            row[1]        # department_name
        ))
        .collectAsMap()
    )

    # 2. Read doctors CSV
    doctors_rdd = sc.textFile(
        "data/raw/doctors.csv"
    )

    doctor_header = doctors_rdd.first()

    doctors = (
        doctors_rdd
        .filter(lambda line: line != doctor_header)
        .map(parse_csv)
        .map(lambda row: (
            int(row[0]),  # doctor_id
            (
                f"{row[1]} {row[2]}",
                departments.get(
                    int(row[6]), "Unknown Department"
                )
            )
        ))
        .collectAsMap()
    )

    # 3. Broadcast small doctor lookup
    broadcast_doctors = sc.broadcast(doctors)

    # 4. Enrich appointments
    def enrich(row):

        doctor_id = int(row[2])

        doctor_name, department = (
            broadcast_doctors.value.get(
                doctor_id,
                ("Unknown Doctor", "Unknown Department")
            )
        )

        return {
            "appointment_id": int(row[0]),
            "patient_id": int(row[1]),
            "doctor_id": doctor_id,
            "doctor_name": doctor_name,
            "department": department,
            "fees": float(row[3]),
            "status": row[4],
            "appointment_date": row[8],
            "appointment_time": row[9]
        }

    enriched_rdd = appointments_rdd.map(enrich)

    return enriched_rdd
