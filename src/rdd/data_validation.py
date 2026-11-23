
def validate_appointments(sc, appointments_rdd):

    invalid_count = sc.accumulator(0)

    valid_statuses = {
        "CONFIRMED",
        "PENDING",
        "CANCELLED"
    }

    def validate(row):
        try:
            # Validate CSV column count
            if len(row) != 11:
                invalid_count.add(1)
                return False

            appointment_id = int(row[0])
            patient_id = int(row[1])
            doctor_id = int(row[2])
            fees = float(row[3])
            status = row[4].strip().upper()

            # Validate required IDs
            if (
                appointment_id <= 0
                or patient_id <= 0
                or doctor_id <= 0
            ):
                invalid_count.add(1)
                return False

            # Validate consultation fees
            if not (0 < fees < float("inf")):
                invalid_count.add(1)
                return False

            # Validate appointment status
            if status not in valid_statuses:
                invalid_count.add(1)
                return False

            return True

        except (ValueError, TypeError, OverflowError):
            invalid_count.add(1)
            return False

    # Lazy transformation
    valid_rdd = appointments_rdd.filter(validate)

    # Action triggers Spark execution
    valid_count = valid_rdd.count()

    print("Valid records:", valid_count)
    print("Invalid records:", invalid_count.value)

    return valid_count
