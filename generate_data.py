
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

output = Path("data/appointments.csv")
output.parent.mkdir(parents=True, exist_ok=True)

statuses = ["PENDING", "CONFIRMED", "CANCELLED", "COMPLETED"]
times = ["09:00", "09:15", "09:30", "09:45", "10:00", "10:15", "10:30", "10:45", "11:00", 
         "11:15", "11:30", "11:45", "12:00", "12:15", "12:45" "14:00", "15:00", "16:00"]

with output.open("w", newline="") as file:
    writer = csv.writer(file)

    writer.writerow([
        "appointment_id",
        "patient_id",
        "doctor_id",
        "appointment_date",
        "appointment_time",
        "status",
        "fees"
    ])

    for i in range(1, 5001):
        appointment_date = date(2026, 1, 1) + timedelta(
            days=random.randint(0, 364)
        )

        writer.writerow([
            i,
            random.randint(1, 100000),
            random.randint(1, 100),
            appointment_date.isoformat(),
            random.choice(times),
            random.choice(statuses),
            random.choice([500, 800, 1000, 1200, 1500])
        ])

print("Generated 5000 appointment records")
