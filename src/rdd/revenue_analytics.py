
from decimal import Decimal, InvalidOperation


def calculate_total_fees(appointments_rdd):

    # 1. Filter confirmed appointments
    confirmed = appointments_rdd.filter(
        lambda row: row[4].strip().upper() == "CONFIRMED"
    )

    # 2. Extract consultation fees
    fees_rdd = confirmed.map(
        lambda row: Decimal(row[3])
    )

    # 3. Calculate total fees
    total_fees = fees_rdd.fold(
        Decimal("0.00"),
        lambda a, b: a + b
    )

    return total_fees
