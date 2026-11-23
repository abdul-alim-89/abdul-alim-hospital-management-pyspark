from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = (SparkSession.builder.
         appName("HMS-Analytics").
         master("local[4]").
         getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext


def analyze_logs(sc, lines, action_map):
    bc_action_map = sc.broadcast(action_map)
    malformed_count = sc.accumulator(0)

    rdd = sc.parallelize(lines)

    def is_valid_log(line):
        parts = line.split(",")

        if len(parts) != 4:
            malformed_count.add(1)
            return None

        code = parts[2]
        response_time = parts[3]

        if not response_time.isdigit():
            malformed_count.add(1)
            return None

        return (code, int(response_time))

    parse_lines = rdd.map(is_valid_log)
    filter_lines = parse_lines.filter(lambda x: x is not None)
    filter_lines.cache()

    avg_rdd = (
        filter_lines
        .map(lambda x: (x[0], (x[1], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .map(lambda x: (bc_action_map.value[x[0]], x[1][0] / x[1][1]))
    )

    max_rdd = (
        filter_lines
        .reduceByKey(max)
        .map(lambda x: (bc_action_map.value[x[0]], x[1]))
    )

    avg_list = sorted(avg_rdd.collect(), key=lambda x: x[0])
    max_list = sorted(max_rdd.collect(), key=lambda x: x[0])

    return avg_list, max_list, int(malformed_count.value)


lines = [
    "2026-11-18T10:00:00,u1,A1,120",
    "2026-11-18T10:00:01,u2,A3,250",
    "2026-11-18T10:00:02,u3,A2,90",
    "2026-11-18T10:00:03,u1,A3,310",
    "2026-11-18T10:00:04,u2,A4,480",
    "2026-11-18T10:00:05,u4,A5,150",
    "2026-11-18T10:00:06,u3,A3,200",
    "2026-11-18T10:00:07,u1,A2,75",
    "2026-11-18T10:00:08,u5,A9,300",
]

action_map = {
    "A1": "login",
    "A2": "logout",
    "A3": "view_patient",
    "A4": "book_appointment",
    "A5" : "create_payment",
    "A9": "create_patient",
}

result = analyze_logs(sc, lines, action_map=action_map)
print(result)