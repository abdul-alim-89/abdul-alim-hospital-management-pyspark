import json
from pyspark.sql import SparkSession


def spark_simulator(data, operations, sc):
    rdd = sc.parallelize(data)
    results = []

    for operation in operations:
        type = operation["type"]
        
        if type == "map":
            expression = operation["func"]
            rdd = rdd.map(lambda x, expr=expression: eval(expr, {"x": x}))

        elif type == "filter":
            expression = operation["func"]

            rdd = rdd.filter(lambda x, expr=expression: eval(expr, {"x": x}))

        elif type == "count":
            results.append(rdd.count())

        elif type == "collect":
            results.append(rdd.collect())

        elif type == "show":
            n = operation.get("n", 20)
            results.append(rdd.take(n))

    return results

spark = SparkSession.builder.appName("question1").getOrCreate()

with open("data/question.json", "r") as file:
    input_data = json.load(file)



results = spark_simulator(input_data["data"],input_data["operations"], spark.sparkContext)
print(json.dumps(results))
spark.stop()


