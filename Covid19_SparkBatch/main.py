from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Spark").getOrCreate()

# Connecting to HDFS Cluster and reading the CSV
data = spark.read.option("header", "true").option(
    "inferSchema", "true").csv("hdfs://127.0.0.1:9000/covid19/data.csv")

confirmed = data.groupBy("Country").agg(func.round(
    func.avg("Confirmed"), 2).alias("Most Confirmed Cases")).orderBy("Most Confirmed Cases", ascending=False).show(50, truncate=False)


# Sorting by Most Cases by date and generating an Dict using Collect
# confirmed = data.groupBy("Country").agg(func.round(
#     func.avg("Confirmed"), 2).alias("Most Confirmed Cases")).orderBy("Most Confirmed Cases", ascending=False).collect()

# Prints the
# for a in confirmed:
#     print(a['Country'] + "-" + str(a['Most Confirmed Cases']))
