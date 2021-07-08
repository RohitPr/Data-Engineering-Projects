from pyspark.sql import SparkSession
from pyspark.sql import Row

# SparkSession Created
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Mapping Schema Created as no Header Rows


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")),
               age=int(fields[2]), numFriends=int(fields[3]))


lines = spark.sparkContext.textFile(
    "/media/rohit/My Files/Study/DE Projects/Friends_SparkSQL/data.csv")
people = lines.map(mapper)

# Creating View as per Schema
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
