
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("/media/rohit/My Files/Study/DE Projects/Friends_SparkSQL_Header/data.csv")

print("Here is the Schema:")
people.printSchema()

print("Display Name Column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Add 10 Years to Age:")
people.select(people.name, people.age + 10).show()

spark.stop()
=======
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("/media/rohit/My Files/Study/DE Projects/Friends_SparkSQL_Header/data.csv")

print("Here is the Schema:")
people.printSchema()

print("Display Name Column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Add 10 Years to Age:")
people.select(people.name, people.age + 10).show()

spark.stop()

