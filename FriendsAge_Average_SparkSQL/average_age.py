from pyspark.sql import SparkSession
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "/media/rohit/My Files/Study/DE Projects/Friends_SparkSQL_Header/data.csv")

# Taking only 2 Rows i.e. Age and Friends
friends = people.select("age", "friends")

# Averaging Friends by Age
friends.groupBy("age").avg("friends").show()

# Sorting by Age
friends.groupBy("age").avg("friends").sort("age").show()

# Rounding Values to 2 Decimal points
friends.groupBy("age").agg(func.round(
    func.avg("friends"), 2)).sort("age").show()

# Giving Row Header an Alias
friends.groupBy("age").agg(func.round(func.avg("friends"),
                                      2).alias("Avg Friends")).sort("age").show()
