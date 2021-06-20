from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "/media/rohit/My Files/Study/DE Projects/Friends_SparkSQL_Header/data.csv")


people.groupBy("age").avg("friends").show()
