<<<<<<< HEAD
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(
    "/media/rohit/My Files/Study/DE Projects/Marvel_Connection_SparkSQL/names.txt")

lines = spark.read.text(
    "/media/rohit/My Files/Study/DE Projects/Marvel_Connection_SparkSQL/graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]).withColumn("connections", func.size(
    func.split(func.col("value"), " ")) - 1).groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(
    func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " +
      str(mostPopular[1]) + " co-appearances.")
=======
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(
    "/media/rohit/My Files/Study/DE Projects/Marvel_Connection_SparkSQL/names.txt")

lines = spark.read.text(
    "/media/rohit/My Files/Study/DE Projects/Marvel_Connection_SparkSQL/graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]).withColumn("connections", func.size(
    func.split(func.col("value"), " ")) - 1).groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(
    func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " +
      str(mostPopular[1]) + " co-appearances.")
>>>>>>> a36d51d4f01a602def4b5ca237d9cd16ee6cc130
