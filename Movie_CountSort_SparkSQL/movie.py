from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema
schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)])


moviesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "/media/rohit/My Files/Study/DE Projects/Movie_CountSort_SparkSQL/data/ratings.csv")

topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Top 10 movies
topMovieIDs.show(10)

spark.stop()
