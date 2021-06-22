
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

moviesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "/media/rohit/My Files/Study/DE Projects/Movie_CountSort_SparkSQL/data/ratings.csv")

topMovieIDs = moviesDF.groupBy("movieId").count().orderBy(func.desc("count"))

# Top 10 movies
topMovieIDs.show(10)

spark.stop()
=======
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

moviesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "/media/rohit/My Files/Study/DE Projects/Movie_CountSort_SparkSQL/data/ratings.csv")

topMovieIDs = moviesDF.groupBy("movieId").count().orderBy(func.desc("count"))

# Top 10 movies
topMovieIDs.show(10)

spark.stop()

