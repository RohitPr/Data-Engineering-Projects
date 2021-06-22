<<<<<<< HEAD
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import codecs


def loadMovieNames():
    movieNames = {}
    with codecs.open("/media/rohit/My Files/Study/DE Projects/MovieCount_Broadcast_SparkSQL/data/movies.csv") as f:
        for line in f:
            fields = line.split(',')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

moviesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "/media/rohit/My Files/Study/DE Projects/Movie_CountSort_SparkSQL/data/ratings.csv")

movieCounts = moviesDF.groupBy("movieId").count()

# UDF to locate Movie Names from  Dict


def lookupName(movieID):
    return nameDict.value[movieID]


lookupNameUDF = func.udf(lookupName)

# New column created using UDF
moviesWithNames = movieCounts.withColumn(
    "movieTitle", lookupNameUDF(func.col("movieID")))

sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(10, False)

spark.stop()
=======
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import codecs


def loadMovieNames():
    movieNames = {}
    with codecs.open("/media/rohit/My Files/Study/DE Projects/MovieCount_Broadcast_SparkSQL/data/movies.csv") as f:
        for line in f:
            fields = line.split(',')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

moviesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "/media/rohit/My Files/Study/DE Projects/Movie_CountSort_SparkSQL/data/ratings.csv")

movieCounts = moviesDF.groupBy("movieId").count()

# UDF to locate Movie Names from  Dict


def lookupName(movieID):
    return nameDict.value[movieID]


lookupNameUDF = func.udf(lookupName)

# New column created using UDF
moviesWithNames = movieCounts.withColumn(
    "movieTitle", lookupNameUDF(func.col("movieID")))

sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(10, False)

spark.stop()
>>>>>>> a36d51d4f01a602def4b5ca237d9cd16ee6cc130
