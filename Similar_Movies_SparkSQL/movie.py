from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys


def computeCosineSimilarity(spark, data):
    pairScores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2"))

    calculateSimilarity = pairScores \
        .groupBy("movie1", "movie2") \
        .agg(
            func.sum(func.col("xy")).alias("numerator"),
            (func.sqrt(func.sum(func.col("xx"))) *
             func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
            func.count(func.col("xy")).alias("numPairs")
        )

    result = calculateSimilarity \
        .withColumn("score",
                    func.when(func.col("denominator") != 0, func.col(
                        "numerator") / func.col("denominator"))
                    .otherwise(0)
                    ).select("movie1", "movie2", "score", "numPairs")

    return result


def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName(
    "MovieSimilarities").master("local[*]").getOrCreate()


movieNamesSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])

moviesSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)])


movieNames = spark.read \
    .option("sep", ",") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("/media/rohit/My Files/Study/DE Projects/Similar_Movies_SparkSQL/movies.csv")

movies = spark.read \
    .option("sep", ",") \
    .schema(moviesSchema) \
    .csv("/media/rohit/My Files/Study/DE Projects/Similar_Movies_SparkSQL/ratings.csv")


ratings = movies.select("userId", "movieId", "rating")

moviePairs = ratings.alias("ratings1") \
    .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId"))
          & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
    .select(func.col("ratings1.movieId").alias("movie1"),
            func.col("ratings2.movieId").alias("movie2"),
            func.col("ratings1.rating").alias("rating1"),
            func.col("ratings2.rating").alias("rating2"))


moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
        (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results:
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
            similarMovieID = result.movie2

        print(getMovieName(movieNames, similarMovieID) + "\tscore: "
              + str(result.score) + "\tstrength: " + str(result.numPairs))
