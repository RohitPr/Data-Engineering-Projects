
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of the  book into DF
inputDF = spark.read.text(
    "/media/rohit/My Files/Study/DE Projects/Word_CountSort_SparkSQL/book.txt")

# Split using a regular expression that extracts words and removes spaces
words = inputDF.select(func.explode(
    func.split(inputDF.value, "\\W+")).alias("word"))

words.filter(words.word != "")

lowercaseWords = words.select(func.lower(words.word).alias("word"))

wordCounts = lowercaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")

# To Show all the results instead of default 20
wordCountsSorted.show(wordCountsSorted.count())
=======
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of the  book into DF
inputDF = spark.read.text(
    "/media/rohit/My Files/Study/DE Projects/Word_CountSort_SparkSQL/book.txt")

# Split using a regular expression that extracts words and removes spaces
words = inputDF.select(func.explode(
    func.split(inputDF.value, "\\W+")).alias("word"))

words.filter(words.word != "")

lowercaseWords = words.select(func.lower(words.word).alias("word"))

wordCounts = lowercaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")

# To Show all the results instead of default 20
wordCountsSorted.show(wordCountsSorted.count())

