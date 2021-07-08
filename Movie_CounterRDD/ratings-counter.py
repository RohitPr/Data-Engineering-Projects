from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile(
    "/media/rohit/My Files/Study/Data Engineering Projects/Movie_Counter/data/ratings.csv"
)


def parseline(lines):
    fields = lines.split(",")
    return fields[2]


ratings = lines.map(parseline)
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

