from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinimumTemp")
sc = SparkContext(conf=conf)


def parsefunc(lines):
    fields = lines.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = fields[3]
    return (stationID, entryType, temperature)


lines = sc.textFile(
    "/media/rohit/My Files/Study/Data Engineering Projects/Minimum_Temp/temp.csv"
)
parsed_results = lines.map(parsefunc)
minTemps = parsed_results.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = minTemps.collect()

for result in results:
    print(result[0] + result[1])
