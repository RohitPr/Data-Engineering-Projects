from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("CountTotal")
sc = SparkContext(conf=conf)


file = sc.textFile(
    '/media/rohit/My Files/Study/DE Projects/Customer_TotalRDD/customer.csv')


def parseline(file):
    fields = file.split(",")
    return (int(fields[0]), float(fields[2]))


parsed_results = file.map(parseline)
total_count = parsed_results.reduceByKey(lambda x, y: x + y)
sorted_count = total_count.sortByKey()
results = sorted_count.collect()

for result in results:
    print(result)
