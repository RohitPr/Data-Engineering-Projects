import re
from pyspark import SparkConf, SparkContext


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

file = sc.textFile(
    "/media/rohit/My Files/Study/DE Projects/Words_CountREGEX/book.txt")

words = file.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
