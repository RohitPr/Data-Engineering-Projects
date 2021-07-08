from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


file = sc.textFile(
    "/media/rohit/My Files/Study/Data Engineering Projects/Words_CountRDD/book.txt")

words = file.flatMap(lambda x: x.split())
words_count = words.countByValue()

for word, count in words_count.items():
    clean_word = word.encode("ascii", "ignore")
    if clean_word:
        print(clean_word, count)
