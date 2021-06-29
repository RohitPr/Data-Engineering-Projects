from pyspark.sql import SparkSession
import mysql.connector
import pyspark.sql.functions as func
import pandas as pd


spark = SparkSession.builder.appName("Spark").getOrCreate()

# Creating Directory : hadoop fs -mkdir /covid/
# Traversing : hdfs dfs -ls /covid
# Removing : hdfs dfs -rmr hdfs://localhost:9000/covid/data
# Copying : hadoop fs -copyFromLocal /local/path /hdfs/path

# Connecting to HDFS Cluster and reading the CSV
data = spark.read.option("header", "true").option(
    "inferSchema", "true").csv("hdfs://127.0.0.1:9000/covid/data/data.csv")

confirmed = data.groupBy("Country").agg(func.round(
    func.avg("Confirmed"), 2).alias("Cases")).orderBy("Cases", ascending=False)

# Sorting by Most Cases by date and generating an Dict using Collect
# confirmed = data.groupBy("Country").agg(func.round(
#     func.avg("Confirmed"), 2).alias("Most Confirmed Cases")).orderBy("Most Confirmed Cases", ascending=False).collect()

# Prints the
# for a in confirmed:
#     print(a['Country'] + "-" + str(a['Most Confirmed Cases']))

conn = mysql.connector.connect(user='rohit', database='covid',
                               password='kar98',
                               host="localhost",
                               port=3306)
cursor = conn.cursor()

confirmed.write.format('jdbc').options(
    url='jdbc:mysql://localhost/covid',
    driver='com.mysql.jdbc.Driver',
    dbtable='covid',
    user='rohit',
    password='kar98').mode('append').save()

# Command to Write data to MySQL Server : spark-submit --jars external/mysql-connector-java-5.1.46-bin.jar /home/rohit/Desktop/Covid/main.py

query = "SELECT * FROM covid LIMIT 10;"
# Create a pandas dataframe
pdf = pd.read_sql(query, con=conn)
conn.close()

# Convert Pandas dataframe to spark DataFrame
df_2 = spark.createDataFrame(pdf)

df_2.show()
