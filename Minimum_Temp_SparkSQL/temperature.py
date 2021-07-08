
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# Create Custom Schema to Store the required data from the file
schema = StructType([
    StructField("stationID", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)])

df = spark.read.schema(schema).csv(
    "/media/rohit/My Files/Study/DE Projects/Minimum_Temp_SparkSQL/temp.csv")
df.printSchema()

minTemps = df.filter(df.measure_type == "TMIN")

stationTemps = minTemps.select("stationID", "temperature")

minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

minTempsByStationF = minTempsByStation.select(
    "stationID", "temperature").sort("temperature")

results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
