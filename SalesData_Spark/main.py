from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Sales").getOrCreate()

products = spark.read.option("header", "true").option("inferSchema", "true").csv(
    'data/products.csv')

sales_pipeline = spark.read.option("header", "true").option("inferSchema", "true").csv(
    'data/sales_pipeline.csv')

sales_teams = spark.read.option("header", "true").option("inferSchema", "true").csv(
    'data/sales_teams.csv')

# Display 'Manager' and 'Grand Total Sales', for sales done by the sales agents reporting these managers

sales_done_join = sales_teams.join(sales_pipeline, on="sales_agent", how="inner").join(
    products, on="product", how="inner")

aggregated_data = sales_done_join.where(col("deal_stage") == 'Won').groupBy("manager").agg(func.sum(
    "sales_price").alias("Grand Total Sales"))

final_data = aggregated_data.select(col("manager").alias(
    "Manager"), "Grand Total Sales").orderBy("Grand Total Sales", ascending=False).show()

spark.stop()
