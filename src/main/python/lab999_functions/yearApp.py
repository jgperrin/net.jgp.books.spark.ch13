"""
 year function.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql import (functions as F)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/functions/dates.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("year function") \
    .master("local[*]").getOrCreate()

df = spark.read.format("csv") \
    .option("header", True) \
    .option("imferSchema", True) \
    .load(absolute_file_path)

df = df.withColumn("year", F.year(F.col("date_time")))

df.show(5, False)
df.printSchema()

spark.stop()