"""
 asin function: inverse sine of a value in radians.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql import (functions as F)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/functions/trigo_arc.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("asin function") \
    .master("local[*]").getOrCreate()

df = spark.read.format("csv") \
    .option("header", True) \
    .load(absolute_file_path)

df = df.withColumn("asin", F.asin(F.col("val"))) \
       .withColumn("asin_by_name", F.asin("val"))


df.show()

spark.stop()