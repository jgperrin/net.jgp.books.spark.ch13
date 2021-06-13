"""
 base64 function.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql import (functions as F)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/functions/strings.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("base64 function") \
    .master("local[*]").getOrCreate()

df = spark.read.format("csv") \
    .option("header", True) \
    .load(absolute_file_path)

df = df.withColumn("base64", F.base64(F.col("fname")))

df.show(5)

spark.stop()