"""
 acos function: inverse cosine of a value in radians

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql import (functions as F)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/functions/trigo_arc.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("acos function") \
    .master("local[*]").getOrCreate()

df = spark.read.format("csv") \
    .option("header", True) \
    .load(absolute_file_path)

df = df.withColumn("acos", F.acos(F.col("val"))) \
       .withColumn("acos_by_name", F.acos("val"))

df.show(5)

spark.stop()