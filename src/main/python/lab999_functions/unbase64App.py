"""
 unbase64 function.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql import (functions as F)
from pyspark.sql.types import StringType
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/functions/strings.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("unbase64 function") \
    .master("local[*]").getOrCreate()

df = spark.read.format("csv") \
    .option("header", True) \
    .load(absolute_file_path)

print("Output as array of bytes:")

df = df.withColumn("base64", F.base64(F.col("fname"))) \
       .withColumn("unbase64", F.unbase64(F.col("base64")))

df.show(5)

print("Output as strings:")

df = df.withColumn("name", F.col("unbase64").cast(StringType()))
# Or use can use this
# df = df.withColumn("name", F.col("unbase64").cast("string"))

df.show(5)

spark.stop()