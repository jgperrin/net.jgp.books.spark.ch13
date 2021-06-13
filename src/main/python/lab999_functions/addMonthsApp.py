"""
 add_months function.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql import (functions as F)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/functions/dates.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("add_months function") \
    .master("local[*]").getOrCreate()

df = spark.read.format("csv") \
    .option("header", True) \
    .option("imferSchema", True) \
    .load(absolute_file_path)

df = df.withColumn("add_months+2", F.add_months(F.col("date_time"), 2))
# As of now, no support for this operation in PySpark API
# df = df.withColumn("add_months+val", F.add_months(F.col("date_time"), F.col("val")))

df.show(5, False)
df.printSchema()

df.show(5)

spark.stop()