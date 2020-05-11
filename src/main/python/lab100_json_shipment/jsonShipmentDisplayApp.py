"""
 Processing of invoices formatted using the schema.org format.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/json/shipment.json"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Display of shipment") \
    .master("local[*]").getOrCreate()

# Reads a JSON, stores it in a dataframe
df = spark.read.format("json") \
        .option("multiline", True) \
        .load(absolute_file_path)

# Shows at most 5 rows from the dataframe (there's only one anyway)
df.show(5, 16)
df.printSchema()

spark.stop()
