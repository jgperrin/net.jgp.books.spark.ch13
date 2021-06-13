"""
 Ingesting a CSV with embedded JSON.
 Turns a Row into JSON. Not very fail safe, but done to illustrate.

 @author rambabu.posa
"""
# TODO: Not working, should work on it. Very complex scenario

from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField, ArrayType, StringType)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/misc/csv_with_embedded_json.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Processing of invoices") \
    .master("local[*]").getOrCreate()

df = spark.read.option("header", "True") \
    .option("delimiter", "|") \
    .csv(absolute_file_path)

df.show(5, False)
df.printSchema()

jdf = spark.read.json(df.select("emp_json").rdd.map(lambda s: str(s)))

jdf.show(truncate=False)
jdf.printSchema()

schema = \
    StructType([
        StructField("emp",ArrayType(
            StructType([
                StructField("address",
                        StructType([
                            StructField("city",  StringType(), True),
                            StructField("street",StringType(), True),
                            StructField("unit",  StringType(), True)]),
                        True),
                StructField("name",
                        StructType([
                            StructField("firstName", StringType(), True),
                            StructField("lastName", StringType(), True)]),
                        True)
            ])
        ,True)
   ,True)
])

#    .select(F.explode(F.col("employee")).alias("emp")) \


jdf = jdf.select(F.col("*"), F.from_json(F.col("_corrupt_record"), schema).alias("emp"))

jdf = jdf.select("emp.name.firstName","emp.name.lasteName","emp.address.street","emp.address.unit","emp.address.city")

jdf.printSchema()

#jdf = jdf.withColumn("dept", F.lit("finance")) \
#    .withColumn("city", F.lit("OH")) \

jdf.show(truncate=False)
jdf.printSchema()

spark.stop()