"""
 Building a nested document.

 @author rambabu.posa
"""
import os
import logging
from pyspark.sql import (SparkSession, functions as F)

# Creates a session on a local master
spark = SparkSession.builder.appName("Building a restaurant fact sheet") \
    .master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("warn")

def absolute_file_path(filename):
    current_dir = os.path.dirname(__file__)
    rpath = "../../../../data/orangecounty_restaurants/{}".format(filename)
    apath = os.path.join(current_dir, rpath)
    return apath

filename1 = "businesses.CSV"
file_path1 = absolute_file_path(filename1)

# Ingests businesses into dataframe
businessDf = spark.read.format("csv") \
        .option("header", True) \
        .load(file_path1)

filename2 = "inspections.CSV"
file_path2 = absolute_file_path(filename2)
# Ingests businesses into dataframe
inspectionDf = spark.read.format("csv") \
        .option("header", True) \
        .load(file_path2)

# Shows at most 3 rows from the dataframe
businessDf.show(3)
businessDf.printSchema()

inspectionDf.show(3)
inspectionDf.printSchema()

join_condition = businessDf['business_id'] == inspectionDf['business_id']

resDf = businessDf.join(inspectionDf, join_condition, "inner") \
    .drop(inspectionDf['business_id'])

resDf.printSchema()
resDf.show(3)

struct_field = F.struct(resDf.business_id, resDf.score, resDf.date, resDf.type)

factSheetDf = resDf.withColumn("inspections", struct_field) \
    .drop("score", "date", "type")

factSheetDf.printSchema()
factSheetDf.show(3)

logging.warning("Before nested join, we have {} rows.".format(factSheetDf.count()))

left_columns = businessDf.columns

factSheetDf = factSheetDf.groupBy(left_columns).agg(F.collect_list(F.col("inspections")))

factSheetDf.printSchema()
factSheetDf.show(3)

logging.warning("After nested join, we have {} rows.".format(factSheetDf.count()))

spark.stop()