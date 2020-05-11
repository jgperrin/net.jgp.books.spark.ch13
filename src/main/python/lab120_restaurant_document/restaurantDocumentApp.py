"""
 Building a nested document.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

# Creates a session on a local master
spark = SparkSession.builder.appName("Building a restaurant fact sheet") \
    .master("local[*]").getOrCreate()

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

resDf = businessDf.join(inspectionDf, businessDf['business_id'] == inspectionDf['business_id'], "inner")

resDf.printSchema()
resDf.show(3)

def getColumns(df):
    fieldnames = df.columns




