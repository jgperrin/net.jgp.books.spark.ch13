"""
 Processing of invoices formatted using the schema.org format.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, DataFrame, functions as F)
import os

array_type = "Array"
struct_type = "Struc"

def flattenNestedStructure(df:DataFrame) -> DataFrame:
    array_cols = [c[0] for c in df.dtypes if c[1][:5] == 'array']
    new_df = df.select('*', F.explode(*array_cols))
    struct_cols = [c[0] for c in new_df.dtypes if c[1][:6] == 'struct']
    new_df2 = new_df.select(*[c + ".*" for c in struct_cols])
    array_cols2 = [c[0] for c in new_df2.dtypes if c[1][:5] == 'array']
    flat_df = new_df2.select('*', F.explode(*array_cols2)).drop(*array_cols2)
    return flat_df

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/json/nested_array.json"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Automatic flattening of a JSON document") \
    .master("local[*]").getOrCreate()

# Reads a JSON, stores it in a dataframe
invoicesDf = spark.read.format("json") \
    .option("multiline", True) \
    .load(absolute_file_path)

# Shows at most 3 rows from the dataframe
invoicesDf.show(3)
invoicesDf.printSchema()

flatInvoicesDf = flattenNestedStructure(invoicesDf)

flatInvoicesDf.show(20, False)
flatInvoicesDf.printSchema()

spark.stop()