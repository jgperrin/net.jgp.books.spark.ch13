"""
 Ingesting a CSV with embedded JSON.

 @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, DataFrame, functions as F)

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def jsonifier(df):
    print(df.schema)
    return ''


def main(spark, path, filename):

    absolute_file_path = get_absolute_file_path(path, filename)

    df = spark.read.option("header", True) \
        .option("delimiter", "|") \
        .option("inferSchema", True) \
        .csv(absolute_file_path)
    return df

'''
  It works only for this scenario
  To write this as a generic function, please reimplement this using recursion technique 
'''
def flatten_nested_structure(df:DataFrame) -> DataFrame:
    array_cols = [c[0] for c in df.dtypes if c[1][:5] == 'array']
    new_df = df.select('*', F.explode(*array_cols)).drop('employee')

    struct_cols = [c[0] for c in new_df.dtypes if c[1][:6] == 'struct']
    new_df2 = new_df.select(*[c + ".*" for c in struct_cols])

    struct_cols2 = [c[0] for c in new_df2.dtypes if c[1][:6] == 'struct']
    new_df3 = new_df2.select(*[c + ".*" for c in struct_cols2])
    return new_df3

def rename_all_df_columns(df,new_column_names):
    return df.toDF(*new_column_names)

if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Processing of invoices") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('warn')

    path = '../../../../data/misc/'
    filename = 'csv_with_embedded_json2.csv'

    df = main(spark, path, filename)
    df.show(5, False)
    df.printSchema()

    new_df = spark.read.json(df.rdd.map(lambda r: r.emp_json))
    new_df.show(5, False)
    new_df.printSchema()

    flat_df = flatten_nested_structure(new_df)
    flat_df.show(5, False)
    flat_df.printSchema()

    flat_df = flat_df.withColumn("emp_name", F.concat(F.col("firstName"), F.lit(" "), F.col("lastName"))) \
        .withColumn("emp_address", F.concat(F.col("street"), F.lit(" "), F.col("unit"))) \
        .withColumn("emp_city", F.col("city")) \
        .drop('firstName', 'lastName', 'street', 'unit', 'city')

    flat_df.show()
    flat_df.printSchema()

    trimmed_df = df.select('budget', 'dept', 'deptId', 'location') \
        .withColumn("idx", F.monotonically_increasing_id())
    flat_df = flat_df.withColumn("idx", F.monotonically_increasing_id())

    final_df = trimmed_df.join(flat_df, 'idx', 'outer')
    final_df.show()
    final_df.printSchema()

    spark.stop()