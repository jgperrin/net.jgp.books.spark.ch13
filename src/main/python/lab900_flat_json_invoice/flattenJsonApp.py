"""
 Processing of invoices formatted using the schema.org format.

 @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, DataFrame, functions as F)

array_type = "Array"
struct_type = "Struc"

'''
  It works only for this scenario
  To write this as a generic function, please reimplement this using recursion technique 
'''
def flatten_nested_structure(df:DataFrame) -> DataFrame:
    array_cols = [c[0] for c in df.dtypes if c[1][:5] == 'array']
    new_df = df.select('*', F.explode(*array_cols))

    struct_cols = [c[0] for c in new_df.dtypes if c[1][:6] == 'struct']
    new_df2 = new_df.select(*[c + ".*" for c in struct_cols])

    array_cols2 = [c[0] for c in new_df2.dtypes if c[1][:5] == 'array']
    flat_df = new_df2.select('*', F.explode(*array_cols2)).drop(*array_cols2) \
        .withColumnRenamed("col", "salesByMonth")
    return flat_df

def rename_all_df_columns(df, new_column_names):
    return df.toDF(*new_column_names)

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path


def main(spark, path, filename):
    absolute_file_path = get_absolute_file_path(path, filename)

    # Reads a JSON, stores it in a dataframe
    invoices_df = spark.read.format("json") \
        .option("multiline", True) \
        .load(absolute_file_path)

    # Shows at most 3 rows from the dataframe
    invoices_df.show(3)
    invoices_df.printSchema()

    flat_invoices_df = flatten_nested_structure(invoices_df)

    new_columns = ['author_city','author_country','author_name','author_state',
                   'publisher_city','publisher_country','publisher_name','publisher_state',
                   'books_title','books_salesByMonth'
                   ]
    flat_invoices_df = rename_all_df_columns(flat_invoices_df, new_columns) \
        .withColumn('date', F.lit('2019-10-05'))

    flat_invoices_df.show(20, False)
    flat_invoices_df.printSchema()


if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Automatic flattening of a JSON document") \
        .master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    path = '../../../../data/json/'
    filename = 'nested_array.json'

    main(spark, path, filename)
    spark.stop()