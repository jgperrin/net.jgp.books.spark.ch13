"""
 Processing of invoices formatted using the schema.org format.

 @author rambabu.posa
"""
import os
from pyspark.sql import SparkSession

def get_absolute_file_path(path, filename):
    # To get absolute path for a given filename
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def main(spark):
    # The processing code.
    filename = 'shipment.json'
    path = '../../../../data/json/'
    absolute_file_path = get_absolute_file_path(path, filename)
    # Reads a JSON, stores it in a dataframe
    df = spark.read.format("json") \
            .option("multiline", True) \
            .load(absolute_file_path)

    # Shows at most 5 rows from the dataframe (there's only one anyway)
    df.show(5, 16)
    df.printSchema()

if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Display of shipment") \
        .master("local[*]").getOrCreate()
    # Comment this line to see full log
    spark.sparkContext.setLogLevel('error')
    main(spark)
    spark.stop()
