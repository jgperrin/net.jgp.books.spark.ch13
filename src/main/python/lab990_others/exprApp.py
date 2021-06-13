"""
 Use of expr().

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

def main(spark):
    schema = StructType([
        StructField('title', StringType(), False),
        StructField('start', IntegerType(), False),
        StructField('end', IntegerType(), False)
    ])

    rows = [("bla", 10, 30)]

    df = spark.createDataFrame(rows, schema)
    df.show()

    df = df.withColumn("time_spent", F.expr("end - start")) \
        .drop("start") \
        .drop("end")

    df.show()

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("expr()") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()