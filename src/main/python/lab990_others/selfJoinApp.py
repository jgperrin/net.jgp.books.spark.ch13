"""
 Self join.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

def createDataFrame(spark):
    # to create a DataFrame logic
    schema = StructType([
        StructField('tid', IntegerType(), False),
        StructField('acct', IntegerType(), False),
        StructField('bssn', IntegerType(), False),
        StructField('name', StringType(), False)
    ])

    rows = [
        (1, 123, 111, "Peter"),
        (2, 123, 222, "Paul"),
        (3, 456, 333, "John"),
        (4, 567, 444, "Casey")
    ]
    return spark.createDataFrame(rows, schema)

def main(spark):
    # The processing code.
    df = createDataFrame(spark)
    df.show(truncate=False)

    rightDf = df.withColumnRenamed("acct", "acct2") \
        .withColumnRenamed("bssn", "bssn2") \
        .withColumnRenamed("name", "name2") \
        .drop("tid")

    joinedDf = df.join(rightDf, df["acct"] == rightDf["acct2"], "leftsemi") \
        .drop(rightDf["acct2"]) \
        .drop(rightDf["name2"]) \
        .drop(rightDf["bssn2"])

    joinedDf.show(truncate=False)

    listDf = joinedDf.groupBy(F.col("acct")) \
        .agg(F.collect_list("bssn"), F.collect_list("name"))

    listDf.show(truncate=False)

    setDf = joinedDf.groupBy(F.col("acct")) \
        .agg(F.collect_set("bssn"), F.collect_set("name"))

    setDf.show(truncate=False)

if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName('Self join') \
        .master('local[*]').getOrCreate()

    main(spark)
    spark.stop()