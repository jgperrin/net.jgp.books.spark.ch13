"""
 Self join.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField, StringType)

def createDataFrame(spark):
    # to create a DataFrame logic
    schema = StructType([
        StructField('src', StringType(), False),
        StructField('predicate', StringType(), False),
        StructField('dst', StringType(), False)
    ])

    rows = [
        ('a', 'r1', ':b1'),
        ('a', 'r2', 'k'),
        ('b1', 'r3', ':b4'),
        ('b1', 'r10', 'd'),
        (':b4', 'r4', 'f'),
        (':b4', 'r5', ':b5'),
        (':b5', 'r9', 't'),
        (':b5', 'r10', 'e')
    ]
    return spark.createDataFrame(rows, schema)

def main(spark):
    # The processing code.
    inputDf = createDataFrame(spark)
    inputDf.show(truncate=False)

    left = inputDf.withColumnRenamed('dst', 'dst2')
    left.show()

    right = inputDf.withColumnRenamed('src', 'dst2')
    right.show()

    r = left.join(right, left['dst2'] == right['dst2'])
    r.show()

    resultOption1Df = r.select(F.col('src'), F.col('dst'))
    resultOption1Df.show()

    resultOption2Df = r.select(F.col('src'), F.col('dst'))
    resultOption2Df.show()

    resultOption3Df = r.select('src', 'dst')
    resultOption3Df.show()

if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName('Self join') \
        .master('local[*]').getOrCreate()

    main(spark)
    spark.stop()
