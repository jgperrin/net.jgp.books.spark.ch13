'''
 Keeping the order of rows during transformations.

 @author rambabu.posa
'''
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType, DoubleType)

def createDataframe(spark):
    schema = StructType([
        StructField('col1', IntegerType(), False),
        StructField('col2', StringType(), False),
        StructField('sum', DoubleType(), False)
    ])

    rows = [
        (1, 'a', 3555204326.27),
        (4, 'b', 22273491.72),
        (5, 'c', 219175.0),
        (3, 'a', 219175.0),
        (2, 'c', 75341433.37)
    ]
    return spark.createDataFrame(rows, schema)


def main(spark):
    df = createDataframe(spark)
    df.show()

    df = df.withColumn('__idx', F.monotonically_increasing_id())
    df.show()

    df = df.dropDuplicates(['col2']) \
        .orderBy('__idx') \
        .drop('__idx')

    df.show()


if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName('Splitting a dataframe to collect it') \
        .master('local[*]').getOrCreate()

    main(spark)
    spark.stop()
