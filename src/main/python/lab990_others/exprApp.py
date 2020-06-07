"""
 Use of expr().

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

# Creates a session on a local master
spark = SparkSession.builder.appName('expr()') \
    .master('local[*]').getOrCreate()

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

spark.stop()