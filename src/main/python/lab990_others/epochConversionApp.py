"""
 Use of from_unixtime().

 @author rambabu.posa
"""
import time
import random
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

# Creates a session on a local master
spark = SparkSession.builder.appName("from_unixtime function") \
    .master("local[*]").getOrCreate()

schema = StructType([
    StructField('event', IntegerType(), False),
    StructField('ts', StringType(), False)
])

now = int(time.time())

data = []

# Building a df with a sequence of chronological timestamps
for i in range(0,1000):
    data = data + [(i, now)]
    now = now + (random.randint(1,3) + 1)

df = spark.createDataFrame(data, schema)
df.show()

# Turning the timestamps to dates
df = df.withColumn("date", F.from_unixtime(F.col("ts")))
df.show()

# Collecting the result and printing out
timeRows = [row for row in df.collect()]

for row in timeRows:
    print("{} : {} ({})".format(row[0], row[1], row[2]))

spark.stop()