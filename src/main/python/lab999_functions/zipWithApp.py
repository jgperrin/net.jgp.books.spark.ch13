"""
 zip_with function.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, Row)
from pyspark.sql.types import (StructType, StructField, IntegerType, ArrayType)
from pyspark.sql import (functions as F)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/functions/functions.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("zip_with function") \
    .master("local[*]").getOrCreate()


def create_dataFrame(spark):
    schema = StructType([StructField("c1", ArrayType(IntegerType()), False),
                         StructField("c2", ArrayType(IntegerType()), False)])
    df = spark.createDataFrame([Row(c1=[1010, 1012], c2=[1021, 1023, 1025]),
                                Row(c1=[2010, 2012, 2014], c2=[2021, 2023]),
                                Row(c1=[3010, 3012], c2=[3021, 3023])
                                ], schema)
    return df


df = df = create_dataFrame(spark)

print("Input")
df.show(5, False)

df.printSchema()

def myfun(c1,c2):
    return c1+c2

#df = df.withColumn("zip_with", F.arrays_zip(F.col("c1"), F.col("c2")))
#df = df.withColumn("zipped_add", F.expr("transform(zip_with, x -> (x.c1 + x.c2))"))
df = df.withColumn("zip_with", F.expr("zip_with(c1, c2, (x,y) -> (x+y))"))
#df = df.withColumn("zipped_final2", F.expr("zip_with(c1, c2, (x,y) -> (x+y)"))
#df = df.select("c1,c2,zip_with(c1,c2, (x,y) -> sum(x,y))")
#df = df.withColumn("zip_with", F.expr("transform(zip_with, x -> when(x.c1.isNull() | x.c2.isNull(), lit(-1)).otherwise(x.c1 + x.c2))"))


print("After zip_with")
df.show(5, False)
df.printSchema()

spark.stop()
