package net.jgp.books.spark.ch13.lab999_functions

import java.util.ArrayList

import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, RowFactory, SparkSession, functions => F}

/**
 * zip_with function.
 *
 * @author rambabu.posa
 */
object ZipWithScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    val spark: SparkSession = SparkSession.builder
      .appName("zip_with function")
      .master("local[*]")
      .getOrCreate

    val df = createDataframe(spark)

    println("Input")
    df.show(5, false)

    val df2 = df.withColumn("zip_with",
        F.zip_with(F.col("c1"), F.col("c2"),
          (x,y)=>F.when(x.isNull.or(y.isNull),  F.lit(-1)).otherwise(x+y)))

    // OR
    //al zipWithFunction = (c1: Column, c2: Column) => { F.when(c1.isNull.or(c2.isNull), F.lit(-1)).otherwise(c1 + c2) }
    //val df2 = df.withColumn("zip_with",
    //  F.zip_with(F.col("c1"), F.col("c2"), zipWithFunction)


    println("After zip_with")
    df2.show(5, false)

    spark.stop
  }

  /**
   * Creates a dataframe containing arrays of integers.
   */
  private def createDataframe(spark: SparkSession): DataFrame = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("c1", ArrayType(DataTypes.IntegerType), false),
      DataTypes.createStructField("c2", ArrayType(DataTypes.IntegerType), false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create(Array[Int](1010, 1012), Array[Int](1021, 1023, 1025)))
    rows.add(RowFactory.create(Array[Int](2010, 2012, 2014), Array[Int](2021, 2023)))
    rows.add(RowFactory.create(Array[Int](3010, 3012), Array[Int](3021, 3023)))

    spark.createDataFrame(rows, schema)
  }

}
