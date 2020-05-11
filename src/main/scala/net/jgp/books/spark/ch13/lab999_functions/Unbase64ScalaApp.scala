package net.jgp.books.spark.ch13.lab999_functions

import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * unbase64 function
 *
 * @author rambabu.posa
 */
object Unbase64ScalaApp {

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
      .appName("unbase64 function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .load("data/functions/strings.csv")

    println("Output as array of bytes:")
    val df2 = df
      .withColumn("base64", F.base64(F.col("fname")))
      .withColumn("unbase64", F.unbase64(F.col("base64")))

    df2.show(5)

    println("Output as strings:")
    val df3 = df2.withColumn("name", F.col("unbase64").cast(StringType))

    df3.show(5)

    spark.stop
  }

}
