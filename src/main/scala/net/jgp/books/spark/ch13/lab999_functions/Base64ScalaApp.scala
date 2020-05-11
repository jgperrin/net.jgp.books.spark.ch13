package net.jgp.books.spark.ch13.lab999_functions

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * base64 function
 *
 * @author rambabu.posa
 */
object Base64ScalaApp {

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
      .appName("base64 function")
      .master("local[*]")
      .getOrCreate

    var df = spark.read
      .format("csv")
      .option("header", true)
      .load("data/functions/strings.csv")

    df = df.withColumn("base64", F.base64(F.col("fname")))

    df.show(5)

    spark.stop
  }

}
