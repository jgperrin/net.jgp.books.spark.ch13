package net.jgp.books.spark.ch13.lab999_functions

import org.apache.spark.sql.{Dataset, Row, SparkSession, functions => F}

/**
 * abs function.
 *
 * @author rambabu.posa
 */
object AbsScalaApp {

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
      .appName("abs function")
      .master("local[*]")
      .getOrCreate

    var df: Dataset[Row] = spark.read
      .format("csv")
      .option("header", true)
      .load("data/functions/functions.csv")

    df = df.withColumn("abs", F.abs(F.col("val")))

    df.show(5)

    spark.stop
  }

}
