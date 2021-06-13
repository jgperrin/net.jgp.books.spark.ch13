package net.jgp.books.spark.ch13.lab999_functions

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * asin function: inverse sine of a value in radians
 *
 * @author rambabu.posa
 */
object AsinScalaApp {

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
      .appName("asin function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .load("data/functions/trigo_arc.csv")

    val df2 = df.withColumn("asin", F.asin(F.col("val")))
                .withColumn("asin_by_name", F.asin("val"))

    df2.show()

    spark.stop
  }

}
