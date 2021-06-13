package net.jgp.books.spark.ch13.lab999_functions

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * add_months function
 *
 * @author rambabu.posa
 */
object AddMonthsScalaApp {

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
      .appName("add_months function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("imferSchema", true)
      .load("data/functions/dates.csv")

    val df2 = df
        .withColumn("add_months+2",
          F.add_months(F.col("date_time"), 2))
        .withColumn("add_months+val",
          F.add_months(F.col("date_time"), F.col("val")))

    df2.show(5, false)
    df2.printSchema()

    spark.stop
  }

}
