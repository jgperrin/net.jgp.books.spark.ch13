package net.jgp.books.spark.ch13.lab999_functions

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * acos function: inverse cosine of a value in radians
 *
 * @author rambabu.posa
 */
object AcosScalaApp {

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
      .appName("acos function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .load("data/functions/trigo_arc.csv")

    val df2 = df.withColumn("acos", F.acos(F.col("val")))
                .withColumn("acos_by_name", F.acos("val"))

    df2.show()

    spark.stop
  }

}
