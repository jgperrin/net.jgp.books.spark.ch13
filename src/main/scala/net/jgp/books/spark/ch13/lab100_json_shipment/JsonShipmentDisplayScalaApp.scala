package net.jgp.books.spark.ch13.lab100_json_shipment

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Processing of invoices formatted using the schema.org format.
 *
 * @author rambabu.posa
 */
object JsonShipmentDisplayScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Display of shipment")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, stores it in a dataframe
    // Dataset[Row] == DataFrame
    val df: DataFrame = spark.read
      .format("json")
      .option("multiline", true)
      .load("data/json/shipment.json")

    // Shows at most 5 rows from the dataframe (there's only one anyway)
    df.show(5, 16)
    df.printSchema()

    spark.stop
  }

}
