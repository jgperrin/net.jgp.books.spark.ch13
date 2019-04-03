package net.jgp.books.spark.ch13.lab100_json_shipment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Processing of invoices formatted using the schema.org format.
 * 
 * @author jgp
 */
public class JsonShipmentDisplayApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    JsonShipmentDisplayApp app = new JsonShipmentDisplayApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Display of shipment")
        .master("local")
        .getOrCreate();

    // Reads a JSON, stores it in a dataframe
    Dataset<Row> df = spark.read()
        .format("json")
        .option("multiline", true)
        .load("data/json/shipment.json");

    // Shows at most 5 rows from the dataframe (there's only one anyway)
    df.show(5, 16);
    df.printSchema();

  }
}
