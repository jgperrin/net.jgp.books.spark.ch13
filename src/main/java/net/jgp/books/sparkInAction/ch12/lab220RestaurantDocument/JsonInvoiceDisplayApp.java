package net.jgp.books.sparkInAction.ch12.lab220RestaurantDocument;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.explode;

/**
 * Processing of invoices formatted using the schema.org format.
 * 
 * @author jgp
 */
public class JsonInvoiceDisplayApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    JsonInvoiceDisplayApp app =
        new JsonInvoiceDisplayApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Processing of invoices")
        .master("local")
        .getOrCreate();

    // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
    Dataset<Row> invoicesDf = spark.read()
        .format("json")
        .option("multiline", true)
        .load("data/invoice/good-invoice*.json");

    // Shows at most 3 rows from the dataframe
    invoicesDf.show(3);
    invoicesDf.printSchema();

    Dataset<Row> invoiceAmountDf = invoicesDf.select("totalPaymentDue.*");
    invoiceAmountDf.show(5);
    invoiceAmountDf.printSchema();

    Dataset<Row> elementsOrderedByAccountDf = invoicesDf.select(
        invoicesDf.col("accountId"),
        explode(invoicesDf.col("referencesOrder")).as("order"));
    elementsOrderedByAccountDf = elementsOrderedByAccountDf
        .withColumn(
            "type",
            elementsOrderedByAccountDf.col("order.orderedItem.@type"))
        .withColumn(
            "description",
            elementsOrderedByAccountDf.col("order.orderedItem.description"))
        .withColumn(
            "name",
            elementsOrderedByAccountDf.col("order.orderedItem.name"))
        .drop(elementsOrderedByAccountDf.col("order"));
    elementsOrderedByAccountDf.show(10);
    elementsOrderedByAccountDf.printSchema();
  }
}
