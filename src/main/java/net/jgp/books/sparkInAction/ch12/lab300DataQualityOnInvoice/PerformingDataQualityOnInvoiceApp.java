package net.jgp.books.sparkInAction.ch12.lab300DataQualityOnInvoice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * @author jgp
 */
public class PerformingDataQualityOnInvoiceApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    PerformingDataQualityOnInvoiceApp app =
        new PerformingDataQualityOnInvoiceApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Data quality")
        .master("local")
        .getOrCreate();

    // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
    Dataset<Row> invoicesDf = spark.read()
        .format("json")
        .option("multiline", true)
        .load("data/invoice/*invoice*.json");

    // Shows at most 10 rows from the dataframe
    invoicesDf.show(10);
    invoicesDf.printSchema();

    Dataset<Row> invoiceAmountDf = invoicesDf.select("totalPaymentDue.*");
    invoiceAmountDf.show(5);
    invoiceAmountDf.printSchema();

    Dataset<Row> elementsOrderedByAccountDf = invoicesDf.select(
        invoicesDf.col("accountId"),
        functions.explode(invoicesDf.col("referencesOrder")).as("order"));
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
