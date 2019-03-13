package net.jgp.books.spark.ch13.lab100_orders;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orders analytics.
 * 
 * @author jgp
 */
public class OrderStatisticsApp {
  private static Logger log =
      LoggerFactory.getLogger(OrderStatisticsApp.class);

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    OrderStatisticsApp app =
        new OrderStatisticsApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Orders analytics")
        .master("local[*]")
        .getOrCreate();

    // Reads a CSV file with header, called orders.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load("data/orders/orders.csv");

    // Calculating the average enrollment for each school
    df = df
        .groupBy(col("firstName"), col("lastName"), col("state"))
        .agg(sum("quantity"), sum("revenue"), avg("revenue"));
    df.show(20);
  }
}
