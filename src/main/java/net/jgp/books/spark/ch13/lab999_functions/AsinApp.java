package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.asin;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * asin function: inverse sine of a value in radians
 * 
 * @author jgp
 */
public class AsinApp {

  public static void main(String[] args) {
    AsinApp app = new AsinApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("acos function")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .load("data/functions/trigo_arc.csv");

    df = df.withColumn("asin", asin(col("val")));
    df = df.withColumn("asin_by_name", asin("val"));

    df.show();
  }
}
