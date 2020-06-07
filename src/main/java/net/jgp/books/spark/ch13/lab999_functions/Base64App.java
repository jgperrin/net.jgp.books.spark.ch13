package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * base64 function.
 * 
 * @author jgp
 */
public class Base64App {

  public static void main(String[] args) {
    Base64App app = new Base64App();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("base64 function")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .load("data/functions/strings.csv");

    df = df.withColumn("base64", base64(col("fname")));

    df.show(5);
  }
}
