package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * concat function.
 * 
 * @author jgp
 */
public class ConcatWsApp {

  public static void main(String[] args) {
    ConcatWsApp app = new ConcatWsApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("abs function")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .load("data/functions/strings.csv");

    df = df.withColumn(
        "jeanify",
        concat_ws("-", lit("Jean"), col("fname")));

    df.show(5);
  }
}
