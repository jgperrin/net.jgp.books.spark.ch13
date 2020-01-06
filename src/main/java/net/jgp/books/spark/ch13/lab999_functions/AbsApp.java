package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * abs function.
 * 
 * @author jgp
 */
public class AbsApp {

  public static void main(String[] args) {
    AbsApp app = new AbsApp();
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
        .load("data/functions/functions.csv");

    df = df.withColumn("abs", abs(col("val")));

    df.show(5);
  }
}
