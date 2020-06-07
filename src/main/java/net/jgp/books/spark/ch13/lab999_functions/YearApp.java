package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * year function.
 * 
 * @author jgp
 */
public class YearApp {

  public static void main(String[] args) {
    YearApp app = new YearApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("year function")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("imferSchema", true)
        .load("data/functions/dates.csv");

    df = df.withColumn("year", year(col("date_time")));

    df.show(5, false);
    df.printSchema();
  }
}
