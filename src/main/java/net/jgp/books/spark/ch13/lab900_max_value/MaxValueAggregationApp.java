package net.jgp.books.spark.ch13.lab900_max_value;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MaxValueAggregationApp {
  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    MaxValueAggregationApp app = new MaxValueAggregationApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Aggregates max values")
        .master("local[*]")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    Dataset<Row> rawDf = spark.read().format("csv")
        .option("header", true)
        .option("sep", "|")
        .load("data/misc/courses.csv");

    // Shows at most 20 rows from the dataframe
    rawDf.show(20);

    // Performs the aggregation, grouping on columns id, batch_id, and
    // session_name
    Dataset<Row> maxValuesDf = rawDf.select("*")
        .groupBy(col("id"), col("batch_id"), col("session_name"))
        .agg(max("time"));
    maxValuesDf.show(5);
  }
}
