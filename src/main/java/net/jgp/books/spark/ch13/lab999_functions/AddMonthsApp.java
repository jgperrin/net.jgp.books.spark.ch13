package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.add_months;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * add_months function.
 * 
 * @author jgp
 */
public class AddMonthsApp {

  public static void main(String[] args) {
    AddMonthsApp app = new AddMonthsApp();
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
        .option("imferSchema", true)
        .load("data/functions/dates.csv");

    df = df
        .withColumn("add_months+2", add_months(col("date_time"), 2))
        .withColumn("add_months+val",
            add_months(col("date_time"), col("val")));

    df.show(5, false);
    df.printSchema();
  }
}
