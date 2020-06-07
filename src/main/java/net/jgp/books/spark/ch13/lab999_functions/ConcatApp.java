package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * concat function.
 * 
 * @author jgp
 */
public class ConcatApp {

  public static void main(String[] args) {
    ConcatApp app = new ConcatApp();
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

    df = df.withColumn("jeanify", concat(lit("Jean-"), col("fname")));

    df.show(5);
  }
}
