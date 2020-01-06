package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.acos;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * acos function: inverse cosine of a value in radians
 * 
 * @author jgp
 */
public class AcosApp {

  public static void main(String[] args) {
    AcosApp app = new AcosApp();
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

    df = df.withColumn("acos", acos(col("val")));
    df = df.withColumn("acos_by_name", acos("val"));

    df.show();
  }
}
