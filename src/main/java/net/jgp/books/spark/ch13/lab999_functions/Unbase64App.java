package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unbase64;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * unbase64 function.
 * 
 * @author jgp
 */
public class Unbase64App {

  public static void main(String[] args) {
    Unbase64App app = new Unbase64App();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("unbase64 function")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .load("data/functions/strings.csv");

    System.out.println("Output as array of bytes:");
    df = df
        .withColumn("base64", base64(col("fname")))
        .withColumn("unbase64", unbase64(col("base64")));
    df.show(5);

    System.out.println("Output as strings:");
    df = df
        .withColumn("name", col("unbase64").cast(DataTypes.StringType));
    df.show(5);

  }
}
