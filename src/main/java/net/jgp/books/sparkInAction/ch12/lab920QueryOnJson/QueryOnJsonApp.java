package net.jgp.books.sparkInAction.ch12.lab920QueryOnJson;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Using JSONpath-like in SQL queries.
 * 
 * @author jgp
 */
public class QueryOnJsonApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    QueryOnJsonApp app = new QueryOnJsonApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Query on a JSON doc")
        .master("local")
        .getOrCreate();

    // Reads a JSON, stores it in a dataframe
    Dataset<Row> df = spark.read()
        .format("json")
        .option("multiline", true)
        .load("data/json/store.json");

    // Explode the array
    df = df
        .withColumn("items", functions.explode(df.col("store.book")));

    // Creates a view so I can use SQL
    df.createOrReplaceTempView("books");
    Dataset<Row> authorsOfReferenceBookDf =
        spark.sql(
            "SELECT items.author FROM books WHERE items.category = 'reference'");
    authorsOfReferenceBookDf.show(false);
  }
}
