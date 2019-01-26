package net.jgp.books.sparkInAction.ch12.lab300Join;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Join.
 * 
 * @author jgp
 */
public class RecordTransformationApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    RecordTransformationApp app = new RecordTransformationApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creation of the session
    SparkSession spark = SparkSession.builder()
        .appName("Join")
        .master("local")
        .getOrCreate();

    // Ingestion of the census data
    Dataset<Row> censusDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/census/PEP_2017_PEPANNRES.csv");
    censusDf.show(3, false);
    censusDf.printSchema();

    Dataset<Row> higherEdDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/dapip/InstitutionCampus.csv");
    higherEdDf.show(3, false);
    higherEdDf.printSchema();

  }
}
