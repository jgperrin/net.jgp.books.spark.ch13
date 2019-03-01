package net.jgp.books.spark.ch13.lab100_xx;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSV ingestion in a dataframe with a Schema.
 * 
 * @author jgp
 */
public class NewYorkSchoolStatisticsApp {
  private static Logger log = LoggerFactory
      .getLogger(NewYorkSchoolStatisticsApp.class);
  

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    NewYorkSchoolStatisticsApp app =
        new NewYorkSchoolStatisticsApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Complex CSV with a schema to Dataframe")
        .master("local[*]")
        .getOrCreate();

    // Creates the schema
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "schoolId",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "date",
            DataTypes.DateType,
            false),
        DataTypes.createStructField(
            "schoolYear",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "enrolled",
            DataTypes.IntegerType,
            false), 
        DataTypes.createStructField(
            "present",
            DataTypes.IntegerType,
            false), 
        DataTypes.createStructField(
            "absent",
            DataTypes.IntegerType,
            false), 
        DataTypes.createStructField(
            "released",
            DataTypes.IntegerType,
            false) });

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .option("dateFormat", "yyyyMMdd")
        .schema(schema)
        .load("data/nyc_school_attendance/20*.csv");

    // Shows at most 5 rows from the dataframe
    df.show(5);
    df.printSchema();
    log.debug("Datasets contains {} rows", df.count());
    
    // Unique schools
    Dataset<Row> uniqueSchoolsDf = df.select("schoolId").distinct();
    uniqueSchoolsDf.show(5);
    uniqueSchoolsDf.printSchema();
    log.debug("Datasets contains {} rows", uniqueSchoolsDf.count());
  }
}
