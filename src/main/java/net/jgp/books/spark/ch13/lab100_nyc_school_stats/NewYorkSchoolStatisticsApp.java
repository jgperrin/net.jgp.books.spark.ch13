package net.jgp.books.spark.ch13.lab100_nyc_school_stats;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NYC schools analytics.
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
        .appName("NYC schools analytics")
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

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .option("dateFormat", "yyyyMMdd")
        .schema(schema)
        .load("data/nyc_school_attendance/20*.csv");
    log.debug("Datasets contains {} rows", df.count());

    // FIltering, data preparation
    df = df.filter(length(col("schoolYear")).$greater$eq(8));
    df = df
        .withColumn(
            "year",
            col("schoolYear").substr(0, 4).cast(DataTypes.IntegerType))
        .drop("schoolYear");

    // Shows at most 5 rows from the dataframe
    df.show(5);
    df.printSchema();
    log.debug("Datasets contains {} rows", df.count());

    // Unique schools
    Dataset<Row> uniqueSchoolsDf = df.select("schoolId").distinct();
    uniqueSchoolsDf.show(5);
    uniqueSchoolsDf.printSchema();
    log.debug("Datasets contains {} rows", uniqueSchoolsDf.count());

    // Calculating the average enrollment for each school
    Dataset<Row> averageEnrollmentDf = df
        .groupBy(col("schoolId"), col("year"))
        .avg("enrolled", "present", "absent")
        .orderBy("schoolId", "year");
    averageEnrollmentDf.show(20);
  }
}
