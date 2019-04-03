package net.jgp.books.spark.ch13.lab910_nyc_to_postgresql;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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
public class NewYorkSchoolsToPostgreSqlApp {
  private static Logger log = LoggerFactory
      .getLogger(NewYorkSchoolsToPostgreSqlApp.class);

  private SparkSession spark = null;

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    NewYorkSchoolsToPostgreSqlApp app =
        new NewYorkSchoolsToPostgreSqlApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    long t0 = System.currentTimeMillis();

    // Creates a session on a local master
    spark = SparkSession.builder()
        .appName("NYC schools to PostgreSQL")
        .master("local[*]")
        .getOrCreate();

    long t1 = System.currentTimeMillis();

    Dataset<Row> df =
        loadDataUsing2018Format("data/nyc_school_attendance/2018*.csv");

    df = df.unionByName(
        loadDataUsing2015Format("data/nyc_school_attendance/2015*.csv"));

    df = df.unionByName(
        loadDataUsing2006Format(
            "data/nyc_school_attendance/200*.csv",
            "data/nyc_school_attendance/2012*.csv"));

    long t2 = System.currentTimeMillis();

    String dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs";

    // Properties to connect to the database, the JDBC driver is part of our
    // pom.xml
    Properties prop = new Properties();
    prop.setProperty("driver", "org.postgresql.Driver");
    prop.setProperty("user", "jgp");
    prop.setProperty("password", "Spark<3Java");

    // Write in a table called ch02
    df.write()
        .mode(SaveMode.Overwrite)
        .jdbc(dbConnectionUrl, "ch13_nyc_schools", prop);
    long t3 = System.currentTimeMillis();

    log.info("Dataset contains {} rows, processed in {} ms.", df.count(),
        (t3 - t0));
    log.info("Spark init ... {} ms.", (t1 - t0));
    log.info("Ingestion .... {} ms.", (t2 - t1));
    log.info("Output ....... {} ms.", (t3 - t2));
    df.sample(.5).show(5);
    df.printSchema();

  }

  /**
   * Loads a data file matching the 2018 format, then prepares it.
   * 
   * @param fileNames
   * @return
   */
  private Dataset<Row> loadDataUsing2018Format(String... fileNames) {
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
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("dateFormat", "yyyyMMdd")
        .schema(schema)
        .load(fileNames);
    return df
        .withColumn("schoolYear", lit(2018));
  }

  /**
   * Load a data file matching the 2006 format.
   * 
   * @param fileNames
   * @return
   */
  private Dataset<Row> loadDataUsing2006Format(String... fileNames) {
    return loadData(fileNames, "yyyyMMdd");
  }

  /**
   * Load a data file matching the 2015 format.
   * 
   * @param fileNames
   * @return
   */
  private Dataset<Row> loadDataUsing2015Format(String... fileNames) {
    return loadData(fileNames, "MM/dd/yyyy");
  }

  /**
   * Common loader for most datasets, accepts a date format as part of the
   * parameters.
   * 
   * @param fileNames
   * @param dateFormat
   * @return
   */
  private Dataset<Row> loadData(String[] fileNames, String dateFormat) {
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

    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("dateFormat", dateFormat)
        .schema(schema)
        .load(fileNames);

    return df.withColumn("schoolYear", substring(col("schoolYear"), 1, 4));
  }

}
