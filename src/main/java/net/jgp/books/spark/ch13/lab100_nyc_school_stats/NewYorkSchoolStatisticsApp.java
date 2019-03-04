package net.jgp.books.spark.ch13.lab100_nyc_school_stats;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
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

    // Evolution of # of students in the schools
    Dataset<Row> studentCountPerYearDf = df
        .groupBy(col("year"))
        .agg(sum("enrolled"))
        .withColumnRenamed("sum(enrolled)", "enrolled")
        .orderBy("year");
    studentCountPerYearDf.show(20);
    long max = studentCountPerYearDf
        .orderBy(col("enrolled").desc())
        .first()
        .getLong(1);
    log.debug("Max students in school system: {}", max);

    // Evolution of # of students in the schools
    Dataset<Row> relativeStudentCountPerYearDf = studentCountPerYearDf
        .withColumn("max", lit(max))
        .withColumn("delta", expr("max - enrolled"))
        .drop("max")
        .orderBy("year");
    relativeStudentCountPerYearDf.show(20);

    // Most enrolled per school for each year
    Dataset<Row> maxEnrolledPerSchooldf = df
        .groupBy(col("schoolId"), col("year"))
        .max("enrolled")
        .orderBy("schoolId", "year");
    maxEnrolledPerSchooldf.show(20);

    // Min absent per school for each year
    Dataset<Row> minAbsenteeDf = df
        .groupBy(col("schoolId"), col("year"))
        .min("absent")
        .orderBy("schoolId", "year");
    minAbsenteeDf.show(20);

    // Min absent per school for each year
    Dataset<Row> absenteeRatioDf = df
        .groupBy(col("schoolId"), col("year"))
        .agg(
            max("enrolled").alias("enrolled"),
            min("absent").as("absent"))
        .orderBy("schoolId", "year");
    absenteeRatioDf = absenteeRatioDf
        .groupBy(col("schoolId"), col("enrolled"), col("absent"))
        .agg(
            avg("enrolled").as("avg_enrolled"),
            avg("absent").as("avg_absent"))
        .drop("enrolled")
        .drop("absent")
        .withColumn("%", expr("avg_absent / avg_enrolled * 100"))
        .orderBy("%");
    absenteeRatioDf.show(5);

    absenteeRatioDf
        .orderBy(col("%").desc())
        .show(5);
  }
}
