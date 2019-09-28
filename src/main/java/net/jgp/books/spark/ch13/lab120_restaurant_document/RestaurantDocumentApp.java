package net.jgp.books.spark.ch13.lab120_restaurant_document;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.struct;

import java.util.Arrays;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Building a nested document.
 * 
 * @author jgp
 */
public class RestaurantDocumentApp {
  private static Logger log =
      LoggerFactory.getLogger(RestaurantDocumentApp.class);

  public static final String TEMP_COL = "temp_column";

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    RestaurantDocumentApp app = new RestaurantDocumentApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Building a restaurant fact sheet")
        .master("local")
        .getOrCreate();

    // Ingests businesses into dataframe
    Dataset<Row> businessDf = spark.read()
        .format("csv")
        .option("header", true)
        .load("data/orangecounty_restaurants/businesses.CSV");

    // Ingests businesses into dataframe
    Dataset<Row> inspectionDf = spark.read()
        .format("csv")
        .option("header", true)
        .load("data/orangecounty_restaurants/inspections.CSV");

    // Shows at most 3 rows from the dataframe
    businessDf.show(3);
    businessDf.printSchema();

    inspectionDf.show(3);
    inspectionDf.printSchema();

    Dataset<Row> factSheetDf = nestedJoin(
        businessDf,
        inspectionDf,
        "business_id",
        "business_id",
        "inner",
        "inspections");
    factSheetDf.show(3);
    factSheetDf.printSchema();
  }

  /**
   * Builds a nested document from two dataframes.
   * 
   * @param leftDf
   *          The left or master document.
   * @param rightDf
   *          The right or details.
   * @param leftJoinCol
   *          Column to link on in the left dataframe.
   * @param rightJoinCol
   *          Column to link on in the right dataframe.
   * @param joinType
   *          Type of joins, any type supported by Spark.
   * @param nestedCol
   *          Name of the nested column.
   * @return
   */
  public static Dataset<Row> nestedJoin(
      Dataset<Row> leftDf,
      Dataset<Row> rightDf,
      String leftJoinCol,
      String rightJoinCol,
      String joinType,
      String nestedCol) {

    // Performs the join
    Dataset<Row> resDf = leftDf.join(
        rightDf,
        rightDf.col(rightJoinCol).equalTo(leftDf.col(leftJoinCol)),
        joinType);

    // Makes a list of the left columns (the columns in the master)
    Column[] leftColumns = getColumns(leftDf);
    if (log.isDebugEnabled()) {
      log.debug(
          "  We have {} columns to work with: {}",
          leftColumns.length,
          Arrays.toString(leftColumns));
      log.debug("Schema and data:");
      resDf.printSchema();
      resDf.show(3);
    }

    // Copies all the columns from the left/master
    Column[] allColumns =
        Arrays.copyOf(leftColumns, leftColumns.length + 1);

    // Adds a column, which is a structure containing all the columns from
    // the details
    allColumns[leftColumns.length] =
        struct(getColumns(rightDf)).alias(TEMP_COL);

    // Performs a select on all columns
    resDf = resDf.select(allColumns);
    if (log.isDebugEnabled()) {
      resDf.printSchema();
      resDf.show(3);
    }

    //
    resDf = resDf
        .groupBy(leftColumns)
        .agg(
            collect_list(col(TEMP_COL)).as(nestedCol));

    if (log.isDebugEnabled()) {
      resDf.printSchema();
      resDf.show(3);
      log.debug("  After nested join, we have {} rows.", resDf.count());
    }

    return resDf;
  }

  private static Column[] getColumns(Dataset<Row> df) {
    String[] fieldnames = df.columns();
    Column[] columns = new Column[fieldnames.length];
    int i = 0;
    for (String fieldname : fieldnames) {
      columns[i++] = df.col(fieldname);
    }
    return columns;
  }
}
