package net.jgp.books.sparkInAction.ch12.lab220RestaurantDocument;

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
 * Processing of invoices formatted using the schema.org format.
 * 
 * @author jgp
 */
public class RestaurantDocumentApp {
  private static Logger log =
      LoggerFactory.getLogger(RestaurantDocumentApp.class);

  public static final String TEMP = "temp_column";

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

    Dataset<Row> factSheetDf = nestedJoin(businessDf, inspectionDf,
        "business_id", "business_id", "left", "inspections");
    factSheetDf.show(3);
    factSheetDf.printSchema();
  }

  /**
   * 
   * @param leftDf
   * @param rightDf
   * @param leftJoinColumnName
   * @param rightJoinColumnName
   * @param joinType
   * @param resultingColumnName
   * @return
   */
  static public Dataset<Row> nestedJoin(
      Dataset<Row> leftDf,
      Dataset<Row> rightDf,
      String leftJoinColumnName,
      String rightJoinColumnName,
      String joinType,
      String resultingColumnName) {

    Dataset<Row> resDf = leftDf.join(
        rightDf,
        rightDf.col(rightJoinColumnName).equalTo(leftDf.col(leftJoinColumnName)));

    String[] leftFieldnames = leftDf.columns();
    Column[] leftColumns = new Column[leftFieldnames.length];
    for (int i = 0; i < leftFieldnames.length; i++) {
      leftColumns[i] = leftDf.col(leftFieldnames[i]);
    }

    log.debug("  We have {} columns to work with: {}",
        leftColumns.length,
        Arrays.toString(leftColumns));

    Column[] allColumns = buildColumn(leftColumns, rightDf);
    resDf = resDf.select(allColumns);
    resDf = resDf.groupBy(leftColumns).agg(collect_list(col(TEMP)))
        .withColumnRenamed("collect_list(" + TEMP + ")",
            resultingColumnName);

    if (log.isDebugEnabled()) {
      resDf.printSchema();
      resDf.show();
      log.debug("  After nested join, we have {} rows.", resDf.count());
    }

    return resDf;
  }

  /**
   * Creates an array of columns with the appropriate structure to
   * reorganize the Dataframe.
   * 
   * @param claimColumns
   * @param anyClaimDetailsDf
   * @return
   */
  static private Column[] buildColumn(Column[] claimColumns, Dataset<
      Row> detailsDf) {

    // The size of the array is the same size as the number of columns in
    // the
    // claim,
    Column[] c = new Column[claimColumns.length + 1];

    // Copy all claim columns
    int i;
    for (i = 0; i < claimColumns.length; i++) {
      c[i] = claimColumns[i];
    }

    String[] detailsColumnNames = detailsDf.columns();
    int detailsColumnCount = detailsColumnNames.length;
    Column[] details = new Column[detailsColumnCount];
    for (int j = 0; j < detailsColumnCount; j++) {
      details[j] = detailsDf.col(detailsColumnNames[j]);
    }

    c[i] = struct(details).alias(TEMP);

    return c;
  }
}
