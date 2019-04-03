package net.jgp.books.spark.ch13.lab990_others;

import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Keeping the order of rows during transformations.
 * 
 * @author jgp
 */
public class KeepingOrderApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    KeepingOrderApp app = new KeepingOrderApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Splitting a dataframe to collect it")
        .master("local")
        .getOrCreate();

    Dataset<Row> df = createDataframe(spark);
    df.show();

    df = df.withColumn("__idx", monotonically_increasing_id());
    df.show();

    df = df.dropDuplicates("col2").orderBy("__idx").drop("__idx");
    df.show();
  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "col1",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "col2",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "sum",
            DataTypes.DoubleType,
            false) });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, "a", 3555204326.27));
    rows.add(RowFactory.create(4, "b", 22273491.72));
    rows.add(RowFactory.create(5, "c", 219175.0));
    rows.add(RowFactory.create(3, "a", 219175.0));
    rows.add(RowFactory.create(2, "c", 75341433.37));

    return spark.createDataFrame(rows, schema);
  }
}
