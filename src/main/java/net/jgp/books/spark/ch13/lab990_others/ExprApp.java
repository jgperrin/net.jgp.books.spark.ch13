package net.jgp.books.spark.ch13.lab990_others;

import static org.apache.spark.sql.functions.expr;

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
 * Use of expr().
 * 
 * @author jgp
 */
public class ExprApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    ExprApp app = new ExprApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("expr()")
        .master("local")
        .getOrCreate();

    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "title",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "start",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "end",
            DataTypes.IntegerType,
            false) });

    List<Row> rows = new ArrayList<Row>();
    rows.add(RowFactory.create("bla", 10, 30));
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    df.show();

    df = df
        .withColumn("time_spent", expr("end - start"))
        .drop("start")
        .drop("end");
    df.show();

  }
}
