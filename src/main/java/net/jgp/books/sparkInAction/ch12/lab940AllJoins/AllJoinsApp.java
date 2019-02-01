package net.jgp.books.sparkInAction.ch12.lab940AllJoins;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.explode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * All joins in a single app, inspired by
 * https://stackoverflow.com/questions/45990633/what-are-the-various-join-types-in-spark.
 * 
 * @author jgp
 */
public class AllJoinsApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    AllJoinsApp app = new AllJoinsApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Processing of invoices")
        .master("local")
        .getOrCreate();

    // val r1 = Seq(Row(1, "A1"), Row(2, "A2"), Row(3, "A3"), Row(4,
    // "A4")).toDS()
    // val r2 = Seq(Row(3, "A3"), Row(4, "A4"), Row(4, "A4_1"), Row(5,
    // "A5"), Row(6, "A6")).toDS()

    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "id",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "value",
            DataTypes.StringType,
            false) });

    List<Row> rows = new ArrayList<Row>();
    rows.add(RowFactory.create(1, "A1"));
    rows.add(RowFactory.create(2, "A2"));
    rows.add(RowFactory.create(3, "A3"));
    rows.add(RowFactory.create(4, "A4"));
    Dataset<Row> dfLeft = spark.createDataFrame(rows, schema);
    dfLeft.show();

    rows = new ArrayList<Row>();
    rows.add(RowFactory.create(3, "A3"));
    rows.add(RowFactory.create(4, "A4"));
    rows.add(RowFactory.create(4, "A4_1"));
    rows.add(RowFactory.create(5, "A5"));
    rows.add(RowFactory.create(6, "A6"));
    Dataset<Row> dfRight = spark.createDataFrame(rows, schema);
    dfRight.show();

    String[] joinTypes = new String[] { 
        "inner", // v2.0.0. default
        "cross", // v2.2.0
        "outer", // v2.0.0
        "full", // v2.1.1
        "full_outer", // v2.1.1
        "left", // v2.1.1
        "left_outer", // v2.0.0
        "right", // v2.1.1
        "right_outer", // v2.0.0
        "left_semi", // v2.0.0, was leftsemi before v2.1.1
        "left_anti" // v2.1.1
        };

    // joinTypes foreach {joinType =>
    // println(s"${joinType.toUpperCase()} JOIN")
    // r1.join(right = r2, usingColumns = Seq("id"), joinType =
    // joinType).orderBy("id").show()
    // }
    for (String joinType : joinTypes) {
      System.out.println(joinType.toUpperCase() + " JOIN");
      Dataset<Row> df = dfLeft.join(
          dfRight, 
          dfLeft.col("id").equalTo(dfRight.col("id")), 
          joinType);
      df.orderBy(dfLeft.col("id")).show();
    }
  }
}
