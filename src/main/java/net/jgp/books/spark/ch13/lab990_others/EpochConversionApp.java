package net.jgp.books.spark.ch13.lab990_others;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Use of from_unixtime().
 * 
 * @author jgp
 */
public class EpochConversionApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   * @throws InterruptedException
   */
  public static void main(String[] args) {
    EpochConversionApp app = new EpochConversionApp();
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
            "event",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "ts",
            DataTypes.StringType,
            false) });

    // Building a df with a sequence of chronological timestamps
    List<Row> rows = new ArrayList<>();
    long now = System.currentTimeMillis() / 1000;
    for (int i = 0; i < 1000; i++) {
      rows.add(RowFactory.create(i, String.valueOf(now)));
      now += new Random().nextInt(3) + 1;
    }
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    df.show();

    // Turning the timestamps to dates
    df = df.withColumn("date", from_unixtime(col("ts")));
    df.show();

    // Collecting the result and printing ou
    List<Row> timeRows = df.collectAsList();
    for (Row r : timeRows) {
      System.out.printf("[%d] : %s (%s)\n",
          r.getInt(0),
          r.getString(1),
          r.getString(2));
    }
  }
}
