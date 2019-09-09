package net.jgp.books.spark.ch13.lab990_others;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;

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
 * Self join.
 * 
 * @author jgp
 */
public class SelfJoinApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    SelfJoinApp app = new SelfJoinApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Self join")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = createDataframe(spark);
    df.show(false);

    Dataset<Row> rightDf = df
        .withColumnRenamed("acct", "acct2")
        .withColumnRenamed("bssn", "bssn2")
        .withColumnRenamed("name", "name2")
        .drop("tid");

    Dataset<Row> joinedDf = df
        .join(
            rightDf,
            df.col("acct").equalTo(rightDf.col("acct2")),
            "leftsemi")
        .drop(rightDf.col("acct2"))
        .drop(rightDf.col("name2"))
        .drop(rightDf.col("bssn2"));
    joinedDf.show(false);

    Dataset<Row> listDf = joinedDf
        .groupBy(joinedDf.col("acct"))
        .agg(collect_list("bssn"), collect_list("name"));
    listDf.show(false);

    Dataset<Row> setDf = joinedDf
        .groupBy(joinedDf.col("acct"))
        .agg(collect_set("bssn"), collect_set("name"));
    setDf.show(false);
  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "tid",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "acct",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "bssn",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "name",
            DataTypes.StringType,
            false) });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1, 123, 111, "Peter"));
    rows.add(RowFactory.create(2, 123, 222, "Paul"));
    rows.add(RowFactory.create(3, 456, 333, "John"));
    rows.add(RowFactory.create(4, 567, 444, "Casey"));

    return spark.createDataFrame(rows, schema);
  }
}
