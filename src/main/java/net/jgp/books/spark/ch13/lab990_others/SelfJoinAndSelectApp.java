package net.jgp.books.spark.ch13.lab990_others;

import static org.apache.spark.sql.functions.col;

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
public class SelfJoinAndSelectApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    SelfJoinAndSelectApp app = new SelfJoinAndSelectApp();
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

    Dataset<Row> inputDf = createDataframe(spark);
    inputDf.show(false);

    Dataset<Row> left = inputDf.withColumnRenamed("dst", "dst2");
    left.show();

    Dataset<Row> right = inputDf.withColumnRenamed("src", "dst2");
    right.show();

    Dataset<Row> r = left.join(
        right,
        left.col("dst2").equalTo(right.col("dst2")));
    r.show();

    Dataset<Row> resultOption1Df = r.select(left.col("src"), r.col("dst"));
    resultOption1Df.show();

    Dataset<Row> resultOption2Df = r.select(col("src"), col("dst"));
    resultOption2Df.show();

    Dataset<Row> resultOption3Df = r.select("src", "dst");
    resultOption3Df.show();
  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "src",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "predicate",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "dst",
            DataTypes.StringType,
            false) });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("a", "r1", ":b1"));
    rows.add(RowFactory.create("a", "r2", "k"));
    rows.add(RowFactory.create("b1", "r3", ":b4"));
    rows.add(RowFactory.create("b1", "r10", "d"));
    rows.add(RowFactory.create(":b4", "r4", "f"));
    rows.add(RowFactory.create(":b4", "r5", ":b5"));
    rows.add(RowFactory.create(":b5", "r9", "t"));
    rows.add(RowFactory.create(":b5", "r10", "e"));

    return spark.createDataFrame(rows, schema);
  }
}
