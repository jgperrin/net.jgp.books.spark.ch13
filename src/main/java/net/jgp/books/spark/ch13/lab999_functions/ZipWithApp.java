package net.jgp.books.spark.ch13.lab999_functions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.zip_with;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Function2;

/**
 * zip_with function.
 * 
 * @author jgp
 */
public class ZipWithApp {
  private SparkSession spark = null;

  /**
   * Takes two values and adds them. If one of the value is null, as the
   * addition cannot be performed, the result is -1.
   * 
   * The processing is taking place in the apply() method of the function.
   * It can be implemented as a Java lambda function.
   * 
   * @author jgp
   */
  public class ZipWithFunction
      implements Function2<Column, Column, Column> {

    @Override
    public Column apply(Column v1, Column v2) {
      return when(v1.isNull().or(v2.isNull()), lit(-1))
          .otherwise(v1.$plus(v2));
    }

  }

  public static void main(String[] args) {
    ZipWithApp app = new ZipWithApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    spark = SparkSession.builder()
        .appName("abs function")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = createDataframe();
    System.out.println("Input");
    df.show(5, false);

    df = df.withColumn("zip_with",
        zip_with(col("c1"), col("c2"), new ZipWithFunction()));

    System.out.println("After zip_with");
    df.show(5, false);
  }

  /**
   * Creates a dataframe containing arrays of integers.
   */
  private Dataset<Row> createDataframe() {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "c1",
            DataTypes.createArrayType(DataTypes.IntegerType),
            false),
        DataTypes.createStructField(
            "c2",
            DataTypes.createArrayType(DataTypes.IntegerType),
            false) });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(
        new int[] { 1010, 1012 },
        new int[] { 1021, 1023, 1025 }));
    rows.add(RowFactory.create(
        new int[] { 2010, 2012, 2014 },
        new int[] { 2021, 2023 }));
    rows.add(RowFactory.create(
        new int[] { 3010, 3012 },
        new int[] { 3021, 3023 }));

    return spark.createDataFrame(rows, schema);
  }
}
